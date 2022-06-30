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

#define USING_LOG_PREFIX SQL_CG

#include "ob_static_engine_cg.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/engine/ob_operator_factory.h"
#include "sql/engine/basic/ob_limit_op.h"
#include "sql/engine/basic/ob_material_op.h"
#include "sql/engine/basic/ob_count_op.h"
#include "sql/engine/basic/ob_values_op.h"
#include "sql/engine/sort/ob_sort_op.h"
#include "sql/engine/recursive_cte/ob_recursive_union_all_op.h"
#include "sql/engine/set/ob_merge_union_op.h"
#include "sql/engine/set/ob_merge_intersect_op.h"
#include "sql/engine/set/ob_merge_except_op.h"
#include "sql/engine/set/ob_hash_union_op.h"
#include "sql/engine/set/ob_hash_intersect_op.h"
#include "sql/engine/set/ob_hash_except_op.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/aggregate/ob_hash_distinct_op.h"
#include "sql/engine/aggregate/ob_merge_distinct_op.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/engine/basic/ob_expr_values_op.h"
#include "sql/engine/dml/ob_table_insert_op.h"
#include "sql/engine/dml/ob_table_insert_returning_op.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/basic/ob_monitoring_dump_op.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/px/exchange/ob_px_receive_op.h"
#include "sql/engine/px/exchange/ob_px_ms_receive_op.h"
#include "sql/engine/px/exchange/ob_px_dist_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_repart_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_reduce_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_fifo_coord_op.h"
#include "sql/engine/px/exchange/ob_px_ms_coord_op.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/connect_by/ob_nl_cnnt_by_with_index_op.h"
#include "sql/engine/join/ob_hash_join_op.h"
#include "sql/engine/join/ob_nested_loop_join_op.h"
#include "sql/engine/sequence/ob_sequence_op.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"
#include "sql/engine/subquery/ob_subplan_scan_op.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/aggregate/ob_scalar_aggregate_op.h"
#include "sql/engine/aggregate/ob_merge_groupby_op.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/join/ob_merge_join_op.h"
#include "sql/engine/basic/ob_topk_op.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/engine/table/ob_table_lookup_op.h"
#include "sql/engine/table/ob_multi_part_table_scan_op.h"
#include "sql/engine/dml/ob_table_delete_op.h"
#include "sql/engine/dml/ob_table_merge_op.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/engine/dml/ob_table_delete_returning_op.h"
#include "sql/engine/dml/ob_table_update_op.h"
#include "sql/engine/dml/ob_table_update_returning_op.h"
#include "sql/engine/dml/ob_table_lock_op.h"
#include "sql/engine/dml/ob_multi_part_lock_op.h"
#include "sql/engine/dml/ob_multi_part_insert_op.h"
#include "sql/engine/dml/ob_multi_part_delete_op.h"
#include "sql/engine/dml/ob_multi_part_update_op.h"
#include "sql/engine/set/ob_append_op.h"
#include "sql/engine/table/ob_table_row_store_op.h"
#include "sql/engine/dml/ob_table_insert_up_op.h"
#include "sql/engine/dml/ob_multi_table_insert_up_op.h"
#include "sql/engine/dml/ob_table_replace_op.h"
#include "sql/engine/window_function/ob_window_function_op.h"
#include "sql/engine/dml/ob_multi_table_replace_op.h"
#include "sql/engine/dml/ob_table_conflict_row_fetcher_op.h"
#include "sql/engine/table/ob_row_sample_scan_op.h"
#include "sql/engine/table/ob_block_sample_scan_op.h"
#include "sql/engine/table/ob_table_scan_with_index_back_op.h"
#include "sql/engine/dml/ob_multi_table_merge_op.h"
#include "sql/executor/ob_direct_receive_op.h"
#include "sql/executor/ob_direct_transmit_op.h"
#include "sql/engine/basic/ob_temp_table_access_op.h"
#include "sql/engine/basic/ob_temp_table_insert_op.h"
#include "sql/engine/basic/ob_temp_table_transformation_op.h"
#include "common/ob_smart_call.h"
#include "sql/engine/pdml/static/ob_px_multi_part_delete_op.h"
#include "sql/engine/pdml/static/ob_px_multi_part_insert_op.h"
#include "sql/engine/pdml/static/ob_px_multi_part_update_op.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql {

struct ObFilterTSC {
  bool operator()(const ObOpSpec& spec) const
  {
    return spec.is_table_scan() && PHY_FAKE_TABLE != spec.type_;
  }
};

int ObStaticEngineCG::generate(const ObLogPlan& log_plan, ObPhysicalPlan& phy_plan)
{
  int ret = OB_SUCCESS;
  phy_plan_ = &phy_plan;
  ObOpSpec* root_spec = NULL;
  if (OB_INVALID_ID != log_plan.get_max_op_id()) {
#ifndef NDEBUG
    phy_plan.bit_set_.reset();
#endif
    phy_plan.set_next_phy_operator_id(log_plan.get_max_op_id());
  }
  const bool in_root_job = true;
  const bool is_subplan = false;
  bool check_eval_once = true;
  if (OB_ISNULL(log_plan.get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no logical plan root", K(ret));
  } else if (OB_FAIL(postorder_generate_op(
                 *log_plan.get_plan_root(), root_spec, in_root_job, is_subplan, check_eval_once))) {
    LOG_WARN("failed to generate plan", K(ret));
  } else if (OB_ISNULL(root_spec)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("generated root spec is NULL", K(ret));
  } else {
    phy_plan.set_root_op_spec(root_spec);
    if (OB_FAIL(set_other_properties(log_plan, phy_plan))) {
      LOG_WARN("set other properties failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (phy_plan.contains_temp_table() && phy_plan.is_remote_plan()) {
      ret = STATIC_ENG_NOT_IMPLEMENT;
      LOG_WARN("static engine not implement remote plan with temp table, will retry", K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::postorder_generate_op(
    ObLogicalOperator& op, ObOpSpec*& spec, const bool in_root_job, const bool is_subplan, bool& check_eval_once)
{
  int ret = OB_SUCCESS;
  const int64_t child_num = op.get_num_of_child();
  const bool is_exchange = log_op_def::LOG_EXCHANGE == op.get_type();
  spec = NULL;
  // generate child first.
  ObSEArray<ObOpSpec*, 2> children;
  check_eval_once = true;

  for (int64_t i = 0; OB_SUCC(ret) && i < child_num; i++) {
    ObLogicalOperator* child_op = op.get_child(i);
    ObOpSpec* child_spec = NULL;
    bool child_op_check_eval_once = true;
    if (OB_ISNULL(child_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is NULL", K(ret));
    } else if (OB_FAIL(SMART_CALL(postorder_generate_op(
                   *child_op, child_spec, in_root_job && is_exchange, is_subplan, child_op_check_eval_once)))) {
      LOG_WARN("generate child op failed", K(ret), K(op.get_name()));
    } else if (OB_ISNULL(child_spec)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate operator spec is NULL", K(ret));
    } else if (OB_FAIL(children.push_back(child_spec))) {
      LOG_WARN("array push back failed", K(ret));
    } else if (!child_op_check_eval_once) {
      // if current op has any dml child op, current op won't check if expr is calculated
      check_eval_once = false;
    }
  }
  if (OB_SUCC(ret) && (op.is_dml_operator() || (log_op_def::LOG_FOR_UPD == op.get_type()) ||
                          (log_op_def::LOG_MERGE == op.get_type()))) {
    // if current op is dml child op, it won't check if expr is calculated
    check_eval_once = false;
  }
  // allocate operator spec
  ObPhyOperatorType type;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_phy_op_type(op, type, in_root_job))) {
    LOG_WARN("get phy op type failed", K(ret));
  } else if (type == PHY_INVALID || type >= PHY_END) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid phy operator type", K(ret), K(type));
  } else if (NULL == phy_plan_ || OB_FAIL(phy_plan_->alloc_op_spec(type, children.count(), spec, op.get_op_id()))) {
    ret = NULL == phy_plan_ ? OB_INVALID_ARGUMENT : ret;
    LOG_WARN("allocate operator spec failed", K(ret), KP(phy_plan_), K(ob_phy_operator_type_str(type)));
  } else if (NULL == spec) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL operator spec returned", K(ret));
  } else {
    for (int64_t i = 0; i < children.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(spec->set_child(i, children.at(i)))) {
        LOG_WARN("set child failed", K(ret));
      }
    }
  }

  ObSEArray<ObRawExpr*, 8> tmp_cur_op_exprs;
  ObSEArray<ObRawExpr*, 8> tmp_cur_op_self_produced_exprs;
  if (is_subplan) {
    OZ(tmp_cur_op_exprs.assign(cur_op_exprs_));
    OZ(tmp_cur_op_self_produced_exprs.assign(cur_op_self_produced_exprs_));
  }
  cur_op_exprs_.reset();
  cur_op_self_produced_exprs_.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObOperatorFactory::generate_spec(*this, op, *spec, in_root_job))) {
    LOG_WARN("generate operator spec failed", K(ret), KP(phy_plan_), K(ob_phy_operator_type_str(type)));
  } else if (OB_FAIL(generate_spec_basic(op, *spec, check_eval_once))) {
    LOG_WARN("generate operator spec basic failed", K(ret));
  } else if (OB_FAIL(generate_spec_final(op, *spec))) {
    LOG_WARN("generate operator spec final failed", K(ret));
  } else if (spec->is_dml_operator()) {
    ObTableModifySpec* dml_spec = static_cast<ObTableModifySpec*>(spec);
    ObMultiDMLInfo* multi_dml_info = dynamic_cast<ObMultiDMLInfo*>(spec);
    if (OB_NOT_NULL(multi_dml_info)) {
      // success to dynamic_cast, spec is multi_XXX_dml_operator
      if (!dml_spec->is_multi_dml()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("multi_xxx_dml operator is_multi_dml tag should br true", K(dml_spec->get_type()));
      }
    }
  }
  if (is_subplan) {
    cur_op_exprs_.reset();
    cur_op_self_produced_exprs_.reset();
    if (OB_FAIL(cur_op_exprs_.assign(tmp_cur_op_exprs))) {
      LOG_WARN("assign exprs failed", K(ret));
    } else if (OB_FAIL(cur_op_self_produced_exprs_.assign(tmp_cur_op_self_produced_exprs))) {
      LOG_WARN("assign exprs failed", K(ret));
    }
  }

  return ret;
}

int ObStaticEngineCG::check_expr_columnlized(const ObRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_const_expr() || expr->has_const_or_const_expr_flag() || expr->has_flag(IS_PSEUDO_COLUMN) ||
             T_PDML_PARTITION_ID == expr->get_expr_type()  // PDML partition_id is pseudo column
             || T_QUESTIONMARK == expr->get_expr_type() || T_EXEC_VAR == expr->get_expr_type() ||
             T_CTE_SEARCH_COLUMN == expr->get_expr_type() || T_CTE_CYCLE_COLUMN == expr->get_expr_type() ||
             expr->is_set_op_expr() ||
             (expr->is_sys_func_expr() && 0 == expr->get_param_count())  // sys func with no param
             || expr->is_query_ref_expr() || expr->is_udf_expr()) {
    // skip
  } else if ((expr->is_aggr_expr() || (expr->is_win_func_expr())) && !expr->has_flag(IS_COLUMNLIZED)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr_expr or win_func_expr should be columnlized", K(ret), KPC(expr));
  } else if (!expr->has_flag(IS_COLUMNLIZED)) {
    if (0 == expr->get_param_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr should be columnlized", K(ret), K(expr), KPC(expr));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_FAIL(SMART_CALL(check_expr_columnlized(expr->get_param_expr(i))))) {
          LOG_WARN("check expr columnlized failed", K(ret), KPC(expr->get_param_expr(i)));
        }
      }
    }
  }

  return ret;
}

int ObStaticEngineCG::check_exprs_columnlized(ObLogicalOperator& op)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> child_outputs;

  // clear IS_COLUMNLIZED flag
  if (OB_FAIL(clear_all_exprs_specific_flag(cur_op_exprs_, IS_COLUMNLIZED))) {
    LOG_WARN("clear all exprs specific flag failed", K(ret));
  }
  // get all child output exprs
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_num_of_child(); ++i) {
    ObLogicalOperator* child_op = op.get_child(i);
    if (OB_ISNULL(child_op)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(i));
    } else if (OB_FAIL(set_specific_flag_to_exprs(child_op->get_output_exprs(), IS_COLUMNLIZED))) {
      LOG_WARN("fail to set specific flag to exprs", K(ret));
    }
  }
  // set IS_COLUMNLIZED flag to child_outputs_exprs and self_produced_exprs
  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_specific_flag_to_exprs(cur_op_self_produced_exprs_, IS_COLUMNLIZED))) {
      LOG_WARN("fail to set specific flag to exprs", K(ret));
    }
  }
  // check if exprs columnlized
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_op_exprs_.count(); ++i) {
    if (OB_ISNULL(cur_op_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (OB_FAIL(check_expr_columnlized(cur_op_exprs_.at(i)))) {
      LOG_WARN("check expr columnlized failed", K(ret), K(i), K(cur_op_exprs_.at(i)), KPC(cur_op_exprs_.at(i)));
    }
  }

  return ret;
}

int ObStaticEngineCG::mark_expr_self_produced(ObRawExpr* expr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(cur_op_self_produced_exprs_.push_back(expr))) {
    LOG_WARN("push back expr to cur_op_product_exprs_ failed", K(ret), KPC(expr));
  }

  return ret;
}

int ObStaticEngineCG::mark_expr_self_produced(const ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(cur_op_self_produced_exprs_.push_back(exprs.at(i)))) {
      LOG_WARN("push back expr to cur_op_product_exprs_ failed", K(ret));
    }
  }

  return ret;
}

int ObStaticEngineCG::mark_expr_self_produced(const ObIArray<ObColumnRefRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(cur_op_self_produced_exprs_.push_back(exprs.at(i)))) {
      LOG_WARN("push back expr to cur_op_product_exprs_ failed", K(ret));
    }
  }

  return ret;
}

int ObStaticEngineCG::set_specific_flag_to_exprs(const ObIArray<ObRawExpr*>& exprs, ObExprInfoFlag flag)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i), K(exprs));
    } else {
      exprs.at(i)->add_flag(flag);
    }
  }

  return ret;
}

int ObStaticEngineCG::clear_all_exprs_specific_flag(const ObIArray<ObRawExpr*>& exprs, ObExprInfoFlag flag)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i), K(exprs));
    } else {
      exprs.at(i)->clear_flag(flag);
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_rt_expr(const ObRawExpr& src, ObExpr*& dst)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStaticEngineExprCG::generate_rt_expr(src, cur_op_exprs_, dst))) {
    LOG_WARN("fail to push cur op expr", K(ret), K(cur_op_exprs_));
  } else {
    // add by , just for debug, in generate_rt_expr
    // LOG_WARN("add by , just for debug, in generate_rt_expr", K(ret), K(&src), K(src), K(lbt()));
  }
  return ret;
}

int ObStaticEngineCG::generate_rt_exprs(const ObIArray<ObRawExpr*>& src, ObIArray<ObExpr*>& dst)
{
  int ret = OB_SUCCESS;
  dst.reset();
  if (!src.empty()) {
    if (OB_FAIL(dst.reserve(src.count()))) {
      LOG_WARN("init fixed array failed", K(ret), K(src.count()));
    } else {
      FOREACH_CNT_X(raw_expr, src, OB_SUCC(ret))
      {
        ObExpr* e = NULL;
        CK(OB_NOT_NULL(*raw_expr));
        OZ(generate_rt_expr(*(*raw_expr), e));
        CK(OB_NOT_NULL(e));
        OZ(dst.push_back(e));
      }
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_spec_basic(ObLogicalOperator& op, ObOpSpec& spec, const bool check_eval_once)
{
  int ret = OB_SUCCESS;
  if (0 == spec.rows_) {
    spec.rows_ = ceil(op.get_card());
  }
  spec.cost_ = op.get_cost();
  spec.width_ = op.get_width();
  spec.plan_depth_ = op.get_plan_depth();
  spec.px_est_size_factor_ = op.get_px_est_size_factor();

  OZ(generate_rt_exprs(op.get_startup_exprs(), spec.startup_filters_));

  if (log_op_def::LOG_MV_TABLE_SCAN == op.get_type() ||
      (log_op_def::LOG_TABLE_SCAN == op.get_type() && PHY_FAKE_CTE_TABLE != spec.type_)) {
    // sanity check table scan type, dynamic_cast is acceptable in CG.
    ObLogTableScan* log_tsc = dynamic_cast<ObLogTableScan*>(&op);
    ObTableScanSpec* tsc_spec = dynamic_cast<ObTableScanSpec*>(&spec);
    CK(NULL != log_tsc);
    CK(NULL != tsc_spec);
    OZ(generate_tsc_filter(*log_tsc, *tsc_spec));
  } else {
    OZ(generate_rt_exprs(op.get_filter_exprs(), spec.filters_));
  }
  OZ(generate_rt_exprs(op.get_output_exprs(), spec.output_));
  if (OB_SUCC(ret)) {
    if (OB_FAIL(check_exprs_columnlized(op))) {
      LOG_WARN("check exprs columnlized failed",
          K(ret),
          K(op.get_name()),
          K(op.get_op_id()),
          K(op.get_type()),
          K(cur_op_exprs_.count()));
    }
  }
  if (OB_SUCC(ret)) {
    // get all child output exprs
    ObSEArray<ObRawExpr*, 16> child_outputs;
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_num_of_child(); i++) {
      ObLogicalOperator* child_op = op.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(i));
      } else if (OB_FAIL(append(child_outputs, child_op->get_output_exprs()))) {
        LOG_WARN("fail to append child exprs", K(ret));
      }
    }  // for end
    // get calc exprs
    OZ(generate_calc_exprs(child_outputs, cur_op_exprs_, spec.calc_exprs_, check_eval_once),
        op.get_op_id(),
        op.get_name(),
        K(op.get_type()));
    // add by , just for debug 2, after generate_calc_exprs
    // LOG_WARN("add by , just for debug, after generate_calc_exprs", K(ret), K(op.get_op_id()), K(op.get_name()),
    // K(op.get_type()));
  }
  return ret;
}

int ObStaticEngineCG::generate_calc_exprs(const ObIArray<ObRawExpr*>& dep_exprs, const ObIArray<ObRawExpr*>& cur_exprs,
    ObIArray<ObExpr*>& calc_exprs, bool check_eval_once)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> calc_raw_exprs;
  ObRawExprUniqueSet flattened_cur_op_exprs(phy_plan_->get_allocator());
  OZ(flattened_cur_op_exprs.init());
  auto filter_func = [&](ObRawExpr* e) { return !has_exist_in_array(dep_exprs, e); };
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObRawExprUtils::flatten_raw_exprs(cur_exprs, flattened_cur_op_exprs, filter_func))) {
    LOG_WARN("fail to flatten rt exprs", K(ret));
  }
  const ObIArray<ObRawExpr*>& flattened_cur_exprs_arr = flattened_cur_op_exprs.get_expr_array();
  for (int64_t i = 0; OB_SUCC(ret) && i < flattened_cur_exprs_arr.count(); i++) {
    ObRawExpr* raw_expr = flattened_cur_exprs_arr.at(i);
    CK(OB_NOT_NULL(raw_expr));
    if (OB_SUCC(ret)) {
      if (raw_expr->get_expr_type() == T_PDML_PARTITION_ID) {
        // bypass
      } else if (!raw_expr->is_column_ref_expr() && !has_exist_in_array(dep_exprs, flattened_cur_exprs_arr.at(i)) &&
                 (raw_expr->has_flag(CNT_VOLATILE_CONST) ||
                     !(raw_expr->is_const_expr() || raw_expr->has_flag(IS_CONST) ||
                         raw_expr->has_flag(IS_CONST_EXPR)))) {
        /* TODO: this check will be reopened after following two issues solved:
        if (check_eval_once
            && T_FUN_SYS_ROWNUM != raw_expr->get_expr_type()
            && T_CTE_SEARCH_COLUMN != raw_expr->get_expr_type()
            && T_CTE_CYCLE_COLUMN != raw_expr->get_expr_type()
            && !(raw_expr->is_const_expr() || raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR))) {
          if (raw_expr->is_calculated()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is not from the child_op_output but it has been caculated already",
                     K(ret), K(raw_expr), K(raw_expr->has_flag(CNT_VOLATILE_CONST)), K(raw_expr->is_const_expr()),
        KPC(raw_expr)); } else { raw_expr->set_is_calculated(true);
            // add by , just for debug 1, raw_expr->set_is_calculated
            // LOG_WARN("add by , just for debug, raw_expr->set_is_calculated", K(ret), K(raw_expr), KPC(raw_expr));
          }
        }
        */
        UNUSED(check_eval_once);
        /*
        if (check_eval_once) {
          if (raw_expr->is_calculated()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is not from the child_op_output but it has been caculated already",
                     K(ret), K(raw_expr), KPC(raw_expr));
          } else {
            raw_expr->set_is_calculated(true);
          }
        }
        */
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(calc_raw_exprs.push_back(raw_expr))) {
          LOG_WARN("fail to push output expr", K(ret));
        }
      }
    }
  }  // for end
  if (OB_SUCC(ret) && !calc_raw_exprs.empty()) {
    if (OB_FAIL(generate_rt_exprs(calc_raw_exprs, calc_exprs))) {
      LOG_WARN("fail to append calc exprs", K(ret), K(calc_raw_exprs));
    }
  }

  return ret;
}

// CAUTION: generate_rt_expr()/generate_rt_exprs() can't be invoked in this function,
// because calc_exprs_ is already generated.
int ObStaticEngineCG::generate_spec_final(ObLogicalOperator& op, ObOpSpec& spec)
{
  int ret = OB_SUCCESS;

  UNUSED(op);
  if (PHY_SUBPLAN_FILTER == spec.type_) {
    FOREACH_CNT_X(e, spec.calc_exprs_, OB_SUCC(ret))
    {
      if (T_REF_QUERY == (*e)->type_) {
        ObExprSubQueryRef::ExtraInfo::get_info(**e).op_id_ = spec.id_;
      }
    }
  }

  if (PHY_NESTED_LOOP_CONNECT_BY == spec.type_ || PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX == spec.type_) {
    FOREACH_CNT_X(e, spec.calc_exprs_, OB_SUCC(ret))
    {
      if (T_OP_PRIOR == (*e)->type_) {
        (*e)->extra_ = spec.id_;
      }
    }
  }

  // mark insert on dup update scope for T_FUN_SYS_VALUES
  if (PHY_INSERT_ON_DUP == spec.type_ || PHY_MULTI_TABLE_INSERT_UP == spec.type_) {
    FOREACH_CNT_X(e, spec.calc_exprs_, OB_SUCC(ret))
    {
      if (T_FUN_SYS_VALUES == (*e)->type_) {
        (*e)->extra_ = 1;
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogLimit& op, ObLimitSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  spec.calc_found_rows_ = op.get_is_calc_found_rows();
  spec.is_top_limit_ = op.is_top_limit();
  spec.is_fetch_with_ties_ = op.is_fetch_with_ties();
  if (NULL != op.get_limit_count()) {
    CK(op.get_limit_count()->get_result_type().is_integer_type());
    OZ(generate_rt_expr(*op.get_limit_count(), spec.limit_expr_));
    OZ(mark_expr_self_produced(op.get_limit_count()));
  }
  if (NULL != op.get_limit_offset()) {
    CK(op.get_limit_offset()->get_result_type().is_integer_type());
    OZ(generate_rt_expr(*op.get_limit_offset(), spec.offset_expr_));
    OZ(mark_expr_self_produced(op.get_limit_offset()));
  }
  if (NULL != op.get_limit_percent()) {
    CK(op.get_limit_percent()->get_result_type().is_double());
    OZ(generate_rt_expr(*op.get_limit_percent(), spec.percent_expr_));
    OZ(mark_expr_self_produced(op.get_limit_percent()));
  }
  if (OB_SUCC(ret) && op.is_fetch_with_ties()) {
    OZ(spec.sort_columns_.init(op.get_expected_ordering().count()));
    FOREACH_CNT_X(it, op.get_expected_ordering(), OB_SUCC(ret))
    {
      CK(NULL != it->expr_);
      ObExpr* e = NULL;
      OZ(generate_rt_expr(*it->expr_, e));
      OZ(mark_expr_self_produced(it->expr_));
      OZ(spec.sort_columns_.push_back(e));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogDistinct& op, ObMergeDistinctSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (op.get_block_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge distinct has no block mode", K(op.get_algo()), K(op.get_block_mode()), K(ret));
  } else if (OB_FAIL(spec.cmp_funcs_.init(op.get_distinct_exprs().count()))) {
    LOG_WARN("failed to init sort functions", K(ret));
  } else if (OB_FAIL(spec.distinct_exprs_.init(op.get_distinct_exprs().count()))) {
    LOG_WARN("failed to init distinct exprs", K(ret));
  } else {
    ObExpr* expr = nullptr;
    ARRAY_FOREACH(op.get_distinct_exprs(), i)
    {
      const ObRawExpr* raw_expr = op.get_distinct_exprs().at(i);
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null pointer", K(ret));
      } else if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
        continue;
      } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (OB_FAIL(spec.distinct_exprs_.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        ObCmpFunc cmp_func;
        // no matter null first or null last.
        cmp_func.cmp_func_ = expr->basic_funcs_->null_last_cmp_;
        CK(NULL != cmp_func.cmp_func_);
        OZ(spec.cmp_funcs_.push_back(cmp_func));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(
  ObLogDistinct &op, ObHashDistinctSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  spec.is_block_mode_ = op.get_block_mode();
  int64_t init_count = op.get_distinct_exprs().count();
  if (1 != op.get_num_of_child()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child count of hash distinct", K(ret), K(op.get_num_of_child()));
  } else if (OB_FAIL(spec.cmp_funcs_.init(init_count))) {
    LOG_WARN("failed to init cmp functions", K(ret));
  } else if (OB_FAIL(spec.hash_funcs_.init(init_count))) {
    LOG_WARN("failed to init hash functions", K(ret));
  } else if (OB_FAIL(spec.sort_collations_.init(init_count))) {
    LOG_WARN("failed to init sort functions", K(ret));
  } else if (OB_FAIL(spec.distinct_exprs_.init(op.get_distinct_exprs().count() 
                                               + op.get_child(0)->get_output_exprs().count()))) {
    LOG_WARN("failed to init distinct exprs", K(ret));
  } else {
    ObArray<ObRawExpr *> additional_exprs;
    ObExpr *expr = nullptr;
    ARRAY_FOREACH(op.get_child(0)->get_output_exprs(), i) {
      ObRawExpr* raw_expr = op.get_child(0)->get_output_exprs().at(i);
      bool is_distinct_expr = has_exist_in_array(op.get_distinct_exprs(), raw_expr);
      if (!is_distinct_expr) {
        OZ (additional_exprs.push_back(raw_expr));
      }
    }
    if (OB_SUCC(ret)) {
      int64_t dist_cnt = 0;
      ARRAY_FOREACH(op.get_distinct_exprs(), i) {
        ObRawExpr* raw_expr = op.get_distinct_exprs().at(i);
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("null pointer", K(ret));
        } else if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
            continue;
        } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.distinct_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_ISNULL(expr->basic_funcs_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: basic funcs is not init", K(ret));
        } else {
          ObOrderDirection order_direction = default_asc_direction();
          bool is_ascending = is_ascending_direction(order_direction);
          ObSortFieldCollation field_collation(dist_cnt,
            expr->datum_meta_.cs_type_,
            is_ascending,
            (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
          ObCmpFunc cmp_func;
          cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
                                expr->datum_meta_.type_,
                                expr->datum_meta_.type_,
                                NULL_LAST,
                                expr->datum_meta_.cs_type_,
                                lib::is_oracle_mode());
          ObHashFunc hash_func;
          hash_func.hash_func_ = expr->basic_funcs_->murmur_hash_;
          if (OB_ISNULL(cmp_func.cmp_func_) || OB_ISNULL(hash_func.hash_func_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cmp_func or hash func is null, check datatype is valid",
                    K(cmp_func.cmp_func_), K(hash_func.hash_func_), K(ret));
          } else if (OB_FAIL(spec.sort_collations_.push_back(field_collation))) {
            LOG_WARN("failed to push back sort collation", K(ret));
          } else if (OB_FAIL(spec.cmp_funcs_.push_back(cmp_func))) {
            LOG_WARN("failed to push back sort function", K(ret));
          } else if (OB_FAIL(spec.hash_funcs_.push_back(hash_func))) {
            LOG_WARN("failed to push back hash funcs", K(ret));
          } else {
            ++dist_cnt;
          }
        }
      }
    }
    // complete distinct exprs
    if (OB_SUCC(ret) && 0 != additional_exprs.count()) {
      ARRAY_FOREACH(additional_exprs, i) {
        const ObRawExpr* raw_expr = additional_exprs.at(i);
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("null pointer", K(ret));
        } else if (raw_expr->has_flag(IS_CONST) || raw_expr->has_flag(IS_CONST_EXPR)) {
            continue;
        } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.distinct_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } 
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogMaterial& op, ObMaterialSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet& op, ObHashUnionSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_hash_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet& op, ObHashIntersectSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_hash_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet& op, ObHashExceptSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_hash_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_hash_set_spec(ObLogSet& op, ObHashSetSpec& spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> out_raw_exprs;
  if (OB_FAIL(op.extra_set_exprs(out_raw_exprs))) {
    LOG_WARN("failed to get output exprs", K(ret));
  } else if (OB_FAIL(mark_expr_self_produced(out_raw_exprs))) {  // set expr
    LOG_WARN("fail to mark exprs self produced", K(ret));
  } else if (OB_FAIL(spec.set_exprs_.init(out_raw_exprs.count()))) {
    LOG_WARN("failed to init set exprs", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(out_raw_exprs, spec.set_exprs_))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else if (OB_FAIL(spec.sort_collations_.init(spec.set_exprs_.count()))) {
    LOG_WARN("failed to init sort collations", K(ret));
  } else if (OB_FAIL(spec.sort_cmp_funs_.init(spec.set_exprs_.count()))) {
    LOG_WARN("failed to compare function", K(ret));
  } else if (OB_FAIL(spec.hash_funcs_.init(spec.set_exprs_.count()))) {
    LOG_WARN("failed to compare function", K(ret));
  } else {
    for (int64_t i = 0; i < spec.set_exprs_.count() && OB_SUCC(ret); ++i) {
      ObRawExpr* raw_expr = out_raw_exprs.at(i);
      ObExpr* expr = spec.set_exprs_.at(i);
      ObOrderDirection order_direction = default_asc_direction();
      bool is_ascending = is_ascending_direction(order_direction);
      ObSortFieldCollation field_collation(i,
          expr->datum_meta_.cs_type_,
          is_ascending,
          (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
      if (raw_expr->get_expr_type() != expr->type_ || !(T_OP_SET < expr->type_ && expr->type_ <= T_OP_EXCEPT)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: expr type is not match", K(raw_expr->get_expr_type()), K(expr->type_));
      } else if (OB_ISNULL(expr->basic_funcs_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: basic funcs is not init", K(ret));
      } else if (OB_FAIL(spec.sort_collations_.push_back(field_collation))) {
        LOG_WARN("failed to push back sort collation", K(ret));
      } else {
        ObSortCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
            expr->datum_meta_.type_,
            field_collation.null_pos_,
            field_collation.cs_type_,
            lib::is_oracle_mode());
        ObHashFunc hash_func;
        hash_func.hash_func_ = expr->basic_funcs_->default_hash_;
        if (OB_ISNULL(cmp_func.cmp_func_) || OB_ISNULL(hash_func.hash_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmp_func or hash func is null, check datatype is valid",
              K(cmp_func.cmp_func_),
              K(hash_func.hash_func_),
              K(ret));
        } else if (OB_FAIL(spec.sort_cmp_funs_.push_back(cmp_func))) {
          LOG_WARN("failed to push back sort function", K(ret));
        } else if (OB_FAIL(spec.hash_funcs_.push_back(hash_func))) {
          LOG_WARN("failed to push back hash funcs", K(ret));
        }
      }
    }
    spec.is_distinct_ = op.is_set_distinct();
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet& op, ObMergeUnionSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_merge_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet& op, ObMergeIntersectSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_merge_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet& op, ObMergeExceptSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_merge_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_cte_pseudo_column_row_desc(ObLogSet& op, ObRecursiveUnionAllSpec& phy_set_op)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*>& output_raw_exprs = op.get_output_exprs();
  int64_t column_num = output_raw_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_num; ++i) {
    if (T_CTE_SEARCH_COLUMN == output_raw_exprs.at(i)->get_expr_type()) {
      ObPseudoColumnRawExpr* expr = static_cast<ObPseudoColumnRawExpr*>(output_raw_exprs.at(i));
      ObExpr* search_expr = nullptr;
      if (OB_FAIL(generate_rt_expr(*expr, search_expr))) {
        LOG_WARN("generate rt expr failed", K(ret), KPC(expr));
      } else if (OB_FAIL(mark_expr_self_produced(expr))) {  // T_CTE_SEARCH_COLUMN
        LOG_WARN("mark expr self produced failed", K(ret));
      } else if (OB_ISNULL(search_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rt expr of search expr is null", K(ret));
      } else {
        phy_set_op.set_search_pseudo_column(search_expr);
      }
    } else if (T_CTE_CYCLE_COLUMN == output_raw_exprs.at(i)->get_expr_type()) {
      ObPseudoColumnRawExpr* expr = static_cast<ObPseudoColumnRawExpr*>(output_raw_exprs.at(i));
      ObExpr* cycle_expr = nullptr;
      if (OB_FAIL(generate_rt_expr(*expr, cycle_expr))) {
        LOG_WARN("generate rt expr failed", K(ret), KPC(expr));
      } else if (OB_FAIL(mark_expr_self_produced(expr))) {  // T_CTE_CYCLE_COLUMN
        LOG_WARN("mark expr self produced failed", K(ret));
      } else if (OB_ISNULL(cycle_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rt expr of cycle expr is null", K(ret));
      } else {
        phy_set_op.set_cycle_pseudo_column(cycle_expr);
        ObRawExpr *v_raw_expr, *d_v_raw_expr;
        expr->get_cte_cycle_value(v_raw_expr, d_v_raw_expr);
        ObExpr *v_expr, *d_v_expr;
        if (OB_ISNULL(v_raw_expr) || OB_ISNULL(d_v_raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Invalid raw expr", K(ret));
        } else if (OB_FAIL(generate_rt_expr(*v_raw_expr, v_expr))) {
          LOG_WARN("Failed to generate rt expr", K(ret));
        } else if (OB_ISNULL(v_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Invalid expr", K(ret), K(v_expr));
        } else if (OB_FAIL(generate_rt_expr(*d_v_raw_expr, d_v_expr))) {
          LOG_WARN("Failed to generate rt expr", K(ret));
        } else if (OB_ISNULL(d_v_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Invalid expr", K(ret), K(d_v_expr));
        } else if (OB_FAIL(phy_set_op.set_cycle_pseudo_values(v_expr, d_v_expr))) {
          LOG_WARN("Failed to set cycle values", K(ret), K(i), K(*expr));
        } else {
          LOG_DEBUG("Cycle values", K(ret), KPC(v_expr), KPC(d_v_expr), KPC(d_v_raw_expr), KPC(v_raw_expr));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet& op, ObRecursiveUnionAllSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  LOG_DEBUG("static engine cg generate recursive union all",
      K(spec.get_left()->output_),
      K(spec.get_right()->output_),
      K(op.get_output_exprs()));
  if (OB_FAIL(generate_recursive_union_all_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_merge_set_spec(ObLogSet& op, ObMergeSetSpec& spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> out_raw_exprs;
  if (OB_FAIL(op.extra_set_exprs(out_raw_exprs))) {
    LOG_WARN("failed to get output exprs", K(ret));
  } else if (OB_FAIL(mark_expr_self_produced(out_raw_exprs))) {  // set expr
    LOG_WARN("fail to mark exprs self produced", K(ret));
  } else if (OB_FAIL(spec.set_exprs_.init(out_raw_exprs.count()))) {
    LOG_WARN("failed to init set exprs", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(out_raw_exprs, spec.set_exprs_))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else if (op.is_set_distinct() &&
             (spec.set_exprs_.count() != op.get_map_array().count() && 0 != op.get_map_array().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output exprs is not match map array", K(ret), K(op.get_map_array().count()), K(spec.set_exprs_.count()));
  } else if (!op.is_set_distinct()) {
  } else if (OB_FAIL(spec.sort_collations_.init(spec.set_exprs_.count()))) {
    LOG_WARN("failed to init sort collations", K(ret));
  } else if (OB_FAIL(spec.sort_cmp_funs_.init(spec.set_exprs_.count()))) {
    LOG_WARN("failed to compare function", K(ret));
  } else {
    for (int64_t i = 0; i < spec.set_exprs_.count() && OB_SUCC(ret); ++i) {
      int64_t idx = (0 == op.get_map_array().count()) ? i : op.get_map_array().at(i);
      if (idx >= spec.set_exprs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: invalid idx", K(idx), K(spec.set_exprs_.count()));
      } else {
        ObExpr* expr = spec.set_exprs_.at(idx);
        ObOrderDirection order_direction = op.get_set_directions().at(i);
        bool is_ascending = is_ascending_direction(order_direction);
        ObSortFieldCollation field_collation(idx,
            expr->datum_meta_.cs_type_,
            is_ascending,
            (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
        if (OB_FAIL(spec.sort_collations_.push_back(field_collation))) {
          LOG_WARN("failed to push back sort collation", K(ret));
        } else {
          ObSortCmpFunc cmp_func;
          cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
              expr->datum_meta_.type_,
              field_collation.null_pos_,
              field_collation.cs_type_,
              lib::is_oracle_mode());
          if (OB_ISNULL(cmp_func.cmp_func_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cmp_func is null, check datatype is valid", K(cmp_func.cmp_func_), K(ret));
          } else if (OB_FAIL(spec.sort_cmp_funs_.push_back(cmp_func))) {
            LOG_WARN("failed to push back sort function", K(ret));
          }
        }
      }
    }
  }
  spec.is_distinct_ = op.is_set_distinct();
  return ret;
}

int ObStaticEngineCG::generate_recursive_union_all_spec(ObLogSet& op, ObRecursiveUnionAllSpec& spec)
{
  int ret = OB_SUCCESS;
  uint64_t last_cte_table_id = OB_INVALID_ID;
  ObOpSpec* left = nullptr;
  ObOpSpec* right = nullptr;
  if (OB_UNLIKELY(spec.get_child_cnt() != 2) || OB_ISNULL(left = spec.get_child(0)) ||
      OB_ISNULL(right = spec.get_child(1)) || OB_UNLIKELY(left->get_output_count() != right->get_output_count()) ||
      OB_UNLIKELY(op.get_output_exprs().count() < left->get_output_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recursive union all spec should have two children", K(ret), K(spec.get_child_cnt()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < left->get_output_count(); i++) {
      if (left->output_.at(i)->datum_meta_.type_ != left->output_.at(i)->datum_meta_.type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left and right of recursive union all should have same output data type", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(generate_cte_pseudo_column_row_desc(op, spec))) {
    LOG_WARN("Failed to generate cte pseudo", K(ret));
  } else if (OB_FAIL(fake_cte_tables_.pop_back(last_cte_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Failed to pop last cte table op", K(ret), K(&fake_cte_tables_), K(fake_cte_tables_));
  } else if (OB_UNLIKELY(OB_INVALID_ID == last_cte_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Last cte table op cann't be null!", K(ret));
  } else {
    spec.set_fake_cte_table(last_cte_table_id);
    if (op.is_breadth_search()) {
      spec.set_search_strategy(ObRecursiveInnerDataOp::SearchStrategyType::BREADTH_FRIST);
    } else {
      spec.set_search_strategy(ObRecursiveInnerDataOp::SearchStrategyType::DEPTH_FRIST);
    }

    OZ(spec.output_union_exprs_.init(left->output_.count()));
    ARRAY_FOREACH(left->output_, i)
    {
      ObRawExpr* output_union_raw_expr = op.get_output_exprs().at(i);
      ObExpr* output_union_expr = nullptr;
      if (OB_ISNULL(output_union_raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("output expr is null", K(ret), K(i));
      } else if (OB_FAIL(generate_rt_expr(*output_union_raw_expr, output_union_expr))) {
        LOG_WARN("generate rt expr failed", K(ret));
      } else if (OB_ISNULL(output_union_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("output expr is null", K(ret), K(i));
      } else if (OB_UNLIKELY(T_OP_UNION != output_union_expr->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("recursive union all invalid output", K(i), K(*output_union_expr));
      } else if (OB_FAIL(mark_expr_self_produced(output_union_raw_expr))) {  // set expr
        LOG_WARN("fail to mark expr self produced", K(ret));
      } else if (OB_FAIL(spec.output_union_exprs_.push_back(output_union_expr))) {
        LOG_WARN("array push back failed", K(ret));
      }
    }

    const ObIArray<OrderItem>& search_order = op.get_search_ordering();
    OZ(spec.sort_collations_.init(search_order.count()));
    ARRAY_FOREACH(search_order, i)
    {
      const ObRawExpr* raw_expr = search_order.at(i).expr_;
      if (raw_expr->is_column_ref_expr()) {
        const ObColumnRefRawExpr* col_expr = static_cast<const ObColumnRefRawExpr*>(raw_expr);
        int64_t sort_idx = col_expr->get_cte_generate_column_projector_offset();
        ObOrderDirection order_direction = search_order.at(i).order_type_;
        bool is_ascending = is_ascending_direction(order_direction);
        ObSortFieldCollation field_collation(sort_idx,
            raw_expr->get_collation_type(),
            is_ascending,
            (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
        if (OB_FAIL(spec.sort_collations_.push_back(field_collation))) {
          LOG_WARN("failed to push back sort collation", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The search by expr must be cte table column raw expr", K(ret));
      }
    }
    const ObIArray<ColumnItem>& cycle_items = op.get_cycle_items();
    OZ(spec.cycle_by_col_lists_.init(cycle_items.count()));
    ARRAY_FOREACH(cycle_items, i)
    {
      const ObRawExpr* raw_expr = cycle_items.at(i).expr_;
      if (raw_expr->is_column_ref_expr()) {
        uint64_t index = cycle_items.at(i).column_id_;
        if (OB_FAIL(spec.cycle_by_col_lists_.push_back(index))) {
          LOG_WARN("Failed to add cycle by order", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("The cycle by expr must be cte table column raw expr", K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::fill_sort_info(
    const ObIArray<OrderItem>& sort_keys, ObSortCollations& collations, ObIArray<ObExpr*>& sort_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(collations.init(sort_keys.count()))) {
    LOG_WARN("failed to init collations", K(ret));
  } else {
    int64_t start_pos = sort_exprs.count();
    for (int64_t i = 0; i < sort_keys.count() && OB_SUCC(ret); ++i) {
      const OrderItem& order_item = sort_keys.at(i);
      ObExpr* expr = nullptr;
      if (order_item.expr_->has_flag(IS_CONST) || order_item.expr_->has_flag(IS_CONST_EXPR)) {
        LOG_TRACE("trace sort const", K(*order_item.expr_));
        continue;  // sort by const value, just ignore
      } else if (OB_FAIL(generate_rt_expr(*order_item.expr_, expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (OB_FAIL(sort_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        ObSortFieldCollation field_collation(start_pos++,
            expr->datum_meta_.cs_type_,
            order_item.is_ascending(),
            (order_item.is_null_first() ^ order_item.is_ascending()) ? NULL_LAST : NULL_FIRST);
        if (OB_FAIL(collations.push_back(field_collation))) {
          LOG_WARN("failed to push back field collation", K(ret));
        } else {
          LOG_DEBUG("succ to push back field collation", K(field_collation), K(start_pos), K(i), K(order_item));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::fill_sort_funcs(
    const ObSortCollations& collations, ObSortFuncs& sort_funcs, const ObIArray<ObExpr*>& sort_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_funcs.init(collations.count()))) {
    LOG_WARN("failed to init sort functions", K(ret));
  } else {
    for (int64_t i = 0; i < collations.count() && OB_SUCC(ret); ++i) {
      const ObSortFieldCollation& sort_collation = collations.at(i);
      ObExpr* expr = nullptr;
      if (OB_FAIL(sort_exprs.at(sort_collation.field_idx_, expr))) {
        LOG_WARN("failed to get sort exprs", K(ret));
      } else {
        ObSortCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
            expr->datum_meta_.type_,
            sort_collation.null_pos_,
            sort_collation.cs_type_,
            lib::is_oracle_mode());
        if (OB_ISNULL(cmp_func.cmp_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmp_func is null, check datatype is valid", K(ret));
        } else if (OB_FAIL(sort_funcs.push_back(cmp_func))) {
          LOG_WARN("failed to push back sort function", K(ret));
        }
      }
    }
  }
  return ret;
}

// all_expr: sort_exprs + output_exprs
int ObStaticEngineCG::generate_spec(ObLogSort& op, ObSortSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 4> output_exprs;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_rt_exprs(op.get_output_exprs(), output_exprs))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else {
    if (OB_NOT_NULL(op.get_topn_count())) {
      spec.is_fetch_with_ties_ = op.is_fetch_with_ties();
      OZ(generate_rt_expr(*op.get_topn_count(), spec.topn_expr_));
      if (OB_NOT_NULL(spec.topn_expr_) && !ob_is_integer_type(spec.topn_expr_->datum_meta_.type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("topn must be int", K(ret), K(*spec.topn_expr_));
      }
    }
    if (OB_NOT_NULL(op.get_topk_limit_count())) {
      OZ(generate_rt_expr(*op.get_topk_limit_count(), spec.topk_limit_expr_));
      if (OB_NOT_NULL(op.get_topk_offset_count())) {
        OZ(generate_rt_expr(*op.get_topk_offset_count(), spec.topk_offset_expr_));
        if (OB_NOT_NULL(spec.topk_offset_expr_) && !ob_is_integer_type(spec.topk_offset_expr_->datum_meta_.type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("topn must be int", K(ret), K(*spec.topk_offset_expr_));
        }
      }
      spec.minimum_row_count_ = op.get_minimum_row_count();
      spec.topk_precision_ = op.get_topk_precision();
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(spec.get_child())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret));
      } else if (spec.all_exprs_.init(op.get_sort_keys().count() + spec.get_child()->output_.count())) {
        LOG_WARN("failed to init all exprs", K(ret));
      } else if (OB_FAIL(fill_sort_info(op.get_sort_keys(), spec.sort_collations_, spec.all_exprs_))) {
        LOG_WARN("failed to sort info", K(ret));
      } else if (OB_FAIL(fill_sort_funcs(spec.sort_collations_, spec.sort_cmp_funs_, spec.all_exprs_))) {
        LOG_WARN("failed to sort funcs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(spec.all_exprs_, spec.get_child()->output_))) {
        LOG_WARN("failed to append array no dup", K(ret));
      } else {
        spec.prefix_pos_ = op.get_prefix_pos();
        spec.is_local_merge_sort_ = op.is_local_merge_sort();
        LOG_TRACE("trace order by", K(spec.all_exprs_.count()), K(spec.all_exprs_));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogCount& op, ObCountSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (NULL != op.get_rownum_limit_expr()) {
    CK(op.get_rownum_limit_expr()->get_result_type().is_integer_type());
    OZ(generate_rt_expr(*op.get_rownum_limit_expr(), spec.rownum_limit_));
  }
  // Anti monotone filters are add to spec.filters_ too in generate_spec_basic(),
  // ideally we should only add non_anti_monotone_filters to spec.filters_.
  // Since expr evaluating twice is cheap, we add it twice make CG simpler.
  if (OB_SUCC(ret) && !op.get_filter_exprs().empty()) {
    ObSEArray<ObRawExpr*, 4> anti_monotone_filters;
    ObSEArray<ObRawExpr*, 4> non_anti_monotone_filters;
    OZ(classify_anti_monotone_filter_exprs(op.get_filter_exprs(), non_anti_monotone_filters, anti_monotone_filters));
    if (OB_SUCC(ret) && !anti_monotone_filters.empty()) {
      OZ(generate_rt_exprs(anti_monotone_filters, spec.anti_monotone_filters_));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogValues& op, ObValuesSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(spec.row_store_.assign(op.get_row_store()))) {
    LOG_WARN("row store assign failed", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExprValues& op, ObExprValuesSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (!op.get_value_exprs().empty()) {
    if (OB_FAIL(spec.values_.prepare_allocate(op.get_value_exprs().count()))) {
      LOG_WARN("init fixed array failed", K(ret), K(op.get_value_exprs().count()));
    } else if (OB_FAIL(spec.str_values_array_.prepare_allocate(op.get_output_exprs().count()))) {
      LOG_WARN("init fixed array failed", K(ret), K(op.get_output_exprs().count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < op.get_value_exprs().count(); i++) {
        ObRawExpr* raw_expr = op.get_value_exprs().at(i);
        ObExpr* expr = NULL;
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("raw_expr is null", K(ret), K(i), K(raw_expr));
        } else if (OB_FAIL(mark_expr_self_produced(raw_expr))) {  // expr values
          LOG_WARN("mark expr self produced failed", K(ret), KPC(raw_expr));
        } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
          LOG_WARN("fail to generate_rt_expr", K(ret), K(i), KPC(raw_expr));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("value_info.expr_ is null", K(ret), K(i), KPC(raw_expr));
        } else {
          spec.values_.at(i) = expr;
        }
      }
      // Add str_values to spec: str_values_ is worked for enum/set type for type conversion.
      // According to code in ob_expr_values_op.cpp, it should be in the same order as output_exprs.
      for (int64_t i = 0; OB_SUCC(ret) && i < op.get_output_exprs().count(); i++) {
        ObRawExpr *output_raw_expr = op.get_output_exprs().at(i);
        const common::ObIArray<common::ObString> &str_values =
          output_raw_expr->get_enum_set_values();
        if (OB_FAIL(spec.str_values_array_.at(i).assign(str_values))) {
          LOG_WARN("fail to assign", K(ret), K(i), K(str_values));
        }
      }
      LOG_DEBUG("finish assign str_values_array", K(ret), K(spec.str_values_array_));
    }
  }
  if (OB_SUCC(ret)) {
    if (0 != op.get_output_exprs().count()) {
      spec.rows_ = spec.get_value_count() / op.get_output_exprs().count();
      // expr values
      if (OB_FAIL(mark_expr_self_produced(op.get_output_exprs()))) {
        LOG_WARN("mark expr self produced failed", K(ret));
      }
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogMerge& op, ObMultiTableMergeSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_spec(op, static_cast<ObTableMergeSpec&>(spec), in_root_job))) {
    LOG_WARN("fail to generate spec", K(ret));
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogMerge& op, ObTableMergeSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  CK(OB_NOT_NULL(op.get_all_table_columns()),
      !op.get_all_table_columns()->empty(),
      !op.get_all_table_columns()->at(0).index_dml_infos_.empty());
  CK(OB_NOT_NULL(op.get_primary_key_ids()));
  CK(OB_NOT_NULL(op.get_stmt()));
  if (OB_SUCC(ret)) {
    ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(op.get_stmt());
    spec.table_id_ = op.get_all_table_columns()->at(0).index_dml_infos_.at(0).loc_table_id_;
    spec.index_tid_ = op.get_all_table_columns()->at(0).index_dml_infos_.at(0).index_tid_;
    spec.has_insert_clause_ = merge_stmt->has_insert_clause();
    spec.has_update_clause_ = merge_stmt->has_update_clause();
    spec.gi_above_ = op.is_gi_above();
    const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
    OZ(mark_expr_self_produced(*table_columns));  // dml columns
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_column_infos(op, spec, *table_columns))) {
      LOG_WARN("failed to add column infos", K(ret));
    } else if (OB_FAIL(spec.primary_key_ids_.init(op.get_primary_key_ids()->count()))) {
      LOG_WARN("failed to init primary key ids", K(ret));
    } else if (OB_FAIL(spec.primary_key_ids_.assign(*op.get_primary_key_ids()))) {
      LOG_WARN("failed to assign primary key idx", K(ret));
    } else if (OB_FAIL(add_table_column_ids(op, &spec, *table_columns))) {
      LOG_WARN("fail to add column id", K(ret));
    } else if (OB_ISNULL(op.get_rowkey_exprs())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rowkey exprs is NULL", K(ret));
    } else if (OB_FAIL(generate_rt_exprs(*op.get_rowkey_exprs(), spec.rowkey_exprs_))) {
      LOG_WARN("failed to generate rt exprs", K(ret));
    } else if (merge_stmt->has_update_clause()) {
      LOG_TRACE("trace cg update clause");
      const ObTableAssignment& table_assigns = op.get_tables_assignments()->at(0);
      OZ(convert_update_assignments(*table_columns, table_assigns.assignments_, spec));
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(op.get_delete_condition())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid parameter", K(op.get_delete_condition()), K(ret));
        } else if (op.get_delete_condition()->count() <= 0) {
          // no delete condition
        } else if (OB_FAIL(generate_rt_exprs(*op.get_delete_condition(), spec.delete_conds_))) {
          LOG_WARN("failed to generate rt exprs", K(ret), K(*op.get_delete_condition()));
        } else {
          OZ(spec.delete_column_ids_.init(table_columns->count()));
          for (int64_t i = 0; OB_SUCC(ret) && i < table_columns->count(); ++i) {
            ObColumnRefRawExpr* raw_expr = table_columns->at(i);
            if (OB_ISNULL(raw_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to get column expr", K(ret), K(i));
            } else if (OB_FAIL(spec.add_delete_column_id(raw_expr->get_column_id()))) {
              LOG_WARN("failed to add column id", K(ret));
            }
          }
        }
      }
    } else {
      OZ(spec.old_row_.init(table_columns->count()));
      ARRAY_FOREACH(*table_columns, i)
      {
        ObExpr* expr = nullptr;
        ObRawExpr* raw_expr = table_columns->at(i);
        if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.old_row_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && merge_stmt->has_insert_clause()) {
      CK(OB_NOT_NULL(op.get_column_convert_exprs()));
      OZ(generate_rt_exprs(*op.get_column_convert_exprs(), spec.storage_row_output_));
    }
    if (OB_SUCC(ret)) {
      if (nullptr != op.get_update_condition()) {
        OZ(generate_rt_exprs(*op.get_update_condition(), spec.update_conds_));
      }
      if (nullptr != op.get_insert_condition()) {
        OZ(generate_rt_exprs(*op.get_insert_condition(), spec.insert_conds_));
      }
      if (nullptr != op.get_delete_condition()) {
        OZ(generate_rt_exprs(*op.get_delete_condition(), spec.delete_conds_));
      }
    }
    OZ(convert_check_constraint(op, spec));
    OZ(convert_table_dml_param(op, spec));
    CK(spec.check_constraint_exprs_.count() % 2 == 0);

    if (OB_SUCC(ret) && op.is_multi_part_dml()) {
      ObTableDMLInfo table_dml_info;
      ObOpSpec* append_op = NULL;
      ObSEArray<ObOpSpec*, 8> subplan_roots;
      ObMultiTableMergeSpec& multi_table_merge = static_cast<ObMultiTableMergeSpec&>(spec);
      CK(1 == op.get_all_table_columns()->count())
      OZ(multi_table_merge.init_table_dml_info_array(1));
      const ObTablesAssignments* tas = op.get_tables_assignments();
      const TableColumns& all_table_columns = op.get_all_table_columns()->at(0);
      OZ(convert_global_index_merge_info(op, all_table_columns, subplan_roots, table_dml_info));
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(phy_plan_->alloc_op_spec(PHY_APPEND, subplan_roots.count(), append_op, OB_INVALID_ID))) {
        LOG_WARN("alloc operator by type failed", K(ret));
      } else {
        multi_table_merge.set_subplan_root(append_op);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
        subplan_roots.at(i)->set_parent(append_op);
        OZ(append_op->set_child(static_cast<int32_t>(i), subplan_roots.at(i)));
      }
      OZ(multi_table_merge.add_table_dml_info(0, table_dml_info));
    }
  }
  return ret;
}

int ObStaticEngineCG::convert_global_index_merge_info(ObLogMerge& op, const TableColumns& table_columns,
    common::ObIArray<ObOpSpec*>& subplan_roots, ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = nullptr;
  ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(op.get_stmt());
  CK(OB_NOT_NULL(op.get_plan()));
  CK(OB_NOT_NULL(schema_guard = op.get_plan()->get_optimizer_context().get_sql_schema_guard()));
  CK(OB_NOT_NULL(merge_stmt));
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), table_columns.index_dml_infos_.count()));

  table_dml_info.set_enable_row_movement(true);
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.index_dml_infos_.count(); i++) {
    const ObTableSchema* table_schema = nullptr;
    const IndexDMLInfo& index_dml_info = table_columns.index_dml_infos_.at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    SeDMLSubPlanArray& subplans = phy_dml_info.se_subplans_;

    if (OB_FAIL(schema_guard->get_table_schema(index_dml_info.index_tid_, table_schema))) {
      LOG_WARN("get table schema failed", K(index_dml_info.index_tid_), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(index_dml_info.index_tid_), K(ret));
    } else if (table_schema->is_user_table() && !table_schema->is_enable_row_movement()) {
      table_dml_info.set_enable_row_movement(false);
    }

    OZ(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiTableMergeOp::DML_OP_CNT));
    OZ(phy_dml_info.calc_part_id_exprs_.allocate_array(phy_plan_->get_allocator(), 3));
    OZ(generate_rt_expr(*index_dml_info.calc_part_id_exprs_.at(0), phy_dml_info.calc_part_id_exprs_.at(0)));
    OZ(generate_rt_expr(*index_dml_info.calc_part_id_exprs_.at(1), phy_dml_info.calc_part_id_exprs_.at(1)));
    OZ(generate_rt_expr(*index_dml_info.calc_part_id_exprs_.at(2), phy_dml_info.calc_part_id_exprs_.at(2)));

    SeDMLSubPlan& delete_subplan = subplans.at(ObMultiTableMergeOp::DELETE_OP);
    SeDMLSubPlan& update_insert_subplan = subplans.at(ObMultiTableMergeOp::UPDATE_INSERT_OP);
    SeDMLSubPlan& insert_subplan = subplans.at(ObMultiTableMergeOp::INSERT_OP);
    int64_t access_cnt = index_dml_info.column_exprs_.count();
    delete_subplan.access_exprs_.set_allocator(&phy_plan_->get_allocator());
    update_insert_subplan.access_exprs_.set_allocator(&phy_plan_->get_allocator());
    insert_subplan.access_exprs_.set_allocator(&phy_plan_->get_allocator());
    OZ(delete_subplan.access_exprs_.init(access_cnt));
    OZ(update_insert_subplan.access_exprs_.init(access_cnt));
    OZ(insert_subplan.access_exprs_.init(access_cnt));
    OZ(generate_merge_subplan_access_exprs(merge_stmt->has_update_clause(),
        index_dml_info.assignments_,
        index_dml_info.column_exprs_,
        delete_subplan.access_exprs_,
        update_insert_subplan.access_exprs_));
    if (merge_stmt->has_update_clause()) {
      OZ(convert_delete_subplan(op, index_dml_info, delete_subplan));
      OZ(subplan_roots.push_back(delete_subplan.subplan_root_));
      OZ(convert_insert_subplan(op, index_dml_info, update_insert_subplan));
      OZ(subplan_roots.push_back(update_insert_subplan.subplan_root_));
    }
    if (merge_stmt->has_insert_clause()) {
      OZ(mark_expr_self_produced(*op.get_table_columns()));
      OZ(generate_insert_subplan_access_exprs(index_dml_info.column_exprs_,
          *op.get_table_columns(),
          *op.get_column_convert_exprs(),
          insert_subplan.access_exprs_));
      OZ(convert_insert_subplan(op, index_dml_info, insert_subplan));
      OZ(subplan_roots.push_back(insert_subplan.subplan_root_));
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_merge_subplan_access_exprs(const bool has_update_clause, const ObAssignments& assigns,
    const ObIArray<ObColumnRefRawExpr*>& index_exprs, ObIArray<ObExpr*>& delete_access_exprs,
    ObIArray<ObExpr*>& update_insert_access_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> tmp_raw_exprs;
  ObExpr* expr = NULL;
  OZ(generate_exprs_replace_spk(index_exprs, delete_access_exprs));
  OZ(update_insert_access_exprs.assign(delete_access_exprs));
  if (OB_SUCC(ret) && has_update_clause) {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      ObColumnRefRawExpr* col = assigns.at(i).column_expr_;
      ObRawExpr* value = assigns.at(i).expr_;
      CK(NULL != col);
      CK(NULL != value);
      int64_t idx = OB_INVALID_INDEX;
      if (OB_SUCC(ret)) {
        bool exists = has_exist_in_array(index_exprs, col, &idx);
        if (!exists) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update column not found in all columns", K(ret), K(col));
        }
      }
      if (OB_SUCC(ret)) {
        ObExpr* expr = NULL;
        OZ(generate_rt_expr(*value, expr));
        if (OB_SUCC(ret)) {
          update_insert_access_exprs.at(idx) = expr;
        }
      }
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert& op, ObMultiPartInsertSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  spec.is_returning_ = op.is_returning();
  if (OB_FAIL(generate_spec(op, static_cast<ObTableInsertSpec&>(spec), in_root_job))) {
    LOG_WARN("fail to generate spec", K(ret));
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert& op, ObTableInsertSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  CK(OB_NOT_NULL(op.get_all_table_columns()),
      !op.get_all_table_columns()->empty(),
      !op.get_all_table_columns()->at(0).index_dml_infos_.empty());
  if (OB_SUCC(ret)) {
    CK(NULL != op.get_primary_key_ids());
    OZ(spec.set_primary_key_ids(*op.get_primary_key_ids()));
  }
  if (OB_SUCC(ret)) {
    spec.table_id_ = op.get_all_table_columns()->at(0).index_dml_infos_.at(0).loc_table_id_;
    spec.index_tid_ = op.get_all_table_columns()->at(0).index_dml_infos_.at(0).index_tid_;
    spec.is_ignore_ = op.is_ignore();
    spec.gi_above_ = op.is_gi_above();
    spec.plan_->set_ignore(op.is_ignore());
    const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
    if (OB_ISNULL(table_columns)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table columns", K(table_columns));
    } else if (OB_FAIL(add_table_column_ids(op, &spec, *table_columns))) {
      LOG_WARN("fail to add column id", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObColumnRefRawExpr*>* columns = op.get_table_columns();
    const ObIArray<ObRawExpr*>* column_convert_exprs = op.get_column_convert_exprs();
    if (OB_ISNULL(columns) || OB_ISNULL(column_convert_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected", K(columns), K(column_convert_exprs));
    } else if (OB_FAIL(add_column_infos(op, spec, *columns))) {
      LOG_WARN("failed to add column infos", K(ret));
    } else if (OB_FAIL(add_column_conv_infos(spec, *columns, *column_convert_exprs))) {
      LOG_WARN("failed to add column conv infos", K(ret));
    } else if (OB_FAIL(mark_expr_self_produced(*columns))) {
      // table columns exprs in dml need to set IS_COLUMNLIZED flag
      LOG_WARN("failed to mark expr self produced", K(ret));
    } else if (OB_FAIL(mark_expr_self_produced(*column_convert_exprs))) {
      LOG_WARN("failed to mark expr self produced", K(ret));
    }
  }
  if (OB_SUCC(ret) && PHY_MULTI_PART_INSERT == spec.type_) {
    ObMultiPartInsertSpec& multi_insert = static_cast<ObMultiPartInsertSpec&>(spec);
    if (OB_FAIL(convert_insert_index_info(op, multi_insert))) {
      LOG_WARN("convert insert index info failed", K(ret));
    }
  }

  // value exprs is used for storage row right now.
  OZ(generate_rt_exprs(op.get_value_exprs(), spec.storage_row_output_));

  if (OB_SUCC(ret)) {
    OZ(convert_foreign_keys(op, spec));
    OZ(convert_check_constraint(op, spec));
  }
  // construct ObTableDMLParam
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, spec))) {
      LOG_ERROR("fail to convert table dml param", K(ret));
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert& op, ObTableInsertReturningSpec& spec, const bool in_root_job)
{
  spec.is_returning_ = true;
  return generate_spec(op, static_cast<ObTableInsertSpec&>(spec), in_root_job);
}

int ObStaticEngineCG::generate_spec(ObLogicalOperator& op, ObAppendSpec& spec, const bool in_root_job)
{
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  return OB_SUCCESS;
}

int ObStaticEngineCG::generate_spec(ObLogicalOperator& op, ObTableRowStoreSpec& spec, const bool in_root_job)
{
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  return OB_SUCCESS;
}

int ObStaticEngineCG::convert_insert_index_info(ObLogInsert& op, ObMultiPartInsertSpec& spec)
{
  int ret = OB_SUCCESS;
  ObOpSpec* append_op = NULL;
  ObSEArray<ObOpSpec*, 8> subplan_roots;
  ObTableDMLInfo table_dml_info;
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  const ObIArray<ObRawExpr*>& value_exprs = op.get_value_exprs();
  const ObIArray<IndexDMLInfo>* index_dml_infos = NULL;
  if (OB_ISNULL(op.get_all_table_columns()) || OB_ISNULL(table_columns) || OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phy_plan_), KP(table_columns));
  } else if (FALSE_IT(index_dml_infos = &(op.get_all_table_columns()->at(0).index_dml_infos_))) {
    // do nothing
  } else if (OB_FAIL(spec.init_table_dml_info_array(1))) {
    LOG_WARN("init table insert info array failed", K(ret));
  } else if (OB_UNLIKELY(table_columns->count() != value_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column count", K(table_columns->count()), K(value_exprs.count()));
  }
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), index_dml_infos->count()));
  CK(OB_NOT_NULL(static_cast<ObLogInsert&>(op).get_column_convert_exprs()));
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos->count(); ++i) {
    const IndexDMLInfo& index_dml_info = index_dml_infos->at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    if (OB_FAIL(phy_dml_info.calc_part_id_exprs_.allocate_array(phy_plan_->get_allocator(), 1))) {
      LOG_WARN("allocate table location array failed", K(ret));
    } else if (OB_FAIL(generate_rt_expr(
                   *index_dml_info.calc_part_id_exprs_.at(0), phy_dml_info.calc_part_id_exprs_.at(0)))) {
      LOG_WARN("generate index table location failed", K(ret));
    } else {
      ObSEArray<ObRawExpr*, 8> calc_part_exprs;
      OZ(calc_part_exprs.push_back(index_dml_info.calc_part_id_exprs_.at(0)));
      phy_dml_info.calc_exprs_.set_allocator(&phy_plan_->get_allocator());
      OZ(generate_calc_exprs(
          *static_cast<ObLogInsert&>(op).get_column_convert_exprs(), calc_part_exprs, phy_dml_info.calc_exprs_, false));
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(phy_dml_info.se_subplans_.allocate_array(phy_plan_->get_allocator(), 1))) {
      LOG_WARN("allocate dml subplan temp array failed", K(ret));
    } else {
      SeDMLSubPlan& dml_subplan = phy_dml_info.se_subplans_.at(0);
      CK(OB_NOT_NULL(op.get_table_columns()));
      if (OB_SUCC(ret)) {
        int64_t index_col_cnt = index_dml_info.column_exprs_.count();
        phy_plan_->set_ignore(op.is_ignore());
        dml_subplan.access_exprs_.set_allocator(&phy_plan_->get_allocator());
        if (OB_FAIL(dml_subplan.access_exprs_.init(index_col_cnt))) {
          LOG_WARN("fail to allocate array", K(ret));
        }
      }
      OZ(mark_expr_self_produced(*op.get_table_columns()));
      OZ(generate_insert_subplan_access_exprs(index_dml_info.column_exprs_,
          *op.get_table_columns(),
          *static_cast<ObLogInsert&>(op).get_column_convert_exprs(),
          dml_subplan.access_exprs_));
      OZ(convert_insert_subplan(op, index_dml_info, dml_subplan));
      OZ(subplan_roots.push_back(dml_subplan.subplan_root_));
    }

    if (OB_SUCC(ret) && 0 == i && op.get_part_hint() != NULL) {
      // if multi partition insert values come from select clause,
      // we can't calculate partitions accurately in
      // the generating plan phase, so delay the partition matching to the execution phase
      phy_dml_info.hint_part_ids_.set_allocator(&phy_plan_->get_allocator());
      if (OB_FAIL(phy_dml_info.hint_part_ids_.assign(op.get_part_hint()->part_ids_))) {
        LOG_WARN("add part hint ids failed", K(ret));
      }
    }
  }  // for index_dml_infos end
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(phy_plan_->alloc_op_spec(PHY_APPEND, subplan_roots.count(), append_op, OB_INVALID_ID))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    spec.set_subplan_root(append_op);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    subplan_roots.at(i)->set_parent(append_op);
    OZ(append_op->set_child(static_cast<int32_t>(i), subplan_roots.at(i)));
  }
  OZ(spec.add_table_dml_info(0, table_dml_info));
  return ret;
}

int ObStaticEngineCG::convert_insert_subplan(
    ObLogDelUpd& op, const IndexDMLInfo& index_dml_info, SeDMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(phy_plan_));
  if (OB_SUCC(ret)) {
    phy_plan_->set_ignore(op.is_ignore());
  }
  OZ(convert_common_dml_subplan(op, PHY_INSERT, dml_subplan.access_exprs_, index_dml_info, dml_subplan));
  if (OB_SUCC(ret) && log_op_def::LOG_INSERT == op.get_type()) {
    if (OB_FAIL(convert_foreign_keys(op, *dml_subplan.subplan_root_))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  if (OB_SUCC(ret) && log_op_def::LOG_INSERT == op.get_type()) {
    if (OB_FAIL(convert_table_dml_param(op, *dml_subplan.subplan_root_))) {
      LOG_WARN("fail to convert table dml param", K(ret));
    }
  }

  return ret;
}

int ObStaticEngineCG::convert_common_dml_subplan(ObLogDelUpd& op, ObPhyOperatorType phy_op_type,
    ObIArray<ObExpr*>& access_exprs, const IndexDMLInfo& index_dml_info, SeDMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  ObOpSpec* op_spec = NULL;
  ObOpSpec* values_spec = NULL;
  CK(OB_NOT_NULL(phy_plan_));
  OZ(convert_index_values(index_dml_info.index_tid_, access_exprs, values_spec));
  CK(OB_NOT_NULL(values_spec));
  OZ(phy_plan_->alloc_op_spec(phy_op_type, 1, op_spec, OB_INVALID_ID));
  if (OB_SUCC(ret)) {
    values_spec->set_parent(op_spec);
  }
  OZ(op_spec->set_child(0, values_spec));
  if (OB_SUCC(ret)) {
    ObTableModifySpec* dml_spec = static_cast<ObTableModifySpec*>(op_spec);
    dml_spec->table_id_ = index_dml_info.loc_table_id_;
    dml_spec->index_tid_ = index_dml_info.index_tid_;
    dml_spec->from_multi_table_dml_ = true;
    dml_spec->is_ignore_ = op.is_ignore();
    add_column_infos(op, *dml_spec, index_dml_info.column_exprs_);
    OZ(dml_spec->storage_row_output_.reserve(access_exprs.count()));
    FOREACH_CNT_X(expr, access_exprs, OB_SUCC(ret))
    {
      CK(OB_NOT_NULL(*expr));
      OZ(dml_spec->storage_row_output_.push_back(*expr));
    }
    dml_subplan.subplan_root_ = dml_spec;
    const ObIArray<ObColumnRefRawExpr*>& table_columns = index_dml_info.column_exprs_;
    OZ(add_table_column_ids(op, dml_spec, table_columns, index_dml_info.rowkey_cnt_));
  }
  return ret;
}

int ObStaticEngineCG::generate_exprs_replace_spk(
    const ObIArray<ObColumnRefRawExpr*>& index_exprs, ObIArray<ObExpr*>& access_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_exprs.count(); i++) {
    CK(OB_NOT_NULL(index_exprs.at(i)));
    if (OB_SUCC(ret)) {
      ObExpr* expr = NULL;
      if (is_shadow_column(index_exprs.at(i)->get_column_id())) {
        const ObRawExpr* spk_expr = index_exprs.at(i)->get_dependant_expr();
        CK(OB_NOT_NULL(spk_expr));
        OZ(generate_rt_expr(*spk_expr, expr));
      } else {  // not shadow column
        ObRawExpr* col_expr = index_exprs.at(i);
        OZ(generate_rt_expr(*col_expr, expr));
      }
      OZ(access_exprs.push_back(expr));
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_insert_subplan_access_exprs(const ObIArray<ObColumnRefRawExpr*>& index_exprs,
    const ObIArray<ObColumnRefRawExpr*>& base_table_columns, const ObIArray<ObRawExpr*>& conv_columns,
    ObIArray<ObExpr*>& access_exprs)
{
  int ret = OB_SUCCESS;
  ObExpr* expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_exprs.count(); i++) {
    CK(OB_NOT_NULL(index_exprs.at(i)));
    if (OB_SUCC(ret)) {
      if (is_shadow_column(index_exprs.at(i)->get_column_id())) {
        const ObRawExpr* spk_expr = index_exprs.at(i)->get_dependant_expr();
        CK(OB_NOT_NULL(spk_expr));
        OZ(generate_rt_expr(*spk_expr, expr));
        OZ(access_exprs.push_back(expr));
      } else {  // not shadow column
        int64_t conv_idx = 0;
        for (; conv_idx < base_table_columns.count(); conv_idx++) {
          if (index_exprs.at(i) == base_table_columns.at(conv_idx)) {
            break;
          }
        }
        if (conv_idx >= base_table_columns.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to find columns", K(ret), K(index_exprs.at(i)));
        } else {
          ObRawExpr* conv_expr = conv_columns.at(conv_idx);
          if (OB_ISNULL(conv_expr)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid argument", K(ret));
          } else if (OB_FAIL(generate_rt_expr(*conv_expr, expr))) {
            LOG_WARN("fail to push cur op expr", K(ret), K(index_exprs));
          } else if (OB_FAIL(access_exprs.push_back(expr))) {
            LOG_WARN("fail to push expr", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObStaticEngineCG::convert_delete_subplan(
    ObLogDelUpd& op, const IndexDMLInfo& index_dml_info, SeDMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(phy_plan_));
  if (OB_SUCC(ret)) {
    phy_plan_->set_ignore(op.is_ignore());
  }
  OZ(convert_common_dml_subplan(op, PHY_DELETE, dml_subplan.access_exprs_, index_dml_info, dml_subplan));
  OZ(convert_foreign_keys(op, *dml_subplan.subplan_root_));
  if (OB_SUCC(ret) && log_op_def::LOG_DELETE == op.get_type()) {
    if (OB_FAIL(convert_table_dml_param(op, *dml_subplan.subplan_root_))) {
      LOG_WARN("fail to convert table dml param", K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::convert_index_values(uint64_t table_id, ObIArray<ObExpr*>& output_exprs, ObOpSpec*& trs_spec)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    OZ(phy_plan_->alloc_op_spec(PHY_TABLE_ROW_STORE, 0, trs_spec, OB_INVALID_ID));
    OZ(trs_spec->output_.reserve(output_exprs.count()));
    FOREACH_CNT_X(expr, output_exprs, OB_SUCC(ret))
    {
      CK(OB_NOT_NULL(*expr));
      OZ(trs_spec->output_.push_back(*expr));
    }
    if (OB_SUCC(ret)) {
      static_cast<ObTableRowStoreSpec*>(trs_spec)->table_id_ = table_id;
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_delete_spec_common(ObLogDelete& op, ObTableDeleteSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<TableColumns>* tables_columns = op.get_all_table_columns();
  CK(NULL != op.get_plan());
  CK(NULL != op.get_child(0));
  CK(OB_NOT_NULL(tables_columns));
  CK(1 == tables_columns->count());
  CK(tables_columns->at(0).index_dml_infos_.count() > 0);
  if (OB_SUCC(ret)) {
    const IndexDMLInfo& primary_dml_info = tables_columns->at(0).index_dml_infos_.at(0);
    spec.table_id_ = primary_dml_info.loc_table_id_;
    spec.index_tid_ = primary_dml_info.index_tid_;
    spec.need_filter_null_row_ = primary_dml_info.need_filter_null_;
    spec.distinct_algo_ = primary_dml_info.distinct_algo_;
    spec.gi_above_ = op.is_gi_above();
    if (OB_FAIL(add_table_column_ids(op, &spec, primary_dml_info.column_exprs_, primary_dml_info.rowkey_cnt_))) {
      LOG_WARN("fail to add column id to table update operator", K(ret));
    }
    // no need to copy projector from child
  }

  if (OB_SUCC(ret) && PHY_DELETE == spec.type_) {
    OZ(generate_rt_exprs(op.get_child(0)->get_output_exprs(), spec.storage_row_output_));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_foreign_keys(op, spec))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  OZ(convert_table_dml_param(op, spec));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert& op, ObMultiTableReplaceSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_spec(op, static_cast<ObTableReplaceSpec&>(spec), in_root_job))) {
    LOG_WARN("fail to generate spec", K(ret));
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert& op, ObTableReplaceSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);

  const ObIArray<TableColumns>* all_table_cols = op.get_all_table_columns();
  CK(OB_NOT_NULL(all_table_cols));
  CK(!all_table_cols->empty());
  CK(!all_table_cols->at(0).index_dml_infos_.empty());
  CK(1 == spec.get_child_cnt());
  if (OB_SUCC(ret)) {
    CK(NULL != op.get_primary_key_ids());
    OZ(spec.set_primary_key_ids(*op.get_primary_key_ids()));
  }
  if (OB_SUCC(ret)) {
    spec.table_id_ = all_table_cols->at(0).index_dml_infos_.at(0).loc_table_id_;
    spec.index_tid_ = all_table_cols->at(0).index_dml_infos_.at(0).index_tid_;
    spec.only_one_unique_key_ = op.is_only_one_unique_key();
    spec.gi_above_ = op.is_gi_above();
    spec.is_ignore_ = op.is_ignore();
    phy_plan_->set_ignore(op.is_ignore());

    const ObIArray<ObColumnRefRawExpr*>* cols = op.get_table_columns();
    CK(OB_NOT_NULL(cols));
    OZ(add_table_column_ids(op, &spec, *cols));
    OZ(add_column_infos(op, spec, *cols));

    const ObIArray<uint64_t>* primary_cols = op.get_primary_key_ids();
    CK(OB_NOT_NULL(primary_cols));
    OZ(spec.set_primary_key_ids(*primary_cols));

    OZ(spec.table_column_exprs_.init(cols->count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < cols->count(); ++i) {
      ObExpr* expr = NULL;
      ObColumnRefRawExpr* col = cols->at(i);
      CK(OB_NOT_NULL(col));
      OZ(generate_rt_expr(*col, expr));
      CK(OB_NOT_NULL(expr));
      OZ(spec.table_column_exprs_.push_back(expr));
    }
  }

  if (OB_SUCC(ret) && op.is_multi_part_dml()) {
    ObMultiTableReplaceSpec& multi_table_replace = static_cast<ObMultiTableReplaceSpec&>(spec);
    if (OB_FAIL(convert_multi_table_replace_info(op, multi_table_replace))) {
      LOG_WARN("convert replace global index info failed", K(ret));
    } else if (OB_FAIL(convert_duplicate_key_checker(
                   op.get_dupkey_checker(), multi_table_replace.duplicate_key_checker_))) {
      LOG_WARN("convert duplicate key checker failed", K(ret));
    } else {
      multi_table_replace.duplicate_key_checker_.set_table_columns(&spec.table_column_exprs_);
      multi_table_replace.duplicate_key_checker_.set_dml_flag(storage::INSERT_RETURN_ALL_DUP);
    }
  }

  OZ(convert_table_dml_param(op, spec));

  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_foreign_keys(op, spec))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  // table columns exprs in dml need to set IS_COLUMNLIZED flag
  OZ(mark_expr_self_produced(*(op.get_table_columns())));
  return ret;
}

int ObStaticEngineCG::convert_duplicate_key_scan_info(ObLogicalOperator* log_scan_op,
    const ObIArray<ObRawExpr*>& table_column_exprs, ObRawExpr* calc_part_expr, ObUniqueIndexScanInfo& scan_info)
{
  int ret = OB_SUCCESS;
  ObLogPlan* log_plan = NULL;
  ObOpSpec* scan_root = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  ObSQLSessionInfo* session_info = NULL;
  bool in_root_job = false;
  bool is_subplan = true;
  bool check_eval_once = false;
  if (OB_ISNULL(log_scan_op) || OB_ISNULL(phy_plan_) || OB_ISNULL(log_plan = log_scan_op->get_plan()) ||
      OB_ISNULL(session_info = log_plan->get_optimizer_context().get_session_info()) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(log_scan_op), K(phy_plan_), K(log_plan), K(session_info), K(schema_guard));
  } else if (OB_UNLIKELY(!log_scan_op->is_duplicated_checker_op())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log operator isn't duplicated checker op", KPC(log_scan_op));
  } else if (OB_FAIL(postorder_generate_op(*log_scan_op, scan_root, in_root_job, is_subplan, check_eval_once))) {
    LOG_WARN("failed to convert scan root", K(ret));
  } else {
    // generate rowkey expr
    ObLogConflictRowFetcher* log_fetcher_op = static_cast<ObLogConflictRowFetcher*>(log_scan_op);
    const ObIArray<ObColumnRefRawExpr*>& conflict_exprs = log_fetcher_op->get_conflict_exprs();
    scan_info.conflict_column_cnt_ = conflict_exprs.count();
    scan_info.index_fetcher_spec_ = scan_root;
    scan_info.table_id_ = log_fetcher_op->get_table_id();
    scan_info.index_tid_ = log_fetcher_op->get_index_tid();
    if (OB_FAIL(schema_guard->get_partition_cnt(scan_info.index_tid_, scan_info.part_cnt_))) {
      LOG_WARN("get partition cnt from schema guard failed", K(ret), K(scan_info));
    }
    scan_info.se_index_conflict_exprs_.set_allocator(&phy_plan_->get_allocator());
    OZ(scan_info.se_index_conflict_exprs_.init(conflict_exprs.count()));
    OZ(generate_exprs_replace_spk(conflict_exprs, scan_info.se_index_conflict_exprs_));
  }
  if (OB_SUCC(ret)) {
    // generate index location
    CK(OB_NOT_NULL(calc_part_expr));
    OZ(generate_rt_expr(*calc_part_expr, scan_info.calc_index_part_id_expr_));
    if (OB_SUCC(ret)) {
      ObSEArray<ObRawExpr*, 8> calc_part_exprs;
      OZ(calc_part_exprs.push_back(calc_part_expr));
      scan_info.calc_exprs_.set_allocator(&phy_plan_->get_allocator());
      OZ(generate_calc_exprs(table_column_exprs, calc_part_exprs, scan_info.calc_exprs_, false));
    }
  }
  return ret;
}

int ObStaticEngineCG::convert_duplicate_key_checker(
    ObLogDupKeyChecker& log_dupkey_checker, ObDuplicatedKeyChecker& phy_dupkey_checker)
{
  int ret = OB_SUCCESS;
  ObUniqueIndexScanInfo& table_scan_info = phy_dupkey_checker.get_table_scan_info();
  ObArrayWrap<ObUniqueIndexScanInfo>& gui_scan_infos = phy_dupkey_checker.get_gui_scan_infos();
  ObUniqueIndexScanInfo& gui_lookup_info = phy_dupkey_checker.get_gui_lookup_info();
  ObLogicalOperator* table_scan_root = log_dupkey_checker.get_table_scan_root();
  ObIArray<ObLogicalOperator*>& gui_scan_roots = log_dupkey_checker.get_gui_scan_roots();
  ObLogicalOperator* gui_lookup_root = log_dupkey_checker.get_gui_lookup_root();
  phy_dupkey_checker.set_unique_index_cnt(log_dupkey_checker.get_unique_index_cnt());
  phy_dupkey_checker.set_physical_plan(phy_plan_);
  phy_dupkey_checker.set_is_static_engine(true);
  CK(OB_NOT_NULL(table_scan_root));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(convert_duplicate_key_scan_info(table_scan_root,
                 table_scan_root->get_output_exprs(),
                 log_dupkey_checker.get_tsc_calc_part_expr(),
                 table_scan_info))) {
    LOG_WARN("convert duplicate key scan info failed", K(ret));
  } else if (!gui_scan_roots.empty()) {
    ObOpSpec* append_op = NULL;
    if (OB_FAIL(convert_duplicate_key_scan_info(gui_lookup_root,
            table_scan_root->get_output_exprs(),
            log_dupkey_checker.get_gui_lookup_calc_part_expr(),
            gui_lookup_info))) {
      LOG_WARN("convert duplicate key scan info failed", K(ret));
    } else if (OB_FAIL(phy_plan_->alloc_op_spec(PHY_APPEND, gui_scan_roots.count(), append_op, OB_INVALID_ID))) {
      LOG_WARN("alloc operator by type failed", K(ret));
    } else if (OB_FAIL(gui_scan_infos.allocate_array(phy_plan_->get_allocator(), gui_scan_roots.count()))) {
      LOG_WARN("allocate gui scan info failed", K(ret), K(gui_scan_infos.count()));
    } else {
      phy_dupkey_checker.set_gui_scan_root(append_op);
    }
    CK(gui_scan_roots.count() == log_dupkey_checker.get_gui_scan_calc_part_exprs().count());
    for (int64_t i = 0; OB_SUCC(ret) && i < gui_scan_roots.count(); ++i) {
      if (OB_FAIL(convert_duplicate_key_scan_info(gui_scan_roots.at(i),
              table_scan_root->get_output_exprs(),
              log_dupkey_checker.get_gui_scan_calc_part_exprs().at(i),
              gui_scan_infos.at(i)))) {
        LOG_WARN("convert duplicate key scan info failed", K(ret));
      } else {
        ObOpSpec* fetcher_spec = gui_scan_infos.at(i).index_fetcher_spec_;
        fetcher_spec->set_parent(append_op);
        OZ(append_op->set_child(static_cast<int32_t>(i), fetcher_spec));
        if (i == 0) {
          OZ(append_op->output_.assign(fetcher_spec->output_));
        } else {
          if (append_op->output_.count() != fetcher_spec->output_.count()) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("append child output is not same", K(ret));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < append_op->output_.count(); j++) {
            if (append_op->output_.at(j) != fetcher_spec->output_.at(j)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("append child output is not same", K(ret));
            }
          }
        }
      }
    }
  }
  // convert unique constraint checker info
  if (OB_SUCC(ret)) {
    const ObIArray<ObUniqueConstraintInfo>* log_constraint_infos = log_dupkey_checker.get_constraint_infos();
    ObArrayWrap<ObPhyUniqueConstraintInfo>& phy_constraint_infos = phy_dupkey_checker.get_unique_constraint_infos();
    CK(OB_NOT_NULL(log_constraint_infos));
    OZ(phy_constraint_infos.allocate_array(phy_plan_->get_allocator(), log_constraint_infos->count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < log_constraint_infos->count(); ++i) {
      const ObIArray<ObColumnRefRawExpr*>& constraint_columns = log_constraint_infos->at(i).constraint_columns_;
      ExprFixedArray& phy_constraint_columns = phy_constraint_infos.at(i).se_constraint_columns_;
      phy_constraint_columns.set_allocator(&phy_plan_->get_allocator());
      OZ(phy_constraint_columns.init(constraint_columns.count()));
      ObSEArray<ObRawExpr*, 8> constraint_raw_exprs;
      for (int64_t j = 0; OB_SUCC(ret) && j < constraint_columns.count(); ++j) {
        ObExpr* expr = NULL;
        ObColumnRefRawExpr* col_expr = constraint_columns.at(j);
        CK(OB_NOT_NULL(col_expr));
        if (OB_SUCC(ret)) {
          if (is_shadow_column(col_expr->get_column_id())) {
            ObRawExpr* spk_expr = col_expr->get_dependant_expr();
            CK(OB_NOT_NULL(spk_expr));
            OZ(generate_rt_expr(*spk_expr, expr));
            OZ(constraint_raw_exprs.push_back(spk_expr));
          } else {
            OZ(generate_rt_expr(*col_expr, expr));
            OZ(constraint_raw_exprs.push_back(col_expr));
          }
        }
        OZ(phy_constraint_columns.push_back(expr));
      }
      phy_constraint_infos.at(i).calc_exprs_.set_allocator(&phy_plan_->get_allocator());
      OZ(generate_calc_exprs(
          table_scan_root->get_output_exprs(), constraint_raw_exprs, phy_constraint_infos.at(i).calc_exprs_, false));
    }  // for constraint_columns end
  }

  return ret;
}

int ObStaticEngineCG::convert_multi_table_replace_info(ObLogInsert& op, ObMultiTableReplaceSpec& phy_op)
{
  int ret = OB_SUCCESS;
  ObOpSpec* append_op = NULL;
  ObSEArray<ObOpSpec*, 8> subplan_roots;
  ObTableDMLInfo table_dml_info;
  const ObIArray<IndexDMLInfo>* index_dml_infos = NULL;
  if (OB_ISNULL(op.get_all_table_columns()) || OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phy_plan_));
  } else if (FALSE_IT(index_dml_infos = &(op.get_all_table_columns()->at(0).index_dml_infos_))) {
    // do nothing
  } else if (OB_FAIL(phy_op.init_table_dml_info_array(1))) {
    LOG_WARN("init table insert info array failed", K(ret));
  }
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), index_dml_infos->count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos->count(); ++i) {
    const IndexDMLInfo& index_dml_info = index_dml_infos->at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    SeDMLSubPlanArray& subplans = phy_dml_info.se_subplans_;
    OZ(phy_dml_info.calc_part_id_exprs_.allocate_array(phy_plan_->get_allocator(), 1));
    OZ(generate_rt_expr(*index_dml_info.calc_part_id_exprs_.at(0), phy_dml_info.calc_part_id_exprs_.at(0)));
    OZ(generate_subplan_calc_exprs(op.get_table_columns(), index_dml_info, phy_dml_info));
    OZ(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiTableReplace::DML_OP_CNT));
    if (OB_SUCC(ret)) {
      int64_t index_col_cnt = index_dml_info.column_exprs_.count();
      subplans.at(ObMultiTableReplaceOp::DELETE_OP).access_exprs_.set_allocator(&phy_plan_->get_allocator());
      OZ(subplans.at(ObMultiTableReplaceOp::DELETE_OP).access_exprs_.init(index_col_cnt));
      subplans.at(ObMultiTableReplaceOp::INSERT_OP).access_exprs_.set_allocator(&phy_plan_->get_allocator());
    }
    OZ(generate_exprs_replace_spk(
        index_dml_info.column_exprs_, subplans.at(ObMultiTableReplaceOp::DELETE_OP).access_exprs_));
    OZ(convert_delete_subplan(op, index_dml_info, subplans.at(ObMultiTableReplaceOp::DELETE_OP)));
    OZ(subplan_roots.push_back(subplans.at(ObMultiTableReplace::DELETE_OP).subplan_root_));

    OZ(subplans.at(ObMultiTableReplaceOp::INSERT_OP)
            .access_exprs_.assign(subplans.at(ObMultiTableReplace::DELETE_OP).access_exprs_));
    OZ(convert_insert_subplan(op, index_dml_info, subplans.at(ObMultiTableReplace::INSERT_OP)));
    OZ(subplan_roots.push_back(subplans.at(ObMultiTableReplace::INSERT_OP).subplan_root_));

    if (OB_SUCC(ret) && 0 == i && op.get_part_hint() != NULL) {
      // if multi partition insert values come from select clause,
      // we can't calculate partitions accurately in
      // the generating plan phase, so delay the partition matching to the execution phase
      phy_dml_info.hint_part_ids_.set_allocator(&phy_plan_->get_allocator());
      if (OB_FAIL(phy_dml_info.hint_part_ids_.assign(op.get_part_hint()->part_ids_))) {
        LOG_WARN("add part hint ids failed", K(ret));
      }
    }
  }
  // table columns exprs in dml need to set IS_COLUMNLIZED flag
  OZ(mark_expr_self_produced(*(op.get_table_columns())));
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(phy_plan_->alloc_op_spec(PHY_APPEND, subplan_roots.count(), append_op, OB_INVALID_ID))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    phy_op.set_subplan_root(append_op);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    subplan_roots.at(i)->set_parent(append_op);
    OZ(append_op->set_child(static_cast<int32_t>(i), subplan_roots.at(i)));
  }
  OZ(phy_op.add_table_dml_info(0, table_dml_info));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogDelete& op, ObTableDeleteReturningSpec& spec, const bool in_root_job)
{
  spec.is_returning_ = true;
  return generate_delete_spec_common(op, spec, in_root_job);
}

int ObStaticEngineCG::generate_spec(ObLogDelete& op, ObTableDeleteSpec& spec, const bool in_root_job)
{
  return generate_delete_spec_common(op, spec, in_root_job);
}

int ObStaticEngineCG::convert_global_index_delete_info(ObLogDelUpd& op, const TableColumns& table_columns,
    common::ObIArray<ObOpSpec*>& subplan_roots, ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), table_columns.index_dml_infos_.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.index_dml_infos_.count(); ++i) {
    const IndexDMLInfo& index_dml_info = table_columns.index_dml_infos_.at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    SeDMLSubPlanArray& subplans = phy_dml_info.se_subplans_;
    if (0 == i) {
      table_dml_info.need_check_filter_null_ = index_dml_info.need_filter_null_;
      table_dml_info.rowkey_cnt_ = index_dml_info.rowkey_cnt_;
      table_dml_info.distinct_algo_ = index_dml_info.distinct_algo_;
    }
    if (OB_FAIL(phy_dml_info.calc_part_id_exprs_.allocate_array(phy_plan_->get_allocator(), 1))) {
      LOG_WARN("allocate table location array failed", K(ret));
    } else if (OB_FAIL(generate_rt_expr(
                   *index_dml_info.calc_part_id_exprs_.at(0), phy_dml_info.calc_part_id_exprs_.at(0)))) {
      LOG_WARN("generate index table location failed", K(ret));
    } else if (OB_FAIL(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiPartDeleteOp::DML_OP_CNT))) {
      LOG_WARN("allocate dml subplan temp array failed", K(ret));
    } else {
      SeDMLSubPlan& dml_subplan = subplans.at(ObMultiPartDeleteOp::DELETE_OP);
      const ObIArray<ObColumnRefRawExpr*>& index_exprs = index_dml_info.column_exprs_;
      if (OB_SUCC(ret)) {
        int64_t index_col_cnt = index_exprs.count();
        dml_subplan.access_exprs_.set_allocator(&phy_plan_->get_allocator());
        if (OB_FAIL(dml_subplan.access_exprs_.init(index_col_cnt))) {
          LOG_WARN("fail to allocate array", K(ret));
        }
      }
      OZ(generate_exprs_replace_spk(index_exprs, dml_subplan.access_exprs_));
      OZ(convert_delete_subplan(op, index_dml_info, dml_subplan));
      OZ(subplan_roots.push_back(dml_subplan.subplan_root_));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogDelete& op, ObMultiPartDeleteSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  spec.is_returning_ = op.is_returning();
  UNUSED(in_root_job);
  ObOpSpec* append_op = NULL;
  ObSEArray<ObOpSpec*, 8> subplan_roots;
  ObTableDMLInfo table_dml_info;
  const ObIArray<TableColumns>* table_columns = op.get_all_table_columns();
  CK(OB_NOT_NULL(table_columns));
  if (OB_FAIL(spec.init_table_dml_info_array(table_columns->count()))) {
    LOG_WARN("init table delete info array failed", K(ret), K(table_columns->count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_all_table_columns()->count(); ++i) {
    table_dml_info.reset();
    const TableColumns& table_columns = op.get_all_table_columns()->at(i);
    OZ(convert_global_index_delete_info(op, table_columns, subplan_roots, table_dml_info));
    OZ(spec.add_table_dml_info(i, table_dml_info));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(phy_plan_->alloc_op_spec(PHY_APPEND, subplan_roots.count(), append_op, OB_INVALID_ID))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    spec.set_subplan_root(append_op);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    subplan_roots.at(i)->set_parent(append_op);
    OZ(append_op->set_child(static_cast<int32_t>(i), subplan_roots.at(i)));
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogUpdate& op, ObMultiPartUpdateSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  spec.is_returning_ = op.is_returning();
  UNUSED(in_root_job);
  ObOpSpec* append_op = NULL;
  ObSEArray<ObOpSpec*, 8> subplan_roots;
  const ObIArray<TableColumns>* all_table_columns = op.get_all_table_columns();
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  const ObTablesAssignments* tas = op.get_tables_assignments();
  CK(OB_NOT_NULL(all_table_columns));
  CK(OB_NOT_NULL(table_columns));
  CK(OB_NOT_NULL(tas));
  CK(all_table_columns->count() == tas->count());
  OZ(add_column_infos(op, spec, *table_columns));
  OZ(spec.init_table_dml_info_array(all_table_columns->count()));
  CK(OB_NOT_NULL(op.get_lock_row_flag_expr()));
  OZ(generate_rt_expr(*op.get_lock_row_flag_expr(), spec.lock_row_flag_expr_));
  // if (OB_SUCC(ret) && op.get_stmt_id_expr() != nullptr) {
  //   int64_t stmt_id_idx = OB_INVALID_INDEX;
  //   if (OB_FAIL(input_row_desc->get_idx(op.get_stmt_id_expr(), stmt_id_idx))) {
  //     LOG_WARN("get index failed", K(ret), KPC(op.get_stmt_id_expr()));
  //   } else if (OB_INVALID_INDEX == stmt_id_idx) {
  //     ret = OB_ERR_UNEXPECTED;
  //     LOG_WARN("stmt_id_idx is invalid", K(ret));
  //   } else {
  //     phy_op->set_stmt_id_idx(stmt_id_idx);
  //   }
  // }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns->count(); ++i) {
    ObTableDMLInfo table_dml_info;
    const TableColumns& table_columns = all_table_columns->at(i);
    const ObTableAssignment& ta = tas->at(i);
    OZ(convert_global_index_update_info(op, table_columns, subplan_roots, table_dml_info));
    auto &dst_assign_cols = spec.table_dml_infos_.at(i).assign_columns_;
    OX(dst_assign_cols.old_row_.set_allocator(&phy_plan_->get_allocator()));
    OX(dst_assign_cols.new_row_.set_allocator(&phy_plan_->get_allocator()));
    OZ(spec.add_table_dml_info(i, table_dml_info));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(phy_plan_->alloc_op_spec(PHY_APPEND, subplan_roots.count(), append_op, OB_INVALID_ID))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    spec.set_subplan_root(append_op);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    subplan_roots.at(i)->set_parent(append_op);
    OZ(append_op->set_child(static_cast<int32_t>(i), subplan_roots.at(i)));
  }

  return ret;
}

int ObStaticEngineCG::convert_global_index_update_info(ObLogUpdate& op, const TableColumns& table_columns,
    common::ObIArray<ObOpSpec*>& subplan_roots, ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard = NULL;
  if (OB_ISNULL(op.get_plan()) || OB_ISNULL(op.get_plan()->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan is null or schema guard is null");
  } else {
    schema_guard = op.get_plan()->get_optimizer_context().get_sql_schema_guard();
  }
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), table_columns.index_dml_infos_.count()));
  table_dml_info.set_enable_row_movement(true);
  for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.index_dml_infos_.count(); ++i) {
    bool is_global_index = (0 != i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    const IndexDMLInfo& index_dml_info = table_columns.index_dml_infos_.at(i);
    const ObTableSchema* table_schema = NULL;
    ObTableUpdateSpec* update_spec = NULL;
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    SeDMLSubPlanArray& subplans = phy_dml_info.se_subplans_;
    if (0 == i) {
      // primary index table
      table_dml_info.need_check_filter_null_ = index_dml_info.need_filter_null_;
      table_dml_info.rowkey_cnt_ = index_dml_info.rowkey_cnt_;
      table_dml_info.distinct_algo_ = index_dml_info.distinct_algo_;
    }

    // oracle mode has 'enable row movement' option, mysql mode always allow it.
    if (share::is_oracle_mode()) {
      if (OB_FAIL(schema_guard->get_table_schema(index_dml_info.index_tid_, table_schema))) {
        LOG_WARN("get table schema failed", K(index_dml_info.index_tid_), K(ret));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exist", K(index_dml_info.index_tid_));
      } else if (table_schema->is_user_table() && !table_schema->is_enable_row_movement()) {
        table_dml_info.set_enable_row_movement(false);
      }
    }

    OZ(phy_dml_info.calc_part_id_exprs_.allocate_array(phy_plan_->get_allocator(), 2));
    OZ(generate_rt_expr(*index_dml_info.calc_part_id_exprs_.at(0), phy_dml_info.calc_part_id_exprs_.at(0)));
    OZ(generate_rt_expr(*index_dml_info.calc_part_id_exprs_.at(1), phy_dml_info.calc_part_id_exprs_.at(1)));
    ObSEArray<ObRawExpr*, 8> dep_exprs;
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < index_dml_info.column_exprs_.count(); col_idx++) {
      OZ(dep_exprs.push_back(index_dml_info.column_exprs_.at(col_idx)));
    }
    for (int64_t am_idx = 0; OB_SUCC(ret) && am_idx < index_dml_info.assignments_.count(); am_idx++) {
      OZ(dep_exprs.push_back(index_dml_info.assignments_.at(am_idx).expr_));
    }
    phy_dml_info.calc_exprs_.set_allocator(&phy_plan_->get_allocator());
    OZ(generate_calc_exprs(dep_exprs, index_dml_info.calc_part_id_exprs_, phy_dml_info.calc_exprs_, false));

    OZ(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiPartUpdateOp::DML_OP_CNT));
    // multi part update may cause 3 operations:
    // update in seme partition: update in the partition.
    // update across partition: delete from the old partition and insert into the new partition.
    // generate update subplan first, then delete and insert subplan can reuse old_row and new_row.
    OZ(convert_update_subplan(op, index_dml_info, subplans.at(ObMultiPartUpdate::UPDATE_OP)));
    if (OB_SUCC(ret)) {
      update_spec = static_cast<ObTableUpdateSpec*>(
          subplans.at(ObMultiPartUpdate::UPDATE_OP).subplan_root_);
    }
    CK(OB_NOT_NULL(update_spec));

    if (OB_SUCC(ret)) {
      subplans.at(ObMultiPartUpdateOp::DELETE_OP).access_exprs_.set_allocator(&phy_plan_->get_allocator());
      subplans.at(ObMultiPartUpdateOp::INSERT_OP).access_exprs_.set_allocator(&phy_plan_->get_allocator());
    }
    OZ(subplans.at(ObMultiPartUpdateOp::DELETE_OP).access_exprs_.assign(update_spec->old_row_));
    OZ(convert_delete_subplan(op, index_dml_info, subplans.at(ObMultiPartUpdateOp::DELETE_OP)));

    OZ(subplans.at(ObMultiPartUpdateOp::INSERT_OP).access_exprs_.assign(update_spec->new_row_));
    OZ(convert_insert_subplan(op, index_dml_info, subplans.at(ObMultiPartUpdateOp::INSERT_OP)));

    OZ(subplan_roots.push_back(subplans.at(ObMultiPartUpdateOp::DELETE_OP).subplan_root_));
    OZ(subplan_roots.push_back(subplans.at(ObMultiPartUpdateOp::INSERT_OP).subplan_root_));
    OZ(subplan_roots.push_back(subplans.at(ObMultiPartUpdateOp::UPDATE_OP).subplan_root_));
  }  // for index_dml_infos end
  OZ(convert_update_assignments(table_columns.index_dml_infos_.at(0).column_exprs_,
      table_columns.index_dml_infos_.at(0).assignments_,
      table_dml_info.assign_columns_));
  return ret;
}

int ObStaticEngineCG::convert_update_subplan(
    ObLogDelUpd& op, const IndexDMLInfo& index_dml_info, SeDMLSubPlan& dml_subplan)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    int64_t access_cnt =
        index_dml_info.column_exprs_.count() + index_dml_info.assignments_.count() + 1 /*lock_row_flag_expr*/;
    dml_subplan.access_exprs_.set_allocator(&phy_plan_->get_allocator());
    if (OB_FAIL(dml_subplan.access_exprs_.init(access_cnt))) {
      LOG_WARN("fail to allocate array", K(ret));
    }
  }
  OZ(generate_exprs_replace_spk(index_dml_info.column_exprs_, dml_subplan.access_exprs_));
  ObExpr* expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info.assignments_.count(); ++i) {
    CK(OB_NOT_NULL(index_dml_info.assignments_.at(i).expr_));
    OZ(generate_rt_expr(*index_dml_info.assignments_.at(i).expr_, expr));
    OZ(dml_subplan.access_exprs_.push_back(expr));
  }
  CK(OB_NOT_NULL(op.get_lock_row_flag_expr()));
  OZ(generate_rt_expr(*op.get_lock_row_flag_expr(), expr));
  OZ(dml_subplan.access_exprs_.push_back(expr));

  OZ(convert_common_dml_subplan(op, PHY_UPDATE, dml_subplan.access_exprs_, index_dml_info, dml_subplan));
  CK(OB_NOT_NULL(phy_plan_));
  CK(OB_NOT_NULL(dml_subplan.subplan_root_));
  OZ(convert_update_assignments(index_dml_info.column_exprs_,
      index_dml_info.assignments_,
      *static_cast<ObTableUpdateSpec*>(dml_subplan.subplan_root_)));
  if (OB_SUCC(ret)) {
    phy_plan_->set_ignore(op.is_ignore());
    dml_subplan.subplan_root_->lock_row_flag_expr_ = expr;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_foreign_keys(op, *static_cast<ObTableUpdateSpec*>(dml_subplan.subplan_root_)))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  OZ(convert_check_constraint(op, *dml_subplan.subplan_root_));
  OZ(convert_table_dml_param(op, *dml_subplan.subplan_root_));

  return ret;
}

template <typename T>
int ObStaticEngineCG::convert_update_assignments(
    const ObIArray<ObColumnRefRawExpr*>& all_columns, const ObAssignments& assigns, T& spec)
{
  int ret = OB_SUCCESS;

  CK(all_columns.count() > 0);
  CK(assigns.count() > 0);
  OZ(spec.init_updated_column_count(phy_plan_->get_allocator(), assigns.count()));
  OZ(spec.old_row_.init(all_columns.count()));
  OZ(spec.new_row_.init(all_columns.count()));

  // <1> init new_row_ same with old_row_
  OZ(generate_exprs_replace_spk(all_columns, spec.old_row_));
  OZ(spec.new_row_.assign(spec.old_row_));

  // <2> replace new_row_ expr with the assignments
  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
    ObColumnRefRawExpr* col = assigns.at(i).column_expr_;
    ObRawExpr* value = assigns.at(i).expr_;
    CK(NULL != col);
    CK(NULL != value);
    int64_t idx = OB_INVALID_INDEX;
    if (OB_SUCC(ret)) {
      bool exists = has_exist_in_array(all_columns, col, &idx);
      if (!exists) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update column not found in all columns", K(ret), K(col));
      }
    }
    if (OB_SUCC(ret)) {
      ObExpr* expr = NULL;
      OZ(generate_rt_expr(*value, expr));
      if (OB_SUCC(ret)) {
        spec.new_row_.at(idx) = expr;
      }

      OZ(spec.set_updated_column_info(
          i, assigns.at(i).base_column_id_, idx, col->get_result_type().has_result_flag(OB_MYSQL_ON_UPDATE_NOW_FLAG)));
    }
  }

  return ret;
}

/* UPDATE convention:
 * 1. transfer all columns of a row to storage layer if update primary key column.
 * 2. when storage layer call get_next_row() of udpate, it will get old row first,
 *    then new row, and then old-new-old-new.
 */
int ObStaticEngineCG::generate_update(ObLogUpdate& op, ObTableUpdateSpec& spec)
{
  int ret = OB_SUCCESS;
  const ObIArray<TableColumns>* all_table_columns = op.get_all_table_columns();
  CK(NULL != op.get_plan());
  CK(NULL != all_table_columns);
  CK(1 == op.get_tables_assignments()->count() && 1 == all_table_columns->count());
  CK(1 == spec.get_child_cnt());
  if (OB_SUCC(ret)) {
    const IndexDMLInfo& primary_index_info = all_table_columns->at(0).index_dml_infos_.at(0);
    const ObIArray<ObColumnRefRawExpr*>& table_columns = primary_index_info.column_exprs_;
    const ObTableAssignment& table_assigns = op.get_tables_assignments()->at(0);
    // basic setup
    spec.table_id_ = primary_index_info.loc_table_id_;
    spec.index_tid_ = primary_index_info.index_tid_;
    spec.need_filter_null_row_ = primary_index_info.need_filter_null_;
    spec.distinct_algo_ = primary_index_info.distinct_algo_;
    spec.gi_above_ = op.is_gi_above();
    spec.is_ignore_ = op.is_ignore();
    phy_plan_->set_ignore(op.is_ignore());
    // 1. set columns and exprs required by update interface
    OZ(add_table_column_ids(op, &spec, table_columns, primary_index_info.rowkey_cnt_));

    const ObIArray<ObColumnRefRawExpr*>* columns = op.get_table_columns();
    CK(NULL != columns);
    OZ(add_column_infos(op, spec, *columns));
    OZ(convert_update_assignments(table_columns, table_assigns.assignments_, spec));
  }
  if (OB_SUCC(ret)) {
    OZ(convert_foreign_keys(op, spec));
    OZ(convert_check_constraint(op, spec));
    OZ(convert_table_dml_param(op, spec));
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogUpdate& op, ObTableUpdateSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  CK(typeid(spec) == typeid(ObTableUpdateSpec));
  OZ(generate_update(op, spec));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogUpdate& op, ObTableUpdateReturningSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  spec.is_returning_ = true;
  OZ(generate_update(op, spec));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogForUpdate& op, ObTableLockSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  // infos of the lock table
  TableItem* lock_table = NULL;
  ObSEArray<ObColumnRefRawExpr*, 4> table_columns;
  ObSEArray<ObColumnRefRawExpr*, 4> rowkey_exprs;
  ObSEArray<ObRawExpr*, 4> lock_exprs;
  bool is_nullable = false;
  UNUSED(in_root_job);

  if (OB_ISNULL(stmt = op.get_stmt()) || OB_UNLIKELY(op.get_for_update_tables().count() != 1) ||
      OB_ISNULL(lock_table = stmt->get_table_item_by_id(op.get_for_update_tables().at(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(op.get_for_update_tables()), K(lock_table));
  } else if (OB_FAIL(op.get_table_columns(lock_table->table_id_, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(op.get_rowkey_exprs(lock_table->table_id_, rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  } else if (OB_FAIL(op.is_rowkey_nullable(lock_table->table_id_, is_nullable))) {
    LOG_WARN("failed to check is rowkey nullable", K(ret));
  } else if (OB_FAIL(add_table_column_ids(op, &spec, table_columns, rowkey_exprs.count()))) {
    LOG_WARN("fail to add column id to table update operator", K(ret));
  } else if (OB_FAIL(add_column_infos(op, spec, table_columns))) {
    LOG_WARN("failed to add column infos", K(ret));
  } else if (OB_FAIL(append(lock_exprs, rowkey_exprs))) {
    LOG_WARN("failed to append lock exprs", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(lock_exprs, spec.storage_row_output_))) {
    LOG_WARN("failed to generate rowkey exprs", K(ret));
  } else {
    spec.table_id_ = lock_table->table_id_;
    spec.index_tid_ = lock_table->ref_id_;
    spec.need_filter_null_row_ = is_nullable;
    spec.set_wait_time(op.get_wait_ts());
    spec.gi_above_ = op.is_gi_above();
    phy_plan_->set_for_update(true);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(convert_table_dml_param(op, spec))) {
      LOG_WARN("failed to convert table dml param", K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogForUpdate& op, ObMultiPartLockSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  // infos of the lock table
  // phy operator related
  ObOpSpec* append_spec = NULL;
  ObTableDMLInfo table_dml_info;
  UNUSED(in_root_job);

  if (OB_FAIL(spec.init_table_dml_info_array(op.get_for_update_tables().count()))) {
    LOG_WARN("failed to init table dml info array", K(ret));
  } else if (OB_FAIL(phy_plan_->alloc_op_spec(
                 PHY_APPEND, op.get_for_update_tables().count(), append_spec, OB_INVALID_ID))) {
    LOG_WARN("failed to alloc append spec", K(ret));
  } else {
    spec.set_subplan_root(append_spec);
    phy_plan_->set_for_update(true);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_for_update_tables().count(); ++i) {
    ObOpSpec* subplan_root = NULL;
    table_dml_info.reset();
    if (OB_FAIL(convert_for_update_subplan(op, op.get_for_update_tables().at(i), subplan_root, table_dml_info))) {
      LOG_WARN("failed to convert subplan for update", K(ret));
    } else if (OB_FAIL(spec.add_table_dml_info(i, table_dml_info))) {
      LOG_WARN("failed to add table dml info", K(ret));
    } else if (OB_FAIL(append_spec->set_child(static_cast<int32_t>(i), subplan_root))) {
      LOG_WARN("failed to set child", K(ret));
    } else {
      subplan_root->set_parent(append_spec);
    }
  }
  return ret;
}

int ObStaticEngineCG::convert_for_update_subplan(
    ObLogForUpdate& op, const uint64_t table_id, ObOpSpec*& subplan_root, ObTableDMLInfo& table_dml_info)
{
  int ret = OB_SUCCESS;
  ObRawExpr* part_id_expr = NULL;
  ObSEArray<ObColumnRefRawExpr*, 4> table_columns;
  ObSEArray<ObColumnRefRawExpr*, 4> rowkey_exprs;
  TableItem* table_item = NULL;
  bool is_nullable = false;
  UNUSED(subplan_root);
  if (OB_ISNULL(op.get_plan()) || OB_ISNULL(op.get_stmt()) ||
      OB_ISNULL(table_item = op.get_stmt()->get_table_item_by_id(table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(op.get_plan()), K(op.get_stmt()));
  } else if (OB_FAIL(op.get_rowkey_exprs(table_id, rowkey_exprs))) {
    LOG_WARN("failed to get rowkey exprs", K(ret));
  } else if (OB_FAIL(op.get_table_columns(table_id, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(op.is_rowkey_nullable(table_id, is_nullable))) {
    LOG_WARN("failed to check is rowkey nullable", K(ret));
  } else if (OB_FAIL(op.get_calc_part_expr(table_id, part_id_expr))) {
    LOG_WARN("failed to get calc part expr", K(ret));
  } else if (OB_ISNULL(part_id_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to gen calc part id expr", K(ret));
  } else if (OB_FAIL(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), 1))) {
    LOG_WARN("failed to allocate index array", K(ret));
  } else {
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(0);
    SeDMLSubPlanArray& subplans = phy_dml_info.se_subplans_;
    phy_dml_info.table_id_ = table_id;
    phy_dml_info.index_tid_ = table_item->ref_id_;

    table_dml_info.need_check_filter_null_ = is_nullable;
    table_dml_info.distinct_algo_ = T_DISTINCT_NONE;
    table_dml_info.rowkey_cnt_ = rowkey_exprs.count();

    // 1. create partition id expr
    if (OB_SUCC(ret)) {
      if (OB_FAIL(phy_dml_info.calc_part_id_exprs_.allocate_array(phy_plan_->get_allocator(), 1))) {
        LOG_WARN("failed to allocate calc partition id expr", K(ret));
      } else if (OB_FAIL(generate_rt_expr(*part_id_expr, phy_dml_info.calc_part_id_exprs_.at(0)))) {
        LOG_WARN("failed to generate part id expr", K(ret));
      }
    }
    // 2. create sub lock plan
    if (OB_SUCC(ret)) {
      ObOpSpec* lock_spec = NULL;
      ObOpSpec* values_spec = NULL;
      ObSEArray<ObRawExpr*, 4> lock_exprs;
      if (OB_FAIL(subplans.allocate_array(phy_plan_->get_allocator(), 1))) {
        LOG_WARN("failed to allocate subplan array", K(ret));
      } else {
        subplans.at(0).access_exprs_.set_allocator(&phy_plan_->get_allocator());
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append(lock_exprs, rowkey_exprs))) {
          LOG_WARN("failed to append lock exprs", K(ret));
        } else if (OB_FAIL(generate_rt_exprs(lock_exprs, subplans.at(0).access_exprs_))) {
          LOG_WARN("failed to generate access exprs", K(ret));
        } else if (OB_FAIL(convert_index_values(table_id, subplans.at(0).access_exprs_, values_spec))) {
          LOG_WARN("failed to convert index values", K(ret));
        } else if (OB_FAIL(phy_plan_->alloc_op_spec(PHY_LOCK, 1, lock_spec, OB_INVALID_ID))) {
          LOG_WARN("failed to alloc lock op spec", K(ret));
        } else if (OB_FAIL(lock_spec->set_child(0, values_spec))) {
          LOG_WARN("failed to set lock spec child", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        values_spec->set_parent(lock_spec);
        ObTableLockSpec* dml_spec = static_cast<ObTableLockSpec*>(lock_spec);
        dml_spec->table_id_ = table_id;
        dml_spec->index_tid_ = table_item->ref_id_;
        dml_spec->set_from_multi_table_dml(true);
        dml_spec->set_wait_time(op.get_wait_ts());
        if (OB_FAIL(add_table_column_ids(op, dml_spec, table_columns, rowkey_exprs.count()))) {
          LOG_WARN("failed to add column id to table lock spec", K(ret));
        } else if (OB_FAIL(add_column_infos(op, *dml_spec, table_columns))) {
          LOG_WARN("failed to add column infos", K(ret));
        } else if (OB_FAIL(dml_spec->storage_row_output_.assign(subplans.at(0).access_exprs_))) {
          LOG_WARN("failed to assign rowkey exprs", K(ret));
        } else if (OB_FAIL(convert_table_dml_param(op, *dml_spec))) {
          LOG_WARN("failed to convert table dml param", K(ret));
        } else {
          subplans.at(0).subplan_root_ = dml_spec;
          subplan_root = dml_spec;
        }
      }
    }
  }
  return ret;
}

// same with the build_update_source_row_desc
template <typename OP_SPEC>
int ObStaticEngineCG::build_scan_column_ids(
    ObLogicalOperator& op, const ObIArray<ObColumnRefRawExpr*>& all_columns, OP_SPEC& spec)
{
  int ret = OB_SUCCESS;
  OZ(spec.scan_column_ids_.init(all_columns.count()));
  FOREACH_CNT_X(col, all_columns, OB_SUCC(ret))
  {
    uint64_t base_cid = OB_INVALID_ID;
    OZ(get_column_ref_base_cid(op, *col, base_cid));
    OZ(spec.scan_column_ids_.push_back(base_cid));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert& op, ObTableInsertUpSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  spec.table_id_ = op.get_all_table_columns()->at(0).index_dml_infos_.at(0).loc_table_id_;
  spec.index_tid_ = op.get_all_table_columns()->at(0).index_dml_infos_.at(0).index_tid_;
  spec.gi_above_ = op.is_gi_above();
  spec.is_ignore_ = op.is_ignore();
  spec.plan_->set_ignore(op.is_ignore());
  const ObIArray<ObColumnRefRawExpr*>* table_columns = op.get_table_columns();
  CK(NULL != table_columns);
  OZ(add_table_column_ids(op, &spec, *table_columns));
  OZ(add_column_infos(op, spec, *table_columns));
  // primary key for insert
  const ObIArray<uint64_t>* duplicate_columns = op.get_primary_key_ids();
  CK(NULL != duplicate_columns);
  OZ(spec.primary_key_ids_.assign(*duplicate_columns));

  OZ(generate_rt_exprs(op.get_value_exprs(), spec.insert_row_));
  OZ(build_scan_column_ids(op, *table_columns, spec));

  OZ(spec.update_related_column_ids_.init(table_columns->count()));
  FOREACH_CNT_X(col, *table_columns, OB_SUCC(ret))
  {
    CK(NULL != *col);
    uint64_t base_cid = OB_INVALID_ID;
    OZ(get_column_ref_base_cid(op, *col, base_cid));
    OZ(spec.update_related_column_ids_.push_back(base_cid));
  }

  // assignment column
  const ObTableAssignment& table_assigns = op.get_tables_assignments()->at(0);
  CK(op.get_tables_assignments()->count() == 1);
  OZ(convert_update_assignments(*table_columns, table_assigns.assignments_, spec));

  if (OB_SUCC(ret) && op.is_multi_part_dml()) {
    ObMultiTableInsertUpSpec& multi_table_insert_up = static_cast<ObMultiTableInsertUpSpec&>(spec);
    if (OB_FAIL(convert_multi_table_insert_up_info(op, multi_table_insert_up))) {
      LOG_WARN("convert multi table insert update info failed", K(ret));
    } else if (OB_FAIL(convert_duplicate_key_checker(
                   op.get_dupkey_checker(), multi_table_insert_up.duplicate_key_checker_))) {
      LOG_WARN("convert duplicate key checker failed", K(ret));
    } else {
      multi_table_insert_up.duplicate_key_checker_.set_table_columns(&multi_table_insert_up.table_column_exprs_);
      multi_table_insert_up.duplicate_key_checker_.set_dml_flag(storage::INSERT_RETURN_ONE_DUP);
    }
  }
  if (OB_SUCC(ret) && !op.is_multi_part_dml()) {
    if (OB_FAIL(convert_foreign_keys(op, spec))) {
      LOG_WARN("failed to convert foreign keys", K(ret));
    }
  }
  // table columns exprs in dml need to set IS_COLUMNLIZED flag
  OZ(mark_expr_self_produced(*table_columns));
  return ret;
}

int ObStaticEngineCG::generate_subplan_calc_exprs(const ObIArray<ObColumnRefRawExpr*>* table_columns,
    const IndexDMLInfo& index_dml_info, ObGlobalIndexDMLInfo& phy_dml_info)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(phy_plan_));
  CK(OB_NOT_NULL(table_columns));
  ObSEArray<ObRawExpr*, 8> dep_exprs;
  ObSEArray<ObRawExpr*, 8> cur_exprs;
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < table_columns->count(); col_idx++) {
    OZ(dep_exprs.push_back(table_columns->at(col_idx)));
  }
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < index_dml_info.column_exprs_.count(); col_idx++) {
    ObColumnRefRawExpr* index_column_expr = index_dml_info.column_exprs_.at(col_idx);
    CK(OB_NOT_NULL(index_column_expr));
    if (OB_SUCC(ret) && is_shadow_column(index_column_expr->get_column_id())) {
      OZ(cur_exprs.push_back(index_column_expr->get_dependant_expr()));
    }
  }
  OZ(append(cur_exprs, index_dml_info.calc_part_id_exprs_));
  if (OB_SUCC(ret)) {
    phy_dml_info.calc_exprs_.set_allocator(&phy_plan_->get_allocator());
  }
  OZ(generate_calc_exprs(dep_exprs, cur_exprs, phy_dml_info.calc_exprs_, false));

  return ret;
}

int ObStaticEngineCG::convert_multi_table_insert_up_info(ObLogInsert& op, ObMultiTableInsertUpSpec& phy_op)
{
  int ret = OB_SUCCESS;
  ObOpSpec* append_op = NULL;
  ObSEArray<ObOpSpec*, 8> subplan_roots;
  ObTableDMLInfo table_dml_info;
  const TableColumns* table_columns = NULL;
  const ObIArray<IndexDMLInfo>* index_dml_infos = NULL;
  CK(OB_NOT_NULL(op.get_all_table_columns()));
  CK(op.get_all_table_columns()->count() > 0);
  if (OB_SUCC(ret)) {
    table_columns = &(op.get_all_table_columns()->at(0));
    index_dml_infos = &(table_columns->index_dml_infos_);
    const ObIArray<ObColumnRefRawExpr*>* cols = op.get_table_columns();
    CK(OB_NOT_NULL(cols));
    OZ(phy_op.table_column_exprs_.init(cols->count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < cols->count(); ++i) {
      ObExpr* expr = NULL;
      ObColumnRefRawExpr* col = cols->at(i);
      CK(OB_NOT_NULL(col));
      // table columns exprs in dml need to set IS_COLUMNLIZED flags
      OZ(mark_expr_self_produced(col));
      OZ(generate_rt_expr(*col, expr));
      CK(OB_NOT_NULL(expr));
      OZ(phy_op.table_column_exprs_.push_back(expr));
    }
  }
  CK(OB_NOT_NULL(op.get_lock_row_flag_expr()));
  OZ(generate_rt_expr(*op.get_lock_row_flag_expr(), phy_op.lock_row_flag_expr_));
  OZ(phy_op.init_table_dml_info_array(1));
  OZ(table_dml_info.index_infos_.allocate_array(phy_plan_->get_allocator(), index_dml_infos->count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos->count(); ++i) {
    const IndexDMLInfo& index_dml_info = index_dml_infos->at(i);
    ObGlobalIndexDMLInfo& phy_dml_info = table_dml_info.index_infos_.at(i);
    phy_dml_info.table_id_ = index_dml_info.loc_table_id_;
    phy_dml_info.index_tid_ = index_dml_info.index_tid_;
    phy_dml_info.part_cnt_ = index_dml_info.part_cnt_;
    SeDMLSubPlanArray& subplans = phy_dml_info.se_subplans_;
    OZ(phy_dml_info.calc_part_id_exprs_.allocate_array(phy_plan_->get_allocator(), 2));
    OZ(subplans.allocate_array(phy_plan_->get_allocator(), ObMultiTableInsertUp::DML_OP_CNT));
    OZ(generate_rt_expr(*index_dml_info.calc_part_id_exprs_.at(0), phy_dml_info.calc_part_id_exprs_.at(0)));
    if (OB_SUCC(ret) && 0 == i && op.get_part_hint() != NULL) {
      phy_dml_info.hint_part_ids_.set_allocator(&phy_plan_->get_allocator());
      if (OB_FAIL(phy_dml_info.hint_part_ids_.assign(op.get_part_hint()->part_ids_))) {
        LOG_WARN("add part hint ids failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("index assignment", K(index_dml_info.assignments_), K(i));
      if (index_dml_info.assignments_.empty()) {
        phy_dml_info.calc_part_id_exprs_.at(1) = NULL;
      } else {
        OZ(generate_rt_expr(*index_dml_info.calc_part_id_exprs_.at(1), phy_dml_info.calc_part_id_exprs_.at(1)));
        if (OB_SUCC(ret) && 0 == i && op.get_part_hint() != NULL) {
          phy_dml_info.hint_part_ids_.set_allocator(&phy_plan_->get_allocator());
          if (OB_FAIL(phy_dml_info.hint_part_ids_.assign(op.get_part_hint()->part_ids_))) {
            LOG_WARN("add part hint ids failed", K(ret));
          }
        }
      }
    }
    OZ(generate_subplan_calc_exprs(op.get_table_columns(), index_dml_info, phy_dml_info));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (!index_dml_info.assignments_.empty()) {
      OZ(convert_update_subplan(op, index_dml_info, subplans.at(ObMultiPartUpdate::UPDATE_OP)));
      ObTableUpdateSpec* update_spec =
          static_cast<ObTableUpdateSpec*>(subplans.at(ObMultiPartUpdate::UPDATE_OP).subplan_root_);

      CK(OB_NOT_NULL(update_spec));
      // delete subplan will be produced by update operation of insert_up
      // so if assignments is empty, it indicates no need to generate delete subplan
      if (OB_SUCC(ret)) {
        subplans.at(ObMultiPartUpdateOp::DELETE_OP).access_exprs_.set_allocator(&phy_plan_->get_allocator());
        subplans.at(ObMultiPartUpdateOp::INSERT_OP).access_exprs_.set_allocator(&phy_plan_->get_allocator());
      }

      OZ(subplans.at(ObMultiPartUpdateOp::DELETE_OP).access_exprs_.assign(update_spec->old_row_));
      OZ(convert_delete_subplan(op, index_dml_info, subplans.at(ObMultiTableInsertUpOp::DELETE_OP)));

      OZ(subplans.at(ObMultiPartUpdateOp::INSERT_OP).access_exprs_.assign(update_spec->old_row_));
      OZ(convert_insert_subplan(op, index_dml_info, subplans.at(ObMultiPartUpdateOp::INSERT_OP)));

      OZ(subplan_roots.push_back(subplans.at(ObMultiPartUpdateOp::DELETE_OP).subplan_root_));
      OZ(subplan_roots.push_back(subplans.at(ObMultiPartUpdateOp::INSERT_OP).subplan_root_));
      OZ(subplan_roots.push_back(subplans.at(ObMultiPartUpdateOp::UPDATE_OP).subplan_root_));
    } else {
      SeDMLSubPlan& insert_subplan = subplans.at(ObMultiPartUpdateOp::INSERT_OP);
      int64_t access_cnt = index_dml_info.column_exprs_.count();
      insert_subplan.access_exprs_.set_allocator(&phy_plan_->get_allocator());
      if (OB_FAIL(insert_subplan.access_exprs_.init(access_cnt))) {
        LOG_WARN("fail to allocate array", K(ret));
      }
      OZ(generate_exprs_replace_spk(index_dml_info.column_exprs_, insert_subplan.access_exprs_));
      OZ(convert_insert_subplan(op, index_dml_info, insert_subplan));
      OZ(subplan_roots.push_back(insert_subplan.subplan_root_));
    }
  }  // for index_dml_infos end
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(phy_plan_->alloc_op_spec(PHY_APPEND, subplan_roots.count(), append_op, OB_INVALID_ID))) {
    LOG_WARN("alloc operator by type failed", K(ret));
  } else {
    phy_op.set_subplan_root(append_op);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < subplan_roots.count(); ++i) {
    subplan_roots.at(i)->set_parent(append_op);
    OZ(append_op->set_child(static_cast<int32_t>(i), subplan_roots.at(i)));
  }
  OZ(convert_update_assignments(table_columns->index_dml_infos_.at(0).column_exprs_,
      table_columns->index_dml_infos_.at(0).assignments_,
      table_dml_info.assign_columns_));
  OZ(phy_op.add_table_dml_info(0, table_dml_info));

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert& op, ObMultiTableInsertUpSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_spec(op, static_cast<ObTableInsertUpSpec&>(spec), in_root_job))) {
    LOG_WARN("fail to generate spec", K(ret));
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTopk& op, ObTopKSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);

  CK(typeid(spec) == typeid(ObTopKSpec));

  if (NULL != op.get_topk_limit_count()) {
    CK(op.get_topk_limit_count()->get_result_type().is_integer_type());
    OZ(generate_rt_expr(*op.get_topk_limit_count(), spec.org_limit_));
  }
  if (NULL != op.get_topk_limit_offset()) {
    CK(op.get_topk_limit_offset()->get_result_type().is_integer_type());
    OZ(generate_rt_expr(*op.get_topk_limit_offset(), spec.org_offset_));
  }
  OX(spec.minimum_row_count_ = op.get_minimum_row_count());
  OX(spec.topk_precision_ = op.get_topk_precision());
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSequence& op, ObSequenceSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(spec.nextval_seq_ids_.init(op.get_sequence_ids().count()))) {
    LOG_WARN("failed to init sequence indexes", K(ret));
  } else {
    const ObIArray<uint64_t>& ids = op.get_sequence_ids();
    ARRAY_FOREACH_X(ids, idx, cnt, OB_SUCC(ret))
    {
      if (OB_FAIL(spec.add_uniq_nextval_sequence_id(ids.at(idx)))) {
        LOG_WARN("failed to set sequence", K(ids), K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogMonitoringDump& op, ObMonitoringDumpSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  spec.flags_ = op.get_flags();
  spec.dst_op_id_ = op.get_dst_op_id();
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogGranuleIterator& op, ObGranuleIteratorSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  ObLogicalOperator* child_log_op = op.get_child(0);
  spec.set_tablet_size(op.get_tablet_size());
  spec.set_gi_flags(op.get_gi_flags());
  if (log_op_def::LOG_TABLE_SCAN == child_log_op->get_type()) {
    ObLogTableScan* log_tsc = NULL;
    log_tsc = static_cast<ObLogTableScan*>(child_log_op);
    spec.set_related_id(log_tsc->get_ref_table_id());
  }
  ObPhyPlanType execute_type = spec.plan_->get_plan_type();
  if (execute_type == OB_PHY_PLAN_LOCAL) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported at this time", K(ret));
  } else {
    LOG_DEBUG("executor will fill the input of GI operator");
  }
  LOG_TRACE("convert gi operator",
      K(ret),
      "tablet size",
      op.get_tablet_size(),
      "affinitize",
      op.access_all(),
      "pwj gi",
      op.pwj_gi(),
      "param down",
      op.with_param_down(),
      "asc",
      op.asc_partition_order(),
      "desc",
      op.desc_partition_order(),
      "flags",
      op.get_gi_flags());
  return ret;
}

int ObStaticEngineCG::generate_basic_transmit_spec(ObLogExchange& op, ObPxTransmitSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (!op.is_producer()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: it's not producer", K(ret));
  } else if (op.is_slave_mapping()) {
    ret = STATIC_ENG_NOT_IMPLEMENT;
    LOG_WARN("static engine not implement slave mapping, will retry", K(ret));
  } else {
    if (op.get_is_remote()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: produce is remote", K(ret));
    } else {
      int spliter_type = ObTaskSpliter::INVALID_SPLIT;
      switch (spec.get_child()->get_type()) {
        case PHY_INSERT:
        case PHY_REPLACE:
        case PHY_INSERT_ON_DUP:
          spliter_type = ObTaskSpliter::INSERT_SPLIT;
          break;
        default:
          spliter_type = ObTaskSpliter::DISTRIBUTED_SPLIT;
          break;
      }
      spec.set_split_task_count(op.get_slice_count());
      spec.set_parallel_server_count(1);
      spec.set_server_parallel_thread_count(1);
      // spec.get_job_conf().set_task_split_type(spliter_type);
      spec.repartition_type_ = op.get_repartition_type();
      spec.repartition_table_id_ = op.get_repartition_table_id();
      spec.dist_method_ = op.get_dist_method();
      spec.unmatch_row_dist_method_ = op.get_unmatch_row_dist_method();
      spec.set_px_single(op.is_px_single());
      spec.set_px_dop(op.get_px_dop());
      spec.set_px_id(op.get_px_id());
      spec.set_dfo_id(op.get_dfo_id());
      spec.set_slave_mapping_type(op.get_slave_mapping_type());
      spec.plan_->inc_px_exchange_out_op_count();
      LOG_TRACE("CG transmit", K(op.get_dfo_id()), K(op.get_dist_method()));
    }
  }
  // handle PDML partition_id pseudo column.
  if (OB_SUCC(ret)) {
    if (spec.type_ == PHY_PX_REPART_TRANSMIT) {
      const common::ObIArray<ObRawExpr*>& output_exprs = op.get_output_exprs();
      for (int64_t i = 0, N = output_exprs.count(); OB_SUCC(ret) && i < N; i++) {
        ObRawExpr* expr = output_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret), K(expr));
        } else if (expr->get_expr_type() == T_PDML_PARTITION_ID) {
          spec.set_partition_id_column_idx(i);
          break;
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_basic_receive_spec(ObLogExchange& op, ObPxReceiveSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if ((in_root_job || op.is_rescanable()) && op.is_local_order()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected plan that has merge sort receive with local order in root job", K(ret));
  } else {
    if (OB_FAIL(spec.child_exprs_.init(spec.get_child()->output_.count()))) {
      LOG_WARN("failed to init child exprs", K(ret));
    } else if (OB_FAIL(spec.child_exprs_.assign(spec.get_child()->output_))) {
      LOG_WARN("failed to append child exprs", K(ret));
    } else if (PHY_PX_MERGE_SORT_COORD == spec.get_type() || PHY_PX_FIFO_COORD == spec.get_type()) {
      ObPxCoordSpec* coord = static_cast<ObPxCoordSpec*>(&spec);
      coord->set_expected_worker_count(op.get_expected_worker_count());
      const ObTransmitSpec* transmit_spec = static_cast<const ObTransmitSpec*>(spec.get_child());
      coord->qc_id_ = transmit_spec->get_px_id();
      LOG_TRACE("map worker to px coordinator",
          K(spec.get_type()),
          "id",
          op.get_op_id(),
          "count",
          op.get_expected_worker_count());
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObPxFifoReceiveSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic receive spec", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObPxMSCoordSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 4> output_exprs;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic receive spec", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(op.get_output_exprs(), output_exprs))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else if (OB_FAIL(spec.all_exprs_.init(op.get_sort_keys().count() + output_exprs.count()))) {
    LOG_WARN("failed to init all exprs", K(ret));
  } else if (OB_FAIL(fill_sort_info(op.get_sort_keys(), spec.sort_collations_, spec.all_exprs_))) {
    LOG_WARN("failed to sort info", K(ret));
  } else if (OB_FAIL(fill_sort_funcs(spec.sort_collations_, spec.sort_cmp_funs_, spec.all_exprs_))) {
    LOG_WARN("failed to sort funcs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(spec.all_exprs_, output_exprs))) {
    LOG_WARN("failed to append array no dup", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObDirectTransmitSpec& spec, const bool in_root_job)
{
  // do nothing
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  return OB_SUCCESS;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObDirectReceiveSpec& spec, const bool in_root_job)
{
  // do nothing
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  return OB_SUCCESS;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObPxMSReceiveSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 4> output_exprs;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic receive spec", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(op.get_output_exprs(), output_exprs))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else if (OB_FAIL(spec.all_exprs_.init(op.get_sort_keys().count() + output_exprs.count()))) {
    LOG_WARN("failed to init all exprs", K(ret));
  } else if (OB_FAIL(fill_sort_info(op.get_sort_keys(), spec.sort_collations_, spec.all_exprs_))) {
    LOG_WARN("failed to sort info", K(ret));
  } else if (OB_FAIL(fill_sort_funcs(spec.sort_collations_, spec.sort_cmp_funs_, spec.all_exprs_))) {
    LOG_WARN("failed to sort funcs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(spec.all_exprs_, output_exprs))) {
    LOG_WARN("failed to append array no dup", K(ret));
  } else {
    spec.local_order_ = op.is_local_order();
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObPxDistTransmitSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_transmit_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  } else if (OB_FAIL(generate_hash_func_exprs(op.get_hash_dist_exprs(), spec.dist_exprs_, spec.dist_hash_funcs_))) {
    LOG_WARN("fail generate hash func exprs", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_hash_func_exprs(const common::ObIArray<ObExchangeInfo::HashExpr>& hash_dist_exprs,
    ExprFixedArray& dist_exprs, common::ObHashFuncs& dist_hash_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dist_exprs.init(hash_dist_exprs.count()))) {
    LOG_WARN("failed to init dist exprs", K(ret));
  } else if (OB_FAIL(dist_hash_funcs.init(hash_dist_exprs.count()))) {
    LOG_WARN("failed to init dist exprs", K(ret));
  } else {
    ObExpr* dist_expr = nullptr;
    FOREACH_CNT_X(expr, hash_dist_exprs, OB_SUCC(ret))
    {
      if (OB_ISNULL(expr->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL expr", K(ret));
      } else if (OB_FAIL(mark_expr_self_produced(expr->expr_))) {
        LOG_WARN("failed to add columnized flag", K(ret));
      } else if (OB_FAIL(generate_rt_expr(*expr->expr_, dist_expr))) {
        LOG_WARN("generate expr failed", K(ret));
      } else if (OB_FAIL(dist_exprs.push_back(dist_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        ObHashFunc hash_func;
        hash_func.hash_func_ = dist_expr->basic_funcs_->default_hash_;
        if (OB_ISNULL(hash_func.hash_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("hash func is null, check datatype is valid", K(ret));
        } else if (OB_FAIL(dist_hash_funcs.push_back(hash_func))) {
          LOG_WARN("failed to push back hash function", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObPxRepartTransmitSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_transmit_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  } else if (OB_ISNULL(op.get_calc_part_id_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("repart_transmit's calc_part_id_expr is null", K(ret));
  } else if (OB_FAIL(generate_rt_expr(*op.get_calc_part_id_expr(), spec.calc_part_id_expr_))) {
    LOG_WARN("fail to generate calc part id expr", K(ret), KP(op.get_calc_part_id_expr()));
  }

  if (OB_SUCC(ret) && op.get_hash_dist_exprs().count() > 0) {
    if (OB_FAIL(generate_hash_func_exprs(op.get_hash_dist_exprs(), spec.dist_exprs_, spec.dist_hash_funcs_))) {
      LOG_WARN("fail generate hash func exprs", K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObPxReduceTransmitSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_transmit_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange& op, ObPxFifoCoordSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTempTableAccess& op, ObTempTableAccessOpSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
  if (OB_FAIL(spec.init_output_index(access_exprs.count()))) {
    LOG_WARN("failed to init output index.", K(ret));
  } else if (OB_FAIL(spec.init_access_exprs(access_exprs.count()))) {
    LOG_WARN("failed to init access exprs.", K(ret));
  } else {
    phy_plan_->set_use_temp_table(true);
    bool is_distributed = op.get_sharding_info().get_location_type() == OB_TBL_LOCATION_DISTRIBUTED;
    spec.set_distributed(is_distributed);
    spec.set_temp_table_id(op.get_ref_table_id());
    spec.set_need_release(op.is_last_access());
    ARRAY_FOREACH(access_exprs, i)
    {
      if (OB_ISNULL(access_exprs.at(i)) || !access_exprs.at(i)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected basic column expr", K(ret));
      } else {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(access_exprs.at(i));
        int64_t index = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
        ObExpr* expr = NULL;
        if (OB_FAIL(spec.add_output_index(index))) {
          LOG_WARN("failed to add output index", K(ret), K(index));
        } else if (OB_FAIL(generate_rt_expr(*access_exprs.at(i), expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.add_access_expr(expr))) {
          LOG_WARN("failed to add access expr", K(ret), K(*col_expr));
        } else if (OB_FAIL(mark_expr_self_produced(col_expr))) {  // temp table access need to set IS_COLUMNLIZED flag
          LOG_WARN("mark expr self produced failed", K(ret));
        } else { /*do nothing.*/
        }
      }
    }  // end for
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTempTableInsert& op, ObTempTableInsertOpSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  bool is_distributed = op.get_sharding_info().get_location_type() == OB_TBL_LOCATION_DISTRIBUTED;
  spec.set_distributed(is_distributed);
  spec.set_temp_table_id(op.get_ref_table_id());
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTempTableTransformation& op, ObTempTableTransformationOpSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  UNUSED(spec);
  ObSEArray<ObExpr*, 4> output_exprs;
  if (OB_FAIL(generate_rt_exprs(op.get_output_exprs(), output_exprs))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else { /*do nothing.*/
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan& op, ObTableScanSpec& spec, const bool)
{
  int ret = OB_SUCCESS;

  // generate_spec() interface is override heavy, check type here to avoid generate subclass
  // of ObTableScanSpec unintentionally.
  // e.g.: forget override generate_spec() for subclass of ObTableScanSpec.
  CK(typeid(spec) == typeid(ObTableScanSpec));

  OZ(generate_normal_tsc(op, spec));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan& op, ObFakeCTETableSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_cte_table_spec(op, spec));
  return ret;
}

int ObStaticEngineCG::generate_cte_table_spec(ObLogTableScan& op, ObFakeCTETableSpec& spec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fake_cte_tables_.push_back(spec.get_id()))) {
    LOG_WARN("fake cte table push back failed", K(ret));
  } else {
    const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
    LOG_DEBUG("Table scan's access columns", K(access_exprs.count()));
    OZ(spec.column_involved_offset_.init(access_exprs.count()));
    OZ(spec.column_involved_exprs_.init(access_exprs.count()));
    ARRAY_FOREACH(access_exprs, i)
    {
      ObRawExpr* expr = access_exprs.at(i);
      ObExpr* rt_expr = nullptr;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(expr));
      } else if (expr->has_flag(IS_CONST)) {
      } else if (OB_UNLIKELY(!expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected basic column", K(ret));
      } else if (OB_FAIL(generate_rt_expr(*expr, rt_expr))) {
        LOG_WARN("Fail to generate rt expr", KPC(expr), K(rt_expr));
      } else if (OB_ISNULL(rt_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rt expr is null", K(ret));
      } else if (OB_FAIL(mark_expr_self_produced(
                     expr))) {  // access_exprs in convert_cte_pump need to set IS_COLUMNLIZED flag
        LOG_WARN("mark expr self produced failed", K(ret));
      } else {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        int64_t column_offset = col_expr->get_cte_generate_column_projector_offset();
        if (OB_FAIL(spec.column_involved_offset_.push_back(column_offset))) {
          LOG_WARN("Failed to add column offset", K(ret));
        } else if (OB_FAIL(spec.column_involved_exprs_.push_back(rt_expr))) {
          LOG_WARN("Fail to add column expr", K(ret));
        }
      }
    }  // end for
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogGroupBy& op, ObScalarAggregateSpec& spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op.get_num_of_child() != 1 || OB_ISNULL(op.get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong number of children", K(ret), K(op.get_num_of_child()));
  } else if (OB_FAIL(fill_aggr_infos(op, spec))) {
    OB_LOG(WARN, "fail to fill_aggr_infos", K(ret));
  }
  LOG_DEBUG("finish generate_spec", K(spec), K(ret));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogGroupBy& op, ObMergeGroupBySpec& spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op.get_num_of_child() != 1 || OB_ISNULL(op.get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong number of children", K(ret), K(op.get_num_of_child()));
  } else {
    spec.set_rollup(op.has_rollup());
  }

  // 1. add group columns
  if (OB_SUCC(ret)) {
    common::ObIArray<ObRawExpr*>& group_exprs = op.get_group_by_exprs();
    if (OB_FAIL(spec.init_group_exprs(group_exprs.count()))) {
      OB_LOG(WARN, "fail to init group expr", K(ret));
    }
    ARRAY_FOREACH(group_exprs, i)
    {
      const ObRawExpr* raw_expr = group_exprs.at(i);
      ObExpr* expr = NULL;
      if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_FAIL(spec.add_group_expr(expr))) {
        OB_LOG(WARN, "fail to add_group_expr", K(ret));
      }
    }  // end for
  }

  // 2. add rollup columns
  if (OB_SUCC(ret)) {
    common::ObIArray<ObRawExpr*>& rollup_exprs = op.get_rollup_exprs();
    if (OB_FAIL(spec.init_rollup_exprs(rollup_exprs.count()))) {
      OB_LOG(WARN, "fail to init rollup expr", K(ret));
    } else if (OB_FAIL(spec.init_distinct_rollup_expr(rollup_exprs.count()))) {
      OB_LOG(WARN, "fail to init_distinct_rollup_expr", K(ret));
    }
    bool is_distinct = false;
    ARRAY_FOREACH(rollup_exprs, i)
    {
      const ObRawExpr* raw_expr = rollup_exprs.at(i);
      ObExpr* expr = NULL;
      if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (FALSE_IT(is_distinct = (has_exist_in_array(spec.group_exprs_, expr) ||
                                         has_exist_in_array(spec.rollup_exprs_, expr)))) {
      } else if (OB_FAIL(spec.is_distinct_rollup_expr_.push_back(is_distinct))) {
        OB_LOG(WARN, "fail to push distinct_rollup_expr", K(ret));
      } else if (OB_FAIL(spec.add_rollup_expr(expr))) {
        OB_LOG(WARN, "fail to add_rollup_expr", K(ret));
      }
    }  // end for
  }

  // 3. add aggr columns
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_aggr_infos(op, spec, &spec.group_exprs_, &spec.rollup_exprs_))) {
      OB_LOG(WARN, "fail to fill_aggr_infos", K(ret));
    }
  }
  LOG_DEBUG("succ to generate_spec", K(spec), K(ret));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogGroupBy& op, ObHashGroupBySpec& spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op.get_num_of_child() != 1 || OB_ISNULL(op.get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong number of children", K(ret), K(op.get_num_of_child()));
  } else {
    spec.set_est_group_cnt(op.get_distinct_card());
  }

  // 1. add group columns
  if (OB_SUCC(ret)) {
    common::ObIArray<ObRawExpr*>& group_exprs = op.get_group_by_exprs();
    if (OB_FAIL(spec.init_group_exprs(group_exprs.count()))) {
      OB_LOG(WARN, "fail to init group expr", K(ret));
    } else if (OB_FAIL(spec.cmp_funcs_.init(group_exprs.count()))) {
      OB_LOG(WARN, "fail to init group expr", K(ret));
    }
    ARRAY_FOREACH(group_exprs, i)
    {
      const ObRawExpr* raw_expr = group_exprs.at(i);
      ObExpr* expr = NULL;
      if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_FAIL(spec.add_group_expr(expr))) {
        OB_LOG(WARN, "fail to add_group_expr", K(ret));
      } else {
        ObCmpFunc cmp_func;
        // no matter null first or null last.
        cmp_func.cmp_func_ = expr->basic_funcs_->null_last_cmp_;
        CK(NULL != cmp_func.cmp_func_);
        OZ(spec.cmp_funcs_.push_back(cmp_func));
      }
    }  // end for
  }

  // 2. add aggr columns
  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_aggr_infos(op, spec, &spec.group_exprs_))) {
      OB_LOG(WARN, "fail to fill_aggr_infos", K(ret));
    }
  }
  LOG_DEBUG("succ to generate_spec", K(spec), K(ret));
  return ret;
}

int ObStaticEngineCG::convert_table_param(
    ObLogTableScan& op, ObTableScanSpec& spec, const uint64_t table_id, const uint64_t index_id)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  const ObLogPlan* log_plan = op.get_plan();
  ObSqlSchemaGuard* schema_guard =
      NULL == op.get_plan() ? NULL : op.get_plan()->get_optimizer_context().get_sql_schema_guard();
  CK(OB_NOT_NULL(schema_guard));
  if (OB_INVALID_ID == table_id || OB_INVALID_ID == index_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid id", K(table_id), K(index_id), K(ret));
  } else if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(log_plan));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("get table schema failed", K(index_id), K(ret));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(ret), K(table_schema), K(index_schema));
  } else if (0 == spec.output_column_ids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output column ids is empty", K(ret));
  } else if (OB_FAIL(spec.table_param_.convert(
                 *table_schema, *index_schema, spec.output_column_ids_, op.get_index_back()))) {
    LOG_WARN("convert schema failed",
        K(ret),
        K(*table_schema),
        K(*index_schema),
        K(spec.output_column_ids_),
        K(op.get_index_back()));
  } else {
    spec.real_schema_version_ = index_schema->get_schema_version();
    if (UINT64_MAX != spec.vt_table_id_ && is_oracle_mapping_real_virtual_table(spec.vt_table_id_) &&
        0 < op.get_range_columns().count()) {
      const ObTableSchema* vt_table_schema = NULL;
      VTMapping* vt_mapping = nullptr;
      get_real_table_vt_mapping(op.get_ref_table_id(), vt_mapping);
      if (OB_ISNULL(vt_mapping)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: vt mapping is null", K(ret), K(spec.vt_table_id_));
      } else if (OB_FAIL(spec.key_types_.init(op.get_range_columns().count()))) {
      } else if (OB_FAIL(spec.key_with_tenant_ids_.prepare_allocate(op.get_range_columns().count()))) {
      } else if (OB_FAIL(schema_guard->get_table_schema(spec.vt_table_id_, vt_table_schema))) {
        LOG_WARN("get table schema failed", K(spec.vt_table_id_), K(ret));
      } else {
        // set vt has tenant_id column
        for (int64_t nth_col = 0; nth_col < table_schema->get_column_count() && OB_SUCC(ret); ++nth_col) {
          const ObColumnSchemaV2* col_schema = table_schema->get_column_schema_by_idx(nth_col);
          if (OB_ISNULL(col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema is null", K(ret));
          } else if (0 == col_schema->get_column_name_str().case_compare("TENANT_ID")) {
            spec.has_tenant_id_col_ = true;
            break;
          }
        }
        const ObIArray<ColumnItem>& range_columns = op.get_range_columns();
        for (int64_t k = 0; k < range_columns.count() && OB_SUCC(ret); ++k) {
          uint64_t column_id = UINT64_MAX;
          uint64_t range_column_id = range_columns.at(k).column_id_;
          const ObColumnSchemaV2* vt_col_schema = vt_table_schema->get_column_schema(range_column_id);
          if (OB_ISNULL(vt_col_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: column schema is null", K(range_column_id), K(ret));
          } else if (spec.has_tenant_id_col_ && 0 == k &&
                     0 != vt_col_schema->get_column_name_str().case_compare("TENANT_ID")) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: the first key must be tenant id", K(range_column_id), K(ret));
          }
          for (int64_t nth_col = 0; nth_col < table_schema->get_column_count() && OB_SUCC(ret); ++nth_col) {
            const ObColumnSchemaV2* col_schema = table_schema->get_column_schema_by_idx(nth_col);
            if (OB_ISNULL(col_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema is null", K(ret), K(column_id));
            } else if (0 == col_schema->get_column_name_str().case_compare(vt_col_schema->get_column_name_str())) {
              column_id = col_schema->get_column_id();
              ObObjMeta obj_meta;
              obj_meta.set_type(col_schema->get_data_type());
              obj_meta.set_collation_type(col_schema->get_collation_type());
              OZ(spec.key_types_.push_back(obj_meta));
              bool& extra_tenant_id = spec.key_with_tenant_ids_.at(k);
              extra_tenant_id = false;
              for (int64_t nth_i = vt_mapping->start_pos_;
                   !extra_tenant_id && nth_i < vt_mapping->end_pos_ && OB_SUCC(ret);
                   ++nth_i) {
                if (0 == ObString(with_tenant_id_columns[nth_i]).case_compare(col_schema->get_column_name_str())) {
                  extra_tenant_id = true;
                  break;
                }
              }
              break;
            }
          }
          if (UINT64_MAX == column_id) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: column not found", K(ret), K(range_column_id), K(k));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (op.get_range_columns().count() != spec.key_types_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("key is not match",
              K(op.get_range_columns().count()),
              K(spec.key_types_.count()),
              K(ret),
              K(op.get_range_columns()),
              K(spec.key_types_));
        }
      }
    }
  }
  return ret;
}

// copy from ObCodeGeneratorImpl::convert_normal_table_scan
int ObStaticEngineCG::generate_normal_tsc(ObLogTableScan& op, ObTableScanSpec& spec)
{
  ObString tbl_name;
  ObString index_name;
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard* schema_guard =
      NULL == op.get_plan() ? NULL : op.get_plan()->get_optimizer_context().get_sql_schema_guard();
  CK(OB_NOT_NULL(schema_guard));
  if (OB_SUCC(ret) && NULL != op.get_pre_query_range()) {
    OZ(spec.pre_query_range_.deep_copy(*op.get_pre_query_range()));
    op.get_pre_query_range()->is_get(spec.is_get_);
  }

  OZ(ob_write_string(phy_plan_->get_allocator(), op.get_table_name(), tbl_name));
  OZ(ob_write_string(phy_plan_->get_allocator(), op.get_index_name(), index_name));

  OZ(op.is_top_table_scan(spec.is_top_table_scan_));

  OZ(set_optimization_info(op, spec));
  OZ(set_partition_range_info(op, spec));

  if (OB_SUCC(ret)) {
    spec.table_location_key_ = op.get_table_id();
    spec.ref_table_id_ = op.get_ref_table_id();
    spec.is_index_global_ = op.get_is_index_global();
    spec.for_update_ = op.is_for_update();
    spec.for_update_wait_us_ = op.get_for_update_wait_us();
    phy_plan_->set_for_update(op.is_for_update());
    spec.index_id_ = op.get_index_table_id();
    spec.table_row_count_ = op.get_table_row_count();
    spec.output_row_count_ = static_cast<int64_t>(op.get_output_row_count());
    spec.query_range_row_count_ = static_cast<int64_t>(op.get_query_range_row_count());
    spec.index_back_row_count_ = static_cast<int64_t>(op.get_index_back_row_count());
    spec.estimate_method_ = op.get_estimate_method();
    spec.table_name_ = tbl_name;
    spec.index_name_ = index_name;

    spec.gi_above_ = op.is_gi_above();
    if (op.is_table_whole_range_scan()) {
      phy_plan_->set_contain_table_scan(true);
    }

    if (NULL != op.get_limit_expr()) {
      CK(op.get_limit_expr()->get_result_type().is_integer_type());
      OZ(generate_rt_expr(*op.get_limit_expr(), spec.limit_));
    }
    if (OB_SUCC(ret) && NULL != op.get_offset_expr()) {
      CK(op.get_offset_expr()->get_result_type().is_integer_type());
      OZ(generate_rt_expr(*op.get_offset_expr(), spec.offset_));
    }
  }

  if (OB_SUCC(ret)) {
    ObQueryFlag query_flag;
    if (op.is_need_feedback() && (op.get_plan()->get_phy_plan_type() == OB_PHY_PLAN_LOCAL ||
                                     op.get_plan()->get_phy_plan_type() == OB_PHY_PLAN_REMOTE)) {
      ++(phy_plan_->get_access_table_num());
      query_flag.is_need_feedback_ = true;
    }
    ObOrderDirection scan_direction = op.get_scan_direction();
    query_flag.index_back_ = op.get_index_back();
    if (is_descending_direction(scan_direction)) {
      query_flag.scan_order_ = ObQueryFlag::Reverse;
    } else {
      query_flag.scan_order_ = ObQueryFlag::Forward;
    }
    spec.flags_ = query_flag.flag_;
  }

  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
    LOG_DEBUG("table scan's access columns", K(access_exprs.count()));
    OZ(spec.output_column_ids_.init(access_exprs.count()));
    OZ(generate_rt_exprs(access_exprs, spec.storage_output_));
    // access_exprs in convert_table_scan need to set IS_COLUMNLIZED flag
    OZ(mark_expr_self_produced(access_exprs));
    // 1. add basic column
    ObArray<uint64_t> output_column_ids;
    ARRAY_FOREACH(access_exprs, i)
    {
      ObRawExpr* expr = access_exprs.at(i);
      uint64_t column_id = 0;
      if (OB_UNLIKELY(OB_ISNULL(expr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
        column_id = common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
        spec.need_scn_ = true;
        LOG_DEBUG("need row scn");
      } else {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
        if (!col_expr->has_flag(IS_COLUMN) || col_expr->get_table_id() != op.get_table_id()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("Expected basic column",
              K(col_expr),
              K(col_expr->has_flag(IS_COLUMN)),
              K(col_expr->get_table_id()),
              K(op.get_table_id()));
        } else {
          column_id = col_expr->get_column_id();
        }
      }

      OZ(spec.output_column_ids_.push_back(column_id));
      OZ(output_column_ids.push_back(column_id));
    }  // end for
    // 2. check virtual column
    if (OB_SUCC(ret) && (!op.is_index_scan() || op.get_index_back())) {
      FOREACH_CNT_X(e, access_exprs, OB_SUCC(ret))
      {
        if (T_ORA_ROWSCN != (*e)->get_expr_type()) {
          ObColumnRefRawExpr* col = static_cast<ObColumnRefRawExpr*>(*e);
          if (col->is_generated_column()) {
            ObExpr* rt_expr = NULL;
            OZ(generate_rt_expr(**e, rt_expr));
            // for generate column, arg is the dependant expr.
            CK(1 == rt_expr->arg_cnt_);
            CK(NULL != rt_expr->eval_func_);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObRawExpr* expected_part_id = op.get_expected_part_id();
      if (NULL == expected_part_id) {
        // do nothing
      } else if (OB_FAIL(generate_rt_expr(*expected_part_id, spec.expected_part_id_))) {
        LOG_WARN("fail to generate table location expr", K(ret), K(*expected_part_id));
      }
    }

    if (OB_SUCC(ret) && !is_oracle_mapping_real_virtual_table(op.get_ref_table_id())) {
      OZ(convert_table_param(op, spec, op.get_ref_table_id(), op.get_index_table_id()));
    }
    // TODO : mv/sample table scan CG
    // if (OB_SUCC(ret) && op.get_type() == log_op_def::LOG_MV_TABLE_SCAN) {
    //   const ObTableSchema *right_schema = NULL;
    //   const ObIArray<uint64_t> &depend_tids = index_schema->get_depend_table_ids();
    //   if (depend_tids.count() != 1) {
    //     ret = OB_NOT_SUPPORTED;
    //     LOG_WARN("depend_tid count is not 1", K(depend_tids.count()));
    //   } else {
    //     uint64_t depend_tid = depend_tids.at(0);
    //     if (OB_FAIL(schema_guard->get_table_schema(depend_tid, right_schema))) {
    //       LOG_WARN("fail to get depend table id", K(ret), K(depend_tid));
    //     }
    //   }
    //   if (OB_SUCC(ret)) {
    //     ObMVTableScan *mv_table_scan = static_cast<ObMVTableScan *>(phy_op);
    //     if (OB_FAIL(mv_table_scan->get_right_table_param().convert_join_mv_rparam(*index_schema,
    //                                                                               *right_schema,
    //                                                                               output_column_ids))) {
    //       LOG_WARN("fail to do convert_join_mv_rparam", K(ret));
    //     } else {
    //       mv_table_scan->set_right_table_location_key(right_schema->get_table_id());
    //     }
    //   }
    // }

    if (OB_SUCC(ret) && op.is_sample_scan() && op.get_sample_info().method_ == SampleInfo::ROW_SAMPLE) {
      ObRowSampleScanSpec& sample_scan = static_cast<ObRowSampleScanSpec&>(spec);
      sample_scan.set_sample_info(op.get_sample_info());
    }

    if (OB_SUCC(ret) && op.is_sample_scan() && op.get_sample_info().method_ == SampleInfo::BLOCK_SAMPLE) {
      ObBlockSampleScanSpec& sample_scan = static_cast<ObBlockSampleScanSpec&>(spec);
      sample_scan.set_sample_info(op.get_sample_info());
    }
  }

  if (OB_SUCC(ret) && op.exist_hint()) {
    spec.exist_hint_ = true;
    spec.hint_ = op.get_const_hint();
  }

  OZ(schema_guard->get_table_schema_version(spec.ref_table_id_, spec.schema_version_));
  if (OB_SUCC(ret) && 0 != op.get_session_id()) {
    phy_plan_->set_session_id(op.get_session_id());
  }

  // must last process virtual table for replacing some table info
  if (OB_SUCC(ret) && OB_FAIL(generate_real_virtual_table(op, spec))) {}

  return ret;
}

int ObStaticEngineCG::generate_real_virtual_table(ObLogTableScan& op, ObTableScanSpec& spec)
{
  int ret = OB_SUCCESS;
  uint64_t real_table_id = ObSchemaUtils::get_real_table_mappings_tid(op.get_ref_table_id());
  if (OB_INVALID_ID != real_table_id) {
    LOG_DEBUG("trace generate real virtual table", K(op.get_ref_table_id()));
    uint64_t tenant_id = extract_tenant_id(op.get_ref_table_id());
    spec.is_vt_mapping_ = true;
    spec.vt_table_id_ = op.get_ref_table_id();
    spec.ref_table_id_ = real_table_id;
    spec.index_id_ = real_table_id;
    VTMapping* vt_mapping = nullptr;
    get_real_table_vt_mapping(op.get_ref_table_id(), vt_mapping);
    LOG_DEBUG("debug real virtual table", K(tenant_id), K(real_table_id), K(op.get_ref_table_id()));
    if (OB_ISNULL(vt_mapping)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: virtual table can't get vt mapping", K(ret), K(real_table_id));
    } else if (op.get_index_back()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: virtual table can't scan with index back", K(ret), K(real_table_id));
    } else {
      const ObIArray<ObRawExpr*>& access_exprs = op.get_access_exprs();
      OZ(spec.mapping_exprs_.init(access_exprs.count()));
      OZ(spec.has_extra_tenant_ids_.prepare_allocate(access_exprs.count()));
      LOG_DEBUG("debug origin column ids", K(spec.output_column_ids_));
      OZ(spec.org_output_column_ids_.assign(spec.output_column_ids_));
      spec.output_column_ids_.reset();
      OZ(spec.output_column_ids_.init(spec.org_output_column_ids_.count()));
      OZ(spec.output_row_types_.init(spec.org_output_column_ids_.count()));
      spec.use_real_tenant_id_ = vt_mapping->use_real_tenant_id_;
      ARRAY_FOREACH(access_exprs, i)
      {
        ObRawExpr* expr = access_exprs.at(i);
        uint64_t column_id = UINT64_MAX;
        if (OB_UNLIKELY(OB_ISNULL(expr))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret));
        } else if (T_ORA_ROWSCN == expr->get_expr_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("rowscan not supported", K(ret));
        } else {
          ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(expr);
          ObRawExpr* mapping_expr = col_expr->get_real_expr();
          ObExpr* mapping_rt_expr = nullptr;
          column_id = col_expr->get_column_id();
          ObObjMeta obj_meta;
          obj_meta.set_type(col_expr->get_data_type());
          obj_meta.set_collation_type(col_expr->get_collation_type());
          if (OB_ISNULL(mapping_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("mapping expr is null", K(ret), KPC(col_expr));
          } else if (OB_FAIL(mark_expr_self_produced(
                         mapping_expr))) {  // mapping_exprs in real_virtual_table_scan need to set IS_COLUMNLIZED flag
            LOG_WARN("mark expr self produced failed", K(ret));
          } else if (OB_FAIL(generate_rt_expr(*mapping_expr, mapping_rt_expr))) {
            LOG_WARN("failed to generate rt expr", K(ret));
          } else if (OB_FAIL(spec.mapping_exprs_.push_back(mapping_rt_expr))) {
            LOG_WARN("failed to push back mapping exprs", K(ret));
          } else if (OB_FAIL(spec.output_row_types_.push_back(obj_meta))) {
            LOG_WARN("failed to push back row types", K(ret));
          } else {
            bool& extra_tenant_id = spec.has_extra_tenant_ids_.at(i);
            extra_tenant_id = false;
            for (int64_t nth_i = vt_mapping->start_pos_;
                 !extra_tenant_id && nth_i < vt_mapping->end_pos_ && OB_SUCC(ret);
                 ++nth_i) {
              if (0 == ObString(with_tenant_id_columns[nth_i]).case_compare(col_expr->get_column_name())) {
                extra_tenant_id = true;
                break;
              }
            }
            LOG_DEBUG("debug columns with tenant ids", K(ret), K(col_expr->get_column_name()), K(extra_tenant_id));
          }
          if (OB_FAIL(ret)) {
          } else if (UINT64_MAX == column_id) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column ref", K(ret), K(column_id));
          } else {
            OZ(spec.output_column_ids_.push_back((static_cast<ObColumnRefRawExpr*>(mapping_expr))->get_column_id()));
          }
        }
      }  // end for
      // range key maybe not output
      LOG_DEBUG("debug mapping column ids", K(spec.output_column_ids_), K(spec.has_extra_tenant_ids_));
      if (OB_FAIL(ret)) {
      } else {
        OZ(convert_table_param(op, spec, spec.ref_table_id_, spec.index_id_));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_param_spec(ObLogJoin& op,
    const common::ObIArray<std::pair<int64_t, ObRawExpr*>>& param_raw_exprs,
    ObFixedArray<ObDynamicParamSetter, ObIAllocator>& param_setter)
{
  int ret = OB_SUCCESS;
  ObDynamicParamSetter setter;
  auto& all_exec_params = op.get_plan()->get_stmt()->get_exec_param_ref_exprs();
  OZ(param_setter.init(param_raw_exprs.count()));
  for (int64_t k = 0; OB_SUCC(ret) && k < param_raw_exprs.count(); k++) {
    auto& exec_param = param_raw_exprs.at(k);
    setter.param_idx_ = exec_param.first;
    ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(all_exec_params, exec_param.first);
    CK(NULL != param_expr);
    CK(exec_param.first >= 0);
    CK(NULL != exec_param.second);
    OZ(generate_rt_expr(*exec_param.second, *reinterpret_cast<ObExpr**>(&setter.src_)));
    OZ(generate_rt_expr(*param_expr, *const_cast<ObExpr**>(&setter.dst_)));
    OZ(param_setter.push_back(setter));
  }
  return ret;
}

int ObStaticEngineCG::generate_pseudo_column_expr(ObLogJoin& op, ObNLConnectBySpecBase& spec)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_pseudo_columns().count(); ++i) {
    ObPseudoColumnRawExpr* expr = op.get_pseudo_columns().at(i);
    ObItemType expr_type = expr->get_expr_type();
    OZ(mark_expr_self_produced(expr));  // connect by pseudo column exprs need to set IS_COLUMNLIZED flag
    switch (expr_type) {
      case T_LEVEL:
        OZ(generate_rt_expr(*expr, spec.level_expr_));
        LOG_DEBUG("generate level expr");
        break;
      case T_CONNECT_BY_ISLEAF:
        OZ(generate_rt_expr(*expr, spec.is_leaf_expr_));
        LOG_DEBUG("generate isleaf expr");
        break;
      case T_CONNECT_BY_ISCYCLE:
        OZ(generate_rt_expr(*expr, spec.is_cycle_expr_));
        LOG_DEBUG("generate iscycle expr");
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr", KPC(expr), K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::need_prior_exprs(ObIArray<ObExpr*>& self_output, ObIArray<ObExpr*>& left_output, bool& need_prior)
{
  int ret = OB_SUCCESS;
  need_prior = false;
  for (int64_t i = 0; !need_prior && i < left_output.count(); ++i) {
    ObExpr* expr = left_output.at(i);
    if (is_contain(self_output, expr)) {
      need_prior = true;
    }
  }
  return ret;
}

// left exprs + right exprs
// connect by pump:
//       pump_row :[left|right row], it need same count from left and right
//      output_row: [prior exprs + siblings exprs + pseudo exprs]
int ObStaticEngineCG::generate_pump_exprs(ObLogJoin& op, ObNLConnectBySpecBase& spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 8> self_output;
  ObLogicalOperator* left_op = op.get_child(0);
  ObLogicalOperator* right_op = op.get_child(1);
  ObIArray<ObRawExpr*>& left_output = left_op->get_output_exprs();
  ObIArray<ObRawExpr*>& right_output = right_op->get_output_exprs();
  const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(op.get_stmt());
  spec.is_nocycle_ = select_stmt->is_nocycle();
  spec.has_prior_ = op.get_stmt()->has_prior();
  if (OB_ISNULL(left_op) || OB_ISNULL(right_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: child is null", K(ret));
  } else if (OB_FAIL(generate_pseudo_column_expr(op, spec))) {
    LOG_WARN("failed to generate pseudo column", K(ret));
  } else if (OB_FAIL(spec.connect_by_prior_exprs_.init(left_output.count()))) {
    LOG_WARN("failed to init left pump exprs", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(op.get_connect_by_prior_exprs(), spec.connect_by_prior_exprs_))) {
    LOG_WARN("failed to generate prior exprs", K(ret));
  } else if (OB_FAIL(spec.left_prior_exprs_.init(left_output.count()))) {
    LOG_WARN("failed to init left pump exprs", K(ret));
  } else if (OB_FAIL(spec.right_prior_exprs_.init(right_output.count()))) {
    LOG_WARN("failed to init left pump exprs", K(ret));
  } else {
    ObExpr* left_expr = nullptr;
    ObExpr* right_expr = nullptr;
    for (int64_t i = 0; i < left_output.count() && OB_SUCC(ret); ++i) {
      ObRawExpr* left_raw_expr = left_output.at(i);
      ObRawExpr* right_raw_expr = NULL;
      if (OB_ISNULL(left_raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(get_connect_by_copy_expr(*left_raw_expr, right_raw_expr, right_output))) {
        LOG_WARN("failed to get connect by copy expr", K(ret));
      } else if (OB_ISNULL(right_raw_expr)) {
        // do nothing
      } else if (OB_FAIL(generate_rt_expr(*left_raw_expr, left_expr))) {
        LOG_WARN("failed to generate left rt expr", K(ret));
      } else if (OB_FAIL(generate_rt_expr(*right_raw_expr, right_expr))) {
        LOG_WARN("failed to generate right rt expr", K(ret));
      } else if (OB_FAIL(spec.left_prior_exprs_.push_back(left_expr))) {
        LOG_WARN("failed to push back left expr", K(ret));
      } else if (OB_FAIL(spec.right_prior_exprs_.push_back(right_expr))) {
        LOG_WARN("failed to push back left expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("prior info",
          K(op.get_stmt()->has_prior()),
          K(spec.connect_by_prior_exprs_),
          K(op.get_connect_by_prior_exprs()));
      bool need_prior = false;
      int64_t left_prior_cnt = 1 + select_stmt->get_order_item_size();
      if (OB_FAIL(generate_rt_exprs(op.get_output_exprs(), self_output))) {
        LOG_WARN("failed to generate rt exprs", K(ret));
      } else if (OB_FAIL(need_prior_exprs(self_output, spec.left_prior_exprs_, need_prior))) {
        LOG_WARN("failed to calc prior exprs needed", K(ret));
      } else if (FALSE_IT(
                     need_prior = (need_prior || op.get_stmt()->has_prior() || spec.connect_by_prior_exprs_.count()))) {
      } else if (OB_FAIL(spec.cur_row_exprs_.init(
                     need_prior ? spec.left_prior_exprs_.count() + left_prior_cnt : left_prior_cnt))) {
        LOG_WARN("failed to init cur row exprs", K(ret));
      } else if (need_prior && OB_FAIL(append(spec.cur_row_exprs_, spec.left_prior_exprs_))) {
        LOG_WARN("failed to push back prior exprs", K(ret));
      } else if (nullptr != spec.level_expr_ && PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX == spec.type_ &&
                 OB_FAIL(spec.cur_row_exprs_.push_back(spec.level_expr_))) {
        LOG_WARN("failed to push back prior exprs", K(ret));
      } else if (select_stmt->is_order_siblings()) {
        if (OB_FAIL(spec.sort_siblings_exprs_.init(select_stmt->get_order_item_size()))) {
          LOG_WARN("failed to init left pump exprs", K(ret));
        } else if (OB_FAIL(
                       fill_sort_info(select_stmt->get_order_items(), spec.sort_collations_, spec.cur_row_exprs_))) {
          LOG_WARN("failed to fill sort info", K(ret));
        } else if (OB_FAIL(fill_sort_funcs(spec.sort_collations_, spec.sort_cmp_funs_, spec.cur_row_exprs_))) {
          LOG_WARN("failed to sort funcs", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObExpr* expr = nullptr;
      ObExpr* arg_expr = nullptr;
      ObSEArray<ObRawExpr*, 8> connect_by_root_exprs;
      OZ(select_stmt->get_connect_by_root_exprs(connect_by_root_exprs));
      OZ(spec.connect_by_root_exprs_.init(connect_by_root_exprs.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_root_exprs.count(); ++i) {
        ObRawExpr* root_expr = connect_by_root_exprs.at(i);
        if (OB_FAIL(generate_rt_expr(*root_expr, expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.connect_by_root_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObExpr* expr = nullptr;
      ObExpr* arg_expr = nullptr;
      ObSEArray<ObRawExpr*, 8> sys_connect_by_path_exprs;
      OZ(select_stmt->get_sys_connect_by_path_exprs(sys_connect_by_path_exprs));
      OZ(spec.sys_connect_exprs_.init(sys_connect_by_path_exprs.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < sys_connect_by_path_exprs.count(); ++i) {
        ObRawExpr* sys_expr = sys_connect_by_path_exprs.at(i);
        if (OB_FAIL(generate_rt_expr(*sys_expr, expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.sys_connect_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      OZ(spec.cmp_funcs_.init(spec.connect_by_prior_exprs_.count()));
      for (int64_t i = 0; i < spec.connect_by_prior_exprs_.count() && OB_SUCC(ret); ++i) {
        ObExpr* expr = spec.connect_by_prior_exprs_.at(i);
        ObCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
            expr->datum_meta_.type_,
            NULL_LAST,  // NULL_FIRST and NULL_LAST both OK.
            expr->datum_meta_.cs_type_,
            lib::is_oracle_mode());
        if (OB_ISNULL(cmp_func.cmp_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmp_func is null, check datatype is valid", K(ret));
        } else if (OB_FAIL(spec.cmp_funcs_.push_back(cmp_func))) {
          LOG_WARN("failed to push back sort function", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::get_connect_by_copy_expr(
    ObRawExpr& left_expr, ObRawExpr*& right_expr, ObIArray<ObRawExpr*>& right_exprs)
{
  int ret = OB_SUCCESS;
  right_expr = NULL;
  if (OB_LIKELY(left_expr.is_column_ref_expr())) {
    const uint64_t column_id = static_cast<ObColumnRefRawExpr&>(left_expr).get_column_id();
    for (int64_t i = 0; OB_SUCC(ret) && NULL == right_expr && i < right_exprs.count(); ++i) {
      ObRawExpr* cur_expr = right_exprs.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!cur_expr->is_column_ref_expr()) {
        // do nothing
      } else if (column_id == static_cast<ObColumnRefRawExpr*>(cur_expr)->get_column_id()) {
        right_expr = cur_expr;
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogJoin& op, ObNLConnectBySpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<ObRawExpr*>& other_join_conds = op.get_other_join_conditions();
  if (1 < op.get_exec_params().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: the count of level parameter is more then 2",
        K(ret),
        "param count",
        op.get_exec_params().count());
  } else if (OB_FAIL(generate_param_spec(op, op.get_nl_params(), spec.rescan_params_))) {
    LOG_WARN("failed to generate parameter", K(ret));
  } else if (OB_FAIL(generate_pump_exprs(op, spec))) {
    LOG_WARN("failed to generate pump exprs", K(ret));
  } else if (OB_FAIL(spec.cond_exprs_.init(other_join_conds.count()))) {
    LOG_WARN("failed to init join conditions", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(other_join_conds, spec.cond_exprs_))) {
    LOG_WARN("failed to generate condition rt exprs", K(ret));
  } else if (1 == op.get_exec_params().count()) {
    ObRawExpr* level = op.get_exec_params().at(0).second;
    if (OB_ISNULL(level)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("level expr is null", K(ret));
    } else if (OB_FAIL(generate_rt_expr(*level, spec.level_param_))) {
      LOG_WARN("failed to generate level rt expr", K(ret));
    }
  }
  return ret;
}
int ObStaticEngineCG::generate_spec(ObLogJoin& op, ObNLConnectByWithIndexSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<ObRawExpr*>& other_join_conds = op.get_other_join_conditions();
  if (1 < op.get_exec_params().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: the count of level parameter is more then 2",
        K(ret),
        "param count",
        op.get_exec_params().count());
  } else if (OB_FAIL(generate_param_spec(op, op.get_nl_params(), spec.rescan_params_))) {
    LOG_WARN("failed to generate parameter", K(ret));
  } else if (OB_FAIL(generate_pump_exprs(op, spec))) {
    LOG_WARN("failed to generate pump exprs", K(ret));
  } else if (OB_FAIL(spec.cond_exprs_.init(other_join_conds.count()))) {
    LOG_WARN("failed to init join conditions", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(other_join_conds, spec.cond_exprs_))) {
    LOG_WARN("failed to generate condition rt exprs", K(ret));
  } else if (1 == op.get_exec_params().count()) {
    ObRawExpr* level = op.get_exec_params().at(0).second;
    if (OB_ISNULL(level)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("level expr is null", K(ret));
    } else if (OB_FAIL(generate_rt_expr(*level, spec.level_param_))) {
      LOG_WARN("failed to generate level rt expr", K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogJoin& op, ObHashJoinSpec& spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  return generate_join_spec(op, spec);
}

int ObStaticEngineCG::generate_spec(ObLogJoin& op, ObNestedLoopJoinSpec& spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  return generate_join_spec(op, spec);
}

int ObStaticEngineCG::generate_spec(ObLogJoin& op, ObMergeJoinSpec& spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  return generate_join_spec(op, spec);
}

int ObStaticEngineCG::generate_join_spec(ObLogJoin& op, ObJoinSpec& spec)
{
  int ret = OB_SUCCESS;
  bool is_late_mat = (phy_plan_->get_is_late_materialized() || op.is_late_mat());
  phy_plan_->set_is_late_materialized(is_late_mat);
  // print log if possible
  if (OB_NOT_NULL(op.get_stmt()) &&
      (stmt::T_INSERT == op.get_stmt()->get_stmt_type() || stmt::T_UPDATE == op.get_stmt()->get_stmt_type() ||
          stmt::T_DELETE == op.get_stmt()->get_stmt_type()) &&
      true == is_late_mat) {
    LOG_WARN("INSERT, UPDATE or DELETE smt should not be marked as late materialized.",
        K(op.get_stmt()->get_stmt_type()),
        K(is_late_mat),
        K(*op.get_stmt()));
  }
  if (op.is_partition_wise()) {
    phy_plan_->set_is_wise_join(op.is_partition_wise());  // set is_wise_join
  }
  spec.join_type_ = op.get_join_type();
  if (MERGE_JOIN == op.get_join_algo()) {
    // A.1. add equijoin conditions
    ObMergeJoinSpec& mj_spec = static_cast<ObMergeJoinSpec&>(spec);
    const ObIArray<ObRawExpr*>& equal_join_conds = op.get_equal_join_conditions();
    OZ(mj_spec.equal_cond_infos_.init(equal_join_conds.count()));
    ARRAY_FOREACH(equal_join_conds, i)
    {
      ObMergeJoinSpec::EqualConditionInfo equal_cond_info;
      ObRawExpr* raw_expr = equal_join_conds.at(i);
      CK(OB_NOT_NULL(raw_expr));
      CK(T_OP_EQ == raw_expr->get_expr_type() || T_OP_NSEQ == raw_expr->get_expr_type());
      OZ(generate_rt_expr(*raw_expr, equal_cond_info.expr_));
      CK(OB_NOT_NULL(equal_cond_info.expr_));
      CK(equal_cond_info.expr_->arg_cnt_ == 2)
      CK(OB_NOT_NULL(equal_cond_info.expr_->args_));
      CK(OB_NOT_NULL(equal_cond_info.expr_->args_[0]));
      CK(OB_NOT_NULL(equal_cond_info.expr_->args_[1]));
      if (OB_SUCC(ret)) {
        ObDatumMeta& l = equal_cond_info.expr_->args_[0]->datum_meta_;
        ObDatumMeta& r = equal_cond_info.expr_->args_[1]->datum_meta_;
        CK(l.cs_type_ == r.cs_type_);
        if (OB_SUCC(ret)) {
          equal_cond_info.ns_cmp_func_ =
              ObDatumFuncs::get_nullsafe_cmp_func(l.type_, r.type_, default_null_pos(), l.cs_type_, is_oracle_mode());
          CK(OB_NOT_NULL(equal_cond_info.ns_cmp_func_));
          OZ(calc_equal_cond_opposite(op, *raw_expr, equal_cond_info.is_opposite_));
          OZ(mj_spec.equal_cond_infos_.push_back(equal_cond_info));
          LOG_DEBUG("equijoin condition", K(*raw_expr), K(equal_cond_info));
        }
      }
    }  // end for
    // A.2. add merge directions
    if (OB_SUCC(ret)) {
      const ObIArray<ObOrderDirection>& merge_directions = op.get_merge_directions();
      bool left_unique = false;
      if (OB_FAIL(mj_spec.set_merge_directions(merge_directions))) {
        LOG_WARN("fail to set merge directions", K(ret));
      } else if (OB_FAIL(op.is_left_unique(left_unique))) {
        LOG_WARN("fail to check left unique", K(ret), K(op));
      } else {
        mj_spec.is_left_unique_ = left_unique;
        LOG_DEBUG("merge join left unique", K(left_unique));
      }
    }
  } else if (NESTED_LOOP_JOIN == op.get_join_algo()) {  // nested loop join
    if (0 != op.get_equal_join_conditions().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("equal join conditions' count should equal 0", K(ret));
    } else {
      ObBasicNestedLoopJoinSpec& nlj_spec = static_cast<ObBasicNestedLoopJoinSpec&>(spec);
      nlj_spec.enable_gi_partition_pruning_ = op.is_enable_gi_partition_pruning();
      const ObIArray<std::pair<int64_t, ObRawExpr*>>& nl_params = op.get_nl_params();
      if (nlj_spec.enable_gi_partition_pruning_ && OB_FAIL(do_gi_partition_pruning(op, nlj_spec))) {
        LOG_WARN("fail do gi partition pruning", K(ret));
      } else if (OB_FAIL(nlj_spec.init_param_count(nl_params.count()))) {
        LOG_WARN("fail to init param count", K(ret));
      } else {
        ObIArray<ObRawExpr*>& exec_param_exprs = op.get_stmt()->get_exec_param_ref_exprs();
        ARRAY_FOREACH(nl_params, i)
        {
          const std::pair<int64_t, ObRawExpr*>& nl_param = nl_params.at(i);
          ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(exec_param_exprs, nl_param.first);
          ObExpr* org_rt_expr = NULL;
          ObExpr* param_rt_expr = NULL;
          if (OB_ISNULL(param_expr)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("nl param not find in exec param exprs", K(ret));
          } else if (OB_FAIL(generate_rt_expr(*nl_param.second, org_rt_expr)) ||
                     OB_FAIL(generate_rt_expr(*param_expr, param_rt_expr))) {
            LOG_WARN("fail to generate rt expr", K(ret), K(*nl_param.second), K(*param_expr));
          } else if (OB_ISNULL(org_rt_expr) || OB_ISNULL(param_rt_expr)) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("fail to generate rt expr",
                K(ret),
                K(*nl_param.second),
                K(*param_expr),
                KP(org_rt_expr),
                KP(param_rt_expr));
          } else if (OB_FAIL(nlj_spec.add_nlj_param(nl_param.first, org_rt_expr, param_rt_expr))) {
            LOG_WARN("failed to add nlj param", K(ret), K(org_rt_expr), K(param_rt_expr));
          }
        }
        if (OB_SUCC(ret) && PHY_NESTED_LOOP_JOIN == spec.type_) {
          bool use_batch_nlj = false;
          ObNestedLoopJoinSpec& nlj = static_cast<ObNestedLoopJoinSpec&>(spec);
          if (OB_FAIL(op.can_use_batch_nlj(use_batch_nlj))) {
            LOG_WARN("Failed to check use batch nested loop join", K(ret));
          } else if (use_batch_nlj) {
            nlj.use_group_ = use_batch_nlj;
            if (OB_ISNULL(nlj.get_right()) || PHY_TABLE_SCAN != nlj.get_right()->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("bnl join right op need TABLE SCAN", K(ret), K(nlj));
            } else {
              const ObTableScanSpec* right_tsc = static_cast<const ObTableScanSpec*>(nlj.get_right());
              const_cast<ObTableScanSpec*>(right_tsc)->batch_scan_flag_ = true;
            }
          }
        }
      }
    }
  } else if (HASH_JOIN == op.get_join_algo()) {
    ObSEArray<ObExpr*, 4> right_key_exprs;
    ObSEArray<ObHashFunc, 4> right_hash_funcs;
    ObHashJoinSpec& hj_spec = static_cast<ObHashJoinSpec&>(spec);
    if (OB_FAIL(hj_spec.equal_join_conds_.init(op.get_equal_join_conditions().count()))) {
      LOG_WARN("failed to init equal join conditions", K(ret));
    } else if (OB_FAIL(generate_rt_exprs(op.get_equal_join_conditions(), hj_spec.equal_join_conds_))) {
      LOG_WARN("failed to generate rt exprs", K(ret));
    } else if (OB_FAIL(hj_spec.all_join_keys_.init(2 * hj_spec.equal_join_conds_.count()))) {
      LOG_WARN("failed to init join keys", K(ret));
    } else if (OB_FAIL(hj_spec.all_hash_funcs_.init(2 * hj_spec.equal_join_conds_.count()))) {
      LOG_WARN("failed to init join keys", K(ret));
    } else {
      for (int64_t i = 0; i < hj_spec.equal_join_conds_.count() && OB_SUCC(ret); ++i) {
        ObExpr* expr = hj_spec.equal_join_conds_.at(i);
        ObHashFunc left_hash_func;
        ObHashFunc right_hash_func;
        ObExpr* left_expr = nullptr;
        ObExpr* right_expr = nullptr;
        bool is_opposite = false;
        if (2 != expr->arg_cnt_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: join keys must have 2 arguments", K(ret), K(*expr));
        } else if (OB_FAIL(calc_equal_cond_opposite(op, *op.get_equal_join_conditions().at(i), is_opposite))) {
          LOG_WARN("failed to calc equal condition opposite", K(ret));
        } else {
          if (is_opposite) {
            left_expr = expr->args_[1];
            right_expr = expr->args_[0];
          } else {
            left_expr = expr->args_[0];
            right_expr = expr->args_[1];
          }
          if (OB_FAIL(hj_spec.all_join_keys_.push_back(left_expr))) {
            LOG_WARN("failed to push back left expr", K(ret));
          } else if (OB_FAIL(right_key_exprs.push_back(right_expr))) {
            LOG_WARN("failed to push back right expr", K(ret));
          } else {
            left_hash_func.hash_func_ = left_expr->basic_funcs_->murmur_hash_;
            right_hash_func.hash_func_ = right_expr->basic_funcs_->murmur_hash_;
            if (OB_ISNULL(left_hash_func.hash_func_) || OB_ISNULL(right_hash_func.hash_func_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("hash func is null, check datatype is valid", K(ret));
            } else if (OB_FAIL(hj_spec.all_hash_funcs_.push_back(left_hash_func))) {
              LOG_WARN("failed to push back left expr hash func", K(ret));
            } else if (OB_FAIL(right_hash_funcs.push_back(right_hash_func))) {
              LOG_WARN("failed to push back right expr hash func", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append(hj_spec.all_join_keys_, right_key_exprs))) {
          LOG_WARN("failed to append join keys", K(ret));
        } else if (OB_FAIL(append(hj_spec.all_hash_funcs_, right_hash_funcs))) {
          LOG_WARN("failed to append join keys", K(ret));
        }
      }
    }
  }
  const common::ObIArray<std::pair<int64_t, ObRawExpr*>>& exec_params = op.get_exec_params();
  // level pseudo column as a exec param
  /*  if (OB_SUCC(ret) && CONNECT_BY_JOIN == op.get_join_type()) {*/
  // ObNestedLoopConnectBy *nlj_op = static_cast<ObNestedLoopConnectBy*>(phy_op);
  // if (OB_ISNULL(nlj_op)) {
  // ret = OB_ERR_NULL_VALUE;
  // LOG_WARN("nlj_op is null", K(ret));
  //} else if (exec_params.count() == 0) {
  //// Do nothing
  //} else if (exec_params.count() != 1) {
  //// Only one ? expr for all level expr in connent by clause.
  // ret = OB_ERR_UNEXPECTED;
  // LOG_WARN("unexpected exec params count in connect by", K(exec_params.count()), K(ret));
  //} else if (OB_FAIL(nlj_op->init_exec_param_count(exec_params.count()))) {
  // LOG_WARN("fail to init param count", K(ret));
  //} else {
  // ARRAY_FOREACH(exec_params, i) {
  // const std::pair<int64_t, ObRawExpr*> &param_expr = exec_params.at(i);
  // LOG_DEBUG("connect by", K(param_expr.first), K(param_expr.second), K(ret));
  // if (OB_FAIL(nlj_op->add_exec_param(param_expr.first))) {
  // LOG_WARN("failed to add nlj param", K(ret));
  //}
  //}
  //}
  /*}*/
  // 2. add other join conditions
  const ObIArray<ObRawExpr*>& other_join_conds = op.get_other_join_conditions();

  OZ(spec.other_join_conds_.init(other_join_conds.count()));

  ARRAY_FOREACH(other_join_conds, i)
  {
    ObRawExpr* raw_expr = other_join_conds.at(i);
    ObExpr* expr = NULL;
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null pointer", K(ret));
    } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
      LOG_WARN("fail to generate rt expr", K(ret), K(*raw_expr));
    } else if (OB_FAIL(spec.other_join_conds_.push_back(expr))) {
      LOG_WARN("failed to add sql expr", K(ret), K(*expr));
    } else {
      LOG_DEBUG("equijoin condition", K(*raw_expr), K(*expr));
    }
  }  // end for

  return ret;
}

int ObStaticEngineCG::do_gi_partition_pruning(ObLogJoin& op, ObBasicNestedLoopJoinSpec& spec)
{
  int ret = OB_SUCCESS;
  OZ(generate_rt_expr(*op.get_partition_id_expr(), spec.gi_partition_id_expr_));
  return ret;
}

int ObStaticEngineCG::calc_equal_cond_opposite(const ObLogJoin& op, const ObRawExpr& raw_expr, bool& is_opposite)
{
  int ret = OB_SUCCESS;
  is_opposite = false;
  const ObLogicalOperator* left_child = NULL;
  const ObLogicalOperator* right_child = NULL;
  const ObRawExpr* lexpr = NULL;
  const ObRawExpr* rexpr = NULL;
  CK(T_OP_EQ == raw_expr.get_expr_type() || T_OP_NSEQ == raw_expr.get_expr_type());
  CK(OB_NOT_NULL(left_child = op.get_child(0)));
  CK(OB_NOT_NULL(right_child = op.get_child(1)));
  CK(OB_NOT_NULL(lexpr = raw_expr.get_param_expr(0)));
  CK(OB_NOT_NULL(rexpr = raw_expr.get_param_expr(1)));
  if (OB_SUCC(ret)) {
    if (lexpr->get_relation_ids().is_subset(left_child->get_table_set()) &&
        rexpr->get_relation_ids().is_subset(right_child->get_table_set())) {
      is_opposite = false;
    } else if (lexpr->get_relation_ids().is_subset(right_child->get_table_set()) &&
               rexpr->get_relation_ids().is_subset(left_child->get_table_set())) {
      is_opposite = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid equal condition", K(op), K(raw_expr), K(ret));
    }
  }

  return ret;
}

int ObStaticEngineCG::add_table_column_ids(ObLogicalOperator& op, ObTableModifySpec* phy_op,
    const ObIArray<ObColumnRefRawExpr*>& columns_ids, int64_t rowkey_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null pointer", K(ret));
  } else if (OB_FAIL(phy_op->init_column_ids_count(columns_ids.count()))) {
    LOG_WARN("fail to init column ids count", K(ret));
  } else if (rowkey_cnt > 0) {
    if (OB_FAIL(phy_op->init_primary_key_ids(rowkey_cnt))) {
      LOG_WARN("init primary key ids failed", K(ret), K(rowkey_cnt));
    }
  }
  ARRAY_FOREACH(columns_ids, i)
  {
    ObColumnRefRawExpr* item = columns_ids.at(i);
    uint64_t base_cid = OB_INVALID_ID;
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column item", K(i), K(item));
    } else if (OB_FAIL(get_column_ref_base_cid(op, item, base_cid))) {
      LOG_WARN("get base column id failed", K(ret), K(item));
    } else if (OB_FAIL(phy_op->add_column_id(base_cid))) {
      LOG_WARN("fail to add column id", K(ret));
    } else if (i < rowkey_cnt) {
      if (OB_FAIL(phy_op->add_primary_key_id(base_cid))) {
        LOG_WARN("add primary key id failed", K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::convert_check_constraint(ObLogDelUpd& log_op, ObTableModifySpec& spec)
{
  int ret = OB_SUCCESS;
  ObLogPlan* log_plan = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;

  if (OB_ISNULL(log_op.get_check_constraint_exprs())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("check constraint exprs is NULL", K(ret));
  } else if (OB_ISNULL(log_plan = log_op.get_plan()) ||
             OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(log_op), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(spec.index_tid_, table_schema))) {
    LOG_WARN("failed to get table schema", K(spec.index_tid_), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(spec.index_tid_), K(ret));
  } else if (!(table_schema->is_user_table() || table_schema->is_tmp_table())) {
    // do nothing, especially for global index.
    LOG_DEBUG("skip convert constraint",
        "table_id",
        table_schema->get_table_name_str(),
        "table_type",
        table_schema->get_table_type());
  } else {
    OZ(generate_rt_exprs(*log_op.get_check_constraint_exprs(), spec.check_constraint_exprs_));
  }

  return ret;
}

int ObStaticEngineCG::convert_foreign_keys(ObLogDelUpd& log_op, ObTableModifySpec& spec)
{
  int ret = OB_SUCCESS;
  ObLogPlan* log_plan = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObIArray<ObForeignKeyInfo>* fk_infos = NULL;
  const ObIArray<uint64_t>* value_column_ids = NULL;
  const ObIArray<uint64_t>* name_column_ids = NULL;
  uint64_t name_table_id = OB_INVALID_ID;
  const ObTableSchema* table_schema = NULL;

  if (OB_ISNULL(log_plan = log_op.get_plan()) ||
      OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard()) ||
      OB_ISNULL(schema_guard->get_schema_guard())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(log_op), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(spec.get_index_tid(), table_schema))) {
    LOG_WARN("failed to get table schema", K(spec.get_index_tid()), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(spec.get_index_tid()), K(ret));
  } else if (!table_schema->is_user_table()) {
    // do nothing, especially for global index.
    LOG_DEBUG("skip convert foreign key",
        "table_id",
        table_schema->get_table_name_str(),
        "table_type",
        table_schema->get_table_type());
  } else if (OB_ISNULL(fk_infos = &table_schema->get_foreign_key_infos())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("foreign key infos is null", K(ret));
  } else if (OB_FAIL(spec.init_foreign_key_args(table_schema->get_foreign_key_real_count()))) {
    LOG_WARN("failed to init foreign key stmts", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < fk_infos->count(); i++) {
      const ObForeignKeyInfo& fk_info = fk_infos->at(i);
      ObForeignKeyArg fk_arg;
      if (share::is_oracle_mode()) {
        if (!fk_info.enable_flag_ && fk_info.validate_flag_) {
          const ObSimpleDatabaseSchema* database_schema = NULL;
          if (OB_FAIL(schema_guard->get_schema_guard()->get_database_schema(
                  table_schema->get_database_id(), database_schema))) {
            LOG_WARN("get database schema failed", K(ret), K(table_schema->get_database_id()));
          } else if (OB_ISNULL(database_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database_schema is null", K(ret));
          } else {
            ret = OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE;
            LOG_USER_ERROR(OB_ERR_CONSTRAINT_CONSTRAINT_DISABLE_VALIDATE,
                database_schema->get_database_name_str().length(),
                database_schema->get_database_name_str().ptr(),
                fk_info.foreign_key_name_.length(),
                fk_info.foreign_key_name_.ptr());
            LOG_WARN("no insert/delete/update on table with constraint disabled and validated", K(ret));
          }
        } else if (!fk_info.enable_flag_) {
          continue;
        }
      }
      if (fk_info.child_column_ids_.count() != fk_info.parent_column_ids_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child column count and parent column count is not equal",
            K(ret),
            K(fk_info.child_column_ids_),
            K(fk_info.parent_column_ids_));
      }
      if (OB_SUCC(ret) && fk_info.parent_table_id_ == fk_info.child_table_id_) {
        fk_arg.is_self_ref_ = true;
      }
      if (OB_SUCC(ret) && fk_info.table_id_ == fk_info.child_table_id_) {
        name_table_id = fk_info.parent_table_id_;
        name_column_ids = &fk_info.parent_column_ids_;
        value_column_ids = &fk_info.child_column_ids_;
        if (spec.get_type() == PHY_INSERT || spec.get_type() == PHY_UPDATE || spec.get_type() == PHY_MERGE ||
            spec.get_type() == PHY_MULTI_TABLE_MERGE || spec.get_type() == PHY_INSERT_ON_DUP ||
            spec.get_type() == PHY_INSERT_RETURNING || spec.get_type() == PHY_UPDATE_RETURNING ||
            spec.get_type() == PHY_DELETE_RETURNING || spec.get_type() == PHY_REPLACE) {
          fk_arg.ref_action_ = ACTION_CHECK_EXIST;
        } else {
          fk_arg.ref_action_ = ACTION_INVALID;
        }
        if (OB_FAIL(add_fk_arg_to_phy_op(
                fk_arg, name_table_id, *name_column_ids, *value_column_ids, *schema_guard->get_schema_guard(), spec))) {
          LOG_WARN("failed to add fk arg to phy op", K(ret));
        }
      }
      if (OB_SUCC(ret) && fk_info.table_id_ == fk_info.parent_table_id_) {
        name_table_id = fk_info.child_table_id_;
        name_column_ids = &fk_info.child_column_ids_;
        value_column_ids = &fk_info.parent_column_ids_;
        if (spec.get_type() == PHY_UPDATE || spec.get_type() == PHY_UPDATE_RETURNING || spec.get_type() == PHY_MERGE ||
            spec.get_type() == PHY_MULTI_TABLE_MERGE || spec.get_type() == PHY_INSERT_ON_DUP) {
          fk_arg.ref_action_ = fk_info.update_action_;
        } else if (spec.get_type() == PHY_DELETE || spec.get_type() == PHY_DELETE_RETURNING ||
                   spec.get_type() == PHY_REPLACE) {
          fk_arg.ref_action_ = fk_info.delete_action_;
        } else {
          fk_arg.ref_action_ = ACTION_INVALID;
        }
        if (OB_FAIL(add_fk_arg_to_phy_op(
                fk_arg, name_table_id, *name_column_ids, *value_column_ids, *schema_guard->get_schema_guard(), spec))) {
          LOG_WARN("failed to add fk arg to phy op", K(ret));
        }
      }
    }  // for
  }
  return ret;
}

int ObStaticEngineCG::add_fk_arg_to_phy_op(ObForeignKeyArg& fk_arg, uint64_t name_table_id,
    const ObIArray<uint64_t>& name_column_ids, const ObIArray<uint64_t>& value_column_ids,
    ObSchemaGetterGuard& schema_guard, ObTableModifySpec& spec)
{
  int ret = OB_SUCCESS;
  bool need_handle = true;
  const ObDatabaseSchema* database_schema = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObColumnSchemaV2* column_schema = NULL;
  if (OB_FAIL(need_foreign_key_handle(fk_arg, value_column_ids, spec, need_handle))) {
    LOG_WARN("failed to check if need handle foreign key", K(ret));
  } else if (!need_handle) {
    LOG_DEBUG("skip foreign key handle", K(fk_arg));
  } else if (OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(phy_plan_), K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(name_table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(fk_arg), K(name_table_id), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(name_table_id), K(ret));
  } else if (OB_FAIL(schema_guard.get_database_schema(table_schema->get_database_id(), database_schema))) {
    LOG_WARN("failed to get database schema", K(table_schema->get_database_id()), K(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", K(table_schema->get_database_id()), K(ret));
  } else if (OB_FAIL(deep_copy_ob_string(
                 phy_plan_->get_allocator(), database_schema->get_database_name(), fk_arg.database_name_))) {
    LOG_WARN("failed to deep copy ob string", K(fk_arg), K(table_schema->get_table_name()), K(ret));
  } else if (OB_FAIL(
                 deep_copy_ob_string(phy_plan_->get_allocator(), table_schema->get_table_name(), fk_arg.table_name_))) {
    LOG_WARN("failed to deep copy ob string", K(fk_arg), K(table_schema->get_table_name()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && need_handle && i < name_column_ids.count(); i++) {
    ObForeignKeyColumn fk_column;
    if (OB_ISNULL(column_schema = (table_schema->get_column_schema(name_column_ids.at(i))))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is null", K(fk_arg), K(name_column_ids.at(i)), K(ret));
    } else if (OB_FAIL(deep_copy_ob_string(
                   phy_plan_->get_allocator(), column_schema->get_column_name_str(), fk_column.name_))) {
      LOG_WARN("failed to deep copy ob string", K(fk_arg), K(column_schema->get_column_name_str()), K(ret));
    } else if (fk_arg.is_self_ref_ && 0 > (fk_column.name_idx_ = spec.get_column_idx(name_column_ids.at(i)))) {
      /**
       * fk_column.name_idx_ is used only for self ref row, that is to say name table and
       * value table is same table.
       * otherwise name_column_ids.at(i) will indicate columns in name table, not value table,
       * and spec is value table here.
       */
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("foreign key column id is not in colunm ids", K(fk_arg), K(name_column_ids.at(i)), K(ret));
    } else if (0 > (fk_column.idx_ = spec.get_column_idx(value_column_ids.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("foreign key column id is not in colunm ids", K(fk_arg), K(value_column_ids.at(i)), K(ret));
    } else if (OB_FAIL(fk_arg.columns_.push_back(fk_column))) {
      LOG_WARN("failed to push foreign key column", K(fk_arg), K(fk_column), K(ret));
    }
  }
  if (OB_SUCC(ret) && need_handle) {
    if (OB_FAIL(spec.add_foreign_key_arg(fk_arg))) {
      LOG_WARN("failed to add foreign key arg", K(fk_arg), K(ret));
    } else {
      phy_plan_->set_has_nested_sql(true);
    }
  }
  return ret;
}

bool ObStaticEngineCG::enable_pushdown_filter_to_storage(const ObLogTableScan& op)
{
  int ret = OB_SUCCESS;
  bool pd_filter = false;
  ObBasicSessionInfo* session_info = op.get_plan()->get_optimizer_context().get_session_info();
  if (OB_ISNULL(session_info)) {
  } else {
    uint64_t tenant_id = session_info->get_effective_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      pd_filter = tenant_config->_enable_filter_push_down_storage;
    } else {
      LOG_WARN("failed to init tenant config", K(tenant_id));
    }
  }
  return pd_filter;
}

int ObStaticEngineCG::generate_tsc_filter(const ObLogTableScan& op, ObTableScanSpec& spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> filters;
  OZ(filters.assign(op.get_filter_exprs()));
  if (OB_SUCC(ret) && !op.get_is_fake_cte_table()) {
    int64_t filter_cnt_before_index_back = 0;
    const auto& flags = op.get_filter_before_index_flags();
    FOREACH_CNT(it, flags)
    {
      if (*it) {
        filter_cnt_before_index_back += 1;
      }
    }
    if (!is_virtual_table(op.get_ref_table_id()) && filter_cnt_before_index_back > 0) {
      CK(flags.count() == filters.count());
      OZ(spec.filters_before_index_back_.init(filter_cnt_before_index_back));
      ObArray<ObRawExpr*> raw_filter_index_back;
      for (int64_t i = 0; OB_SUCC(ret) && i < flags.count(); i++) {
        ObExpr* expr = NULL;
        if (flags.at(i)) {
          OZ(generate_rt_expr(*filters.at(i), expr));
          OZ(spec.filters_before_index_back_.push_back(expr));
          OZ(raw_filter_index_back.push_back(filters.at(i)));
        }
      }
      for (int64_t i = flags.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
        if (flags.at(i)) {
          OZ(filters.remove(i));
        }
      }
      if (OB_SUCC(ret) && 0 < raw_filter_index_back.count() && enable_pushdown_filter_to_storage(op)) {
        ObPushdownFilterConstructor filter_constructor(&phy_plan_->get_allocator(), *this);
        if (OB_FAIL(filter_constructor.apply(
                raw_filter_index_back, spec.pd_storage_index_back_filters_.get_pushdown_filter()))) {
          LOG_WARN("failed to apply filter constructor", K(ret));
        } else if (OB_NOT_NULL(spec.pd_storage_index_back_filters_.get_pushdown_filter())) {
          spec.set_pushdown_storage_index_back();
        }
      }
    }
    if (OB_SUCC(ret) && !is_virtual_table(op.get_ref_table_id()) && PHY_TABLE_SCAN == spec.type_ && !filters.empty()) {
      // non virtual table table scan push down filter to storage
      OZ(generate_rt_exprs(filters, spec.pushdown_filters_));
      if (OB_SUCC(ret)) {
        if (enable_pushdown_filter_to_storage(op)) {
          ObPushdownFilterConstructor filter_constructor(&phy_plan_->get_allocator(), *this);
          if (OB_FAIL(filter_constructor.apply(filters, spec.pd_storage_filters_.get_pushdown_filter()))) {
            LOG_WARN("failed to apply filter constructor", K(ret));
          } else if (OB_NOT_NULL(spec.pd_storage_filters_.get_pushdown_filter())) {
            spec.set_pushdown_storage();
          }
        }
      }
      if (OB_SUCC(ret)) {
        filters.reuse();
      }
    }
  }
  if (OB_SUCC(ret)) {
    OZ(generate_rt_exprs(filters, spec.filters_));
  }

  return ret;
}

int ObStaticEngineCG::set_optimization_info(ObLogTableScan& op, ObTableScanSpec& spec)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(phy_plan_));
  OZ(spec.set_est_row_count_record(op.get_est_row_count_record()));
  if (OB_SUCC(ret)) {
    spec.table_row_count_ = op.get_table_row_count();
    spec.output_row_count_ = static_cast<int64_t>(op.get_output_row_count());
    spec.phy_query_range_row_count_ = static_cast<int64_t>(op.get_phy_query_range_row_count());
    spec.query_range_row_count_ = static_cast<int64_t>(op.get_query_range_row_count());
    spec.index_back_row_count_ = static_cast<int64_t>(op.get_index_back_row_count());
  }
  if (OB_NOT_NULL(op.get_table_opt_info())) {
    spec.optimization_method_ = op.get_table_opt_info()->optimization_method_;
    spec.available_index_count_ = op.get_table_opt_info()->available_index_id_.count();
    OZ(spec.set_available_index_name(op.get_table_opt_info()->available_index_name_, phy_plan_->get_allocator()));
    OZ(spec.set_unstable_index_name(op.get_table_opt_info()->unstable_index_name_, phy_plan_->get_allocator()));
    OZ(spec.set_pruned_index_name(op.get_table_opt_info()->pruned_index_name_, phy_plan_->get_allocator()));
  }
  return ret;
}

int ObStaticEngineCG::add_column_conv_infos(ObTableModifySpec& phy_op, const ObIArray<ObColumnRefRawExpr*>& columns,
    const ObIArray<ObRawExpr*>& column_convert_exprs)
{
  int ret = OB_SUCCESS;
  ObExprResType res_type;
  if (OB_UNLIKELY(column_convert_exprs.count() != columns.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column convert exprs count don't match", K(column_convert_exprs.count()), K(columns.count()));
  } else if (OB_FAIL(phy_op.init_column_conv_info_count(columns.count()))) {
    LOG_WARN("fail to init column conv infos count", K(ret));
  }
  common::ObIAllocator& allocator = phy_plan_->get_allocator();
  ARRAY_FOREACH(columns, i)
  {
    const ObColumnRefRawExpr* column = columns.at(i);
    const ObRawExpr* column_convert_expr = column_convert_exprs.at(i);
    if (OB_ISNULL(column) || OB_ISNULL(column_convert_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column expr", K(ret), K(i), K(column), K(column_convert_expr));
    } else if (OB_LIKELY(column_convert_expr->get_expr_type() != T_FUN_COLUMN_CONV)) {
      if (OB_FAIL(phy_op.add_column_conv_info(res_type, 0, allocator))) {
        LOG_WARN("failed to add column conv info", K(ret));
      }
    } else {
      ObSysFunRawExpr* expr = static_cast<ObSysFunRawExpr*>(const_cast<ObRawExpr*>(column_convert_expr));
      ObExprOperator* op = NULL;
      if (OB_ISNULL(op = expr->get_op())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get_op", K(ret), KPC(expr));
      } else {
        ObExprColumnConv* column_conv = static_cast<ObExprColumnConv*>(op);
        if (OB_FAIL(phy_op.add_column_conv_info(
                column->get_result_type(), column->get_column_flags(), allocator, &(column_conv->get_str_values())))) {
          LOG_WARN("failed to add column conv info", K(ret), K(column->get_result_type()));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::convert_table_dml_param(ObLogDelUpd& log_op, ObTableModifySpec& phy_op)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = phy_op.get_index_tid();
  const ObLogPlan* log_plan = log_op.get_plan();
  ObSqlSchemaGuard* schema_guard = NULL;
  if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid table id ", K(ret), K(table_id));
  } else if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL log plan", K(ret), KP(log_plan));
  } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard()) ||
             OB_ISNULL(schema_guard->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("NULL schema guard", K(ret));
  } else if (OB_FAIL(fill_table_dml_param(schema_guard->get_schema_guard(), table_id, phy_op))) {
    LOG_ERROR("fail to fill table dml param", K(ret), K(table_id));
  }
  return ret;
}

// copy from ObCodeGeneratorImpl
int ObStaticEngineCG::set_partition_range_info(ObLogTableScan& op, ObTableScanSpec& spec)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = op.get_table_id();
  uint64_t ref_table_id = op.get_location_table_id();
  uint64_t index_id = op.get_index_table_id();
  ObLogPlan* log_plan = op.get_plan();
  ObDMLStmt* stmt = op.get_stmt();
  const ObTablePartitionInfo* tbl_part_info = op.get_table_partition_info();
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  ObRawExpr* part_expr = op.get_part_expr();
  ObRawExpr* subpart_expr = op.get_subpart_expr();
  ObSEArray<ObRawExpr*, 2> part_column_exprs;
  ObSEArray<ObRawExpr*, 2> subpart_column_exprs;
  ObSEArray<uint64_t, 2> rowkey_column_ids;
  if (PHY_MULTI_PART_TABLE_SCAN == spec.type_) {
    // do nothing, global index back scan don't has tbl_part_info.
  } else if (OB_ISNULL(log_plan) || OB_ISNULL(stmt) || OB_ISNULL(tbl_part_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(spec), K(log_plan), K(tbl_part_info), K(stmt), K(ret));
  } else if (OB_INVALID_ID == table_id || OB_INVALID_ID == ref_table_id || OB_INVALID_ID == index_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid table id", K(table_id), K(ref_table_id), K(index_id), K(ret));
  } else if (is_virtual_table(ref_table_id) || is_inner_table(ref_table_id) || is_cte_table(ref_table_id) ||
             is_link_table_id(ref_table_id)) {
    /*do nothing*/
  } else if (!stmt->is_select_stmt() || tbl_part_info->get_table_location().has_generated_column()) {
    /*do nothing*/
  } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ref_table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(index_id, index_schema))) {
    LOG_WARN("get index schema failed", K(index_id), K(ret));
  } else if (OB_ISNULL(table_schema) || OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(table_schema), K(index_schema), K(ret));
  } else if (!table_schema->is_partitioned_table()) {
    /*do nothing*/
  } else if (OB_FAIL(index_schema->get_rowkey_info().get_column_ids(rowkey_column_ids))) {
    LOG_WARN("failed to get index rowkey column ids", K(ret));
  } else if (OB_ISNULL(part_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null part expr", K(ret));
  } else if (OB_FAIL(mark_expr_self_produced(part_expr))) {  // part expr in table scan need to set IS_COLUMNLIZED flag
    LOG_WARN("mark expr self produced failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, part_column_exprs))) {
    LOG_WARN("failed to check pure column part expr", K(ret));
  } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == table_schema->get_part_level() && OB_ISNULL(subpart_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null subpart expr", K(ret));
  } else if (NULL != subpart_expr &&
             OB_FAIL(ObRawExprUtils::extract_column_exprs(subpart_expr, subpart_column_exprs))) {
    LOG_WARN("failed to check pure column part expr", K(ret));
  } else if (NULL != subpart_expr &&
             OB_FAIL(mark_expr_self_produced(
                 subpart_expr))) {  // subpart expr in table scan need to set IS_COLUMNLIZED flag
    LOG_WARN("mark expr self produced failed", K(ret));
  } else {
    bool is_valid = true;
    ObSEArray<int64_t, 4> part_range_pos;
    ObSEArray<int64_t, 4> subpart_range_pos;
    ObSEArray<ObRawExpr*, 4> part_dep_cols;
    ObSEArray<ObRawExpr*, 4> subpart_dep_cols;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < part_column_exprs.count(); i++) {
      ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(part_column_exprs.at(i));
      CK(OB_NOT_NULL(col_expr));
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < rowkey_column_ids.count(); j++) {
        if (col_expr->get_column_id() == rowkey_column_ids.at(j)) {
          is_find = true;
          OZ(part_range_pos.push_back(j));
          OZ(part_dep_cols.push_back(col_expr));
          OZ(mark_expr_self_produced(col_expr));  // part range key expr in table scan need to set IS_COLUMNLIZED flag
        }
      }
      if (!is_find) {
        is_valid = false;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subpart_column_exprs.count(); i++) {
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < rowkey_column_ids.count(); j++) {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(subpart_column_exprs.at(i));
        CK(OB_NOT_NULL(col_expr));
        if (col_expr->get_column_id() == rowkey_column_ids.at(j)) {
          is_find = true;
          OZ(subpart_range_pos.push_back(j));
          OZ(subpart_dep_cols.push_back(col_expr));
          OZ(mark_expr_self_produced(
              col_expr));  // sub part range key expr in table scan need to set IS_COLUMNLIZED flag
        }
      }
      if (!is_find) {
        is_valid = false;
      }
    }
    OZ(generate_rt_exprs(part_dep_cols, spec.part_dep_cols_));
    OZ(generate_rt_exprs(subpart_dep_cols, spec.subpart_dep_cols_));
    if (OB_SUCC(ret) && is_valid) {
      if (NULL != part_expr) {
        OZ(generate_rt_expr(*part_expr, spec.part_expr_));
      }
      if (NULL != subpart_expr) {
        OZ(generate_rt_expr(*subpart_expr, spec.subpart_expr_));
      }
      OZ(spec.part_range_pos_.assign(part_range_pos));
      OZ(spec.subpart_range_pos_.assign(subpart_range_pos));
      spec.part_level_ = table_schema->get_part_level();
      spec.part_type_ = table_schema->get_part_option().get_part_func_type();
      spec.subpart_type_ = table_schema->get_sub_part_option().get_part_func_type();
      LOG_DEBUG(
          "partition range pos", K(table_schema->get_part_level()), K(part_range_pos), K(subpart_range_pos), K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::fill_table_dml_param(
    share::schema::ObSchemaGetterGuard* guard, const uint64_t table_id, ObTableModifySpec& phy_op)
{
  int ret = OB_SUCCESS;
  int64_t t_version = OB_INVALID_VERSION;
  const ObTableSchema* table_schema = NULL;
  const ObTableSchema* index_schema = NULL;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  ObSEArray<const ObTableSchema*, 16> index_schemas;
  const uint64_t tenant_id = is_sys_table(table_id) ? OB_SYS_TENANT_ID : extract_tenant_id(table_id);
  if (OB_ISNULL(guard) || OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(guard), K(table_id));
  } else if (OB_FAIL(guard->get_table_schema(table_id, table_schema))) {
    LOG_WARN("fail to get schema", K(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(guard->get_schema_version(tenant_id, t_version))) {
    LOG_WARN("get tenant schema version fail", K(ret), K(tenant_id));
  } else if (OB_ISNULL(phy_op.get_phy_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null phy plan", K(ret));
  } else if (OB_FAIL(table_schema->get_simple_index_infos_without_delay_deleted_tid(simple_index_infos))) {
    LOG_WARN("get index tid fail", K(ret), K(table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      index_schema = NULL;
      if (OB_FAIL(guard->get_table_schema(simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("index schema is null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (!index_schema->is_valid()) {
        // skip invalid index
      } else if (!index_schema->is_index_table()) {
        // skip none index table
      } else if (index_schema->is_global_index_table()) {
        // skip global index
      } else if (is_final_invalid_index_status(index_schema->get_index_status(), index_schema->is_dropped_schema())) {
        // skip invalid index status
      } else if (OB_FAIL(index_schemas.push_back(index_schema))) {
        LOG_WARN("push index schema fail", K(ret), K(simple_index_infos.at(i).table_id_));
      }
    }

    if (OB_SUCC(ret) &&
        OB_FAIL(phy_op.get_table_param().convert(table_schema, index_schemas, t_version, phy_op.get_column_ids()))) {
      LOG_WARN("fail to convert table param", K(ret), K(phy_op.get_table_param()));
    }
  }
  return ret;
}

int ObStaticEngineCG::add_column_infos(
    ObLogicalOperator& log_op, ObTableModifySpec& phy_op, const ObIArray<ObColumnRefRawExpr*>& columns)
{
  int ret = OB_SUCCESS;
  ObString column_name;
  ObDMLStmt* stmt = log_op.get_stmt();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(phy_op.init_column_infos_count(columns.count()))) {
    LOG_WARN("fail to init column infos count", K(ret));
  }
  phy_op.set_need_skip_log_user_error(true);
  ARRAY_FOREACH(columns, i)
  {
    const ObColumnRefRawExpr* column = columns.at(i);
    if (OB_ISNULL(column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column expr", K(ret), K(i));
    } else {
      const int64_t table_id = column->get_table_id();
      TableItem* table_item = stmt->get_table_item_by_id(table_id);
      // e.g. create table t1 (c1 int default null);
      //      update (select c1 from t1 where c1 > 10) v set c1 = null;
      // t1.c1 is NULLABLE, but v.c1 is NOT NULL because 'c1 > 10', so we have to get the original
      // column to decide whether the filed can be NULL.
      if (OB_ISNULL(table_item)) {
        // column expr is shadow pk, use column directly.
      } else if (table_item->is_generated_table() && OB_FAIL(recursive_get_column_expr(column, *table_item))) {
        LOG_WARN("failed to recursive get column expr", K(ret));
      }

      if (OB_SUCC(ret)) {
        ColumnContent column_content;
        column_content.auto_filled_timestamp_ = column->get_result_type().has_result_flag(OB_MYSQL_ON_UPDATE_NOW_FLAG);
        column_content.is_nullable_ = !column->get_result_type().has_result_flag(OB_MYSQL_NOT_NULL_FLAG);
        column_content.column_type_ = column->get_data_type();
        column_content.coll_type_ = column->get_collation_type();
        if (OB_FAIL(
                ob_write_string(phy_plan_->get_allocator(), column->get_column_name(), column_content.column_name_))) {
          LOG_WARN("failed to copy column name", K(ret), K(column->get_column_name()));
        } else if (OB_FAIL(phy_op.add_column_info(column_content))) {
          LOG_WARN("failed to add column info", K(ret), K(column->get_column_name()));
        } else {
          LOG_DEBUG("add column info", KPC(column), K(column_content));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::recursive_get_column_expr(const ObColumnRefRawExpr*& column, const TableItem& table_item)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt* stmt = table_item.ref_query_;
  const ObRawExpr* select_expr = NULL;
  if (OB_ISNULL(column) || OB_ISNULL(stmt) || OB_ISNULL(stmt = stmt->get_real_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(column), K(stmt));
  } else {
    const int64_t offset = column->get_column_id() - OB_APP_MIN_COLUMN_ID;
    if (OB_UNLIKELY(offset < 0 || offset >= stmt->get_select_item_size()) ||
        OB_ISNULL(select_expr = stmt->get_select_item(offset).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected select expr", K(ret), K(offset), K(stmt->get_select_item_size()), K(select_expr));
    } else if (OB_UNLIKELY(!select_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(*select_expr));
    } else {
      const ObColumnRefRawExpr* inner_column = static_cast<const ObColumnRefRawExpr*>(select_expr);
      const TableItem* table_item = stmt->get_table_item_by_id(inner_column->get_table_id());
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (table_item->is_generated_table() &&
                 OB_FAIL(recursive_get_column_expr(inner_column, *table_item))) {
        LOG_WARN("faield to recursive get column expr", K(ret));
      } else {
        column = inner_column;
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::need_foreign_key_handle(const ObForeignKeyArg& fk_arg, const ObIArray<uint64_t>& value_column_ids,
    const ObTableModifySpec& spec, bool& need_handle)
{
  int ret = OB_SUCCESS;
  need_handle = true;
  if (ACTION_INVALID == fk_arg.ref_action_) {
    need_handle = false;
  } else if (spec.get_type() == PHY_UPDATE || spec.get_type() == PHY_UPDATE_RETURNING) {
    // check if foreign key operation is necessary.
    // no matter current table is parent table or child table, the value_column_ids will
    // represent the foreign key related columns of the current table. so we only need to
    // check if these columns maybe updated, by checking if the two arrays are intersected.
    const ObIArray<uint64_t>* updated_column_ids = NULL;
    bool has_intersect = false;
    if (PHY_UPDATE == spec.get_type() || PHY_UPDATE_RETURNING == spec.get_type()) {
      const ObTableUpdateSpec& update_spec = static_cast<const ObTableUpdateSpec&>(spec);
      updated_column_ids = &update_spec.updated_column_ids_;
    } else if (PHY_INSERT_ON_DUP == spec.get_type()) {
      const ObTableInsertUpSpec& insert_up_spec = static_cast<const ObTableInsertUpSpec&>(spec);
      updated_column_ids = &insert_up_spec.updated_column_ids_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected phy op type", K(fk_arg), K(spec.get_type()), K(ret));
    }
    if (OB_SUCC(ret) && !OB_ISNULL(updated_column_ids)) {
      for (int64_t i = 0; !has_intersect && i < value_column_ids.count(); i++) {
        for (int64_t j = 0; !has_intersect && j < updated_column_ids->count(); j++) {
          has_intersect = (value_column_ids.at(i) == updated_column_ids->at(j));
        }
      }
    }
    need_handle = has_intersect;
  } else {
    // nothing.
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSubPlanFilter& op, ObSubPlanFilterSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  if (op.is_update_set()) {
    // update set(, ,) = (subquery)
    // the count of subquery should be and must be 1.
    CK(2 == spec.get_child_cnt() && NULL != spec.get_children()[1]);
    if (OB_SUCC(ret)) {
      ObOpSpec* right_child = spec.get_children()[1];
      OZ(spec.update_set_.assign(right_child->output_));
    }
  }
  CK(NULL != op.get_plan() && NULL != op.get_plan()->get_stmt());
  if (OB_SUCC(ret)) {
    auto& all_exec_params = op.get_plan()->get_stmt()->get_exec_param_ref_exprs();
    const ObIArray<std::pair<int64_t, ObRawExpr*>>* exec_params[] = {&op.get_exec_params(), &op.get_onetime_exprs()};
    ObFixedArray<ObDynamicParamSetter, ObIAllocator>* setters[] = {&spec.rescan_params_, &spec.onetime_exprs_};
    static_assert(ARRAYSIZEOF(exec_params) == ARRAYSIZEOF(setters), "array count mismatch");
    ObDynamicParamSetter setter;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(exec_params); i++) {
      OZ(setters[i]->init(exec_params[i]->count()));
      for (int64_t k = 0; OB_SUCC(ret) && k < exec_params[i]->count(); k++) {
        auto& exec_param = exec_params[i]->at(k);
        setter.param_idx_ = exec_param.first;
        ObRawExpr* param_expr = ObOptimizerUtil::find_param_expr(all_exec_params, exec_param.first);
        CK(NULL != param_expr);
        CK(exec_param.first >= 0);
        CK(NULL != exec_param.second);
        OZ(generate_rt_expr(*exec_param.second, *reinterpret_cast<ObExpr**>(&setter.src_)));
        OZ(generate_rt_expr(*param_expr, *const_cast<ObExpr**>(&setter.dst_)));
        OZ(setters[i]->push_back(setter));
      }
    }
  }

  if (OB_SUCC(ret)) {
    OZ(spec.one_time_idxs_.add_members2(op.get_onetime_idxs()));
    OZ(spec.init_plan_idxs_.add_members2(op.get_initplan_idxs()));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSubPlanScan& op, ObSubPlanScanSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = op.get_child(0);
  CK(NULL != child);
  OZ(spec.projector_.init(op.get_access_exprs().count() * 2));
  FOREACH_CNT_X(e, op.get_access_exprs(), OB_SUCC(ret))
  {
    CK(NULL != *e);
    CK((*e)->is_column_ref_expr());
    const ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(*e);
    if (OB_SUCC(ret)) {
      // for generate table column_id is generated by OB_APP_MIN_COLUMN_ID + select_item_index
      int64_t idx = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
      CK(idx >= 0);
      CK(idx < child->get_output_exprs().count());
      CK(NULL != child->get_output_exprs().at(idx));
      if (OB_SUCC(ret)) {
        const ObRawExpr* from = child->get_output_exprs().at(idx);
        ObExpr* rt_expr = NULL;
        OZ(generate_rt_expr(*from, rt_expr));
        OZ(spec.projector_.push_back(rt_expr));
        OZ(generate_rt_expr(*col_expr, rt_expr));
        OZ(spec.projector_.push_back(rt_expr));
        OZ(mark_expr_self_produced(*e));  // table access exprs in convert_subplan_scan need to set IS_COLUMNLIZED flag
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableLookup& op, ObTableLookupSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  ObTableLookup* table_lookup = NULL;
  ObSqlSchemaGuard* schema_guard = NULL;
  const ObTableSchema* table_schema = NULL;
  ObSQLSessionInfo* my_session = NULL;
  ObOpSpec* tsc_spec = nullptr;
  bool in_root_job = false;
  bool is_subplan = true;
  bool check_eval_once = true;
  if (OB_ISNULL(op.get_index_back_scan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table scan operator", K(ret));
  } else if (OB_FAIL(postorder_generate_op(
                 *op.get_index_back_scan(), tsc_spec, in_root_job, is_subplan, check_eval_once))) {
    LOG_WARN("failed to convert table scan", K(ret));
  } else if (OB_ISNULL(op.get_plan()) ||
             OB_ISNULL(schema_guard = op.get_plan()->get_optimizer_context().get_sql_schema_guard()) ||
             OB_ISNULL(schema_guard->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get unexpected null", K(schema_guard), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(op.get_ref_table_id(), table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(my_session = op.get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null session", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null table schema", K(ret));
  } else {
    ObLookupInfo& lookup_info = spec.lookup_info_;
    spec.remote_tsc_spec_ = tsc_spec;
    lookup_info.table_id_ = op.get_table_id();
    lookup_info.ref_table_id_ = op.get_ref_table_id();
    lookup_info.partition_cnt_ = table_schema->get_partition_cnt();
    lookup_info.partition_num_ = table_schema->get_all_part_num();
    lookup_info.is_old_no_pk_table_ = table_schema->is_old_no_pk_table();  // only for old no-pk table
    uint64_t fetch_tenant_id = extract_tenant_id(lookup_info.ref_table_id_);
    if (is_sys_table(lookup_info.ref_table_id_) || is_fake_table(lookup_info.ref_table_id_)) {
      fetch_tenant_id = OB_SYS_TENANT_ID;
    }
    if (OB_FAIL(schema_guard->get_schema_guard()->get_schema_version(fetch_tenant_id, lookup_info.schema_version_))) {
      LOG_WARN("fail to get schema version", K(ret), K(lookup_info));
    }
    // for calc partition id expr
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(op.get_calc_part_id_expr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("repart_transmit's calc_part_id_expr is null", K(ret));
      } else if (OB_FAIL(generate_rt_expr(*op.get_calc_part_id_expr(), spec.calc_part_id_expr_))) {
        LOG_WARN("fail to generate calc part id expr", K(ret), KP(op.get_calc_part_id_expr()));
      }
    }
    if (OB_SUCC(ret)) {
      const ObIArray<ObRawExpr*>& access_exprs = op.get_index_back_scan()->get_access_exprs();
      // table access in index table scan need to set IS_COLUMNLIZED flag
      if (OB_FAIL(mark_expr_self_produced(access_exprs))) {
        LOG_WARN("mark expr self produced failed", K(ret));
      }
    }
  }
#ifndef NDEBUG
  if (OB_SUCC(ret)) {
    // debug
    LOG_DEBUG("operator generate", K(op.get_index_back_scan()->get_name()), K(*op.get_index_back_scan()));
  }
#endif

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan& op, ObMultiPartTableScanSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_normal_tsc(op, spec));

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogConflictRowFetcher& op, ObTableConflictRowFetcherSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  spec.table_id_ = op.get_table_id();
  spec.index_tid_ = op.get_index_tid();
  spec.only_data_table_ = op.get_only_data_table();
  OZ(spec.conf_col_ids_.init(op.get_conflict_exprs().count()));
  OZ(spec.access_col_ids_.init(op.get_access_exprs().count()));
  OZ(spec.conflict_exprs_.init(op.get_conflict_exprs().count()));
  OZ(spec.access_exprs_.init(op.get_access_exprs().count()));
  const ObColumnRefRawExpr* col_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_conflict_exprs().count(); ++i) {
    col_expr = op.get_conflict_exprs().at(i);
    CK(OB_NOT_NULL(col_expr));
    OZ(spec.conf_col_ids_.push_back(col_expr->get_column_id()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_access_exprs().count(); ++i) {
    col_expr = op.get_access_exprs().at(i);
    CK(OB_NOT_NULL(col_expr));
    OZ(spec.access_col_ids_.push_back(col_expr->get_column_id()));
  }
  // table columns exprs using for uk conflict checking in dml need to set IS_COLUMNLIZED flag
  OZ(mark_expr_self_produced(op.get_conflict_exprs()));
  OZ(mark_expr_self_produced(op.get_access_exprs()));
  OZ(generate_exprs_replace_spk(op.get_conflict_exprs(), spec.conflict_exprs_));
  OZ(generate_exprs_replace_spk(op.get_access_exprs(), spec.access_exprs_));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan& op, ObRowSampleScanSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_normal_tsc(op, spec));

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan& op, ObBlockSampleScanSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_normal_tsc(op, spec));

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan& op, ObTableScanWithIndexBackSpec& spec, const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_normal_tsc(op, spec));

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogDelete& op, ObPxMultiPartDeleteSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<TableColumns>* delete_index_table = op.get_all_table_columns();
  if (OB_ISNULL(delete_index_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete table columns is null", K(ret));
  } else if (op.get_num_of_child() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child num is error", K(ret));
  } else if (delete_index_table->count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of dml table is error", K(ret));
  } else if (delete_index_table->at(0).index_dml_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of dml index is error", K(ret));
  } else {
    phy_plan_->set_use_pdml(true);
    spec.is_returning_ = op.pdml_is_returning();
    spec.set_with_barrier(op.need_barrier());
    spec.is_pdml_index_maintain_ = op.is_index_maintenance();
    const IndexDMLInfo& index_dml_info = delete_index_table->at(0).index_dml_infos_.at(0);
    spec.table_id_ = index_dml_info.loc_table_id_;
    spec.index_tid_ = index_dml_info.index_tid_;
    spec.need_filter_null_row_ = index_dml_info.need_filter_null_;
    spec.table_desc_.index_tid_ = index_dml_info.index_tid_;
    spec.table_desc_.partition_cnt_ = index_dml_info.part_cnt_;

    int64_t partition_expr_idx = OB_INVALID_INDEX;
    if (OB_FAIL(get_pdml_partition_id_column_idx(spec.get_child(0)->output_, partition_expr_idx))) {
      LOG_WARN("failed to get partition id column idx", K(ret));
    } else if (OB_FAIL(add_table_column_ids(op, &spec, index_dml_info.column_exprs_, index_dml_info.rowkey_cnt_))) {
      LOG_WARN("failed to add column ids and rowkey count", K(ret));
    } else {
      spec.row_desc_.set_part_id_index(partition_expr_idx);
    }
    LOG_TRACE("pdml static cg information",
        K(ret),
        K(spec.delete_row_exprs_.count()),
        K(partition_expr_idx),
        K(index_dml_info));
    if (OB_SUCC(ret)) {
      spec.delete_row_exprs_.reset();
      const ObIArray<ObColumnRefRawExpr*>& delete_index_exprs = index_dml_info.column_exprs_;
      if (OB_FAIL(spec.delete_row_exprs_.reserve(delete_index_exprs.count()))) {
        LOG_WARN("failed to init delete row exprs array", K(ret), K(delete_index_exprs.count()));
      } else {
        for (int i = 0; i < delete_index_exprs.count() && OB_SUCC(ret); i++) {
          ObExpr* expr = NULL;
          ObColumnRefRawExpr* index_column_expr = delete_index_exprs.at(i);
          CK(OB_NOT_NULL(index_column_expr));
          if (OB_SUCC(ret)) {
            if (is_shadow_column(index_column_expr->get_column_id())) {
              const ObRawExpr* shadow_pk_column = index_column_expr->get_dependant_expr();
              CK(OB_NOT_NULL(shadow_pk_column));
              if (OB_SUCC(ret)) {
                OZ(generate_rt_expr(*shadow_pk_column, expr));
              }
            } else {
              OZ(generate_rt_expr(*index_column_expr, expr));
            }
          }
          OZ(spec.delete_row_exprs_.push_back(expr));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert& op, ObPxMultiPartInsertSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<TableColumns>* insert_index_table = op.get_all_table_columns();
  if (OB_ISNULL(insert_index_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete table columns is null", K(ret));
  } else if (op.get_num_of_child() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child num is error", K(ret));
  } else if (insert_index_table->count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of dml table is error", K(ret));
  } else if (insert_index_table->at(0).index_dml_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of dml index is error", K(ret));
  } else {
    phy_plan_->set_use_pdml(true);
    spec.is_returning_ = op.pdml_is_returning();
    spec.is_pdml_index_maintain_ = op.is_index_maintenance();
    spec.table_location_uncertain_ = op.is_table_location_uncertain();  // row-movement target table
    const IndexDMLInfo& index_dml_info = insert_index_table->at(0).index_dml_infos_.at(0);
    spec.table_id_ = index_dml_info.loc_table_id_;
    spec.index_tid_ = index_dml_info.index_tid_;
    spec.need_filter_null_row_ = index_dml_info.need_filter_null_;
    spec.table_desc_.index_tid_ = index_dml_info.index_tid_;
    spec.table_desc_.partition_cnt_ = index_dml_info.part_cnt_;
    int64_t partition_expr_idx = OB_INVALID_INDEX;
    if (OB_FAIL(get_pdml_partition_id_column_idx(spec.get_child(0)->output_, partition_expr_idx))) {
      LOG_WARN("failed to get partition id column idx", K(ret));
    } else if (OB_FAIL(add_table_column_ids(op, &spec, index_dml_info.column_exprs_, index_dml_info.rowkey_cnt_))) {
      LOG_WARN("failed to add column ids and rowkey count", K(ret));
    } else {
      spec.row_desc_.set_part_id_index(partition_expr_idx);
    }
    LOG_TRACE("pdml static cg information",
        K(ret),
        K(spec.insert_row_exprs_.count()),
        K(partition_expr_idx),
        K(index_dml_info));
    // handle column info and column conv info.
    if (OB_SUCC(ret)) {
      const ObIArray<ObColumnRefRawExpr*>* columns = op.get_table_columns();
      const ObIArray<ObRawExpr*>* column_convert_exprs = op.get_column_convert_exprs();
      if (OB_ISNULL(columns) || OB_ISNULL(column_convert_exprs)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected", K(columns), K(column_convert_exprs));
      } else if (OB_FAIL(add_column_infos(op, spec, *columns))) {
        LOG_WARN("failed to add column infos", K(ret));
      } else if (OB_FAIL(add_column_conv_infos(spec, *columns, *column_convert_exprs))) {
        LOG_WARN("failed to add column conv infos", K(ret));
      }
    }

    if (!op.is_index_maintenance()) {
      CK(OB_NOT_NULL(op.get_column_convert_exprs()));
      OZ(generate_rt_exprs(*op.get_column_convert_exprs(), spec.storage_row_output_));
      OZ(convert_check_constraint(op, spec));
    }

    if (OB_SUCC(ret)) {
      spec.insert_row_exprs_.reset();
      const ObIArray<ObColumnRefRawExpr*>& insert_index_exprs = index_dml_info.column_exprs_;
      if (OB_FAIL(spec.insert_row_exprs_.reserve(insert_index_exprs.count()))) {
        LOG_WARN("failed to init delete row exprs array", K(ret), K(insert_index_exprs.count()));
      } else if (OB_FAIL(generate_pdml_insert_exprs(
                     insert_index_exprs, index_dml_info.column_convert_exprs_, spec.insert_row_exprs_))) {
        LOG_WARN("failed to generate pdml insert exprs",
            K(ret),
            K(insert_index_exprs),
            K(*op.get_table_columns()),
            K(*op.get_column_convert_exprs()));
      }
    }
    // table columns exprs in dml need to set IS_COLUMNLIZED flag
    OZ(mark_expr_self_produced(index_dml_info.column_exprs_));
    OZ(mark_expr_self_produced(index_dml_info.column_convert_exprs_));
  }
  return ret;
}

int ObStaticEngineCG::generate_pdml_insert_exprs(const ObIArray<ObColumnRefRawExpr*>& index_exprs,
    const ObIArray<ObRawExpr*>& index_dml_conv_columns, ObIArray<ObExpr*>& pdml_insert_exprs)
{
  int ret = OB_SUCCESS;
  ObExpr* expr = NULL;
  if (index_exprs.count() != index_dml_conv_columns.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count is not equal", K(ret), K(index_exprs.count()), K(index_dml_conv_columns.count()));
  } else {
    for (int i = 0; i < index_dml_conv_columns.count() && OB_SUCC(ret); i++) {
      ObRawExpr* conv_expr = index_dml_conv_columns.at(i);
      if (OB_ISNULL(conv_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret));
      } else if (OB_FAIL(generate_rt_expr(*conv_expr, expr))) {
        LOG_WARN("fail to push cur op expr", K(ret), K(index_exprs));
      } else if (OB_FAIL(pdml_insert_exprs.push_back(expr))) {
        LOG_WARN("fail to push expr", K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogUpdate& op, ObPxMultiPartUpdateSpec& spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<TableColumns>* insert_index_table = op.get_all_table_columns();
  if (OB_ISNULL(insert_index_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete table columns is null", K(ret));
  } else if (op.get_num_of_child() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child num is error", K(ret));
  } else if (insert_index_table->count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of dml table is error", K(ret));
  } else if (insert_index_table->at(0).index_dml_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of dml index is error", K(ret));
  } else {
    phy_plan_->set_use_pdml(true);
    spec.is_returning_ = op.pdml_is_returning();
    spec.is_pdml_index_maintain_ = op.is_index_maintenance();
    const IndexDMLInfo& index_dml_info = insert_index_table->at(0).index_dml_infos_.at(0);
    spec.table_id_ = index_dml_info.loc_table_id_;
    spec.index_tid_ = index_dml_info.index_tid_;
    spec.need_filter_null_row_ = index_dml_info.need_filter_null_;
    spec.table_desc_.index_tid_ = index_dml_info.index_tid_;
    spec.table_desc_.partition_cnt_ = index_dml_info.part_cnt_;
    int64_t partition_expr_idx = OB_INVALID_INDEX;
    if (OB_FAIL(get_pdml_partition_id_column_idx(spec.get_child(0)->output_, partition_expr_idx))) {
      LOG_WARN("failed to get partition id column idx", K(ret));
    } else if (OB_FAIL(add_table_column_ids(op, &spec, index_dml_info.column_exprs_, index_dml_info.rowkey_cnt_))) {
      LOG_WARN("failed to add column ids and rowkey count", K(ret));
    } else {
      spec.row_desc_.set_part_id_index(partition_expr_idx);
    }
    LOG_TRACE("pdml static cg information", K(ret), K(partition_expr_idx), K(index_dml_info));
    if (OB_SUCC(ret)) {
      if (OB_FAIL(generate_pdml_update_exprs(index_dml_info.column_exprs_, index_dml_info.assignments_, spec))) {
        LOG_WARN("failed to generate pdml update exprs", K(ret));
      }
    }
    if (!op.is_index_maintenance()) {
      const ObIArray<ObColumnRefRawExpr*>* columns = op.get_table_columns();
      CK(NULL != columns);
      OZ(add_column_infos(op, spec, *columns));
      OZ(convert_check_constraint(op, spec));
    }
    // table columns exprs in dml need to set IS_COLUMNLIZED flag
    OZ(mark_expr_self_produced(index_dml_info.column_exprs_));
  }
  return ret;
}

int ObStaticEngineCG::generate_pdml_update_exprs(
    const ObIArray<ObColumnRefRawExpr*>& index_exprs, const ObAssignments& assigns, ObPxMultiPartUpdateSpec& spec)
{
  int ret = OB_SUCCESS;
  if (index_exprs.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of index_exprs is zero", K(ret), K(index_exprs.count()));
  } else if (assigns.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of assigns is zero", K(ret), K(assigns.count()));
  } else if (OB_FAIL(spec.init_updated_column_count(phy_plan_->get_allocator(), assigns.count()))) {
    LOG_WARN("failed to init updated column count", K(ret));
  } else {
    spec.old_row_exprs_.reserve(index_exprs.count());
    spec.new_row_exprs_.reserve(index_exprs.count());
    OZ(generate_exprs_replace_spk(index_exprs, spec.old_row_exprs_));
    OZ(spec.new_row_exprs_.assign(spec.old_row_exprs_));

    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      ObColumnRefRawExpr* col = assigns.at(i).column_expr_;
      ObRawExpr* value = assigns.at(i).expr_;
      CK(NULL != col);
      CK(NULL != value);
      int64_t idx = OB_INVALID_INDEX;
      if (OB_SUCC(ret)) {
        bool exists = has_exist_in_array(index_exprs, col, &idx);
        if (!exists) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update column not found in all columns", K(ret), K(col));
        }
      }
      if (OB_SUCC(ret)) {
        ObExpr* expr = NULL;
        OZ(generate_rt_expr(*value, expr));
        if (OB_SUCC(ret)) {
          spec.new_row_exprs_.at(idx) = expr;
        }
        OZ(spec.set_updated_column_info(i,
            assigns.at(i).base_column_id_,
            idx,
            col->get_result_type().has_result_flag(OB_MYSQL_ON_UPDATE_NOW_FLAG)));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::fill_aggr_infos(ObLogGroupBy& op, ObGroupBySpec& spec,
    common::ObIArray<ObExpr*>* group_exprs /*NULL*/, common::ObIArray<ObExpr*>* rollup_exprs /*NULL*/)
{
  int ret = OB_SUCCESS;
  CK(NULL != phy_plan_);
  const ObIArray<ObRawExpr*>& aggr_exprs = op.get_aggr_funcs();
  // 1.init aggr expr
  ObSEArray<ObExpr*, 8> all_aggr_exprs;
  ARRAY_FOREACH(aggr_exprs, i)
  {
    ObRawExpr* raw_expr = NULL;
    ObAggrInfo aggr_info;
    ObExpr* expr = NULL;
    if (OB_ISNULL(raw_expr = aggr_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw_expr is null ", K(ret), K(expr));
    } else if (OB_UNLIKELY(!raw_expr->has_flag(IS_AGG))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected aggr function", K(ret));
    } else if (OB_FAIL(
                   mark_expr_self_produced(raw_expr))) {  // aggr func exprs in group by need to set IS_COLUMNLIZED flag
      LOG_WARN("add columnlized flag to agg expr failed", K(ret));
    } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
      LOG_WARN("failed to generate_rt_expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null ", K(ret), K(expr));
    } else if (OB_FAIL(all_aggr_exprs.push_back(expr))) {
      LOG_WARN("Add var to array error", K(ret));
    }
  }

  // 2.init non aggr expr
  // oracle mode has the non aggregation expr problem too:
  //
  // explain extended select abs(c1), sum(c2) from t1 group by abs(c1);
  // | ========================================
  // |ID|OPERATOR     |NAME|EST. ROWS|COST  |
  // ----------------------------------------
  // |0 |HASH GROUP BY|    |101      |106673|
  // |1 | TABLE SCAN  |T1  |100000   |68478 |
  // ========================================
  //
  // Outputs & filters:
  // -------------------------------------
  //   0 - output([ABS(T1.C1(0x7f1aa0c43580))(0x7f1aa0c45850)], [T_FUN_SUM(T1.C2(0x7f1aa0c489e0))(0x7f1aa0c48410)]),
  //   filter(nil),
  //       group([ABS(T1.C1(0x7f1aa0c43580))(0x7f1aa0c42a20)]),
  //       agg_func([T_FUN_SUM(T1.C2(0x7f1aa0c489e0))(0x7f1aa0c48410)])
  //   1 - output([T1.C1(0x7f1aa0c43580)], [T1.C2(0x7f1aa0c489e0)], [ABS(T1.C1(0x7f1aa0c43580))(0x7f1aa0c42a20)]),
  //   filter(nil),
  //       access([T1.C1(0x7f1aa0c43580)], [T1.C2(0x7f1aa0c489e0)]), partitions(p0),
  //
  // The output abs(c1) 0x7f1aa0c45850 is not the group by column (raw expr not the same),
  // (the resolver will correct this in future). We need the mysql non aggregate output
  // feature to evaluate it.
  ObSEArray<ObExpr*, 8> all_non_aggr_exprs;
  common::ObIArray<ObExpr*>& child_output = spec.get_children()[0]->output_;
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_output_exprs().count(); ++i) {
    ObExpr* expr = NULL;
    const ObRawExpr& raw_expr = *op.get_output_exprs().at(i);
    if (OB_FAIL(generate_rt_expr(raw_expr, expr))) {
      LOG_WARN("failed to generate_rt_expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expr is null ", K(ret), K(expr));
    } else if (OB_FAIL(extract_non_aggr_expr(
                   expr, &raw_expr, child_output, all_aggr_exprs, group_exprs, rollup_exprs, all_non_aggr_exprs))) {
      OB_LOG(WARN, "fail to extract_non_aggr_expr", "count", all_non_aggr_exprs.count(), KPC(expr), K(ret));
    } else {
      OB_LOG(DEBUG,
          "finish extract_non_aggr_expr",
          KPC(expr),
          K(raw_expr),
          K(child_output),
          K(all_aggr_exprs),
          KPC(group_exprs),
          KPC(rollup_exprs),
          K(all_non_aggr_exprs));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_filter_exprs().count(); ++i) {
    ObExpr* expr = NULL;
    const ObRawExpr& raw_expr = *op.get_filter_exprs().at(i);
    if (OB_FAIL(generate_rt_expr(raw_expr, expr))) {
      LOG_WARN("failed to generate_rt_expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expr is null ", K(ret), K(expr));
    } else if (OB_FAIL(extract_non_aggr_expr(
                   expr, &raw_expr, child_output, all_aggr_exprs, group_exprs, rollup_exprs, all_non_aggr_exprs))) {
      OB_LOG(WARN, "fail to extract_non_aggr_expr", "count", all_non_aggr_exprs.count(), KPC(expr), K(ret));
    } else {
      OB_LOG(DEBUG,
          "finish extract_non_aggr_expr",
          KPC(expr),
          K(raw_expr),
          K(child_output),
          K(all_aggr_exprs),
          KPC(group_exprs),
          KPC(rollup_exprs),
          K(all_non_aggr_exprs));
    }
  }

  // 3.init aggr_infos
  if (OB_SUCC(ret)) {
    if (OB_FAIL(spec.aggr_infos_.prepare_allocate(all_aggr_exprs.count() + all_non_aggr_exprs.count()))) {
      OB_LOG(WARN, "fail to prepare_allocate aggr_infos_", K(ret));
    }
  }

  // 4.add aggr columns
  for (int64_t i = 0; OB_SUCC(ret) && i < all_aggr_exprs.count(); ++i) {
    ObAggrInfo& aggr_info = spec.aggr_infos_.at(i);
    if (OB_FAIL(fill_aggr_info(*static_cast<ObAggFunRawExpr*>(aggr_exprs.at(i)), *all_aggr_exprs.at(i), aggr_info))) {
      LOG_WARN("failed to fill_aggr_info", K(ret));
    }

    // add udf meta
    //      if (OB_SUCC(ret)) {
    //        if (raw_expr->get_expr_type() == T_FUN_AGG_UDF) {
    //          ObAggFunRawExpr *agg_expr = static_cast<ObAggFunRawExpr*>(raw_expr);
    //          ObSEArray<ObString, 16> udf_attributes;
    //          ObSEArray<ObExprResType, 16> udf_attributes_types;
    //          ObSEArray<ObUdfConstArgs, 16> const_results; /* const input expr' param idx */
    //          ObAggUdfMeta agg_udf_meta;
    //          agg_udf_meta.udf_meta_ = agg_expr->get_udf_meta();
    //          const common::ObIArray<ObRawExpr*> &param_exprs = agg_expr->get_real_param_exprs();
    //          ARRAY_FOREACH_X(param_exprs, idx, cnt, OB_SUCC(ret)) {
    //            ObRawExpr *expr = param_exprs.at(idx);
    //            if (OB_ISNULL(expr)) {
    //              ret = OB_ERR_UNEXPECTED;
    //              LOG_WARN("the expr is null", K(ret));
    //            } else if (OB_FAIL(udf_attributes.push_back(expr->get_expr_name()))) {
    //              LOG_WARN("failed to push back", K(ret));
    //            } else if (OB_FAIL(udf_attributes_types.push_back(expr->get_result_type()))) {
    //              LOG_WARN("failed to push back", K(ret));
    //            } else if (expr->has_flag(IS_CALCULABLE_EXPR)
    //                       || expr->has_flag(IS_CONST_EXPR)
    //                       || expr->has_flag(IS_CONST)) {
    //              // generate the sql expression
    //              ObObj tmp_res;
    //              ObNewRow empty_row;
    //              RowDesc row_desc;
    //              ObSqlExpression *sql_expr = NULL;
    //              ObExprGeneratorImpl expr_generator(phy_plan_->get_expr_op_factory(), 0, 0, NULL, row_desc);
    //              if (OB_FAIL(create_expression(phy_op->get_sql_expression_factory(), sql_expr))) {
    //                LOG_WARN("failed to create topn expr", K(ret));
    //              } else if (OB_ISNULL(sql_expr)) {
    //                ret = OB_ERR_UNEXPECTED;
    //                LOG_WARN("topn_expr is null", K(ret));
    //              } else if (OB_FAIL(expr_generator.generate(*expr, *sql_expr))) {
    //                LOG_WARN("failed to generate topn expr", K(ret));
    //              } else {
    //                ObUdfConstArgs const_args;
    //                const_args.sql_calc_ = sql_expr;
    //                const_args.idx_in_udf_arg_ = idx;
    //                if (OB_FAIL(const_results.push_back(const_args))) {
    //                  LOG_WARN("failed to push back const args", K(ret));
    //                }
    //              }
    //            }
    //          }
    //          if (OB_SUCC(ret)) {
    //            if (OB_FAIL(agg_udf_meta.udf_attributes_.assign(udf_attributes))) {
    //              LOG_WARN("assign array failed", K(ret));
    //            } else if (OB_FAIL(agg_udf_meta.udf_attributes_types_.assign(udf_attributes_types))) {
    //              LOG_WARN("assign array failed", K(ret));
    //            } else if (OB_FAIL(agg_udf_meta.calculable_results_.assign(const_results))) {
    //              LOG_WARN("assign const result failed", K(ret));
    //            } else if (OB_FAIL(phy_op->add_udf_meta(agg_udf_meta))) {
    //              LOG_WARN("add udf meta to group by failed", K(ret));
    //            }
    //          }
    //        }
    //      }
  }  // end of for

  // 5. file non_aggr_expr
  for (int64_t i = 0; OB_SUCC(ret) && i < all_non_aggr_exprs.count(); ++i) {
    ObExpr* expr = all_non_aggr_exprs.at(i);
    ObAggrInfo& aggr_info = spec.aggr_infos_.at(all_aggr_exprs.count() + i);
    aggr_info.set_implicit_first_aggr();
    aggr_info.expr_ = expr;
    LOG_TRACE("trace all non aggr exprs", K(*expr), K(all_non_aggr_exprs.count()));
  }
  return ret;
}

int ObStaticEngineCG::fill_aggr_info(ObAggFunRawExpr& raw_expr, ObExpr& expr, ObAggrInfo& aggr_info)
{
  int ret = OB_SUCCESS;
  if (T_FUN_AGG_UDF == raw_expr.get_expr_type()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support T_FUN_AGG_UDF now",
        "func_type",
        raw_expr.get_expr_type(),
        "arg_cnt",
        raw_expr.get_real_param_count(),
        K(ret));
  } else {
    const int64_t group_concat_param_count =
        (is_oracle_mode() && raw_expr.get_expr_type() == T_FUN_GROUP_CONCAT && raw_expr.get_real_param_count() > 1)
            ? (raw_expr.get_real_param_count() - 1)
            : raw_expr.get_real_param_count();

    aggr_info.expr_ = &expr;
    aggr_info.has_distinct_ = raw_expr.is_param_distinct();
    aggr_info.group_concat_param_count_ = group_concat_param_count;

    if (aggr_info.has_distinct_) {
      if (OB_FAIL(aggr_info.distinct_collations_.init(group_concat_param_count))) {
        LOG_WARN("failed to init distinct_collations_", K(ret));
      } else if (OB_FAIL(aggr_info.distinct_cmp_funcs_.init(group_concat_param_count))) {
        LOG_WARN("failed to init distinct_cmp_funcs_", K(ret));
      }
    }

    ObSEArray<ObExpr*, 16> all_param_exprs;
    if (OB_SUCC(ret)) {
      const ObOrderDirection order_direction = default_asc_direction();
      const bool is_ascending = is_ascending_direction(order_direction);
      const common::ObCmpNullPos null_pos = ((is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
      for (int64_t i = 0; OB_SUCC(ret) && i < group_concat_param_count; ++i) {
        ObExpr* expr = NULL;
        const ObRawExpr& param_raw_expr = *raw_expr.get_real_param_exprs().at(i);
        if (OB_FAIL(generate_rt_expr(param_raw_expr, expr))) {
          LOG_WARN("failed to generate_rt_expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null ", K(ret), K(expr));
        } else if ((T_FUN_GROUP_CONCAT == raw_expr.get_expr_type() ||
                       T_FUN_KEEP_WM_CONCAT == raw_expr.get_expr_type() ||
                       T_FUN_WM_CONCAT == raw_expr.get_expr_type()) &&
                   OB_UNLIKELY(!param_raw_expr.get_result_meta().is_string_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_raw_expr is not sting ", K(ret), K(param_raw_expr));
        } else if (OB_FAIL(all_param_exprs.push_back(expr))) {
          LOG_WARN("failed to push_back param_expr", K(ret));
        } else if (aggr_info.has_distinct_) {
          ObSortFieldCollation field_collation(i, expr->datum_meta_.cs_type_, is_ascending, null_pos);
          ObSortCmpFunc cmp_func;
          cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
              expr->datum_meta_.type_,
              field_collation.null_pos_,
              field_collation.cs_type_,
              lib::is_oracle_mode());
          if (OB_ISNULL(cmp_func.cmp_func_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cmp_func is null, check datatype is valid", K(ret));
          } else if (OB_FAIL(aggr_info.distinct_collations_.push_back(field_collation))) {
            LOG_WARN("failed to push back field collation", K(ret));
          } else if (OB_FAIL(aggr_info.distinct_cmp_funcs_.push_back(cmp_func))) {
            LOG_WARN("failed to push back cmp function", K(ret));
          } else {
            LOG_DEBUG("succ to push back field collation", K(field_collation), K(i));
          }
        }
      }
    }

    if (OB_SUCC(ret) &&
        (T_FUN_GROUP_CONCAT == raw_expr.get_expr_type() || T_FUN_GROUP_RANK == raw_expr.get_expr_type() ||
            T_FUN_GROUP_DENSE_RANK == raw_expr.get_expr_type() ||
            T_FUN_GROUP_PERCENT_RANK == raw_expr.get_expr_type() || T_FUN_GROUP_CUME_DIST == raw_expr.get_expr_type() ||
            T_FUN_GROUP_PERCENTILE_CONT == raw_expr.get_expr_type() ||
            T_FUN_GROUP_PERCENTILE_DISC == raw_expr.get_expr_type() || T_FUN_MEDIAN == raw_expr.get_expr_type() ||
            T_FUN_KEEP_SUM == raw_expr.get_expr_type() || T_FUN_KEEP_MAX == raw_expr.get_expr_type() ||
            T_FUN_KEEP_MIN == raw_expr.get_expr_type() || T_FUN_KEEP_COUNT == raw_expr.get_expr_type() ||
            T_FUN_KEEP_WM_CONCAT == raw_expr.get_expr_type())) {
      const ObRawExpr* param_raw_expr =
          (is_oracle_mode() && T_FUN_GROUP_CONCAT == raw_expr.get_expr_type() && raw_expr.get_real_param_count() > 1)
              ? raw_expr.get_real_param_exprs().at(raw_expr.get_real_param_count() - 1)
              : raw_expr.get_separator_param_expr();
      if (param_raw_expr != NULL) {
        ObExpr* expr = NULL;
        if (OB_FAIL(generate_rt_expr(*param_raw_expr, expr))) {
          LOG_WARN("failed to generate_rt_expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null ", K(ret), K(expr));
        } else if (OB_UNLIKELY(!expr->obj_meta_.is_string_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr node is null", K(ret), KPC(expr));
        } else {
          aggr_info.separator_expr_ = expr;
        }
      }

      if (OB_SUCC(ret) &&
          (T_FUN_GROUP_PERCENTILE_CONT == raw_expr.get_expr_type() || T_FUN_MEDIAN == raw_expr.get_expr_type())) {
        ObRawExpr* linear_inter_expr = raw_expr.get_linear_inter_expr();
        if (OB_ISNULL(linear_inter_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("linear inter expr is null ", K(ret), K(linear_inter_expr));
        } else {
          ObExpr* expr = NULL;
          if (OB_FAIL(generate_rt_expr(*linear_inter_expr, expr))) {
            LOG_WARN("failed to generate_rt_expr", K(ret));
          } else if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null ", K(ret), K(expr));
          } else {
            aggr_info.linear_inter_expr_ = expr;
          }
        }
      }

      if (OB_SUCC(ret) && !raw_expr.get_order_items().empty()) {
        aggr_info.has_order_by_ = true;
        if (OB_FAIL(fil_sort_info(raw_expr.get_order_items(),
                                  all_param_exprs,
                                  NULL,
                                  aggr_info.sort_collations_,
                                  aggr_info.sort_cmp_funcs_))) {
          LOG_WARN("failed to fil_sort_info", K(ret));
        } else { /*do nothing*/
        }
      }  // order item
    }    // group concat

    if (OB_SUCC(ret)) {
      if (all_param_exprs.empty() && T_FUN_COUNT != raw_expr.get_expr_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("only count(*) has empty param", K(ret), "expr_type", raw_expr.get_expr_type());
      } else if (OB_FAIL(aggr_info.param_exprs_.assign(all_param_exprs))) {
        LOG_WARN("failed to init param_exprs", K(ret));
      } else {
        LOG_DEBUG("finish fill_aggr_info", K(raw_expr), K(expr), K(aggr_info), K(all_param_exprs));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::extract_non_aggr_expr(ObExpr* input, const ObRawExpr* raw_input,
    common::ObIArray<ObExpr*>& exist_in_child, common::ObIArray<ObExpr*>& not_exist_in_aggr,
    common::ObIArray<ObExpr*>* not_exist_in_groupby, common::ObIArray<ObExpr*>* not_exist_in_rollup,
    common::ObIArray<ObExpr*>& output) const
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    OB_LOG(WARN, "check_stack_overflow failed", "count", output.count(), K(ret), K(lbt()));
  } else if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "input is null", KP(input), K(ret));
  } else if (raw_input != NULL && (raw_input->has_flag(IS_CONST) || raw_input->has_flag(IS_CONST_EXPR))) {
    LOG_DEBUG("no aggr expr is const, ignore", KPC(raw_input));
  } else if (has_exist_in_array(exist_in_child, input) && !has_exist_in_array(not_exist_in_aggr, input) &&
             (NULL == not_exist_in_groupby || !has_exist_in_array(*not_exist_in_groupby, input)) &&
             (NULL == not_exist_in_rollup || !has_exist_in_array(*not_exist_in_rollup, input))) {
    if (OB_FAIL(add_var_to_array_no_dup(output, input))) {
      OB_LOG(WARN, "fail to add_var_to_array_no_dup", "count", output.count(), KPC(input), K(ret));
    }
  } else if (!has_exist_in_array(not_exist_in_aggr, input)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < input->arg_cnt_; ++i) {
      ObExpr* expr = input->args_[i];
      const ObRawExpr* raw_expr = (raw_input != NULL ? raw_input->get_param_expr(i) : NULL);
      if (OB_FAIL(extract_non_aggr_expr(
              expr, raw_expr, exist_in_child, not_exist_in_aggr, not_exist_in_groupby, not_exist_in_rollup, output))) {
        OB_LOG(WARN, "fail to extract_non_aggr_expr", "count", output.count(), KPC(expr), KPC(raw_expr), K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogWindowFunction& op, ObWindowFunctionSpec& spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 16> all_expr;
  if (OB_UNLIKELY(op.get_num_of_child() != 1 || OB_ISNULL(op.get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong number of children", K(ret), K(op.get_num_of_child()));
  } else if (OB_FAIL(spec.wf_infos_.prepare_allocate(op.get_window_exprs().count()))) {
    LOG_WARN("failed to prepare_allocate the window function.", K(ret));
  } else if (OB_FAIL(all_expr.assign(spec.get_child()->output_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    spec.is_parallel_ = op.is_parallel();
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_window_exprs().count(); ++i) {
      ObWinFunRawExpr* wf_expr = op.get_window_exprs().at(i);
      WinFuncInfo& wf_info = spec.wf_infos_.at(i);
      if (OB_ISNULL(wf_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Get unexpected null", K(ret));
      } else if (OB_FAIL(fill_wf_info(all_expr, *wf_expr, wf_info))) {
        LOG_WARN("failed to generate window function info", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(spec.all_expr_.assign(all_expr))) {
      LOG_WARN("failed to assign", K(ret));
    }
  }
  LOG_DEBUG("finish generate_spec", K(spec), K(ret));
  return ret;
}

int ObStaticEngineCG::fill_wf_info(ObIArray<ObExpr*>& all_expr, ObWinFunRawExpr& win_expr, WinFuncInfo& wf_info)
{
  int ret = OB_SUCCESS;
  ObRawExpr* agg_raw_expr = win_expr.get_agg_expr();
  ObExpr* expr = NULL;
  const ObIArray<ObRawExpr*>& func_params = win_expr.get_func_params();
  if (OB_FAIL(generate_rt_expr(win_expr, expr))) {
    LOG_WARN("failed to generate_rt_expr", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null ", K(ret), K(expr));
  } else if (OB_FAIL(mark_expr_self_produced(&win_expr))) {  // win func exprs need to set IS_COLUMNLIZED flag
    LOG_WARN("failed to add columnized flag", K(ret));
  } else if (OB_FAIL(wf_info.init(
                 func_params.count(), win_expr.get_partition_exprs().count(), win_expr.get_order_items().count()))) {
    LOG_WARN("failed to init the func info.", K(ret));
  } else if (NULL != agg_raw_expr && OB_UNLIKELY(!agg_raw_expr->has_flag(IS_AGG))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected aggr function", KPC(agg_raw_expr), K(ret));
  } else {
    if (NULL == agg_raw_expr) {
      // do nothing
    } else if (OB_FAIL(fill_aggr_info(*static_cast<ObAggFunRawExpr*>(agg_raw_expr), *expr, wf_info.aggr_info_))) {
      LOG_WARN("failed to fill_aggr_info", K(ret));
    } else {
      wf_info.aggr_info_.real_aggr_type_ = agg_raw_expr->get_expr_type();
    }

    wf_info.expr_ = expr;
    wf_info.func_type_ = win_expr.get_func_type();
    wf_info.win_type_ = win_expr.get_window_type();
    wf_info.is_ignore_null_ = win_expr.is_ignore_null();
    wf_info.is_from_first_ = win_expr.is_from_first();

    wf_info.upper_.is_preceding_ = win_expr.upper_.is_preceding_;
    wf_info.upper_.is_unbounded_ = BOUND_UNBOUNDED == win_expr.upper_.type_;
    wf_info.upper_.is_nmb_literal_ = win_expr.upper_.is_nmb_literal_;
    wf_info.lower_.is_preceding_ = win_expr.lower_.is_preceding_;
    wf_info.lower_.is_unbounded_ = BOUND_UNBOUNDED == win_expr.lower_.type_;
    wf_info.lower_.is_nmb_literal_ = win_expr.lower_.is_nmb_literal_;

    // add window function params.
    for (int64_t i = 0; OB_SUCC(ret) && i < func_params.count(); ++i) {
      ObRawExpr* raw_expr = func_params.at(i);
      expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw expr is null", K(ret), K(i));
      } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null ", K(ret), K(expr));
      } else if (OB_FAIL(wf_info.param_exprs_.push_back(expr))) {
        LOG_WARN("push back sql expr failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObRawExpr* raw_expr = win_expr.upper_.interval_expr_;
      expr = NULL;
      if (NULL == raw_expr) {
        // do nothing
      } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null ", K(ret), K(expr));
      } else {
        wf_info.upper_.between_value_expr_ = expr;
      }
    }

    if (OB_SUCC(ret)) {
      ObRawExpr* raw_expr = win_expr.lower_.interval_expr_;
      expr = NULL;
      if (NULL == raw_expr) {
        // do nothing
      } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null ", K(ret), K(expr));
      } else {
        wf_info.lower_.between_value_expr_ = expr;
      }
    }

    if (WINDOW_ROWS == wf_info.win_type_) {
      // do nothing
    } else if (win_expr.get_order_items().empty()) {
      // do nothing
    } else {
      bool is_asc = win_expr.get_order_items().at(0).is_ascending();
      ObRawExpr* upper_raw_expr =
          (win_expr.upper_.is_preceding_ ^ is_asc) ? win_expr.upper_.exprs_[0] : win_expr.upper_.exprs_[1];
      ObRawExpr* lower_raw_expr =
          (win_expr.lower_.is_preceding_ ^ is_asc) ? win_expr.lower_.exprs_[0] : win_expr.lower_.exprs_[1];
      if (OB_SUCC(ret)) {
        expr = NULL;
        if (NULL == upper_raw_expr) {
          // do nothing
        } else if (OB_FAIL(generate_rt_expr(*upper_raw_expr, expr))) {
          LOG_WARN("failed to generate_rt_expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null ", K(ret), K(expr));
        } else {
          wf_info.upper_.range_bound_expr_ = expr;
        }
      }

      if (OB_SUCC(ret)) {
        expr = NULL;
        if (NULL == lower_raw_expr) {
          // do nothing
        } else if (OB_FAIL(generate_rt_expr(*lower_raw_expr, expr))) {
          LOG_WARN("failed to generate_rt_expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null ", K(ret), K(expr));
        } else {
          wf_info.lower_.range_bound_expr_ = expr;
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < win_expr.get_partition_exprs().count(); i++) {
      bool skip = false;
      int64_t cell_idx = OB_INVALID_INDEX;
      const ObRawExpr* raw_expr = win_expr.get_partition_exprs().at(i);
      expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw expr is null", K(ret), K(i));
      } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null ", K(ret), K(expr));
      } else if (OB_FAIL(wf_info.partition_exprs_.push_back(expr))) {
        LOG_WARN("push_back failed", K(ret), K(expr));
      }
    }

    if (OB_SUCC(ret) && !win_expr.get_order_items().empty()) {
      if (OB_FAIL(fil_sort_info(win_expr.get_order_items(),
              all_expr,
              &wf_info.sort_exprs_,
              wf_info.sort_collations_,
              wf_info.sort_cmp_funcs_))) {
        LOG_WARN("failed to fil_sort_info", K(ret));
      }
    }
    LOG_DEBUG("finish fill_wf_info", K(win_expr), K(wf_info), K(ret));
  }
  return ret;
}

int ObStaticEngineCG::fil_sort_info(const ObIArray<OrderItem> &sort_keys,
    ObIArray<ObExpr *> &all_exprs, ObIArray<ObExpr *> *sort_exprs,
    ObSortCollations &sort_collations, ObSortFuncs &sort_cmp_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_collations.init(sort_keys.count()))) {
    LOG_WARN("failed to init collations", K(ret));
  } else if (OB_FAIL(sort_cmp_funcs.init(sort_keys.count()))) {
    LOG_WARN("failed to init sort_cmp_funcs_", K(ret));
  } else {
    for (int64_t i = 0; i < sort_keys.count() && OB_SUCC(ret); ++i) {
      const OrderItem& order_item = sort_keys.at(i);
      ObExpr* expr = nullptr;
      int64_t idx = OB_INVALID_INDEX;
      if (OB_FAIL(generate_rt_expr(*order_item.expr_, expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (sort_exprs != NULL && OB_FAIL(sort_exprs->push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (has_exist_in_array(all_exprs, expr, &idx)) {
        if (OB_UNLIKELY(idx < 0) || OB_UNLIKELY(idx >= all_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sort expr not in all expr", K(ret), KPC(expr), K(all_exprs), KP(idx));
        }
      } else {
        if (OB_FAIL(all_exprs.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {
          idx = all_exprs.count() - 1;
        }
      }
      if (OB_SUCC(ret)) {
        ObSortFieldCollation field_collation(idx,
            expr->datum_meta_.cs_type_,
            order_item.is_ascending(),
            (order_item.is_null_first() ^ order_item.is_ascending()) ? NULL_LAST : NULL_FIRST);
        ObSortCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
                                                                 expr->datum_meta_.type_,
                                                                 field_collation.null_pos_,
                                                                 field_collation.cs_type_,
                                                                 lib::is_oracle_mode());
        if (OB_ISNULL(cmp_func.cmp_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmp_func is null, check datatype is valid", K(ret));
        } else if (OB_FAIL(sort_collations.push_back(field_collation))) {
          LOG_WARN("failed to push back field collation", K(ret));
        } else if (OB_FAIL(sort_cmp_funcs.push_back(cmp_func))) {
          LOG_WARN("failed to push back sort function", K(ret));
        } else {
          LOG_DEBUG("succ to push back field collation", K(field_collation), K(i), K(order_item));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::get_pdml_partition_id_column_idx(const ObIArray<ObExpr*>& dml_exprs, int64_t& idx)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; i < dml_exprs.count(); i++) {
    const ObExpr* expr = dml_exprs.at(i);
    if (T_PDML_PARTITION_ID == expr->type_) {
      idx = i;
      found = true;
      break;
    }
  }
  if (!found) {
    idx = NO_PARTITION_ID_FLAG;  // NO_PARTITION_ID_FLAG = -2
  }
  return ret;
}
}  // end namespace sql
}  // end namespace oceanbase
