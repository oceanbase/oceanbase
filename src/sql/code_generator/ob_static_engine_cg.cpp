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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_static_engine_cg.h"
#include "sql/optimizer/ob_logical_operator.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_log_sequence.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_join_filter.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_delete.h"
#include "sql/optimizer/ob_log_insert.h"
#include "sql/optimizer/ob_log_update.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_log_function_table.h"
#include "sql/optimizer/ob_log_json_table.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_subplan_filter.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_material.h"
#include "sql/optimizer/ob_log_distinct.h"
#include "sql/optimizer/ob_log_window_function.h"
#include "sql/optimizer/ob_log_select_into.h"
#include "sql/optimizer/ob_log_topk.h"
#include "sql/optimizer/ob_log_count.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/optimizer/ob_log_link_scan.h"
#include "sql/optimizer/ob_log_link_dml.h"
#include "sql/optimizer/ob_log_monitoring_dump.h"
#include "sql/optimizer/ob_log_temp_table_access.h"
#include "sql/optimizer/ob_log_temp_table_insert.h"
#include "sql/optimizer/ob_log_temp_table_transformation.h"
#include "sql/optimizer/ob_log_unpivot.h"
#include "sql/optimizer/ob_log_insert_all.h"
#include "sql/optimizer/ob_log_err_log.h"
#include "sql/optimizer/ob_insert_log_plan.h"
#include "sql/optimizer/ob_log_stat_collector.h"
#include "sql/optimizer/ob_log_optimizer_stats_gathering.h"
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
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/basic/ob_monitoring_dump_op.h"
#include "sql/engine/px/ob_granule_iterator_op.h"
#include "sql/engine/px/exchange/ob_px_receive_op.h"
#include "sql/engine/px/exchange/ob_px_ms_receive_op.h"
#include "sql/engine/px/exchange/ob_px_dist_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_repart_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_reduce_transmit_op.h"
#include "sql/engine/px/exchange/ob_px_fifo_coord_op.h"
#include "sql/engine/px/exchange/ob_px_ordered_coord_op.h"
#include "sql/engine/px/exchange/ob_px_ms_coord_op.h"
#include "sql/engine/px/ob_px_basic_info.h"
#include "sql/engine/connect_by/ob_nl_cnnt_by_with_index_op.h"
#include "sql/engine/join/ob_hash_join_op.h"
#include "sql/engine/join/ob_nested_loop_join_op.h"
#include "sql/engine/join/ob_join_filter_op.h"
#include "sql/engine/sequence/ob_sequence_op.h"
#include "sql/engine/subquery/ob_subplan_filter_op.h"
#include "sql/engine/subquery/ob_subplan_scan_op.h"
#include "sql/engine/subquery/ob_unpivot_op.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/aggregate/ob_scalar_aggregate_op.h"
#include "sql/engine/aggregate/ob_merge_groupby_op.h"
#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/join/ob_merge_join_op.h"
#include "sql/engine/basic/ob_topk_op.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/engine/dml/ob_table_delete_op.h"
#include "sql/engine/dml/ob_table_merge_op.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/engine/dml/ob_table_update_op.h"
#include "sql/engine/dml/ob_table_lock_op.h"
#include "sql/engine/table/ob_table_row_store_op.h"
#include "sql/engine/dml/ob_table_insert_up_op.h"
#include "sql/engine/dml/ob_table_replace_op.h"
#include "sql/engine/window_function/ob_window_function_op.h"
#include "sql/engine/table/ob_row_sample_scan_op.h"
#include "sql/engine/table/ob_block_sample_scan_op.h"
#include "sql/engine/table/ob_table_scan_with_index_back_op.h"
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
#include "sql/engine/pdml/static/ob_px_sstable_insert_op.h"
#include "sql/engine/dml/ob_err_log_op.h"
#include "sql/engine/basic/ob_select_into_op.h"
#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/basic/ob_json_table_op.h"
#include "sql/engine/table/ob_link_scan_op.h"
#include "sql/engine/dml/ob_link_dml_op.h"
#include "sql/engine/dml/ob_table_insert_all_op.h"
#include "sql/engine/basic/ob_stat_collector_op.h"
#include "sql/engine/opt_statistics/ob_optimizer_stats_gathering_op.h"
#include "lib/utility/ob_tracepoint.h"
#include "sql/engine/cmd/ob_load_data_direct_impl.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "share/stat/ob_stat_define.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace sql
{

struct ObFilterTSC
{
  bool operator()(const ObOpSpec &spec) const
  {
    return spec.is_table_scan() && PHY_FAKE_TABLE != spec.type_;
  }
};

int ObStaticEngineCG::generate(const ObLogPlan &log_plan, ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  phy_plan_ = &phy_plan;
  phy_plan_->set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
  opt_ctx_ = &log_plan.get_optimizer_context();
  ObOpSpec *root_spec = NULL;
  if (OB_INVALID_ID != log_plan.get_max_op_id()) {
#ifndef NDEBUG
    phy_plan.bit_set_.reset();
#endif
    phy_plan.set_next_phy_operator_id(log_plan.get_max_op_id());
  }

  bool need_check_output_datum = false;
  ret = OB_E(EventTable::EN_ENABLE_OP_OUTPUT_DATUM_CHECK) ret;
  if (OB_FAIL(ret)) {
    need_check_output_datum = true;
    ret = OB_SUCCESS;
  }
  const bool in_root_job = true;
  const bool is_subplan = false;
  bool check_eval_once = true;
  if (OB_ISNULL(log_plan.get_plan_root())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no logical plan root", K(ret));
  } else if (OB_FAIL(postorder_generate_op(
              *log_plan.get_plan_root(), root_spec, in_root_job, is_subplan,
              check_eval_once, need_check_output_datum))) {
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
  return ret;
}

int ObStaticEngineCG::postorder_generate_op(ObLogicalOperator &op,
                                            ObOpSpec *&spec,
                                            const bool in_root_job,
                                            const bool is_subplan,
                                            bool &check_eval_once,
                                            const bool need_check_output_datum)
{
  int ret = OB_SUCCESS;
  const int64_t child_num = op.get_num_of_child();
  const bool is_exchange = log_op_def::LOG_EXCHANGE == op.get_type();
  bool is_link_scan = (log_op_def::LOG_LINK_SCAN == op.get_type());
  spec = NULL;
  // generate child first.
  ObSEArray<ObOpSpec *, 2> children;
  check_eval_once = true;
  if (is_link_scan) {
    if (OB_ISNULL(op.get_plan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
    } else if (OB_ISNULL(op.get_plan()->get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ptr", K(ret));
    } else {
      phy_plan_->set_has_link_sfd(op.get_plan()->get_stmt()->has_for_update());
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < child_num && !is_link_scan; i++) {
    ObLogicalOperator *child_op = op.get_child(i);
    ObOpSpec *child_spec = NULL;
    bool child_op_check_eval_once = true;
    if (OB_ISNULL(child_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is NULL", K(ret));
    } else if (OB_FAIL(SMART_CALL(postorder_generate_op(*child_op, child_spec,
                                                        in_root_job && is_exchange, is_subplan,
                                                        child_op_check_eval_once,
                                                        need_check_output_datum)))) {
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
  if(OB_SUCC(ret)
     && (op.is_dml_operator()
         || (log_op_def::LOG_FOR_UPD == op.get_type())
         || (log_op_def::LOG_MERGE == op.get_type()))) {
    // if current op is dml child op, it won't check if expr is calculated
    check_eval_once = false;
  }
  // allocate operator spec
  ObPhyOperatorType type = PHY_INVALID;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_phy_op_type(op, type, in_root_job))) {
    LOG_WARN("get phy op type failed", K(ret));
  } else if (type == PHY_INVALID || type >= PHY_END) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid phy operator type", K(ret), K(type));
  } else if (NULL == phy_plan_
             || OB_FAIL(phy_plan_->alloc_op_spec(
      type, children.count(), spec, op.get_op_id()))) {
    ret = NULL == phy_plan_ ? OB_INVALID_ARGUMENT : ret;
    LOG_WARN("allocate operator spec failed",
             K(ret), KP(phy_plan_), K(ob_phy_operator_type_str(type)));
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

  // Generate operator spec.
  // Corresponding ObStaticEngineCG::generate_spec() will be called by
  // ObOperatorFactory::generate_spec() if operator registered appropriately
  // in ob_operator_reg.h.
  // 对于存在subplan的算子, 比如multi part和table lookup算子, 在生成这些原始算子时,
  // 嵌套调用postorder_generate_op生成subplan, 此时会将原始算子对应的cur_op_exprs_
  // reset掉, 导致原始算子对应的calc_exprs不对, 因此在这里会进行临时备份和复原的处理;
  ObSEArray<ObRawExpr *, 8> tmp_cur_op_exprs;
  ObSEArray<ObRawExpr *, 8> tmp_cur_op_self_produced_exprs;
  if (is_subplan) {
    OZ(tmp_cur_op_exprs.assign(cur_op_exprs_));
    OZ(tmp_cur_op_self_produced_exprs.assign(cur_op_self_produced_exprs_));
  }
  cur_op_exprs_.reset();
  cur_op_self_produced_exprs_.reset();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObOperatorFactory::generate_spec(*this, op, *spec, in_root_job))) {
    LOG_WARN("generate operator spec failed",
             K(ret), KP(phy_plan_), K(ob_phy_operator_type_str(type)));
  } else if (OB_FAIL(generate_spec_basic(op, *spec, check_eval_once, need_check_output_datum))) {
    LOG_WARN("generate operator spec basic failed", K(ret));
  } else if (OB_FAIL(generate_spec_final(op, *spec))) {
    LOG_WARN("generate operator spec final failed", K(ret));
  } else if (spec->is_dml_operator()) {
    ObTableModifySpec *dml_spec = static_cast<ObTableModifySpec *>(spec);
    if (dml_spec->use_dist_das()) {
      ObExprFrameInfo *partial_frame = OB_NEWx(ObExprFrameInfo, (&phy_plan_->get_allocator()),
                                               phy_plan_->get_allocator(),
                                               phy_plan_->get_expr_frame_info().rt_exprs_);
      OV(NULL != partial_frame, OB_ALLOCATE_MEMORY_FAILED);
      OZ(ObStaticEngineExprCG::generate_partial_expr_frame(
              *phy_plan_, *partial_frame, cur_op_exprs_));
      OX(dml_spec->expr_frame_info_ = partial_frame);
    }
  }
  if (OB_SUCC(ret) && is_subplan) {
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

int ObStaticEngineCG::check_expr_columnlized(const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_const_or_param_expr()
             || expr->is_const_expr()
             || expr->has_flag(IS_PSEUDO_COLUMN)
             || expr->is_op_pseudo_column_expr()
             || T_MULTI_LOCK_ROWNUM == expr->get_expr_type() // lock_rownum for skip locked is pseudo column
             || T_QUESTIONMARK == expr->get_expr_type()
             || T_CTE_SEARCH_COLUMN == expr->get_expr_type()
             || T_CTE_CYCLE_COLUMN == expr->get_expr_type()
             // T_TABLET_AUTOINC_NEXTVAL is the hidden_pk for heap_table
             // this column is an pseudo column
             || T_TABLET_AUTOINC_NEXTVAL == expr->get_expr_type()
             || T_PSEUDO_ROW_TRANS_INFO_COLUMN == expr->get_expr_type()
             || expr->is_set_op_expr()
             || (expr->is_sys_func_expr() && 0 == expr->get_param_count()) // sys func with no param
             || expr->is_query_ref_expr()
             || expr->is_udf_expr()
             || (expr->is_column_ref_expr() && static_cast<const ObColumnRefRawExpr*>(expr)->is_virtual_generated_column())
             || (expr->is_column_ref_expr() && is_shadow_column(static_cast<const ObColumnRefRawExpr*>(expr)->get_column_id()))) {
    // skip
  } else if ((expr->is_aggr_expr() || (expr->is_win_func_expr()))
             && !expr->has_flag(IS_COLUMNLIZED)) {
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

int ObStaticEngineCG::check_exprs_columnlized(ObLogicalOperator &op)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> child_outputs;

  // clear IS_COLUMNLIZED flag
  if (OB_FAIL(clear_all_exprs_specific_flag(cur_op_exprs_, IS_COLUMNLIZED))) {
    LOG_WARN("clear all exprs specific flag failed", K(ret));
  }
  // get all child output exprs
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_num_of_child(); ++i) {
    ObLogicalOperator *child_op = op.get_child(i);
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
      LOG_WARN("check expr columnlized failed",
               K(ret), K(i), K(cur_op_exprs_.at(i)), KPC(cur_op_exprs_.at(i)));
    }
  }

  return ret;
}

int ObStaticEngineCG::mark_expr_self_produced(ObRawExpr *expr)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(cur_op_self_produced_exprs_.push_back(expr))) {
    LOG_WARN("push back expr to cur_op_product_exprs_ failed", K(ret), KPC(expr));
  }

  return ret;
}

int ObStaticEngineCG::mark_expr_self_produced(const ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(cur_op_self_produced_exprs_.push_back(exprs.at(i)))) {
      LOG_WARN("push back expr to cur_op_product_exprs_ failed", K(ret));
    }
  }

  return ret;
}

int ObStaticEngineCG::mark_expr_self_produced(const ObIArray<ObColumnRefRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(cur_op_self_produced_exprs_.push_back(exprs.at(i)))) {
      LOG_WARN("push back expr to cur_op_product_exprs_ failed", K(ret));
    }
  }

  return ret;
}

int ObStaticEngineCG::set_specific_flag_to_exprs(
    const ObIArray<ObRawExpr *> &exprs, ObExprInfoFlag flag)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i), K(exprs));
    } else {
      OZ(exprs.at(i)->add_flag(flag));
    }
  }

  return ret;
}

int ObStaticEngineCG::clear_all_exprs_specific_flag(
    const ObIArray<ObRawExpr *> &exprs, ObExprInfoFlag flag)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(i), K(exprs));
    } else if (OB_FAIL(exprs.at(i)->clear_flag(flag))) {
      LOG_WARN("failed to clear flag", K(ret));
    }
  }

  return ret;
}


int ObStaticEngineCG::find_rownum_expr_recursively(bool &found, const ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("find_rownum_expr_recursively begin", K(expr->get_param_count()),
           K(expr->get_expr_type()), K(found));
  if (expr->get_expr_type() == T_FUN_SYS_ROWNUM) {
    found = true;
  } else {
    for (auto i = 0; !found && i < expr->get_param_count(); i++) {
      OZ(SMART_CALL(
          find_rownum_expr_recursively(found, expr->get_param_expr(i))));
    }
  }
  LOG_DEBUG("find_rownum_expr_recursively finished", K(expr->get_param_count()),
           K(expr->get_expr_type()), K(found));
  return ret;
}

int ObStaticEngineCG::find_rownum_expr(
    bool &found, const common::ObIArray<ObRawExpr *> &exprs)
{
  LOG_DEBUG("find_rownum_expr begin", K(exprs.count()), K(found));
  int ret = OB_SUCCESS;
  for (auto i = 0; OB_SUCC(ret) && !found && i < exprs.count(); i++) {
    ObRawExpr *expr = exprs.at(i);
    ret = find_rownum_expr_recursively(found, expr);
    LOG_DEBUG(
        "find_rownum_expr_recursively done:", K(expr->get_expr_type()),
        K(found), K(i), K(expr->get_param_count()));
  }
  return ret;
}

// rownum expr can show up in the following 4 cases, check them all
// - filter expr
// - output expr
// - join conditions: equal ("=")
// - join conditions: filter (">", "<", ">=", "<=")
int ObStaticEngineCG::find_rownum_expr(bool &found, ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("find_rownum_expr debug: ", K(op->get_name()), K(found));
  if (OB_FAIL(find_rownum_expr(found, op->get_filter_exprs()))) {
    LOG_WARN("failure encountered during find rownum expr", K(ret));
  } else if (OB_FAIL(find_rownum_expr(found, op->get_output_exprs()))) {
    LOG_WARN("failure encountered during find rownum expr", K(ret));
  } else if (!found && op->get_type() == log_op_def::LOG_JOIN) {
    ObLogJoin *join_op = dynamic_cast<ObLogJoin *>(op);
    // NO NPE check for join_op as it should NOT be nullptr
    if (OB_FAIL(find_rownum_expr(found, join_op->get_other_join_conditions()))) {
      LOG_WARN("failure encountered during find rownum expr", K(ret));
    } else if (OB_FAIL(find_rownum_expr(found, join_op->get_equal_join_conditions()))) {
      LOG_WARN("failure encountered during find rownum expr", K(ret));
    }
  }

  for (auto i = 0; !found && OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
    OZ(SMART_CALL(find_rownum_expr(found, op->get_child(i))));
  }
  return ret;
}

void ObStaticEngineCG::exprs_not_support_vectorize(const ObIArray<ObRawExpr *> &exprs,
                                       bool &found)
{
  FOREACH_CNT_X(e, exprs, !found) {
    if (T_ORA_ROWSCN != (*e)->get_expr_type()) {
      auto col = static_cast<ObColumnRefRawExpr *>(*e);
      if (col->get_result_type().is_urowid()) {
        found = true;
      } else if (col->get_result_type().is_lob_locator()
                 || col->get_result_type().is_json()
                 || col->get_result_type().is_geometry()
                 || col->get_result_type().get_type() == ObLongTextType
                 || col->get_result_type().get_type() == ObMediumTextType
                 || (IS_CLUSTER_VERSION_BEFORE_4_1_0_0
                     && ob_is_text_tc(col->get_result_type().get_type()))) {
        // all lob types not support vectorize in 4.0
        // tinytext and text support vectorize in 4.1
        found = true;
      }
    }
  }
}

// Enable vectorization as long as one logical operator support vectorization.
// One exception is that if any operator explicitly forbidden vectorization,
// disable vectorization for the whole plan
int ObStaticEngineCG::check_vectorize_supported(bool &support,
                                       bool &stop_checking,
                                       double &scan_cardinality,
                                       ObLogicalOperator *op,
                                       bool is_root_job /* = true */)
{
  int ret = OB_SUCCESS;
  if (NULL != op) {
    ObLogPlan *log_plan = NULL;
    ObSqlSchemaGuard *schema_guard = NULL;
    const ObTableSchema *table_schema = nullptr;
    if (OB_ISNULL(log_plan = op->get_plan()) ||
        OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
      support = false;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(op), K(log_plan), K(schema_guard));
    } else {
      bool disable_vectorize = false;
      ObPhyOperatorType type = PHY_INVALID;
      if (OB_FAIL(get_phy_op_type(*op, type, is_root_job))) {
        LOG_WARN("failed to get_phy_op_type", K(op), K(type));
      } else {
      if (ObOperatorFactory::is_vectorized(type)) {
        support = true;
      }
      // Additional check to overwrite support value
      if (log_op_def::LOG_TABLE_SCAN == op->get_type()) {
        // FIXME: bin.lb: disable vectorization for virtual table and virtual column.
        auto tsc = static_cast<ObLogTableScan *>(op);
        const uint64_t table_id = tsc->get_ref_table_id();
        if (is_sys_table(table_id)
            || is_virtual_table(table_id)) {
          disable_vectorize = true;
        }
        if (!support) {
        } else if (OB_FAIL(schema_guard->get_table_schema(tsc->get_table_id(), tsc->get_ref_table_id(), op->get_stmt(), table_schema))) {
          LOG_WARN("get table schema failed", K(tsc->get_table_id()), K(ret));
        } else if (OB_NOT_NULL(table_schema) && 0 < table_schema->get_aux_vp_tid_count()) {
          disable_vectorize = true;
        }
        exprs_not_support_vectorize(tsc->get_access_exprs(), disable_vectorize);
        LOG_DEBUG("TableScan base table rows ", K(op->get_card()));
        scan_cardinality = common::max(scan_cardinality, op->get_card());
      } else if (log_op_def::LOG_SUBPLAN_FILTER == op->get_type()) {
        auto spf_op = static_cast<ObLogSubPlanFilter *>(op);
        if (spf_op->is_update_set()) {
          disable_vectorize = true;
        }
      } else if (log_op_def::LOG_COUNT == op->get_type()) {
        // Disable vectorization when rownum expr appears in its children.
        // Enabling rownum expression vectorization in LOG_COUNT operator's
        // children would generate a batch of rownum with the same value, which
        // breaks rownum defination.
        // E.G:
        //   select rownum,a.c1  from t1 a left outer join t1 b on a.c1=b.c1
        //   and rownum <=b.c1;
        //
        // Above SQL generates the following plan:
        // | ===================================================
        // |ID|OPERATOR              |NAME|EST. ROWS|COST    |
        // ---------------------------------------------------
        // |0 |COUNT                 |    |32670000 |26856715|
        // |1 | HASH RIGHT OUTER JOIN|    |32670000 |22347539|
        // |2 |  TABLE SCAN          |B   |33334    |78605   |
        // |3 |  TABLE SCAN          |A   |100000   |61860   |
        // ===================================================
        //
        // Outputs & filters:
        // -------------------------------------
        //   0 - output([rownum()], [A.C1]), filter(nil)
        //   1 - output([A.C1]), filter(nil),
        //       equal_conds([A.C1 = B.C1]), other_conds(nil)
        //   2 - output([B.C1]), filter([rownum() <= B.C1]),
        //       access([B.C1]), partitions(p0)
        //   3 - output([A.C1]), filter(nil),
        //       access([A.C1]), partitions(p0)
        //         bool has_rownum_expr = false;
        // Expr rownum() shows up in both operator 0 and 2, which leads circular
        // dependency and breaks rownum's defination.
        //
        bool has_rownum_expr = false;
        for (int64_t i = 0; !has_rownum_expr && OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
          OZ(find_rownum_expr(has_rownum_expr, op->get_child(i)));
        }
        if (has_rownum_expr) {
          LOG_DEBUG("rownum expr is in count operator's subplan tree. Stop vectorization execution",
                    K(has_rownum_expr));
          disable_vectorize = true;
        }
      } else if (log_op_def::LOG_JOIN == op->get_type() &&
                  type == PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX) {
        // TODO qubin.qb: support vectorization for connect by with index
        disable_vectorize = true;
      }
      if (OB_SUCC(ret) && !disable_vectorize) {
        const ObDMLStmt *stmt = NULL;
        if (OB_ISNULL(stmt = op->get_stmt())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("dml stmt is null", K(ret));
        } else {
          const ObIArray<ObUserVarIdentRawExpr *> &user_vars = stmt->get_user_vars();
          for (int64_t i = 0; i < user_vars.count() && OB_SUCC(ret); i++) {
            const ObUserVarIdentRawExpr *user_var_expr = NULL;
            if (OB_ISNULL(user_var_expr = user_vars.at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("user var expr is null", K(ret));
            } else if (user_var_expr->get_is_contain_assign()) {
              disable_vectorize = true;
              break;
            }
          }
        }
      }
      if (disable_vectorize) {
        support = false;
        stop_checking = true;
      }
      LOG_DEBUG("check_vectorize_supported", K(disable_vectorize), K(support), K(stop_checking),
                K(op->get_num_of_child()));
      // continue searching until found an operator with vectorization explicitly disabled
      for (int64_t i = 0; !stop_checking && OB_SUCC(ret) && i < op->get_num_of_child(); i++) {
        const bool root = is_root_job && (log_op_def::LOG_EXCHANGE != op->get_type());
        OZ(SMART_CALL(check_vectorize_supported(
            support, stop_checking, scan_cardinality, op->get_child(i), root)));
      }
    }
    }
  }
  return ret;
}

// 从raw expr中获取rt_expr，并将raw expr push到cur_op_exprs_中
//
// 设置operator的rt expr, 从raw expr中获取时，均需要通过该接口，
// 其中ObStaticEngineExprCG::generate_rt_expr是ObRawExpr的友元函数， 可直接访问ObRawExpr中rt expr，
//
// 为什么不是ObRawExpr中直接提供访问rt expr的接口给外部使用， 而是用友元函数的方式处理？
//
// 因为在CG过程中，我们需要获取到该operator所有需要生成的表达式(也就是执行时需要的表达式),
// 用于当前operator中calc_expr的生成, 因此通过友元的方式限制对ObRawExpr中rt expr的获取必须使用
// 该接口，并收集到当前operator中所有需要的表达式. 如果ObRawExpr直接提过访问rt expr的接口，
// 其他人在实现operator的表达式cg时，可能直接通过该接口获取rt expr并赋值给operator中对应表达式，
// 这样就没办法收集到完整的当前operator涉及到的表达式, 可能导致结果出错。
//
// 没有直接将ObStaticEngineCG::generate_rt_expr()作为ObRawExpr友元函数原因：
// ob_static_engine_cg.h和ob_raw_expr.h会存在相互依赖, 需要整理依赖关系, 暂时没处理
int ObStaticEngineCG::generate_rt_expr(const ObRawExpr &src, ObExpr *&dst)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStaticEngineExprCG::generate_rt_expr(src, cur_op_exprs_, dst))) {
    LOG_WARN("fail to push cur op expr", K(ret), K(cur_op_exprs_));
  }
  return ret;
}

// 从raw expr中获取rt_expr，并将raw expr push到cur_op_exprs_中
int ObStaticEngineCG::generate_rt_exprs(const ObIArray<ObRawExpr *> &src,
                                        ObIArray<ObExpr *> &dst)
{
  int ret = OB_SUCCESS;
  dst.reset();
  if (!src.empty()) {
    if (OB_FAIL(dst.reserve(src.count()))) {
      LOG_WARN("init fixed array failed", K(ret), K(src.count()));
    } else {
      FOREACH_CNT_X(raw_expr, src, OB_SUCC(ret)) {
        ObExpr *e = NULL;
        CK(OB_NOT_NULL(*raw_expr));
        OZ(generate_rt_expr(*(*raw_expr), e));
        CK(OB_NOT_NULL(e));
        OZ(dst.push_back(e));
      }
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_spec_basic(ObLogicalOperator &op,
                                          ObOpSpec &spec,
                                          const bool check_eval_once,
                                          const bool need_check_output_datum)
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

  if (log_op_def::LOG_TABLE_SCAN == op.get_type() && PHY_FAKE_CTE_TABLE != spec.type_) {
    // sanity check table scan type, dynamic_cast is acceptable in CG.
    ObLogTableScan *log_tsc = dynamic_cast<ObLogTableScan *>(&op);
    ObTableScanSpec *tsc_spec = dynamic_cast<ObTableScanSpec *>(&spec);
    CK(NULL != log_tsc);
    CK(NULL != tsc_spec);
    OZ(tsc_cg_service_.generate_tsc_filter(*log_tsc, *tsc_spec));
  } else if (log_op_def::LOG_SUBPLAN_FILTER == op.get_type() && spec.is_vectorized()) {
    ObSubPlanFilterSpec *spf_spec = dynamic_cast<ObSubPlanFilterSpec *>(&spec);
    CK(NULL != spf_spec);
    OZ(generate_rt_exprs(op.get_filter_exprs(), spf_spec->filter_exprs_));
    OZ(generate_rt_exprs(op.get_output_exprs(), spf_spec->output_exprs_));
  } else {
    OZ(generate_rt_exprs(op.get_filter_exprs(), spec.filters_));
  }
  OZ(generate_rt_exprs(op.get_output_exprs(), spec.output_));
  if (OB_SUCC(ret) && log_op_def::LOG_LINK_SCAN != op.get_type()) {
    if (OB_FAIL(check_exprs_columnlized(op))) {
      LOG_WARN("check exprs columnlized failed",
               K(ret), K(op.get_name()), K(op.get_op_id()), K(op.get_type()),
               K(cur_op_exprs_.count()));
    }
  }
  // 生成calc expr
  // 1. 获取所有child operator的output exprs
  // 2. 获取当前operator的所有表达式, 并展开
  // 3. 将展开后的表达式中不存在于子节点output exprs的计算表达式
  //    (非T_REF_COLUMN, T_QUESTIONMARK, IS_CONST_LITERAL, 可计算表达式)
  //    对应ObExpr添加到calc_exprs_
  if (OB_SUCC(ret)) {
    // get all child output exprs
    ObSEArray<ObRawExpr *, 16> child_outputs;
    // table lookup operator don't dependent any output result of child except calc_part_id_expr_
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_num_of_child(); i++) {
      ObLogicalOperator *child_op = op.get_child(i);
      if (OB_ISNULL(child_op)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(i));
      } else if (OB_FAIL(append(child_outputs, child_op->get_output_exprs()))) {
        LOG_WARN("fail to append child exprs", K(ret));
      }
    } // for end
    // when non primary key table partition with generate column;
    // main table and index table in table lookup operator has
    // common generate expr as output expr,
    // for generate column in global index, storage will store data
    // for generate column in main table, storage will not store data;
    //
    // in order to avoid the follow error:
    // 1. table lookup generate calc expr will find dependence exprs have been calculated,
    //   which will report an error;
    // 2. if generate column in global index call dependence expr eval, may by core because
    //   datum of dependence expr may not init by storage;
    // the solution is :
    // for generate column in global index scan operator, don't need to calc dependence expr,
    // and don't flatten generate column when generate calc expr, make the
    // dependence expr and generate column self don't add to calc exprs
    bool need_flatten_gen_col = !(log_op_def::LOG_TABLE_SCAN == op.get_type()
                                  && static_cast<ObLogTableScan&>(op).get_is_index_global());
    // get calc exprs
    OZ(generate_calc_exprs(child_outputs, cur_op_exprs_, spec.calc_exprs_, op.get_type(),
                           check_eval_once, need_flatten_gen_col),
                           op.get_op_id(), op.get_name(), K(op.get_type()));
    LOG_DEBUG("just for debug, after generate_calc_exprs", K(ret), K(op.get_op_id()), K(op.get_name()), K(op.get_type()));
  }
  if (OB_SUCC(ret) && need_check_output_datum) {
    OZ(add_output_datum_check_flag(spec));
  }
  return ret;
}

int ObStaticEngineCG::generate_calc_exprs(
    const ObIArray<ObRawExpr *> &dep_exprs,
    const ObIArray<ObRawExpr *> &cur_exprs,
    ObIArray<ObExpr *> &calc_exprs,
    const log_op_def::ObLogOpType log_type,
    bool check_eval_once,
    bool need_flatten_gen_col)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> calc_raw_exprs;
  ObRawExprUniqueSet flattened_cur_op_exprs(true);
  auto filter_func = [&](ObRawExpr *e) { return !has_exist_in_array(dep_exprs, e); };
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(flattened_cur_op_exprs.flatten_and_add_raw_exprs(
                     cur_exprs, filter_func, need_flatten_gen_col))) {
    LOG_WARN("fail to flatten rt exprs", K(ret));
  }
  const ObIArray<ObRawExpr *> &flattened_cur_exprs_arr = flattened_cur_op_exprs.get_expr_array();
  for (int64_t i = 0; OB_SUCC(ret) && i < flattened_cur_exprs_arr.count(); i++) {
    ObRawExpr *raw_expr = flattened_cur_exprs_arr.at(i);
    CK(OB_NOT_NULL(raw_expr));
    bool contain_batch_stmt_parameter = false;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObOptimizerUtil::check_contain_batch_stmt_parameter(
                                          raw_expr,
                                          contain_batch_stmt_parameter))) {
      LOG_WARN("failed to check contain batch stmt parameter", K(ret));
    } else {
      if (!(raw_expr->is_column_ref_expr())
          && !(raw_expr->is_column_ref_expr()
               && !need_flatten_gen_col)
          && !raw_expr->is_op_pseudo_column_expr()
          && !has_exist_in_array(dep_exprs, flattened_cur_exprs_arr.at(i))
          && (raw_expr->has_flag(CNT_VOLATILE_CONST)
              || contain_batch_stmt_parameter // 计算包含batch优化的折叠参数
              || !raw_expr->is_const_expr())) {
        if (check_eval_once
            && T_FUN_SYS_ROWNUM != raw_expr->get_expr_type()
            && T_CTE_SEARCH_COLUMN != raw_expr->get_expr_type()
            && T_CTE_CYCLE_COLUMN != raw_expr->get_expr_type()
            && T_PSEUDO_EXTERNAL_FILE_COL != raw_expr->get_expr_type()
            && !(raw_expr->is_const_expr() || raw_expr->has_flag(IS_DYNAMIC_USER_VARIABLE))
            && !(T_FUN_SYS_PART_HASH == raw_expr->get_expr_type() || T_FUN_SYS_PART_KEY == raw_expr->get_expr_type())) {
          if (raw_expr->is_calculated()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is not from the child_op_output but it has been caculated already",
                     K(ret), K(raw_expr), K(raw_expr->has_flag(CNT_VOLATILE_CONST)), K(raw_expr->is_const_raw_expr()), KPC(raw_expr));
          } else {
            raw_expr->set_is_calculated(true);
            LOG_DEBUG("just for debug, raw_expr->set_is_calculated", K(ret), K(raw_expr), KPC(raw_expr));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(calc_raw_exprs.push_back(raw_expr))) {
          LOG_WARN("fail to push output expr", K(ret));
        }
      }
    }
  } // for end
  if (OB_SUCC(ret) && !calc_raw_exprs.empty()) {
    //此次调用会将calc_exprs push到cur_op_exprs_中，实际是无意义的，暂不处理
    //因为没有更好的接口来获取ObRawExpr中rt_expr_
    if (OB_FAIL(generate_rt_exprs(calc_raw_exprs, calc_exprs))) {
      LOG_WARN("fail to append calc exprs", K(ret), K(calc_raw_exprs));
    }
  }

  return ret;
}

// CAUTION: generate_rt_expr()/generate_rt_exprs() can't be invoked in this function,
// because calc_exprs_ is already generated.
int ObStaticEngineCG::generate_spec_final(ObLogicalOperator &op, ObOpSpec &spec)
{
  int ret = OB_SUCCESS;

  UNUSED(op);
  if (PHY_SUBPLAN_FILTER == spec.type_) {
    FOREACH_CNT_X(e, spec.calc_exprs_, OB_SUCC(ret)) {
      if (T_REF_QUERY == (*e)->type_) {
        ObExprSubQueryRef::Extra::get_info(**e).op_id_ = spec.id_;
      }
    }
  }

  if (PHY_NESTED_LOOP_CONNECT_BY == spec.type_
      || PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX == spec.type_) {
    FOREACH_CNT_X(e, spec.calc_exprs_, OB_SUCC(ret)) {
      if (T_OP_PRIOR == (*e)->type_) {
        (*e)->extra_ = spec.id_;
      }
    }
  }

  // mark insert on dup update scope for T_FUN_SYS_VALUES
  if (PHY_INSERT_ON_DUP == spec.type_ || PHY_MULTI_TABLE_INSERT_UP == spec.type_) {
    FOREACH_CNT_X(e, spec.calc_exprs_, OB_SUCC(ret)) {
      if (T_FUN_SYS_VALUES == (*e)->type_) {
        (*e)->extra_ = 1;
      }
    }
  }

  if (PHY_TABLE_SCAN == spec.type_ ||
      PHY_ROW_SAMPLE_SCAN == spec.type_ ||
      PHY_BLOCK_SAMPLE_SCAN == spec.type_) {
    ObTableScanSpec &tsc_spec = static_cast<ObTableScanSpec&>(spec);
    ObDASScanCtDef &scan_ctdef = tsc_spec.tsc_ctdef_.scan_ctdef_;
    ObDASScanCtDef *lookup_ctdef = tsc_spec.tsc_ctdef_.lookup_ctdef_;
    if (OB_FAIL(scan_ctdef.pd_expr_spec_.set_calc_exprs(spec.calc_exprs_, tsc_spec.max_batch_size_))) {
      LOG_WARN("assign all pushdown exprs failed", K(ret));
    } else if (lookup_ctdef != nullptr &&
        OB_FAIL(lookup_ctdef->pd_expr_spec_.set_calc_exprs(spec.calc_exprs_, tsc_spec.max_batch_size_))) {
      LOG_WARN("assign all pushdown exprs failed", K(ret));
    }
  }

  if (log_op_def::LOG_INSERT == op.get_type()) {
    auto &insert = static_cast<ObLogInsert&>(op);
    phy_plan_->set_is_insert_select(insert.is_insert_select());
  } else if (log_op_def::LOG_INSERT_ALL == op.get_type()) {
    phy_plan_->set_is_insert_select(true);
  }
  if (log_op_def::LOG_SET == op.get_type()
      && !static_cast<ObLogSet &>(op).is_recursive_union()
      && spec.is_vectorized()) {
    ObSetSpec &set_op = static_cast<ObSetSpec&>(spec);
    for (int64_t i = 0; OB_SUCC(ret) && i < set_op.set_exprs_.count(); ++i) {
      ObExpr *expr = set_op.set_exprs_.at(i);
      if (!expr->is_batch_result()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: set expr is not batch result", K(expr->type_), K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogLimit &op, ObLimitSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  spec.calc_found_rows_ = op.get_is_calc_found_rows();
  spec.is_top_limit_ = op.is_top_limit();
  spec.is_fetch_with_ties_ = op.is_fetch_with_ties();
  if (NULL != op.get_limit_expr()) {
    CK(op.get_limit_expr()->get_result_type().is_integer_type());
    OZ(generate_rt_expr(*op.get_limit_expr(), spec.limit_expr_));
    OZ(mark_expr_self_produced(op.get_limit_expr()));
  }
  if (NULL != op.get_offset_expr()) {
    CK(op.get_offset_expr()->get_result_type().is_integer_type());
    OZ(generate_rt_expr(*op.get_offset_expr(), spec.offset_expr_));
    OZ(mark_expr_self_produced(op.get_offset_expr()));
  }
  if (NULL != op.get_percent_expr()) {
    CK(op.get_percent_expr()->get_result_type().is_double());
    OZ(generate_rt_expr(*op.get_percent_expr(), spec.percent_expr_));
    OZ(mark_expr_self_produced(op.get_percent_expr()));
  }
  if (OB_SUCC(ret) && op.is_fetch_with_ties()) {
    OZ(spec.sort_columns_.init(op.get_ties_ordering().count()));
    FOREACH_CNT_X(it, op.get_ties_ordering(), OB_SUCC(ret)) {
      CK(NULL != it->expr_);
      ObExpr *e = NULL;
      OZ(generate_rt_expr(*it->expr_, e));
      OZ(mark_expr_self_produced(it->expr_));
      OZ(spec.sort_columns_.push_back(e));
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_spec(
  ObLogDistinct &op, ObMergeDistinctSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  spec.by_pass_enabled_ = false;
  if (op.get_block_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge distinct has no block mode", K(op.get_algo()), K(op.get_block_mode()), K(ret));
  } else if (OB_FAIL(spec.cmp_funcs_.init(op.get_distinct_exprs().count()))) {
    LOG_WARN("failed to init sort functions", K(ret));
  } else if (OB_FAIL(spec.distinct_exprs_.init(op.get_distinct_exprs().count()))) {
    LOG_WARN("failed to init distinct exprs", K(ret));
  } else {
    ObExpr *expr = nullptr;
    ARRAY_FOREACH(op.get_distinct_exprs(), i) {
      const ObRawExpr* raw_expr = op.get_distinct_exprs().at(i);
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null pointer", K(ret));
      } else if (is_oracle_mode() && OB_UNLIKELY(ObLongTextType == raw_expr->get_data_type()
                                                 || ObLobType == raw_expr->get_data_type())) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("select distinct lob not allowed", K(ret));
      } else if (is_oracle_mode() && OB_UNLIKELY(ObJsonType == raw_expr->get_data_type())) {
        ret = OB_ERR_INVALID_CMP_OP;
        LOG_WARN("select distinct json not allowed", K(ret));
      } else if (raw_expr->is_const_expr()) {
          // distinct const value, 这里需要注意：distinct 1被跳过了，
          // 但ObMergeDistinct中，如果没有distinct列，则默认所有值都相等，这个语义正好是符合预期的。
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

void ObStaticEngineCG::set_murmur_hash_func(
     ObHashFunc &hash_func, const ObExprBasicFuncs *basic_funcs_)
{
  bool is_murmur_hash_v2_ = cur_cluster_version_ >= CLUSTER_VERSION_4_1_0_0;
  hash_func.hash_func_ = is_murmur_hash_v2_ ?
      basic_funcs_->murmur_hash_v2_ : basic_funcs_->murmur_hash_;
  hash_func.batch_hash_func_ = is_murmur_hash_v2_ ?
      basic_funcs_->murmur_hash_v2_batch_ : basic_funcs_->murmur_hash_batch_;
}

int ObStaticEngineCG::generate_spec(
  ObLogDistinct &op, ObHashDistinctSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  spec.is_block_mode_ = op.get_block_mode();
  spec.is_push_down_ = op.is_push_down();
  int64_t init_count = op.get_distinct_exprs().count();
  spec.by_pass_enabled_ = (op.is_push_down() && !op.force_push_down());
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
        } else if (is_oracle_mode() && OB_UNLIKELY(ObLongTextType == raw_expr->get_data_type()
                                                   || ObLobType == raw_expr->get_data_type())) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("select distinct lob not allowed", K(ret));
        } else if (is_oracle_mode() && OB_UNLIKELY(ObJsonType == raw_expr->get_data_type())) {
          ret = OB_ERR_INVALID_CMP_OP;
          LOG_WARN("select distinct json not allowed", K(ret));
        } else if (raw_expr->is_const_expr()) {
            // distinct const value, 这里需要注意：distinct 1被跳过了，
            // 但ObMergeDistinct中，如果没有distinct列，则默认所有值都相等，这个语义正好是符合预期的。
            continue;
        } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.distinct_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_ISNULL(expr->basic_funcs_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: basic funcs is not init", K(ret));
        } else if (expr->obj_meta_.is_ext() || expr->obj_meta_.is_user_defined_sql_type()) {
          // other udt types not supported, xmltype does not have order or map member function
          ret = OB_ERR_NO_ORDER_MAP_SQL;
          LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
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
                                NULL_LAST,//这里null last还是first无所谓
                                expr->datum_meta_.cs_type_,
                                expr->datum_meta_.scale_,
                                lib::is_oracle_mode(),
                                expr->obj_meta_.has_lob_header());
          ObHashFunc hash_func;
          set_murmur_hash_func(hash_func, expr->basic_funcs_);
          if (OB_ISNULL(cmp_func.cmp_func_) || OB_ISNULL(hash_func.hash_func_)
              || OB_ISNULL(hash_func.batch_hash_func_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cmp_func or hash func is null, check datatype is valid",
                    K(cmp_func.cmp_func_), K(hash_func.hash_func_),
                    K(hash_func.batch_hash_func_), K(ret));
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
        } else if (raw_expr->is_const_expr()) {
            // distinct const value, 这里需要注意：distinct 1被跳过了，
            // 但ObMergeDistinct中，如果没有distinct列，则默认所有值都相等，这个语义正好是符合预期的。
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

/*
 * Material算子由于本身没有其他额外运行时的变量需要进行处理，所以Codegen时，不需要做任何处理
 * 都交给了basic等公共方法弄好了，所以这里实现不需要任何处理，全部为UNUSED
 */
int ObStaticEngineCG::generate_spec(ObLogMaterial &op, ObMaterialSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogOptimizerStatsGathering &op, ObOptimizerStatsGatheringSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  ObExecContext * exec_ctx = nullptr;
  if (OB_ISNULL(exec_ctx = opt_ctx_->get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get exec context", K(ret));
  } else {
    spec.table_id_ = op.get_table_id();
    spec.type_ = op.get_osg_type();
    spec.part_level_ = op.get_part_level();
    if (op.is_gather_osg()) {
      uint64_t target_id = 0;
      // default target is 0(root operator), here we traversal the tree to avoid no osg in the root.
      if (OB_FAIL(op.get_target_osg_id(target_id))) {
        LOG_WARN("fail to get merge osg id", K(ret));
      } else {
        spec.set_target_osg_id(target_id);
      }
    }

    if (OB_SUCC(ret) && spec.is_part_table() && !op.is_merge_osg()) {
      if (OB_ISNULL(op.get_calc_part_id_expr())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("calc_part_id_expr is null", K(ret));
      } else if (OB_FAIL(generate_calc_part_id_expr(*op.get_calc_part_id_expr(), nullptr, spec.calc_part_id_expr_))) {
        LOG_WARN("fail to generate calc part id expr", K(ret), KPC(op.get_calc_part_id_expr()));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(spec.col_conv_exprs_.init(op.get_col_conv_exprs().count()))) {
        LOG_WARN("fail to get generated column count", K(ret));
      } else if (OB_FAIL(generate_rt_exprs(op.get_col_conv_exprs(), spec.col_conv_exprs_))) {
        LOG_WARN("fail to generate generated column", K(ret), K(op.get_col_conv_exprs()));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(spec.generated_column_exprs_.init(op.get_generated_column_exprs().count()))) {
        LOG_WARN("fail to get generated column count", K(ret));
      } else if (OB_FAIL(generate_rt_exprs(op.get_generated_column_exprs(), spec.generated_column_exprs_))) {
        LOG_WARN("fail to generate generated column", K(ret), K(op.get_generated_column_exprs()));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(spec.column_ids_.init(op.get_column_ids().count()))) {
        LOG_WARN("fail to init column_ids", K(ret));
      } else if (OB_FAIL(append(spec.column_ids_, op.get_column_ids()))) {
        LOG_WARN("fail to append column id to spec", K(ret), K(spec.column_ids_));
      }
    }
  }
  UNUSED(in_root_job);
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet &op, ObHashUnionSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_hash_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(
  ObLogSet &op, ObHashIntersectSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_hash_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet &op, ObHashExceptSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_hash_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_hash_set_spec(ObLogSet &op, ObHashSetSpec &spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> out_raw_exprs;
  if (OB_FAIL(op.get_pure_set_exprs(out_raw_exprs))) {
    LOG_WARN("failed to get output exprs", K(ret));
  } else if (OB_FAIL(mark_expr_self_produced(out_raw_exprs))) { // set expr
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
    // 初始化compare func和hash func
    for (int64_t i = 0; i < spec.set_exprs_.count() && OB_SUCC(ret); ++i) {
      ObRawExpr *raw_expr = out_raw_exprs.at(i);
      ObExpr *expr = spec.set_exprs_.at(i);
      ObOrderDirection order_direction = default_asc_direction();
      bool is_ascending = is_ascending_direction(order_direction);
      ObSortFieldCollation field_collation(i,
          expr->datum_meta_.cs_type_,
          is_ascending,
          (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
      if (raw_expr->get_expr_type() != expr->type_ ||
          !(T_OP_SET < expr->type_ && expr->type_ <= T_OP_EXCEPT)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: expr type is not match",
          K(raw_expr->get_expr_type()), K(expr->type_));
      } else if (OB_ISNULL(expr->basic_funcs_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: basic funcs is not init", K(ret));
      } else if (ob_is_user_defined_sql_type(expr->datum_meta_.type_) || ob_is_user_defined_pl_type(expr->datum_meta_.type_)) {
        // other udt types not supported, xmltype does not have order or map member function
        ret = OB_ERR_NO_ORDER_MAP_SQL;
        LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
      } else if (OB_FAIL(spec.sort_collations_.push_back(field_collation))) {
        LOG_WARN("failed to push back sort collation", K(ret));
      } else {
        ObSortCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
                                                                expr->datum_meta_.type_,
                                                                field_collation.null_pos_,
                                                                field_collation.cs_type_,
                                                                expr->datum_meta_.scale_,
                                                                lib::is_oracle_mode(),
                                                                expr->obj_meta_.has_lob_header());
        ObHashFunc hash_func;
        set_murmur_hash_func(hash_func, expr->basic_funcs_);
        if (OB_ISNULL(cmp_func.cmp_func_) || OB_ISNULL(hash_func.hash_func_)
            || OB_ISNULL(hash_func.batch_hash_func_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("cmp_func or hash func is null, check datatype is valid",
                   K(cmp_func.cmp_func_), K(hash_func.hash_func_), K(ret));
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

int ObStaticEngineCG::generate_spec(ObLogSet &op, ObMergeUnionSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_merge_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(
  ObLogSet &op, ObMergeIntersectSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_merge_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet &op, ObMergeExceptSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_merge_set_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_cte_pseudo_column_row_desc(ObLogSet &op,
                                                          ObRecursiveUnionAllSpec &phy_set_op)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> &output_raw_exprs = op.get_output_exprs();
  int64_t column_num = output_raw_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < column_num; ++i) {
    if (T_CTE_SEARCH_COLUMN  == output_raw_exprs.at(i)->get_expr_type()) {
      ObPseudoColumnRawExpr* expr = static_cast<ObPseudoColumnRawExpr*>(output_raw_exprs.at(i));
      ObExpr *search_expr = nullptr;
      if (OB_FAIL(generate_rt_expr(*expr, search_expr))) {
        LOG_WARN("generate rt expr failed", K(ret), KPC(expr));
      } else if (OB_FAIL(mark_expr_self_produced(expr))) { // T_CTE_SEARCH_COLUMN
        LOG_WARN("mark expr self produced failed", K(ret));
      } else if (OB_ISNULL(search_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rt expr of search expr is null", K(ret));
      } else {
        phy_set_op.set_search_pseudo_column(search_expr);
      }
    } else if (T_CTE_CYCLE_COLUMN == output_raw_exprs.at(i)->get_expr_type()) {
      ObPseudoColumnRawExpr* expr = static_cast<ObPseudoColumnRawExpr*>(output_raw_exprs.at(i));
      ObExpr *cycle_expr = nullptr;
      if (OB_FAIL(generate_rt_expr(*expr, cycle_expr))) {
        LOG_WARN("generate rt expr failed", K(ret), KPC(expr));
      } else if (OB_FAIL(mark_expr_self_produced(expr))) { // T_CTE_CYCLE_COLUMN
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
          LOG_DEBUG("Cycle values", K(ret), KPC(v_expr), KPC(d_v_expr), KPC(d_v_raw_expr),
                      KPC(v_raw_expr));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSet &op, ObRecursiveUnionAllSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  LOG_DEBUG("static engine cg generate recursive union all", K(spec.get_left()->output_),
            K(spec.get_right()->output_), K(op.get_output_exprs()));
  if (OB_FAIL(generate_recursive_union_all_spec(op, spec))) {
    LOG_WARN("failed to generate spec set", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_merge_set_spec(ObLogSet &op, ObMergeSetSpec &spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> out_raw_exprs;
  if (OB_FAIL(op.get_pure_set_exprs(out_raw_exprs))) {
    LOG_WARN("failed to get output exprs", K(ret));
  } else if (OB_FAIL(mark_expr_self_produced(out_raw_exprs))) { // set expr
    LOG_WARN("fail to mark exprs self produced", K(ret));
  } else if (OB_FAIL(spec.set_exprs_.init(out_raw_exprs.count()))) {
    LOG_WARN("failed to init set exprs", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(out_raw_exprs, spec.set_exprs_))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else if (op.is_set_distinct()
      && (spec.set_exprs_.count() != op.get_map_array().count() && 0 != op.get_map_array().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("output exprs is not match map array", K(ret), K(op.get_map_array().count()),
      K(spec.set_exprs_.count()));
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
        ObExpr *expr = spec.set_exprs_.at(idx);
        ObOrderDirection order_direction = op.get_set_directions().at(i);
        bool is_ascending = is_ascending_direction(order_direction);
        ObSortFieldCollation field_collation(idx,
            expr->datum_meta_.cs_type_,
            is_ascending,
            (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
        if (OB_FAIL(spec.sort_collations_.push_back(field_collation))) {
          LOG_WARN("failed to push back sort collation", K(ret));
        } else if (ob_is_user_defined_sql_type(expr->datum_meta_.type_) || ob_is_user_defined_pl_type(expr->datum_meta_.type_)) {
          // other udt types not supported, xmltype does not have order or map member function
          ret = OB_ERR_NO_ORDER_MAP_SQL;
          LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
        } else {
          ObSortCmpFunc cmp_func;
          cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
                                                                  expr->datum_meta_.type_,
                                                                  field_collation.null_pos_,
                                                                  field_collation.cs_type_,
                                                                  expr->datum_meta_.scale_,
                                                                  lib::is_oracle_mode(),
                                                                  expr->obj_meta_.has_lob_header());
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

int ObStaticEngineCG::generate_recursive_union_all_spec(ObLogSet &op, ObRecursiveUnionAllSpec &spec)
{
  int ret = OB_SUCCESS;
  uint64_t last_cte_table_id = OB_INVALID_ID;
  ObOpSpec *left = nullptr;
  ObOpSpec *right = nullptr;
  if (OB_UNLIKELY(spec.get_child_cnt() != 2)
      || OB_ISNULL(left = spec.get_child(0))
      || OB_ISNULL(right = spec.get_child(1))
      || OB_UNLIKELY(left->get_output_count() != right->get_output_count())
      || OB_UNLIKELY(op.get_output_exprs().count() < left->get_output_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("recursive union all spec should have two children", K(ret), K(spec.get_child_cnt()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < left->get_output_count(); i++) {
      if (left->output_.at(i)->datum_meta_.type_ != right->output_.at(i)->datum_meta_.type_) {
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

    //recursive union all的输出中的前n项一定是T_OP_UNION,与cte表的非伪列一一对应
    ObSEArray<ObExpr *, 2> output_union_exprs;
    ObSEArray<uint64_t, 2> output_union_offsets;
    OZ(spec.output_union_exprs_.init(left->output_.count()));
    ARRAY_FOREACH(left->output_, i)
    {
      ObSetOpRawExpr *output_union_raw_expr =
          static_cast<ObSetOpRawExpr *>(op.get_output_exprs().at(i));
      ObExpr *output_union_expr = nullptr;
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
      } else if (OB_FAIL(mark_expr_self_produced(output_union_raw_expr))) { // set expr
        LOG_WARN("fail to mark expr self produced", K(ret));
      } else if (OB_FAIL(output_union_exprs.push_back(output_union_expr))) {
        LOG_WARN("array push back failed", K(ret));
      } else if (OB_FAIL(output_union_offsets.push_back(output_union_raw_expr->get_idx()))) {
        LOG_WARN("array push back failed", K(ret));
      } else if (OB_FAIL(spec.output_union_exprs_.push_back(nullptr))) { // init nullptr
        LOG_WARN("array push back failed", K(ret));
      }
    }

    // adjust exprs order in output_union_exprs, and add to spec.output_union_exprs_
    ARRAY_FOREACH(output_union_offsets, i) {
      uint64_t idx = output_union_offsets.at(i);
      if (idx >= spec.output_union_exprs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected index in output_union_offsets", K(ret));
      } else if (OB_NOT_NULL(spec.output_union_exprs_.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value in output_union_exprs_, expected nullptr yet", K(ret));
      } else {
        spec.output_union_exprs_[idx] = output_union_exprs.at(i);
      }
    }

    const ObIArray<OrderItem> &search_order = op.get_search_ordering();
    OZ(spec.sort_collations_.init(search_order.count()));
    ARRAY_FOREACH(search_order, i) {
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
    const ObIArray<ColumnItem> &cycle_items = op.get_cycle_items();
    OZ(spec.cycle_by_col_lists_.init(cycle_items.count()));
    ARRAY_FOREACH(cycle_items, i)
    {
      const ObRawExpr* raw_expr = cycle_items.at(i).expr_;
      if (raw_expr->is_column_ref_expr()) {
        uint64_t index = cycle_items.at(i).column_id_ - OB_APP_MIN_COLUMN_ID;
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
  const ObIArray<OrderItem> &sort_keys,
  ObSortCollations &collations,
  ObIArray<ObExpr*> &sort_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(collations.init(sort_keys.count()))) {
    LOG_WARN("failed to init collations", K(ret));
  } else {
    int64_t start_pos = sort_exprs.count();
    for (int64_t i = 0; i < sort_keys.count() && OB_SUCC(ret); ++i) {
      const OrderItem &order_item = sort_keys.at(i);
      ObExpr *expr = nullptr;
      if (order_item.expr_->is_const_expr()) {
        LOG_TRACE("trace sort const", K(*order_item.expr_));
        continue; // sort by const value, just ignore
      } else if (OB_FAIL(generate_rt_expr(*order_item.expr_, expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (OB_FAIL(sort_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        ObSortFieldCollation field_collation(start_pos++, expr->datum_meta_.cs_type_,
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
  const ObSortCollations &collations,
  ObSortFuncs &sort_funcs,
  const ObIArray<ObExpr*> &sort_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sort_funcs.init(collations.count()))) {
    LOG_WARN("failed to init sort functions", K(ret));
  } else {
    for (int64_t i = 0; i < collations.count() && OB_SUCC(ret); ++i) {
      const ObSortFieldCollation &sort_collation = collations.at(i);
      ObExpr* expr = nullptr;
      if (OB_FAIL(sort_exprs.at(sort_collation.field_idx_, expr))) {
        LOG_WARN("failed to get sort exprs", K(ret));
      } else if (ob_is_user_defined_sql_type(expr->datum_meta_.type_) || ob_is_user_defined_pl_type(expr->datum_meta_.type_)) {
        // other udt types not supported, xmltype does not have order or map member function
        ret = OB_ERR_NO_ORDER_MAP_SQL;
        LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
      } else if (is_oracle_mode() && OB_UNLIKELY(ObLongTextType == expr->datum_meta_.type_
                                                 || ObLobType == expr->datum_meta_.type_)) {
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("order by lob not allowed", K(ret));
      } else if (is_oracle_mode() && OB_UNLIKELY(ObJsonType == expr->datum_meta_.type_)) {
        ret = OB_ERR_INVALID_CMP_OP;
        LOG_WARN("order by json not allowed", K(ret));
      } else {
        ObSortCmpFunc cmp_func;
        cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
                                                                expr->datum_meta_.type_,
                                                                sort_collation.null_pos_,
                                                                sort_collation.cs_type_,
                                                                expr->datum_meta_.scale_,
                                                                lib::is_oracle_mode(),
                                                                expr->obj_meta_.has_lob_header());
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
/**
 * Sort将排序列的ObExpr放在最前面，后面跟output_所有的ObExpr，同时会去重
 * all_expr: sort_exprs + output_exprs
 **/
int ObStaticEngineCG::generate_spec(ObLogSort &op, ObSortSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 4> output_exprs;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_rt_exprs(op.get_output_exprs(), output_exprs))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else {
    if (OB_NOT_NULL(op.get_topn_expr())) {
      spec.is_fetch_with_ties_ = op.is_fetch_with_ties();
      OZ(generate_rt_expr(*op.get_topn_expr(), spec.topn_expr_));
      if (OB_NOT_NULL(spec.topn_expr_) && !ob_is_integer_type(spec.topn_expr_->datum_meta_.type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("topn must be int", K(ret), K(*spec.topn_expr_));
      }
    }
    if (OB_NOT_NULL(op.get_topk_limit_expr())) {
      OZ(generate_rt_expr(*op.get_topk_limit_expr(), spec.topk_limit_expr_));
      if (OB_NOT_NULL(op.get_topk_offset_expr())) {
        OZ(generate_rt_expr(*op.get_topk_offset_expr(), spec.topk_offset_expr_));
        if (OB_NOT_NULL(spec.topk_offset_expr_)
            && !ob_is_integer_type(spec.topk_offset_expr_->datum_meta_.type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("topn must be int", K(ret), K(*spec.topk_offset_expr_));
        }
      }
      spec.minimum_row_count_ = op.get_minimum_row_count();
      spec.topk_precision_ = op.get_topk_precision();
    }
    if (OB_SUCC(ret)) {
      ObSEArray<OrderItem, 1> sortkeys;
      if (op.get_part_cnt() > 0 && OB_FAIL(sortkeys.push_back(op.get_hash_sortkey()))) {
        LOG_WARN("failed to push back hash sortkey", K(ret));
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(append(sortkeys, op.get_sort_keys()))) {
        LOG_WARN("failed to append encode sortkeys", K(ret));
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_ISNULL(spec.get_child())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child is null", K(ret));
      } else if (op.enable_encode_sortkey_opt()) {
        ObExpr *encode_expr = nullptr;
        OrderItem order_item = op.get_encode_sortkeys().at(op.get_encode_sortkeys().count() - 1);
        if (OB_FAIL(spec.all_exprs_.init(1 + sortkeys.count()
                                      + spec.get_child()->output_.count()))) {
          LOG_WARN("failed to init all exprs", K(ret));
        } else if (OB_FAIL(generate_rt_expr(*order_item.expr_, encode_expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.all_exprs_.push_back(encode_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      } else {
        if (OB_FAIL(spec.all_exprs_.init(sortkeys.count()
                                      + spec.get_child()->output_.count()))) {
          LOG_WARN("failed to init all exprs", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(fill_sort_info(sortkeys,
          spec.sort_collations_, spec.all_exprs_))) {
        LOG_WARN("failed to sort info", K(ret));
      } else if (OB_FAIL(fill_sort_funcs(
          spec.sort_collations_, spec.sort_cmp_funs_, spec.all_exprs_))) {
        LOG_WARN("failed to sort funcs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(spec.all_exprs_, spec.get_child()->output_))) {
        LOG_WARN("failed to append array no dup", K(ret));
      } else {
        spec.prefix_pos_ = op.get_prefix_pos();
        spec.is_local_merge_sort_ = op.is_local_merge_sort();
        if (op.get_plan()->get_optimizer_context().is_online_ddl()) {
          spec.prescan_enabled_ = true;
        }
        spec.enable_encode_sortkey_opt_ = op.enable_encode_sortkey_opt();
        spec.part_cnt_ = op.get_part_cnt();
        LOG_TRACE("trace order by", K(spec.all_exprs_.count()), K(spec.all_exprs_));
      }
      if (OB_SUCC(ret)) {
        if (spec.sort_collations_.count() != spec.sort_cmp_funs_.count()
            || (spec.part_cnt_ > 0 && spec.part_cnt_ >= spec.sort_collations_.count())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("part cnt or sort size not meet the expection", K(ret),
            K(OB_NOT_NULL(op.get_topn_expr())), K(OB_NOT_NULL(op.get_topk_limit_expr())),
            K(spec.enable_encode_sortkey_opt_), K(spec.prefix_pos_), K(spec.is_local_merge_sort_),
            K(spec.part_cnt_), K(spec.sort_collations_.count()), K(spec.sort_cmp_funs_.count()));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogCount &op, ObCountSpec &spec, const bool in_root_job)
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
    OZ(classify_anti_monotone_filter_exprs(op.get_filter_exprs(),
                                           non_anti_monotone_filters,
                                           anti_monotone_filters));
    if (OB_SUCC(ret) && !anti_monotone_filters.empty()) {
      OZ(generate_rt_exprs(anti_monotone_filters, spec.anti_monotone_filters_));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogValues &op,
                                    ObValuesSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(spec.row_store_.assign(op.get_row_store()))) {
    LOG_WARN("row store assign failed", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExprValues &op,
                                    ObExprValuesSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (!op.get_value_exprs().empty()) {
    spec.contain_ab_param_ = op.contain_array_binding_param();
    // 对于insert values(x,x,x)的batch优化场景，折叠参数没当成一个参数视图，contain_ab_param_是false
    // 但是为了后续的行为上的一致，还是把contain_ab_param_置为spec.ins_values_batch_opt_
    spec.ins_values_batch_opt_ = op.is_ins_values_batch_opt();
    if (spec.ins_values_batch_opt_) {
      spec.contain_ab_param_ = true;
    }
    bool find_group = false;
    int64_t group_idx = -1;
    ObExecContext * exec_ctx = nullptr;
    if (OB_ISNULL(opt_ctx_) || OB_ISNULL(exec_ctx = opt_ctx_->get_exec_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get exec context", K(ret), KP(opt_ctx_));
    } else if (exec_ctx->has_dynamic_values_table()) {
      if (OB_FAIL(op.get_array_param_group_id(group_idx, find_group))) {
        LOG_WARN("failed to get_array_param_group_id", K(ret));
      } else if (find_group) {
        spec.array_group_idx_ = group_idx;
        spec.contain_ab_param_ = true;
      }
    }

    if (OB_FAIL(ret)) { /* do nothing */
    } else if (OB_FAIL(spec.values_.prepare_allocate(op.get_value_exprs().count()))) {
      LOG_WARN("init fixed array failed", K(ret), K(op.get_value_exprs().count()));
    } else if (OB_FAIL(spec.column_names_.prepare_allocate(op.get_value_desc().count()))) {
      LOG_WARN("init fixed array failed", K(ret), K(op.get_value_desc().count()));
    } else if (OB_FAIL(spec.str_values_array_.prepare_allocate(op.get_output_exprs().count()))) {
      LOG_WARN("init fixed array failed", K(ret), K(op.get_output_exprs().count()));
    } else if (OB_FAIL(spec.is_strict_json_desc_.prepare_allocate(op.get_value_desc().count()))) {
      LOG_WARN("init fixed array failed", K(ret), K(op.get_value_desc().count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < op.get_value_exprs().count(); i++) {
        ObRawExpr *raw_expr = op.get_value_exprs().at(i);
        ObExpr *expr = NULL;
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("raw_expr is null", K(ret), K(i), K(raw_expr));
        } else if (OB_FAIL(mark_expr_self_produced(raw_expr))) { // expr values
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
      for (int64_t i = 0; OB_SUCC(ret) && i < op.get_value_desc().count(); i++) {
        ObColumnRefRawExpr *col_expr = op.get_value_desc().at(i);
        spec.is_strict_json_desc_.at(i) = (col_expr->is_strict_json_column() == IS_JSON_CONSTRAINT_STRICT);
        if (OB_FAIL(
            deep_copy_ob_string(
                phy_plan_->get_allocator(),
                col_expr->get_column_name(),
                spec.column_names_.at(i)))) {
          LOG_WARN("failed to deep copy string", K(ret));
        }
      }
      // Add str_values to spec: str_values_ is worked for enum/set type for type conversion.
      // According to code in ob_expr_values_op.cpp, it should be in the same order as output_exprs.
      for (int64_t i = 0; OB_SUCC(ret) && i < op.get_output_exprs().count(); i++) {
        ObRawExpr *output_raw_expr = op.get_output_exprs().at(i);
        const common::ObIArray<common::ObString> &str_values = output_raw_expr->get_enum_set_values();
        if (!str_values.empty()) {
          if (OB_FAIL(spec.str_values_array_.at(i).prepare_allocate(str_values.count()))) {
            LOG_WARN("init fixed array failed", K(ret));
          } else {
            for (int64_t j = 0; OB_SUCC(ret) && j < str_values.count(); ++j) {
              if (OB_FAIL(deep_copy_ob_string(phy_plan_->get_allocator(), str_values.at(j),
                                              spec.str_values_array_.at(i).at(j)))) {
                LOG_WARN("failed to deep copy string", K(ret), K(str_values));
              }
            }
          }
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

  // cg error logging def
  if (OB_FAIL(ret)) {

  } else if (OB_FAIL(dml_cg_service_.generate_err_log_ctdef(op.get_err_log_define(), spec.err_log_ct_def_))) {
    LOG_WARN("fail to cg err_log_ins_ctdef", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogMerge &op,
                                    ObTableMergeSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_merge_with_das(op, spec, in_root_job))) {
    LOG_WARN("fail to generate spec", K(ret));
  }

  return ret;
}

int ObStaticEngineCG::generate_merge_with_das(ObLogMerge &op,
                                              ObTableMergeSpec &spec,
                                              const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  ObSEArray<uint64_t, 4> modified_index_ids;
  const ObMergeStmt *merge_stmt = NULL;
  CK(OB_NOT_NULL(op.get_stmt()));
  CK(OB_NOT_NULL(op.get_primary_dml_info()));
  OZ (op.get_modified_index_id(modified_index_ids));
  if (OB_SUCC(ret)) {
    merge_stmt = static_cast<const ObMergeStmt*>(op.get_stmt());
    spec.has_insert_clause_ = merge_stmt->has_insert_clause();
    spec.has_update_clause_ = merge_stmt->has_update_clause();
    spec.plan_->need_drive_dml_query_ = true;
    spec.use_dist_das_ = op.is_multi_part_dml();
    spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
    spec.table_location_uncertain_ = op.is_table_location_uncertain();

    spec.merge_ctdefs_.set_capacity(modified_index_ids.count());
    // TODO those 2 line must fixed after remove old engine
    OZ(mark_expr_self_produced(op.get_primary_dml_info()->column_exprs_)); // dml columns

    for (int64_t i = 0; OB_SUCC(ret) && i < modified_index_ids.count(); ++i) {
      ObMergeCtDef *merge_ctdef = nullptr;
      if (OB_FAIL(dml_cg_service_.generate_merge_ctdef(op, merge_ctdef, modified_index_ids.at(i)))) {
        LOG_WARN("generate insert ctdef failed", K(ret));
      } else if (OB_FAIL(spec.merge_ctdefs_.push_back(merge_ctdef))) {
        LOG_WARN("store ins ctdef failed", K(ret));
      }
    } // for index_dml_infos end
  }

  ObSEArray<ObRawExpr *, 4> rowkeys;
  IndexDMLInfo *primary_dml_info = op.get_primary_dml_info();
  CK (OB_NOT_NULL(primary_dml_info));
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr *, 4> distinct_keys;
    IndexDMLInfo *primary_dml_info = op.get_primary_dml_info();
    if (OB_FAIL(dml_cg_service_.get_table_unique_key_exprs(op,
                                                           *primary_dml_info,
                                                           distinct_keys))) {
      LOG_WARN("get table unique key exprs failed", K(ret), KPC(primary_dml_info));
    } else if (OB_FAIL(generate_rt_exprs(distinct_keys, spec.distinct_key_exprs_))) {
      LOG_WARN("failed to generate rt exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    OZ(generate_rt_exprs(op.get_update_condition(), spec.update_conds_));
    OZ(generate_rt_exprs(op.get_insert_condition(), spec.insert_conds_));
    OZ(generate_rt_exprs(op.get_delete_condition(), spec.delete_conds_));
  }

  if (OB_SUCC(ret)) {
    ObMergeCtDef *merge_ctdef = spec.merge_ctdefs_.at(0);
    bool find = false;

    if (OB_NOT_NULL(merge_ctdef->upd_ctdef_)) {
      const ObUpdCtDef &upd_ctdef = *merge_ctdef->upd_ctdef_;
      for (int64_t i = 0; OB_SUCC(ret) && !find && i < upd_ctdef.fk_args_.count(); ++i) {
        const ObForeignKeyArg &fk_arg = upd_ctdef.fk_args_.at(i);
        if (!fk_arg.use_das_scan_) {
          find = true;
        }
      }
    }

    if (OB_SUCC(ret) && !find) {
      if (OB_NOT_NULL(merge_ctdef->del_ctdef_)) {
        const ObDelCtDef &del_ctdef = *merge_ctdef->del_ctdef_;
        if (del_ctdef.fk_args_.count() > 0) {
          find = true;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (find) {
        spec.check_fk_batch_ = false;
      } else {
        spec.check_fk_batch_ = true;
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert &op,
                                    ObTableInsertSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_insert_with_das(op, spec))) {
    LOG_WARN("generate insert with das failed", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogicalOperator &op,
                                    ObTableRowStoreSpec &spec,
                                    const bool in_root_job)
{
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  return OB_SUCCESS;
}

int ObStaticEngineCG::generate_insert_with_das(ObLogInsert &op, ObTableInsertSpec &spec)
{
  int ret = OB_SUCCESS;
  spec.check_fk_batch_ = true;
  const ObIArray<IndexDMLInfo *> &index_dml_infos = op.get_index_dml_infos();
  if (OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phy_plan_));
  }

  if (OB_SUCC(ret) && op.get_stmt_id_expr() != nullptr) {
    if (OB_FAIL(generate_rt_expr(*op.get_stmt_id_expr(), spec.ab_stmt_id_))) {
      LOG_WARN("generate ab stmt id expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(spec.ins_ctdefs_.allocate_array(phy_plan_->get_allocator(), 1))) {
      LOG_WARN("allocate insert ctdef array failed", K(ret), K(1));
    } else if (OB_FAIL(spec.ins_ctdefs_.at(0).allocate_array(phy_plan_->get_allocator(),
                                                             index_dml_infos.count()))) {
      LOG_WARN("allocate insert ctdef array failed", K(ret), K(index_dml_infos.count()));
    } else {
      spec.plan_->set_ignore(op.is_ignore());
      spec.plan_->need_drive_dml_query_ = true;
      spec.use_dist_das_ = op.is_multi_part_dml();
      spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
      spec.is_returning_ = op.is_returning();
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
    const IndexDMLInfo *index_dml_info = index_dml_infos.at(i);
    ObInsCtDef *ins_ctdef = nullptr;
    if (OB_ISNULL(index_dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index dml info is null", K(ret));
    } else if (OB_FAIL(dml_cg_service_.generate_insert_ctdef(op, *index_dml_info, ins_ctdef))) {
      LOG_WARN("generate insert ctdef failed", K(ret));
    } else if (OB_ISNULL(ins_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ins_ctdef is null", K(ret));
    } else {
      ins_ctdef->has_instead_of_trigger_ = op.has_instead_of_trigger();
      spec.ins_ctdefs_.at(0).at(i) = ins_ctdef;
    }
  } // for index_dml_infos end
  return ret;
}

int ObStaticEngineCG::generate_exprs_replace_spk(const ObIArray<ObColumnRefRawExpr*> &index_exprs,
                                                 ObIArray<ObExpr *> &access_exprs) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_exprs.count(); i ++) {
    CK(OB_NOT_NULL(index_exprs.at(i)));
    if (OB_SUCC(ret)) {
      ObExpr *expr = NULL;
      if (is_shadow_column(index_exprs.at(i)->get_column_id())) {
        const ObRawExpr *spk_expr = index_exprs.at(i)->get_dependant_expr();
        CK(OB_NOT_NULL(spk_expr));
        OZ(generate_rt_expr(*spk_expr, expr));
      } else { // not shadow column
        ObRawExpr *col_expr = index_exprs.at(i);
        OZ(generate_rt_expr(*col_expr, expr));
      }
      OZ(access_exprs.push_back(expr));
    }
  }

  return ret;
}

int ObStaticEngineCG::convert_index_values(uint64_t table_id,
                                           ObIArray<ObExpr*> &output_exprs,
                                           ObOpSpec *&trs_spec)
{
  int ret = OB_SUCCESS;
  //TODO shengle 处理虚拟列对应calc expr
  if (OB_SUCC(ret)) {
    OZ(phy_plan_->alloc_op_spec(PHY_TABLE_ROW_STORE,
                                0,
                                trs_spec,
                                OB_INVALID_ID));
    OZ(trs_spec->output_.reserve(output_exprs.count()));
    FOREACH_CNT_X(expr, output_exprs, OB_SUCC(ret)) {
      CK(OB_NOT_NULL(*expr));
      OZ(trs_spec->output_.push_back(*expr));
    }
    if (OB_SUCC(ret)) {
      static_cast<ObTableRowStoreSpec *>(trs_spec)->table_id_ = table_id;
    }
  }

  return ret;
}

int ObStaticEngineCG::generate_delete_with_das(ObLogDelete &op, ObTableDeleteSpec &spec)
{
  int ret = OB_SUCCESS;
  const ObIArray<uint64_t> &delete_table_list = op.get_table_list();
  spec.check_fk_batch_ = false;
  if (OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phy_plan_));
  } else {
    spec.plan_->set_ignore(op.is_ignore());
    spec.plan_->need_drive_dml_query_ = true;
    spec.use_dist_das_ = op.is_multi_part_dml();
    spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
    spec.is_returning_ = op.is_returning();
    if (OB_FAIL(spec.del_ctdefs_.allocate_array(phy_plan_->get_allocator(),
                                                delete_table_list.count()))) {
      LOG_WARN("allocate delete ctdef array failed", K(ret));
    }
  }
  // for batch stmt execute
  if (OB_SUCC(ret) && op.get_stmt_id_expr() != nullptr) {
    if (OB_FAIL(generate_rt_expr(*op.get_stmt_id_expr(), spec.ab_stmt_id_))) {
      LOG_WARN("generate ab stmt id expr failed", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < delete_table_list.count(); ++i) {
    const uint64_t loc_table_id = delete_table_list.at(i);
    ObSEArray<IndexDMLInfo *, 4> index_delete_infos;
    ObTableDeleteSpec::DelCtDefArray &ctdefs = spec.del_ctdefs_.at(i);
    if (OB_FAIL(op.get_index_dml_infos(loc_table_id,
                                       index_delete_infos))) {
      LOG_WARN("failed to get index dml infos", K(ret));
    } else if (OB_FAIL(ctdefs.allocate_array(phy_plan_->get_allocator(),
                                             index_delete_infos.count()))) {
      LOG_WARN("allocate delete ctdef array failed", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < index_delete_infos.count(); ++j) {
      const IndexDMLInfo *index_dml_info = index_delete_infos.at(j);
      ObDelCtDef *del_ctdef = nullptr;
      if (OB_ISNULL(index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (OB_FAIL(dml_cg_service_.generate_delete_ctdef(op, *index_dml_info, del_ctdef))) {
        LOG_WARN("generate delete ctdef failed", K(ret));
      } else if (OB_ISNULL(del_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("del_ctdef is null", K(ret));
      } else {
        del_ctdef->has_instead_of_trigger_ = op.has_instead_of_trigger();
        ctdefs.at(j) = del_ctdef;
      }
    }  // for index_dml_infos end
  } //for table_columns end

  for (int64_t i = 0; OB_SUCC(ret) && i < delete_table_list.count(); ++i) {
    ObTableDeleteSpec::DelCtDefArray &ctdefs = spec.del_ctdefs_.at(i);
    ObDelCtDef &del_ctdef = *ctdefs.at(0);
    const uint64_t del_table_id = del_ctdef.das_base_ctdef_.index_tid_;
    bool is_dup = false;
    for (int j = 0; !is_dup && OB_SUCC(ret) && j < delete_table_list.count(); ++j) {
      const uint64_t root_table_id = spec.del_ctdefs_.at(j).at(0)->das_base_ctdef_.index_tid_;
      DASTableIdList parent_tables(phy_plan_->get_allocator());
      if(OB_FAIL(check_fk_nested_dup_del(del_table_id, root_table_id, parent_tables, is_dup))) {
        LOG_WARN("failed to perform nested duplicate table check", K(ret), K(del_table_id), K(root_table_id));
      }
    }
    if (OB_SUCC(ret) && is_dup) {
      del_ctdef.distinct_algo_ = T_HASH_DISTINCT;
    }
  }
  return ret;
}

// 1、ins_ctdef_->storage_row_output_  使用的是insert的convert_expr
// 2、del_ctdef_->storage_row_output_  使用的是column_ref表达式
// 3、spec.table_column_exprs_ 使用的是column_ref表达式
// 4、scan_ctdef_ storage_row_output_ 使用的是column_ref表达式（用于承载回表查询数据）
int ObStaticEngineCG::generate_spec(ObLogInsert &op, ObTableReplaceSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<IndexDMLInfo *> &insert_dml_infos = op.get_index_dml_infos();;
  const ObIArray<IndexDMLInfo *> &del_dml_infos = op.get_replace_index_dml_infos();
  const IndexDMLInfo *primary_dml_info = insert_dml_infos.at(0);
  CK (NULL != primary_dml_info);
  if (OB_SUCC(ret)) {
    spec.is_ignore_ = op.is_ignore();
    spec.plan_->need_drive_dml_query_ = true;
    spec.use_dist_das_ = op.is_multi_part_dml();
    spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
    phy_plan_->set_ignore(op.is_ignore());

    // todo @wenber.wb delete it after support trigger
    ObLogPlan *log_plan = op.get_plan();
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObTableSchema *table_schema = NULL;
    CK(OB_NOT_NULL(log_plan));
    CK(OB_NOT_NULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard()));
    OZ(schema_guard->get_table_schema(MTL_ID(), primary_dml_info->ref_table_id_, table_schema));
    CK(OB_NOT_NULL(table_schema));
    OZ(check_only_one_unique_key(*log_plan, table_schema, spec.only_one_unique_key_));

    // 记录当前主表的rowkey的column_ref表达式和column_id
    CK(primary_dml_info->column_exprs_.count() == primary_dml_info->column_convert_exprs_.count());
    CK(del_dml_infos.count() == insert_dml_infos.count());
    OZ(spec.replace_ctdefs_.allocate_array(phy_plan_->get_allocator(), insert_dml_infos.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_dml_infos.count(); ++i) {
      const IndexDMLInfo *index_dml_info = insert_dml_infos.at(i);
      const IndexDMLInfo *del_index_dml_info = del_dml_infos.at(i);
      ObReplaceCtDef *replace_ctdef = nullptr;
      if (OB_ISNULL(index_dml_info) || OB_ISNULL(del_index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret), K(index_dml_info), K(del_index_dml_info));
      } else if (OB_FAIL(dml_cg_service_.generate_replace_ctdef(op,
                                                                *index_dml_info,
                                                                *del_index_dml_info,
                                                                replace_ctdef))) {
        LOG_WARN("generate replace ctdef failed", K(ret));
      } else if (del_index_dml_info->is_primary_index_) {
        if (OB_FAIL(dml_cg_service_.generate_conflict_checker_ctdef(op,
                                                                    *del_index_dml_info,
                                                                    spec.conflict_checker_ctdef_))) {
          LOG_WARN("generate conflict_checker failed", K(ret));
        } else if (OB_FAIL(mark_expr_self_produced(index_dml_info->column_exprs_))) {
          LOG_WARN("mark self expr failed", K(ret));
        } else {
          bool is_dup = false;
          const uint64_t replace_table_id = replace_ctdef->del_ctdef_->das_base_ctdef_.index_tid_;
          DASTableIdList parent_tables(phy_plan_->get_allocator());
          if(OB_FAIL(check_fk_nested_dup_del(replace_table_id, replace_table_id, parent_tables, is_dup))) {
            LOG_WARN("failed to perform nested duplicate table check", K(ret), K(replace_table_id));
          } else if (is_dup) {
            replace_ctdef->del_ctdef_->distinct_algo_ = T_HASH_DISTINCT;
          }
        }
      }
      spec.replace_ctdefs_.at(i) = replace_ctdef;
      LOG_DEBUG("print replace_ctdef", K(ret), KPC(replace_ctdef));
    } // for index_dml_infos end
  }

  if (OB_SUCC(ret)) {
    ObReplaceCtDef *replace_ctdef = spec.replace_ctdefs_.at(0);
    if (OB_ISNULL(replace_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replace ctdef is null", K(ret));
    } else {
      const ObInsCtDef *ins_ctdef = replace_ctdef->ins_ctdef_;
      const ObDelCtDef *del_ctdef = replace_ctdef->del_ctdef_;
      if (OB_NOT_NULL(ins_ctdef) && OB_NOT_NULL(del_ctdef)) {
        if (del_ctdef->fk_args_.count() > 0) {
          spec.check_fk_batch_ = false;
        } else {
          spec.check_fk_batch_ = true;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert or delete ctdef is null", K(ret), K(ins_ctdef), K(del_ctdef));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogDelete &op,
                                    ObTableDeleteSpec &spec,
                                    const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  ret = generate_delete_with_das(op, spec);
  return ret;
}

int ObStaticEngineCG::generate_update_with_das(ObLogUpdate &op, ObTableUpdateSpec &spec)
{
  int ret = OB_SUCCESS;
  const ObIArray<uint64_t> &table_list = op.get_table_list();
  if (OB_ISNULL(phy_plan_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(phy_plan_));
  } else {
    spec.plan_->set_ignore(op.is_ignore());
    spec.plan_->need_drive_dml_query_ = true;
    spec.use_dist_das_ = op.is_multi_part_dml();
    spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
    spec.is_returning_ = op.is_returning();
    if (OB_FAIL(spec.upd_ctdefs_.allocate_array(phy_plan_->get_allocator(),
                                                table_list.count()))) {
      LOG_WARN("allocate update ctdef array failed", K(ret), K(table_list));
    }
  }
  if (OB_SUCC(ret) && op.get_stmt_id_expr() != nullptr) {
    if (OB_FAIL(generate_rt_expr(*op.get_stmt_id_expr(), spec.ab_stmt_id_))) {
      LOG_WARN("generate ab stmt id expr failed", K(ret));
    }
  }
  bool find = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_list.count(); ++i) {
    const uint64_t loc_table_id = table_list.at(i);
    ObSEArray<IndexDMLInfo *, 4> index_dml_infos;
    ObTableUpdateSpec::UpdCtDefArray &ctdefs = spec.upd_ctdefs_.at(i);
    if (OB_FAIL(op.get_index_dml_infos(loc_table_id, index_dml_infos))) {
      LOG_WARN("failed to get index dml infos", K(ret));
    } else if (OB_FAIL(ctdefs.allocate_array(phy_plan_->get_allocator(), index_dml_infos.count()))) {
      LOG_WARN("allocate update ctdef array failed", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < index_dml_infos.count(); ++j) {
      const IndexDMLInfo *index_dml_info = index_dml_infos.at(j);
      ObUpdCtDef *upd_ctdef = nullptr;
      if (OB_ISNULL(index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (OB_FAIL(dml_cg_service_.generate_update_ctdef(op, *index_dml_info, upd_ctdef))) {
        LOG_WARN("generate update ctdef failed", K(ret));
      } else if (OB_ISNULL(upd_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("upd_ctdef is null", K(ret));
      } else {
        upd_ctdef->has_instead_of_trigger_ = op.has_instead_of_trigger();
        ctdefs.at(j) = upd_ctdef;
        for (int64_t j = 0; j < upd_ctdef->fk_args_.count() && !find; ++j) {
          const ObForeignKeyArg &fk_arg = upd_ctdef->fk_args_.at(j);
          if (!fk_arg.use_das_scan_) {
            find = true;
          }
        }
      }
    }  // for index_dml_infos end
  } //for table_columns end
  if (OB_SUCC(ret)) {
    spec.check_fk_batch_ = !find;
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogUpdate &op, ObTableUpdateSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  CK(typeid(spec) == typeid(ObTableUpdateSpec));
  OZ(generate_update_with_das(op, spec));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogForUpdate &op,
                                    ObTableLockSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  CK(OB_NOT_NULL(op.get_plan()));
  spec.use_dist_das_ = op.is_multi_part_dml();
  spec.set_is_skip_locked(op.is_skip_locked());
  spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
  spec.for_update_wait_us_ = op.get_wait_ts();
  phy_plan_->set_for_update(true);
  spec.is_multi_table_skip_locked_ = op.is_multi_table_skip_locked();

  if (OB_FAIL(spec.lock_ctdefs_.allocate_array(phy_plan_->get_allocator(),
                                               op.get_index_dml_infos().count()))) {
    LOG_WARN("allocate lock ctdef array failed", K(ret), K(op.get_index_dml_infos().count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_index_dml_infos().count(); ++i) {
      const IndexDMLInfo *index_dml_info = op.get_index_dml_infos().at(i);
      ObTableLockSpec::LockCtDefArray &ctdefs = spec.lock_ctdefs_.at(i);
      ObLockCtDef *lock_ctdef = nullptr;
      if (OB_ISNULL(index_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (OB_FAIL(ctdefs.allocate_array(phy_plan_->get_allocator(), 1))) {
        LOG_WARN("allocate lock ctdef array failed", K(ret), K(index_dml_info));
      } else if (OB_FAIL(dml_cg_service_.generate_lock_ctdef(op, *index_dml_info, lock_ctdef))) {
        LOG_WARN("generate delete ctdef failed", K(ret));
      } else {
        ctdefs.at(0) = lock_ctdef;
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert &op, ObTableInsertUpSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  if (op.get_index_dml_infos().empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert dml info is empty", K(ret));
  } else if (OB_ISNULL(op.get_index_dml_infos().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert primary dml info is null", K(ret));
  } else if (op.get_insert_up_index_dml_infos().empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update dml info is empty", K(ret));
  }
  if (OB_SUCC(ret)) {
    const ObIArray<IndexDMLInfo *> &insert_dml_infos = op.get_index_dml_infos();
    const ObIArray<IndexDMLInfo *> &upd_dml_infos = op.get_insert_up_index_dml_infos();
    const IndexDMLInfo *primary_dml_info = insert_dml_infos.at(0);
    spec.is_ignore_ = op.is_ignore();
    spec.plan_->need_drive_dml_query_ = true;
    spec.use_dist_das_ = op.is_multi_part_dml();
    spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
    phy_plan_->set_ignore(op.is_ignore());

    ObLogPlan *log_plan = op.get_plan();
    ObSchemaGetterGuard *schema_guard = NULL;
    const ObTableSchema *table_schema = NULL;
    CK (OB_NOT_NULL(log_plan));
    CK (OB_NOT_NULL(schema_guard = log_plan->get_optimizer_context().get_schema_guard()));
    OZ (schema_guard->get_table_schema(MTL_ID(), primary_dml_info->ref_table_id_, table_schema));
    CK (OB_NOT_NULL(table_schema));

    OZ(spec.insert_up_ctdefs_.allocate_array(phy_plan_->get_allocator(), insert_dml_infos.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_dml_infos.count(); ++i) {
      const IndexDMLInfo *index_dml_info = insert_dml_infos.at(i);
      const IndexDMLInfo *upd_dml_info = upd_dml_infos.at(i);
      ObInsertUpCtDef *insert_up_ctdef = nullptr;
      if (OB_ISNULL(upd_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update dml info is null", K(ret));
      } else if (OB_FAIL(dml_cg_service_.generate_insert_up_ctdef(op,
                                                                  *index_dml_info,
                                                                  *upd_dml_info,
                                                                  insert_up_ctdef))) {
        LOG_WARN("generate insert ctdef failed", K(ret));
      } else {
        spec.insert_up_ctdefs_.at(i) = insert_up_ctdef;
        LOG_DEBUG("print insert_up_ctdef", KPC(insert_up_ctdef));
      }
    } // for index_dml_infos end
  }

  if (OB_SUCC(ret)) {
    ObLogicalOperator *child_op = op.get_child(0);
    const IndexDMLInfo *upd_pri_dml_info = op.get_insert_up_index_dml_infos().at(0);
    const IndexDMLInfo *ins_pri_dml_info = op.get_index_dml_infos().at(0);
    if (OB_ISNULL(child_op)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child_op is null", K(ret));
    } else if (OB_FAIL(dml_cg_service_.generate_conflict_checker_ctdef(
        op,
        *upd_pri_dml_info,
        spec.conflict_checker_ctdef_))) {
      LOG_WARN("generate conflict_checker failed", K(ret));
    } else if (OB_FAIL(mark_expr_self_produced(upd_pri_dml_info->column_exprs_))) {
      LOG_WARN("mark self expr failed", K(ret));
    } else {
      common::ObIArray<ObRawExpr *> &child_output_exprs = child_op->get_output_exprs();
      ObSEArray<ObRawExpr *, 8> contain_exprs;
      ObSEArray<ObRawExpr *, 32> all_need_save_exprs;
      for (int i = 0; OB_SUCC(ret) && i < upd_pri_dml_info->assignments_.count(); i++) {
        ObRawExpr *raw_expr = upd_pri_dml_info->assignments_.at(i).expr_;
        if (OB_FAIL(ObRawExprUtils::extract_contain_exprs(raw_expr,
                                                          child_output_exprs,
                                                          contain_exprs))) {
          LOG_WARN("fail to extract contain exprs", K(ret));
        } else {
          LOG_DEBUG("print one contain_exprs", KPC(raw_expr) ,K(child_output_exprs), K(contain_exprs));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(append(all_need_save_exprs, ins_pri_dml_info->column_convert_exprs_))) {
          LOG_WARN("fail to append expr to array", K(ret));
        } else if (OB_FAIL(append(all_need_save_exprs, contain_exprs))) {
          LOG_WARN("fail to append expr to array", K(ret));
        } else if (OB_FAIL(generate_rt_exprs(all_need_save_exprs, spec.all_saved_exprs_))) {
          LOG_WARN("fail to generate all_saved_expr", K(ret), K(all_need_save_exprs));
        } else {
          LOG_DEBUG("print all_need_save_exprs", K(all_need_save_exprs));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const ObInsertUpCtDef *insert_up_ctdef = spec.insert_up_ctdefs_.at(0);
    if (OB_ISNULL(insert_up_ctdef)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert update ctdef is nullptr", K(ret));
    } else {
      const ObInsCtDef *ins_ctdef = insert_up_ctdef->ins_ctdef_;
      const ObUpdCtDef *upd_ctdef = insert_up_ctdef->upd_ctdef_;
      if (OB_ISNULL(ins_ctdef) || OB_ISNULL(upd_ctdef)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert or update ctdef is null", K(ret));
      } else {
        spec.check_fk_batch_ = true;
        for (int64_t i = 0; i < upd_ctdef->fk_args_.count() && spec.check_fk_batch_; ++i) {
          const ObForeignKeyArg &fk_arg = upd_ctdef->fk_args_.at(i);
          if (!fk_arg.use_das_scan_) {
            spec.check_fk_batch_ = false;
            break;
          }
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTopk &op,
                                    ObTopKSpec &spec,
                                    const bool in_root_job)
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

int ObStaticEngineCG::generate_spec(ObLogSequence &op, ObSequenceSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(spec.nextval_seq_ids_.init(op.get_sequence_ids().count()))) {
    LOG_WARN("failed to init sequence indexes", K(ret));
  } else {
    const ObIArray<uint64_t> &ids = op.get_sequence_ids();
    ARRAY_FOREACH_X(ids, idx, cnt, OB_SUCC(ret)) {
      if (OB_FAIL(spec.add_uniq_nextval_sequence_id(ids.at(idx)))) {
        LOG_WARN("failed to set sequence", K(ids), K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogMonitoringDump &op, ObMonitoringDumpSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  spec.flags_ = op.get_flags();
  spec.dst_op_id_ = op.get_dst_op_id();
  // monitoring dump op check output datum always.
  if (spec.is_vectorized()) {
    spec.need_check_output_datum_ = true;
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogJoinFilter &op, ObJoinFilterSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  spec.set_mode(op.is_create_filter() ? JoinFilterMode::CREATE : JoinFilterMode::USE);
  spec.set_filter_id(op.get_filter_id());
  spec.set_filter_length(op.get_filter_length());
  spec.set_shared_filter_type(op.get_filter_type());
  spec.is_shuffle_ = op.is_use_filter_shuffle();
  if (OB_FAIL(spec.join_keys_.init(op.get_join_exprs().count()))) {
    LOG_WARN("failed to init join keys", K(ret));
  } else if (OB_NOT_NULL(op.get_tablet_id_expr()) &&
      OB_FAIL(generate_calc_part_id_expr(*op.get_tablet_id_expr(), nullptr, spec.calc_tablet_id_expr_))) {
    LOG_WARN("fail to generate calc part id expr", K(ret), KP(op.get_tablet_id_expr()));
  } else if (OB_FAIL(spec.hash_funcs_.init(op.get_join_exprs().count()))) {
    LOG_WARN("failed to init join keys", K(ret));
  } else if (OB_FAIL(spec.cmp_funcs_.init(op.get_join_exprs().count()))) {
    LOG_WARN("failed to init cmp funcs", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(op.get_join_exprs(), spec.join_keys_))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else if (OB_FAIL(spec.need_null_cmp_flags_.assign(op.get_is_null_safe_cmps()))) {
    LOG_WARN("fail to assign cml flags", K(ret));
  } else {
    if (OB_NOT_NULL(spec.calc_tablet_id_expr_)) {
      ObHashFunc hash_func;
      set_murmur_hash_func(hash_func, spec.calc_tablet_id_expr_->basic_funcs_);
      if (OB_FAIL(spec.hash_funcs_.push_back(hash_func))) {
        LOG_WARN("failed to push back hash func", K(ret));
      }
    } else {
      // for create filter op, the compare funcs are only used for comparing left join key
      // the compare funcs will be stored in rf msg finally
      if (op.is_create_filter()) {
        for (int64_t i = 0; i < spec.join_keys_.count() && OB_SUCC(ret); ++i) {
          ObExpr *join_expr = spec.join_keys_.at(i);
          ObHashFunc hash_func;
          ObCmpFunc null_first_cmp;
          ObCmpFunc null_last_cmp;
          null_first_cmp.cmp_func_ = join_expr->basic_funcs_->null_first_cmp_;
          null_last_cmp.cmp_func_ = join_expr->basic_funcs_->null_last_cmp_;
          set_murmur_hash_func(hash_func, join_expr->basic_funcs_);
          if (OB_ISNULL(hash_func.hash_func_) || OB_ISNULL(hash_func.batch_hash_func_) ||
              OB_ISNULL(null_first_cmp.cmp_func_) ||
              OB_ISNULL(null_last_cmp.cmp_func_ )) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("hash func or cmp func is null, check datatype is valid", K(ret));
          } else if (OB_FAIL(spec.hash_funcs_.push_back(hash_func))) {
            LOG_WARN("failed to push back hash func", K(ret));
          } else if (lib::is_mysql_mode() && OB_FAIL(spec.cmp_funcs_.push_back(null_first_cmp))) {
            LOG_WARN("failed to push back null first cmp func", K(ret));
          } else if (lib::is_oracle_mode() && OB_FAIL(spec.cmp_funcs_.push_back(null_last_cmp))) {
            LOG_WARN("failed to push back null last cmp func", K(ret));
          }
        }
      } else {
      // for use filter op, the compare funcs are used to compare left and right
      // the compare funcs will be stored in ObExprJoinFilterContext finally
        const common::ObIArray<common::ObDatumCmpFuncType> &join_filter_cmp_funcs = op.get_join_filter_cmp_funcs();
        if (join_filter_cmp_funcs.count() != spec.join_keys_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("compare func count not match with join_keys count",
              K(join_filter_cmp_funcs.count()), K(spec.join_keys_.count()));
        }
        for (int64_t i = 0; i < spec.join_keys_.count() && OB_SUCC(ret); ++i) {
          ObExpr *join_expr = spec.join_keys_.at(i);
          ObHashFunc hash_func;
          ObCmpFunc cmp_func;
          cmp_func.cmp_func_ = join_filter_cmp_funcs.at(i);
          set_murmur_hash_func(hash_func, join_expr->basic_funcs_);
          if (OB_ISNULL(hash_func.hash_func_) || OB_ISNULL(hash_func.batch_hash_func_) ||
              OB_ISNULL(cmp_func.cmp_func_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("hash func or cmp func is null, check datatype is valid",
                K(hash_func.hash_func_), K(cmp_func.cmp_func_));
          } else if (OB_FAIL(spec.hash_funcs_.push_back(hash_func))) {
            LOG_WARN("failed to push back hash func", K(ret));
          } else if (OB_FAIL(spec.cmp_funcs_.push_back(cmp_func))) {
            LOG_WARN("failed to push back cmp func", K(ret));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t tenant_id =
        op.get_plan()->get_optimizer_context().get_session_info()->get_effective_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      const char *ptr = NULL;
      if (OB_ISNULL(ptr = tenant_config->_px_bloom_filter_group_size.get_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("each group size ptr is null", K(ret));
      } else if (0 == ObString::make_string("auto").case_compare(ptr)) {
        spec.each_group_size_ = -1;
      } else {
        char *end_ptr = nullptr;
        spec.each_group_size_ = strtoull(ptr, &end_ptr, 10); // get group size from tenant config
        if (*end_ptr != '\0') {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("each group size ptr is unexpected", K(ret));
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tenant config", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    // construct runtime filter exec info
    ObRuntimeFilterInfo rf_info;
    const common::ObIArray<int64_t> &p2p_sequence_ids = op.get_p2p_sequence_ids();
    const common::ObIArray<RuntimeFilterType> &rf_types =
        op.get_join_filter_types();
    CK(p2p_sequence_ids.count() > 0 && p2p_sequence_ids.count() == rf_types.count());
    OZ(spec.rf_infos_.init(rf_types.count()));
    ObExpr *join_filter_expr = nullptr;
    for (int i = 0; i < rf_types.count() && OB_SUCC(ret); ++i) {
      rf_info.reset();
      join_filter_expr = nullptr;
      rf_info.p2p_datahub_id_ = p2p_sequence_ids.at(i);
      rf_info.filter_shared_type_ = op.get_filter_type();
      rf_info.dh_msg_type_ = static_cast<ObP2PDatahubMsgBase::ObP2PDatahubMsgType>(rf_types.at(i));
      if (!op.is_create_filter()) {
        const common::ObIArray<ObRawExpr *> &join_filter_exprs =
            op.get_join_filter_exprs();
        if (OB_ISNULL(join_filter_expr =
            reinterpret_cast<ObExpr *>(
            ObStaticEngineExprCG::get_left_value_rt_expr(*join_filter_exprs.at(i))))) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          rf_info.filter_expr_id_ = join_filter_expr->expr_ctx_id_;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(spec.rf_infos_.push_back(rf_info))) {
        LOG_WARN("fail to push back rf info", K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogGranuleIterator &op, ObGranuleIteratorSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  ObLogicalOperator *child_log_op = op.get_child(0);
  spec.set_tablet_size(op.get_tablet_size());
  spec.set_gi_flags(op.get_gi_flags());
  if (log_op_def::LOG_TABLE_SCAN == child_log_op->get_type()) {
    ObLogTableScan *log_tsc = NULL;
    log_tsc = static_cast<ObLogTableScan*>(child_log_op);
    //这里拿index_table_id和table_scan->get_loc_ref_table_id保持一致。
    spec.set_related_id(log_tsc->get_index_table_id());
  }
  ObPhyPlanType execute_type = spec.plan_->get_plan_type();
  if (execute_type == OB_PHY_PLAN_LOCAL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not supported at this time", K(ret));
  } else if (op.get_join_filter_info().is_inited_) {
    spec.bf_info_ = op.get_join_filter_info();
    if (OB_ISNULL(op.get_tablet_id_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("op is null", K(ret));
    } else if (OB_FAIL(generate_calc_part_id_expr(*op.get_tablet_id_expr(), nullptr, spec.tablet_id_expr_))) {
      LOG_WARN("generate calc part id expr failed", K(ret));
    } else {
      set_murmur_hash_func(spec.hash_func_, spec.tablet_id_expr_->basic_funcs_);
    }
  }
  const bool pwj_gi = ObGranuleUtil::pwj_gi(spec.gi_attri_flag_);
  const bool enable_repart_pruning = ObGranuleUtil::enable_partition_pruning(spec.gi_attri_flag_);
  if (OB_SUCC(ret) && (pwj_gi || enable_repart_pruning)) {
    ObSEArray<int64_t, 32> dml_tsc_op_ids;
    ObSEArray<int64_t, 32> dml_tsc_ref_ids;
    if (OB_FAIL(generate_dml_tsc_ids(spec, op, dml_tsc_op_ids, dml_tsc_ref_ids))) {
      LOG_WARN("generate pw dml tsc ids failed", K(ret));
    } else {
      if (pwj_gi && OB_FAIL(spec.pw_dml_tsc_ids_.assign(dml_tsc_op_ids))) {
        LOG_WARN("assign fixed array failed", K(ret));
      } else if (enable_repart_pruning) {
        int64_t idx = -1;
        for (int64_t i = 0; i < dml_tsc_ref_ids.count(); i++) {
          if (op.get_repartition_ref_table_id() == dml_tsc_ref_ids.at(i)) {
            idx = i;
            break;
          }
        }
        if (-1 == idx) {
          // disable repart partition pruning if the pruned tsc is not below this GI.
          spec.set_gi_flags(op.get_gi_flags() & (~GI_ENABLE_PARTITION_PRUNING));
        } else {
          spec.repart_pruning_tsc_idx_ = idx;
        }
        LOG_TRACE("convert gi, set repart pruning tsc idx", K(op.get_repartition_ref_table_id()),
                  K(dml_tsc_op_ids), K(dml_tsc_ref_ids));
      }
    }
  }
  LOG_TRACE("convert gi operator", K(ret),
      "id", spec.id_,
      "tablet size", op.get_tablet_size(),
      "affinitize", op.access_all(),
      "pwj gi", op.pwj_gi(),
      "param down", op.with_param_down(),
      "asc", op.asc_order(),
      "desc", op.desc_order(),
      "flags", op.get_gi_flags(),
      "tsc_ids", spec.pw_dml_tsc_ids_,
      "repart_pruning_tsc_idx", spec.repart_pruning_tsc_idx_,
      K(pwj_gi), K(enable_repart_pruning));
  return ret;
}

int ObStaticEngineCG::generate_dml_tsc_ids(const ObOpSpec &spec, const ObLogicalOperator &op,
                                          ObIArray<int64_t> &dml_tsc_op_ids,
                                          ObIArray<int64_t> &dml_tsc_ref_ids)
{
  int ret = OB_SUCCESS;
  if (IS_DML(spec.type_)) {
    const ObTableModifySpec &modify_spec = static_cast<const ObTableModifySpec &>(spec);
    if (!modify_spec.use_dist_das()) {
      if (OB_FAIL(dml_tsc_op_ids.push_back(spec.id_))) {
        LOG_WARN("push back failed", K(ret));
      // ref table id of modify operator is useless, because we will not pkey to a modify operator.
      } else if (OB_FAIL(dml_tsc_ref_ids.push_back(OB_INVALID_ID))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  } else if (PHY_TABLE_SCAN == spec.type_) {
    if (static_cast<const ObTableScanSpec&>(spec).use_dist_das()) {
      // avoid das tsc collected and processed by gi
    } else if (OB_UNLIKELY(!op.is_table_scan())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected operator type", K(ret), K(op.get_type()));
    } else if (OB_FAIL(dml_tsc_op_ids.push_back(spec.id_))) {
      LOG_WARN("push back failed", K(ret));
    } else if (OB_FAIL(dml_tsc_ref_ids.push_back(static_cast<const ObLogTableScan &>(op).get_index_table_id()))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const ObOpSpec *child_spec = NULL;
    const ObLogicalOperator *child_op = NULL;
    if (OB_UNLIKELY(spec.get_child_cnt() != op.get_num_of_child())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected number of children", K(ret), K(op.get_type()));
    }
    for (int64_t i = 0; i < spec.get_child_cnt() && OB_SUCC(ret); i++) {
      if (OB_ISNULL(child_spec = spec.get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child spec is null", K(ret), K(i));
      } else if (OB_ISNULL(child_op = op.get_child(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child spec is null", K(ret), K(i));
      } else if (OB_FAIL(SMART_CALL(generate_dml_tsc_ids(*child_spec, *child_op,
                                                         dml_tsc_op_ids, dml_tsc_ref_ids)))) {
        LOG_WARN("generate pw dml tsc ids failed", K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::check_rollup_distributor(ObPxTransmitSpec *spec)
{
  int ret = OB_SUCCESS;
  if (spec->is_rollup_hybrid_) {
    ObOpSpec *root = spec->get_child(0);
    while (OB_NOT_NULL(root) && OB_SUCC(ret)) {
      if (ObPhyOperatorType::PHY_MONITORING_DUMP == root->type_) {
        root = root->get_child(0);
      } else if (ObPhyOperatorType::PHY_MATERIAL == root->type_) {
        root = root->get_child(0);
      } else if (ObPhyOperatorType::PHY_MERGE_GROUP_BY == root->type_) {
        break;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: invalid operator type", K(ret), K(root->type_));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_basic_transmit_spec(
  ObLogExchange &op, ObPxTransmitSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (!op.is_producer()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: it's not producer", K(ret));
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
    spec.repartition_ref_table_id_ = op.get_repartition_ref_table_id();
    spec.dist_method_ = op.get_dist_method();
    spec.unmatch_row_dist_method_ = op.get_unmatch_row_dist_method();
    spec.null_row_dist_method_ = op.get_null_row_dist_method();
    spec.set_px_single(op.is_px_single());
    spec.set_px_dop(op.get_parallel());
    spec.set_px_id(op.get_px_id());
    spec.set_dfo_id(op.get_dfo_id());
    spec.set_slave_mapping_type(op.get_slave_mapping_type());
    spec.need_null_aware_shuffle_ = op.need_null_aware_shuffle();
    spec.is_rollup_hybrid_ = op.is_rollup_hybrid();
    spec.is_wf_hybrid_ = op.is_wf_hybrid();
    spec.sample_type_ = op.get_sample_type();
    spec.repartition_table_id_ = op.get_repartition_table_id();
    OZ(check_rollup_distributor(&spec));
    LOG_TRACE("CG transmit", K(op.get_dfo_id()), K(op.get_op_id()),
              K(op.get_dist_method()), K(op.get_unmatch_row_dist_method()));
    }
  }
  // 处理PDML partition_id 伪列
  if (OB_SUCC(ret)) {
    // 仅仅处理repart情况
    if (spec.type_ == PHY_PX_REPART_TRANSMIT) {
      if (NULL != op.get_partition_id_expr()) {
        OZ(generate_rt_expr(*op.get_partition_id_expr(), spec.tablet_id_expr_));
      }
    }
  }
  if (NULL != op.get_random_expr()) {
    OZ(generate_rt_expr(*op.get_random_expr(), spec.random_expr_));
  }
  if (OB_SUCC(ret) && spec.is_wf_hybrid_) {
    if (OB_ISNULL(op.get_wf_hybrid_aggr_status_expr()) || op.get_wf_hybrid_pby_exprs_cnt_array().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("wf_hybrid_aggr_status_expr is null or array is empty", K(ret), K(op.get_wf_hybrid_aggr_status_expr()), K(op.get_wf_hybrid_pby_exprs_cnt_array().count()));
    } else {
      OZ(generate_rt_expr(*op.get_wf_hybrid_aggr_status_expr(), spec.wf_hybrid_aggr_status_expr_));
      OZ(spec.wf_hybrid_pby_exprs_cnt_array_.assign(op.get_wf_hybrid_pby_exprs_cnt_array()));
    }
  }
  if (OB_SUCC(ret) &&
      (ObPQDistributeMethod::PARTITION_RANGE == op.get_dist_method()
       || ObPQDistributeMethod::RANGE == op.get_dist_method())) {
    ObSEArray<ObExpr *, 16> sampling_saving_row;
    OZ(append(sampling_saving_row, spec.get_child()->output_));
    if (NULL != spec.random_expr_) {
      OZ(sampling_saving_row.push_back(spec.random_expr_));
    }
    OZ(spec.sampling_saving_row_.assign(sampling_saving_row));
  }
  return ret;
}

int ObStaticEngineCG::generate_basic_receive_spec(ObLogExchange &op, ObPxReceiveSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if ((in_root_job || op.is_rescanable()) && op.is_sort_local_order()) {
    // root节点不会出现需要local order的exchange-in
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected plan that has merge sort receive with local order in root job", K(ret));
  } else {
    spec.repartition_table_id_ = op.get_repartition_table_id();
    if (OB_FAIL(spec.child_exprs_.init(spec.get_child()->output_.count()))) {
      LOG_WARN("failed to init child exprs", K(ret));
    } else if (spec.bloom_filter_id_array_.assign(op.get_bloom_filter_ids())) {
      LOG_WARN("failed to append bloom filter ids", K(ret));
    } else if (OB_FAIL(spec.child_exprs_.assign(spec.get_child()->output_))) {
      LOG_WARN("failed to append child exprs", K(ret));
    } else if (OB_FAIL(init_recieve_dynamic_exprs(spec.get_child()->output_, spec))) {
      LOG_WARN("fail to init recieve dynamic expr", K(ret));
    } else if (IS_PX_COORD(spec.get_type())) {
      ObPxCoordSpec *coord = static_cast<ObPxCoordSpec*>(&spec);
      coord->set_expected_worker_count(op.get_expected_worker_count());
      const ObTransmitSpec *transmit_spec = static_cast<const ObTransmitSpec*>(spec.get_child());
      coord->qc_id_ = transmit_spec->get_px_id();
      if (op.get_px_batch_op_id() != OB_INVALID_ID) {
        if (log_op_def::LOG_JOIN == op.get_px_batch_op_type()) {
          coord->set_px_batch_op_info(op.get_px_batch_op_id(), PHY_NESTED_LOOP_JOIN);
        } else if (log_op_def::LOG_SUBPLAN_FILTER == op.get_px_batch_op_type()) {
          coord->set_px_batch_op_info(op.get_px_batch_op_id(), PHY_SUBPLAN_FILTER);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("batch op type is unexpected", K(ret), K(op.get_px_batch_op_type()));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(coord->get_table_locations().prepare_allocate(op.get_pruning_table_locations().count(),
          phy_plan_->get_allocator()))) {
        LOG_WARN("fail to init pruning table locations", K(ret));
      } else {
        for (int i = 0; i < op.get_pruning_table_locations().count() && OB_SUCC(ret); ++i) {
          OZ(coord->get_table_locations().at(i).assign(op.get_pruning_table_locations().at(i)));
        }
      }
      LOG_TRACE("map worker to px coordinator", K(spec.get_type()),
        "id", op.get_op_id(),
        "count", op.get_expected_worker_count());
    }
  }
  return ret;
}

int ObStaticEngineCG::init_recieve_dynamic_exprs(const ObIArray<ObExpr *> &child_outputs,
                                                 ObPxReceiveSpec &spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr *, 2> dynamic_consts;
  for (int64_t i = 0; OB_SUCC(ret) && i < child_outputs.count(); i++) {
    if (child_outputs.at(i)->is_dynamic_const_ && !child_outputs.at(i)->is_static_const_) {
      OZ(dynamic_consts.push_back(child_outputs.at(i)));
    }
  }
  OZ(spec.dynamic_const_exprs_.assign(dynamic_consts));

  return ret;
}

// 目前都是假设receive和transmit数据列一致，如果不一致，则拿receive收transmit数据就需要根据child exprs来拿数据
// 暂时取消这种假设，所以需要额外的child_exprs来获取dtl传过来的数据
int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObPxFifoReceiveSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic receive spec", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObPxMSCoordSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic receive spec", K(ret));
  } else if (OB_FAIL(spec.all_exprs_.init(op.get_sort_keys().count() + spec.child_exprs_.count()))) {
    LOG_WARN("failed to init all exprs", K(ret));
  } else if (OB_FAIL(fill_sort_info(op.get_sort_keys(),
      spec.sort_collations_, spec.all_exprs_))) {
    LOG_WARN("failed to sort info", K(ret));
  } else if (OB_FAIL(fill_sort_funcs(
      spec.sort_collations_, spec.sort_cmp_funs_, spec.all_exprs_))) {
    LOG_WARN("failed to sort funcs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(spec.all_exprs_, spec.child_exprs_))) {
    LOG_WARN("failed to append array no dup", K(ret));
  } else {
    spec.is_old_unblock_mode_ = op.is_old_unblock_mode();
    LOG_TRACE("trace merge sort coord", K(spec.is_old_unblock_mode_));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObDirectTransmitSpec &spec, const bool in_root_job)
{
  // do nothing
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  return OB_SUCCESS;
}

int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObDirectReceiveSpec &spec, const bool in_root_job)
{
  // do nothing
  UNUSED(op);
  UNUSED(spec);
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr *, 2> dynamic_consts;
  for (int64_t i = 0; OB_SUCC(ret) && i < spec.output_.count(); i++) {
    if (spec.output_.at(i)->is_dynamic_const_ && !spec.output_.at(i)->is_static_const_) {
      OZ(dynamic_consts.push_back(spec.output_.at(i)));
    }
  }
  OZ(spec.dynamic_const_exprs_.assign(dynamic_consts));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObPxMSReceiveSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic receive spec", K(ret));
  } else if (OB_FAIL(spec.all_exprs_.init(op.get_sort_keys().count() + spec.child_exprs_.count()))) {
    LOG_WARN("failed to init all exprs", K(ret));
  } else if (OB_FAIL(fill_sort_info(op.get_sort_keys(),
      spec.sort_collations_, spec.all_exprs_))) {
    LOG_WARN("failed to sort info", K(ret));
  } else if (OB_FAIL(fill_sort_funcs(
      spec.sort_collations_, spec.sort_cmp_funs_, spec.all_exprs_))) {
    LOG_WARN("failed to sort funcs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(spec.all_exprs_, spec.child_exprs_))) {
    LOG_WARN("failed to append array no dup", K(ret));
  } else {
    spec.local_order_ = op.is_sort_local_order();
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObPxDistTransmitSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_transmit_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  } else if (OB_FAIL(generate_hash_func_exprs(op.get_hash_dist_exprs(),
                                              spec.dist_exprs_,
                                              spec.dist_hash_funcs_))) {
    LOG_WARN("fail generate hash func exprs", K(ret));
  } else if (op.is_pq_range() && OB_FAIL(generate_range_dist_spec(op, spec))) {
    LOG_WARN("fail to generate range dist", K(ret));
  } else if (ObPQDistributeMethod::PARTITION_HASH == op.get_dist_method()
            || ObPQDistributeMethod::SM_BROADCAST == op.get_dist_method()) {
    if (OB_ISNULL(op.get_calc_part_id_expr())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("slave mapping pk_hash's calc_part_id_expr is null", K(ret));
    } else if (OB_FAIL(generate_calc_part_id_expr(*op.get_calc_part_id_expr(), nullptr, spec.calc_tablet_id_expr_))) {
      LOG_WARN("fail to generate calc part id expr", K(ret), KP(op.get_calc_part_id_expr()));
    }
  } else if (spec.dist_hash_funcs_.count() > 0 &&
             (ObPQDistributeMethod::HYBRID_HASH_BROADCAST == op.get_dist_method()
             || ObPQDistributeMethod::HYBRID_HASH_RANDOM == op.get_dist_method())) {
    if (OB_ISNULL(op.get_popular_values())) {
      // no popular values, skip hybrid hash dist method, use traditional hash-hash dist
    } else if (OB_FAIL(generate_popular_values_hash(
                spec.dist_hash_funcs_.at(0), *op.get_popular_values(), spec.popular_values_hash_))){
      LOG_WARN("fail generate popular values", K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_hash_func_exprs(
    const common::ObIArray<ObExchangeInfo::HashExpr> &hash_dist_exprs,
    ExprFixedArray &dist_exprs,
    common::ObHashFuncs &dist_hash_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dist_exprs.init(hash_dist_exprs.count()))) {
    LOG_WARN("failed to init dist exprs", K(ret));
  } else if (OB_FAIL(dist_hash_funcs.init(hash_dist_exprs.count()))) {
    LOG_WARN("failed to init dist exprs", K(ret));
  } else {
    ObExpr *dist_expr = nullptr;
    FOREACH_CNT_X(expr, hash_dist_exprs, OB_SUCC(ret)) {
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
        set_murmur_hash_func(hash_func, dist_expr->basic_funcs_);
        if (OB_ISNULL(hash_func.hash_func_) || OB_ISNULL(hash_func.batch_hash_func_)) {
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

int ObStaticEngineCG::generate_range_dist_spec(
    ObLogExchange &op,
    ObPxDistTransmitSpec &spec)
{
  int ret = OB_SUCCESS;
  ObArray<OrderItem> new_sort_keys;
  if (OB_FAIL(filter_sort_keys(op, op.get_sort_keys(), new_sort_keys))) {
    LOG_WARN("filter sort keys failed", K(ret));
  } else if (OB_FAIL(spec.dist_exprs_.init(new_sort_keys.count()))) {
    LOG_WARN("failed to init all exprs", K(ret));
  } else if (OB_FAIL(fill_sort_info(new_sort_keys,
      spec.sort_collations_, spec.dist_exprs_))) {
    LOG_WARN("failed to sort info", K(ret));
  } else if (OB_FAIL(fill_sort_funcs(spec.sort_collations_,
      spec.sort_cmp_funs_, spec.dist_exprs_))) {
    LOG_WARN("failed to sort funcs", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_popular_values_hash(
    const common::ObHashFunc &hash_func,
    const ObIArray<ObObj> &popular_values_expr,
    common::ObFixedArray<uint64_t, common::ObIAllocator> &popular_values_hash)
{
  int ret = OB_SUCCESS;
  popular_values_hash.set_capacity(popular_values_expr.count());
  popular_values_hash.set_allocator(&phy_plan_->get_allocator());
  // we allocate a temp buffer for datum, it's enough to hold any datatype
  ObDatum datum;
  char buf[OBJ_DATUM_MAX_RES_SIZE];
  datum.ptr_ = buf;
  uint64_t hash_val = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < popular_values_expr.count(); ++i) {
    if (OB_FAIL(datum.from_obj(popular_values_expr.at(i)))) {
      LOG_WARN("fail convert obj to datum", K(ret));
    } else if (OB_FAIL(hash_func.hash_func_(datum, 0, hash_val))) {
      LOG_WARN("fail to do hash", K(ret));
    } else if (OB_FAIL(popular_values_hash.push_back(hash_val))) {
      LOG_WARN("fail push back values", K(ret));
    }
  }
  LOG_DEBUG("generated popular values", K(popular_values_hash));
  return ret;
}

int ObStaticEngineCG::filter_sort_keys(
    ObLogExchange &op,
    const ObIArray<OrderItem> &old_sort_keys,
    ObIArray<OrderItem> &new_sort_keys)
{
  int ret = OB_SUCCESS;
  // filter out partition id expr
  for (int64_t i = 0; OB_SUCC(ret) && i < old_sort_keys.count(); ++i) {
    ObRawExpr *cur_expr = old_sort_keys.at(i).expr_;
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current raw expr is null", K(ret), K(i), KP(cur_expr));
    } else if (cur_expr->is_calc_part_expr()
               || ObItemType::T_PSEUDO_CALC_PART_SORT_KEY == cur_expr->get_expr_type()) {
      // filter out
    } else if (OB_FAIL(new_sort_keys.push_back(old_sort_keys.at(i)))) {
      LOG_WARN("push back order item failed", K(ret), K(i));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObPxRepartTransmitSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_transmit_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  } else if (OB_ISNULL(op.get_calc_part_id_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("repart_transmit's calc_part_id_expr is null", K(ret));
  } else if (OB_FAIL(generate_calc_part_id_expr(*op.get_calc_part_id_expr(), nullptr, spec.calc_tablet_id_expr_))) {
    LOG_WARN("fail to generate calc part id expr", K(ret), KP(op.get_calc_part_id_expr()));
  }
  // for pkey-hash, need add hash expr
  if (OB_SUCC(ret) && op.get_hash_dist_exprs().count() > 0) {
    if (OB_FAIL(generate_hash_func_exprs(op.get_hash_dist_exprs(),
                                         spec.dist_exprs_,
                                         spec.dist_hash_funcs_))) {
      LOG_WARN("fail generate hash func exprs", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // repartition_exprs_ only use by null aware anti join
    // now just support single join key
    // either repart_keys or repart_sub_keys exists join key
    // so we can generate from one of them directly
    if (op.get_repart_keys().count() > 0) {
      if (OB_FAIL(generate_rt_exprs(op.get_repart_keys(), spec.repartition_exprs_))) {
        LOG_WARN("failed to generate repart exprs", K(ret));
      }
    } else if (op.get_repart_sub_keys().count() > 0) {
      if (OB_FAIL(generate_rt_exprs(op.get_repart_sub_keys(), spec.repartition_exprs_))) {
        LOG_WARN("failed to generate repart exprs", K(ret));
      }
    }
  }
  // for pkey-range, generate spec of sort columns
  if (OB_SUCC(ret) && ObPQDistributeMethod::PARTITION_RANGE == op.get_dist_method()) {
    ObArray<OrderItem> sort_keys;
    if (OB_FAIL(filter_sort_keys(op, op.get_sort_keys(), sort_keys))) {
      LOG_WARN("filter out expr of partition id failed", K(ret));
    } else if (OB_FAIL(spec.dist_exprs_.reserve(sort_keys.count()))) {
      LOG_WARN("init dist exprs failed", K(ret));
    } else if (OB_FAIL(fill_sort_info(sort_keys, spec.sort_collations_, spec.dist_exprs_))) {
      LOG_WARN("fill sort info failed", K(ret));
    } else if (OB_FAIL(fill_sort_funcs(spec.sort_collations_, spec.sort_cmp_funs_, spec.dist_exprs_))) {
      LOG_WARN("fill sort funcs failed", K(ret));
    } else if (OB_UNLIKELY(op.get_repart_all_tablet_ids().count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition ids", K(ret), K(op.get_repart_all_tablet_ids().count()));
    } else if (OB_FAIL(spec.ds_tablet_ids_.assign(op.get_repart_all_tablet_ids()))) {
      LOG_WARN("assign partition ids failed", K(ret), K(op.get_repart_all_tablet_ids()));
    }
  }
  return ret;
}


int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObPxReduceTransmitSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_transmit_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  }
  return ret;
}

// dynamic sample use data hub model and px coord is the hub.
// so generate spec for dynamic sample if need
int ObStaticEngineCG::generate_dynamic_sample_spec_if_need(ObLogExchange &op, ObPxCoordSpec &spec)
{
  int ret = OB_SUCCESS;
  if (op.get_sort_keys().count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_sort_keys().count(); ++i) {
      if (OB_FAIL(mark_expr_self_produced(op.get_sort_keys().at(i).expr_))) {
        LOG_WARN("mark self produced expr failed", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      ObArray<OrderItem> sort_keys;
      if (OB_FAIL(filter_sort_keys(op, op.get_sort_keys(), sort_keys))) {
        LOG_WARN("filter out expr of partition id failed", K(ret));
      } else if (OB_FAIL(spec.sort_exprs_.init(sort_keys.count()))) {
        LOG_WARN("failed to init dynamic sample exprs", K(ret));
      } else if (OB_FAIL(fill_sort_info(sort_keys, spec.sort_collations_, spec.sort_exprs_))) {
        LOG_WARN("failed to fill sort info", K(ret));
      } else if (OB_FAIL(fill_sort_funcs(spec.sort_collations_, spec.sort_cmp_funs_, spec.sort_exprs_))) {
        LOG_WARN("failed to fill sort funcs", K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObPxFifoCoordSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  } else if (OB_FAIL(generate_dynamic_sample_spec_if_need(op, spec))) {
    LOG_WARN("generate px_coord_spec for dynamic sample failed", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogExchange &op, ObPxOrderedCoordSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_basic_receive_spec(op, spec, in_root_job))) {
    LOG_WARN("failed to generate basic transmit spec", K(ret));
  } else if (OB_FAIL(generate_dynamic_sample_spec_if_need(op, spec))) {
    LOG_WARN("generate px_coord_spec for dynamic sample failed", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTempTableAccess &op, ObTempTableAccessOpSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*> &access_exprs = op.get_access_exprs();
  bool is_distributed = false;
  if (OB_FAIL(spec.init_output_index(access_exprs.count()))) {
    LOG_WARN("failed to init output index.", K(ret));
  } else if (OB_FAIL(spec.init_access_exprs(access_exprs.count()))) {
    LOG_WARN("failed to init access exprs.", K(ret));
  } else if (OB_FAIL(get_is_distributed(op, is_distributed))) {
    LOG_WARN("failed get is distributed.", K(ret));
  } else {
    phy_plan_->set_use_temp_table(true);
    spec.set_distributed(is_distributed);
    spec.set_temp_table_id(op.get_temp_table_id());
    ARRAY_FOREACH(access_exprs, i) {
      if (OB_ISNULL(access_exprs.at(i)) || !access_exprs.at(i)->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected basic column expr", K(ret));
      } else {
        ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr *>(access_exprs.at(i));
        int64_t index = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
        ObExpr *expr = NULL;
        if (OB_FAIL(spec.add_output_index(index))) {
          LOG_WARN("failed to add output index", K(ret), K(index));
        } else if (OB_FAIL(generate_rt_expr(*access_exprs.at(i), expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.add_access_expr(expr))) {
          LOG_WARN("failed to add output index", K(ret), K(*col_expr));
        } else if (OB_FAIL(mark_expr_self_produced(col_expr))) { // temp table access need to set IS_COLUMNLIZED flag
          LOG_WARN("mark expr self produced failed", K(ret));
        } else { /*do nothing.*/ }
      }
    } // end for
  }
  return ret;
}

int ObStaticEngineCG::get_is_distributed(ObLogTempTableAccess &op, bool &is_distributed)
{
  int ret = OB_SUCCESS;
  is_distributed = false;
  ObLogicalOperator *parent = NULL;
  ObLogPlan *log_plan = op.get_plan();
  const uint64_t temp_table_id = op.get_temp_table_id();
  if (OB_ISNULL(log_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null", K(ret));
  } else {
    ObIArray<ObSqlTempTableInfo*> &temp_tables = log_plan->get_optimizer_context().get_temp_table_infos();
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < temp_tables.count(); ++i) {
      if (OB_ISNULL(temp_tables.at(i)) || OB_ISNULL(temp_tables.at(i)->table_plan_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected null", K(ret));
      } else if (temp_table_id != temp_tables.at(i)->temp_table_id_) {
        /* do nothing */
      } else if (OB_ISNULL(parent = temp_tables.at(i)->table_plan_->get_parent())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        find = true;
        while (OB_NOT_NULL(parent)) {
          if (log_op_def::LOG_EXCHANGE == parent->get_type()) {
            is_distributed = true;
            break;
          } else if (log_op_def::LOG_TEMP_TABLE_TRANSFORMATION == parent->get_type()) {
            break;
          } else {
            parent = parent->get_parent();
          }
        }
      }
    }
    if (OB_SUCC(ret) && !find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("failed to find table plan", K(ret), K(op));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTempTableInsert &op, ObTempTableInsertOpSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *parent = NULL;
  bool is_distributed = false;
  if (OB_ISNULL(parent = op.get_parent())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (log_op_def::LOG_EXCHANGE == parent->get_type()) {
    is_distributed = true;
  }
  spec.set_distributed(is_distributed);
  spec.set_temp_table_id(op.get_temp_table_id());
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTempTableTransformation &op, ObTempTableTransformationOpSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  UNUSED(spec);
  ObSEArray<ObExpr*, 4> output_exprs;
  if (OB_FAIL(generate_rt_exprs(op.get_output_exprs(), output_exprs))) {
    LOG_WARN("failed to generate rt exprs", K(ret));
  } else { /*do nothing.*/ }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan &op, ObTableScanSpec &spec, const bool)
{
  int ret = OB_SUCCESS;

  // generate_spec() interface is override heavy, check type here to avoid generate subclass
  // of ObTableScanSpec unintentionally.
  // e.g.: forget override generate_spec() for subclass of ObTableScanSpec.
  CK(typeid(spec) == typeid(ObTableScanSpec));
  OZ(generate_normal_tsc(op, spec));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan &op, ObFakeCTETableSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_cte_table_spec(op, spec));
  return ret;
}

int ObStaticEngineCG::generate_cte_table_spec(ObLogTableScan &op, ObFakeCTETableSpec &spec)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fake_cte_tables_.push_back(spec.get_id()))) {
    LOG_WARN("fake cte table push back failed", K(ret));
  } else {
    const ObIArray<ObRawExpr*> &access_exprs = op.get_access_exprs();
    LOG_DEBUG("Table scan's access columns", K(access_exprs.count()));
    OZ(spec.column_involved_offset_.init(access_exprs.count()));
    OZ(spec.column_involved_exprs_.init(access_exprs.count()));
    ARRAY_FOREACH(access_exprs, i) {
      ObRawExpr* expr = access_exprs.at(i);
      ObExpr *rt_expr = nullptr;
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
      } else if (OB_FAIL(mark_expr_self_produced(expr))) { // access_exprs in convert_cte_pump need to set IS_COLUMNLIZED flag
        LOG_WARN("mark expr self produced failed", K(ret));
      } else {
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(expr);
        int64_t column_offset = col_expr->get_cte_generate_column_projector_offset();
        if (OB_FAIL(spec.column_involved_offset_.push_back(column_offset))) {
          LOG_WARN("Failed to add column offset", K(ret));
        } else if (OB_FAIL(spec.column_involved_exprs_.push_back(rt_expr))) {
          LOG_WARN("Fail to add column expr", K(ret));
        }
      }
    } // end for
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogUnpivot &op, ObUnpivotSpec &spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  CK(typeid(spec) == typeid(ObUnpivotSpec));
  OX(spec.unpivot_info_ = op.unpivot_info_);
  ObOpSpec *child = spec.get_child(0);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no child", K(ret));
    } else if (!spec.unpivot_info_.has_unpivot()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unpivot_info_ is invalid", K(ret));
    } else {
      CK(0 != spec.unpivot_info_.get_new_column_count());
      spec.max_part_count_ = (child->get_output_count() - spec.unpivot_info_.old_column_count_)
                            / spec.unpivot_info_.get_new_column_count();
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogGroupBy &op, ObScalarAggregateSpec &spec,
    const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op.get_num_of_child() != 1 || OB_ISNULL(op.get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong number of children", K(ret), K(op.get_num_of_child()));
  } else if (OB_FAIL(fill_aggr_infos(op, spec))) {
    OB_LOG(WARN, "fail to fill_aggr_infos", K(ret));
  } else if (nullptr != op.get_aggr_code_expr()
      && OB_FAIL(generate_rt_expr(*op.get_aggr_code_expr(), spec.aggr_code_expr_))) {
    LOG_WARN("failed to generate aggr code expr", K(ret));
  } else if (OB_FAIL(generate_dist_aggr_group(op, spec))) {
    LOG_WARN("failed to generate distinct aggregate function duplicate columns", K(ret));
  } else {
    spec.by_pass_enabled_ = false;
    OZ(set_3stage_info(op, spec));
  }
  LOG_DEBUG("finish generate_spec", K(spec), K(ret));
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogGroupBy &op, ObMergeGroupBySpec &spec,
    const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op.get_num_of_child() != 1 || OB_ISNULL(op.get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong number of children", K(ret), K(op.get_num_of_child()));
  } else {
    spec.set_rollup(op.has_rollup());
    spec.by_pass_enabled_ = false;
    OZ(set_3stage_info(op, spec));
    OZ(set_rollup_adaptive_info(op, spec));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_dist_aggr_group(op, spec))) {
      LOG_WARN("failed to generate distinct aggregate function duplicate columns", K(ret));
    } else if (OB_FAIL(spec.distinct_exprs_.init(op.get_distinct_exprs().count()))){
      LOG_WARN("failed to init distinct column indexes", K(ret));
    } else if (OB_FAIL(generate_rt_exprs(op.get_distinct_exprs(), spec.distinct_exprs_))) {
      LOG_WARN("failed to generate distinct column", K(ret));
    } else if (nullptr != op.get_aggr_code_expr()
        && OB_FAIL(generate_rt_expr(*op.get_aggr_code_expr(), spec.aggr_code_expr_))) {
      LOG_WARN("failed to generate aggr code expr", K(ret));
    }
  }

  // 1. add group columns
  if (OB_SUCC(ret)) {
    common::ObIArray<ObRawExpr*> &group_exprs = op.get_group_by_exprs();
    if (OB_FAIL(spec.init_group_exprs(group_exprs.count()))) {
      OB_LOG(WARN, "fail to init group expr", K(ret));
    }
    ARRAY_FOREACH(group_exprs, i) {
      const ObRawExpr *raw_expr = group_exprs.at(i);
      ObExpr *expr = NULL;
      if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_FAIL(spec.add_group_expr(expr))) {
        OB_LOG(WARN, "fail to add_group_expr", K(ret));
      }
    } // end for
  }


  // 2. add rollup columns
  if (OB_SUCC(ret)) {
    common::ObIArray<ObRawExpr*> &rollup_exprs = op.get_rollup_exprs();
    if (OB_FAIL(spec.init_rollup_exprs(rollup_exprs.count()))) {
      OB_LOG(WARN, "fail to init rollup expr", K(ret));
    } else if (OB_FAIL(spec.init_duplicate_rollup_expr(rollup_exprs.count()))) {
      OB_LOG(WARN, "fail to init_duplicate_rollup_expr", K(ret));
    }
    bool is_duplicate = false;
    const bool is_oracle_mode = lib::is_oracle_mode();
    // In the two different modes of mysql and oracle, the behavior of rollup duplicate columns
    // is different. For example, when there is one row [1] in t1, in oracle mode
    // select c1 from t1 group by rollup(c1, c1);
    // +------+
    // | c1   |
    // +------+
    // |    1 |
    // |    1 |
    // | NULL |
    // +------+
    // In mysql mode
    // select c1 from t1 group by c1, c1 with rollup;
    // +------+
    // | c1   |
    // +------+
    // |    1 |
    // | NULL |
    // | NULL |
    // +------+
    // So we need to distinguish between two modes when initializing `is_duplicate_rollup_expr_`
    // array. For oracle, it is initialized from left to right. For mysql, it is initialized from
    // right to left.
    ARRAY_FOREACH(rollup_exprs, i) {
      const ObRawExpr* raw_expr = rollup_exprs.at(i);
      ObExpr *expr = NULL;
      if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (FALSE_IT(is_duplicate = (is_oracle_mode && // set is_duplicate to false in mysql
                                           (has_exist_in_array(spec.group_exprs_, expr)
                                             || has_exist_in_array(spec.rollup_exprs_, expr))))) {
      } else if (OB_FAIL(spec.is_duplicate_rollup_expr_.push_back(is_duplicate))) {
        OB_LOG(WARN, "fail to push distinct_rollup_expr", K(ret));
      } else if (OB_FAIL(spec.add_rollup_expr(expr))) {
        OB_LOG(WARN, "fail to add_rollup_expr", K(ret));
      } else {
        LOG_DEBUG("rollup is duplicate key", K(is_duplicate));
      }
    } // end for
    // mysql mode, reinit duplicate rollup expr from right to left
    if (OB_SUCC(ret) && !is_oracle_mode && rollup_exprs.count() > 0) {
      for (int64_t i = spec.rollup_exprs_.count() - 2; OB_SUCC(ret) && i >= 0; --i) {
        if (has_exist_in_array(spec.group_exprs_, spec.rollup_exprs_.at(i))) {
          spec.is_duplicate_rollup_expr_.at(i) = true;
        } else {
          for (int64_t j = i + 1; !spec.is_duplicate_rollup_expr_.at(i)
                                      && j < spec.rollup_exprs_.count(); ++j) {
            if (spec.rollup_exprs_.at(i) == spec.rollup_exprs_.at(j)) {
              spec.is_duplicate_rollup_expr_.at(i) = true;
            }
          }
        }
      }
    }
  }

  // 3. add aggr columns
  if (OB_SUCC(ret)) {
    // first stage should not use merge-groupby
    // TODO: need judge distinct_exprs should to be implicit aggr expr
    if (OB_FAIL(fill_aggr_infos(op, spec, &spec.group_exprs_, &spec.rollup_exprs_, nullptr))) {
      OB_LOG(WARN, "fail to fill_aggr_infos", K(ret));
    }
  }
  LOG_DEBUG("succ to generate_spec", K(spec), K(ret));
  return ret;
}

int ObStaticEngineCG::set_3stage_info(ObLogGroupBy &op, ObGroupBySpec &spec)
{
  int ret = OB_SUCCESS;
  spec.aggr_stage_ = op.get_aggr_stage();
  spec.aggr_code_idx_ = op.get_aggr_code_idx();
  return ret;
}

int ObStaticEngineCG::set_rollup_adaptive_info(ObLogGroupBy &op, ObMergeGroupBySpec &spec)
{
  int ret = OB_SUCCESS;
  spec.rollup_status_ = op.get_rollup_status();
  spec.is_parallel_ = ObRollupStatus::ROLLUP_DISTRIBUTOR == spec.rollup_status_ ? true : false;
  if (nullptr != op.get_rollup_id_expr()) {
    OZ(generate_rt_expr(*op.get_rollup_id_expr(), spec.rollup_id_expr_));
  }
  if (OB_SUCC(ret) && 0 < op.get_inner_sort_keys().count()) {
    ObIArray<OrderItem> &sork_keys = op.get_inner_sort_keys();
    if (!op.has_encode_sort()) {
      if (OB_FAIL(spec.sort_exprs_.init(sork_keys.count()))) {
        LOG_WARN("failed to init all exprs", K(ret));
      }
    } else {
      if (1 != op.get_inner_ecd_sort_keys().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: encode sortkey expr more than one", K(ret), K(op.get_inner_ecd_sort_keys().count()));
      } else {
        ObExpr *encode_expr = nullptr;
        OrderItem order_item = op.get_inner_ecd_sort_keys().at(0);
        if (OB_FAIL(spec.sort_exprs_.init(1 + sork_keys.count()))) {
          LOG_WARN("failed to init all exprs", K(ret));
        } else if (OB_FAIL(generate_rt_expr(*order_item.expr_, encode_expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.sort_exprs_.push_back(encode_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(fill_sort_info(sork_keys, spec.sort_collations_, spec.sort_exprs_))) {
      LOG_WARN("failed to sort info", K(ret));
    } else if (OB_FAIL(fill_sort_funcs(
        spec.sort_collations_, spec.sort_cmp_funcs_, spec.sort_exprs_))) {
      LOG_WARN("failed to sort funcs", K(ret));
    } else {
      spec.enable_encode_sort_ = op.has_encode_sort();
      LOG_TRACE("debug enable encode sort", K(op.has_encode_sort()));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_dist_aggr_group(ObLogGroupBy &op, ObGroupBySpec &spec)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret) && OB_FAIL(spec.dist_aggr_group_idxes_.init(op.get_distinct_aggr_batch().count()))) {
    LOG_WARN("failed to init array", K(ret));
  }
  int64_t aggr_group_idx = 0;
  for (int64_t i = 0; i < op.get_distinct_aggr_batch().count() && OB_SUCC(ret); ++i) {
    const ObDistinctAggrBatch &distinct_batch = op.get_distinct_aggr_batch().at(i);
    aggr_group_idx += distinct_batch.mocked_aggrs_.count();
    if (OB_FAIL(spec.dist_aggr_group_idxes_.push_back(aggr_group_idx))) {
      LOG_WARN("failed to push back aggr group aggr index", K(ret));
    }
  } // end for
  return ret;
}

int ObStaticEngineCG::generate_dist_aggr_distinct_columns(
  ObLogGroupBy &op, ObHashGroupBySpec &spec)
{
  int ret = OB_SUCCESS;
  if (op.is_three_stage_aggr()) {
    // duplicate column start with aggr_code
    int64_t dist_col_group_idx = 0;
    for (int64_t i = 0; i < op.get_distinct_aggr_batch().count() && OB_SUCC(ret); ++i) {
      const ObDistinctAggrBatch &distinct_batch = op.get_distinct_aggr_batch().at(i);
      dist_col_group_idx += distinct_batch.mocked_params_.count();
      LOG_DEBUG("debug distinct columns", K(i), K(distinct_batch.mocked_params_.count()),
        K(dist_col_group_idx));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(spec.dist_col_group_idxs_.init(dist_col_group_idx))) {
        LOG_WARN("failed to init array", K(ret));
      } else if (OB_FAIL(spec.org_dup_cols_.init(dist_col_group_idx))) {
        LOG_WARN("failed to push back org_expr", K(ret));
      } else if (OB_FAIL(spec.new_dup_cols_.init(dist_col_group_idx))) {
        LOG_WARN("failed to push back org_expr", K(ret));
      }
    }
    LOG_DEBUG("debug generate distinct aggr duplicate info", K(ret), K(dist_col_group_idx),
      K(op.get_distinct_aggr_batch().count()));
    dist_col_group_idx = op.get_aggr_code_idx() + 1;
    for (int64_t i = 0; i < op.get_distinct_aggr_batch().count() && OB_SUCC(ret); ++i) {
      const ObDistinctAggrBatch &distinct_batch = op.get_distinct_aggr_batch().at(i);
      ObExpr *org_expr = nullptr;
      ObExpr *dup_expr = nullptr;
      dist_col_group_idx += distinct_batch.mocked_params_.count();
      if (OB_FAIL(spec.dist_col_group_idxs_.push_back(dist_col_group_idx))) {
        LOG_WARN("failed to push back aggr group aggr inndex", K(ret));
      }
      for (int64_t j = 0; op.is_first_stage() && j < distinct_batch.mocked_params_.count() && OB_SUCC(ret); ++j) {
        const std::pair<ObRawExpr *, ObRawExpr *> &pair = distinct_batch.mocked_params_.at(j);
        if (OB_FAIL(generate_rt_expr(*pair.first, org_expr))) {
          LOG_WARN("failed to generate_rt_expr", K(ret));
        } else if (OB_FAIL(generate_rt_expr(*pair.second, dup_expr))) {
          LOG_WARN("failed to generate_rt_expr", K(ret));
        } else if (OB_FAIL(spec.org_dup_cols_.push_back(org_expr))) {
          LOG_WARN("failed to push back org_expr", K(ret));
        } else if (OB_FAIL(spec.new_dup_cols_.push_back(dup_expr))) {
          LOG_WARN("failed to push back org_expr", K(ret));
        }
      } // end inner for
    } // end outer for

    // it's second or third stage
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_dist_aggr_group(op, spec))) {
      LOG_WARN("failed to generate dist aggr group", K(ret), K(spec.id_));
    } else if (0 == spec.dist_col_group_idxs_.count() ||
        spec.dist_col_group_idxs_.count() != spec.dist_aggr_group_idxes_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: distinct columns group is not match distinct aggregate function",
        K(ret), K(spec.id_),
        K(spec.dist_aggr_group_idxes_.count()),
        K(spec.dist_col_group_idxs_.count()));
    } else if (op.is_first_stage()) {
      spec.dist_aggr_group_idxes_.reset();
    } else {
      spec.dist_col_group_idxs_.reset();
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogGroupBy &op, ObHashGroupBySpec &spec,
    const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op.get_num_of_child() != 1 || OB_ISNULL(op.get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong number of children", K(ret), K(op.get_num_of_child()));
  } else {
    spec.set_est_group_cnt(op.get_distinct_card());
    OZ(set_3stage_info(op, spec));
    spec.by_pass_enabled_ = op.is_adaptive_aggregate();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_dist_aggr_distinct_columns(op, spec))) {
      LOG_WARN("failed to generate distinct aggregate function duplicate columns", K(ret));
    } else if (OB_FAIL(spec.distinct_exprs_.init(op.get_distinct_exprs().count()))){
      LOG_WARN("failed to init distinct column indexes", K(ret));
    } else if (OB_FAIL(generate_rt_exprs(op.get_distinct_exprs(), spec.distinct_exprs_))) {
      LOG_WARN("failed to generate distinct column", K(ret));
    } else if (nullptr != op.get_aggr_code_expr()
        && OB_FAIL(generate_rt_expr(*op.get_aggr_code_expr(), spec.aggr_code_expr_))) {
      LOG_WARN("failed to generate aggr code expr", K(ret));
    }
  }

  // 1. add group columns
  if (OB_SUCC(ret)) {
    common::ObIArray<ObRawExpr*> &group_exprs = op.get_group_by_exprs();
    if (OB_FAIL(spec.init_group_exprs(group_exprs.count()))) {
      OB_LOG(WARN, "fail to init group expr", K(ret));
    } else if (OB_FAIL(spec.cmp_funcs_.init(group_exprs.count()))) {
      OB_LOG(WARN, "fail to init group expr", K(ret));
    }
    ARRAY_FOREACH(group_exprs, i) {
      const ObRawExpr *raw_expr = group_exprs.at(i);
      ObExpr *expr = NULL;
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
    } // end for
  }

  // 2. add aggr columns
  if (OB_SUCC(ret)) {
    // TODO: need judge distinct_exprs should to be implicit aggr expr
    if (OB_FAIL(fill_aggr_infos(op, spec, &spec.group_exprs_, nullptr, nullptr))) {
      OB_LOG(WARN, "fail to fill_aggr_infos", K(ret));
    }
  }
  LOG_DEBUG("succ to generate_spec", K(spec), K(ret), K(spec.distinct_exprs_));
  return ret;
}

// copy from ObCodeGeneratorImpl::convert_normal_table_scan
int ObStaticEngineCG::generate_normal_tsc(ObLogTableScan &op, ObTableScanSpec &spec)
{
  ObString tbl_name;
  ObString index_name;
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = OB_ISNULL(op.get_plan())
      ? NULL
      : op.get_plan()->get_optimizer_context().get_sql_schema_guard();
  CK(OB_NOT_NULL(schema_guard));
  if (OB_SUCC(ret) && NULL != op.get_pre_query_range()) {
    OZ(spec.tsc_ctdef_.pre_query_range_.deep_copy(*op.get_pre_query_range()));
    if (OB_FAIL(ret)) {
    } else if (!op.is_skip_scan() && OB_FAIL(spec.tsc_ctdef_.pre_query_range_.reset_skip_scan_range())) {
      LOG_WARN("reset skip scan range failed", K(ret));
    } else if (OB_FAIL(spec.tsc_ctdef_.pre_query_range_.is_get(spec.tsc_ctdef_.scan_ctdef_.is_get_))) {
      LOG_WARN("extract the query range whether get failed", K(ret));
    }
  }

  bool is_equal_and = true;
  ObKeyPart* root = spec.tsc_ctdef_.pre_query_range_.get_table_grapth().key_part_head_;
  ObSEArray<ObQueryRange::ObEqualOff, 1> equal_offs;
  while (OB_SUCC(ret) && NULL != root && is_equal_and) {
    is_equal_and = is_equal_and & root->is_equal_condition();
    if (NULL != root->item_next_ || NULL != root->or_next_) {
      is_equal_and = false;
    } else if (root->is_normal_key()) {
      int64_t param_idx = OB_INVALID_ID;
      ObObj& cur = root->normal_keypart_->start_;
      ObQueryRange::ObEqualOff equal_off;
      if (cur.is_ext() || root->null_safe_) {
        is_equal_and = false; //rollback old version
      } else if (root->is_rowid_key_part()) {
        is_equal_and = false; //not deal with rowid
      } else if (cur.is_unknown()) {
        if (OB_FAIL(cur.get_unknown(param_idx))) {
          LOG_WARN("get question mark value failed", K(ret), K(cur));
        } else {
          equal_off.param_idx_ = param_idx;
          equal_off.pos_off_ = root->pos_.offset_;
          equal_off.pos_type_ = root->pos_.column_type_.get_type();
          equal_offs.push_back(equal_off);
        }
      } else {
        equal_off.only_pos_ = true;
        equal_off.pos_off_ = root->pos_.offset_;
        equal_off.pos_value_ = root->normal_keypart_->start_;
        equal_offs.push_back(equal_off);
      }
    }
    root = root->and_next_;
  }
  // TODO the above optimization is overrode by ObTscCgService::generate_tsc_ctdef before this commit
  // but after the deep copy of pre_query_range_ is removed in ObTscCgService::generate_tsc_ctdef,
  // error is returned in such sql 'set global x=y', should fix this;
  // spec.tsc_ctdef_.pre_query_range_.set_is_equal_and(is_equal_and);
  // spec.tsc_ctdef_.pre_query_range_.get_equal_offs().assign(equal_offs);

  OZ(ob_write_string(phy_plan_->get_allocator(), op.get_table_name(), tbl_name));
  OZ(ob_write_string(phy_plan_->get_allocator(), op.get_index_name(), index_name));

  bool is_top_table_scan = false;
  OZ(op.is_top_table_scan(is_top_table_scan));
  spec.is_top_table_scan_ = is_top_table_scan;

  OZ(set_optimization_info(op, spec));
  OZ(set_partition_range_info(op, spec));

  if (OB_SUCC(ret)) {
    spec.table_loc_id_ = op.get_table_id();
    spec.ref_table_id_ = op.get_ref_table_id();
    spec.is_index_global_ = op.get_is_index_global();
    spec.frozen_version_ = op.get_plan()->get_optimizer_context().get_global_hint().frozen_version_;
    spec.force_refresh_lc_ = op.get_plan()->get_optimizer_context().get_global_hint().force_refresh_lc_;
    spec.use_dist_das_ = op.use_das();
    spec.batch_scan_flag_ = op.use_batch();
    spec.table_row_count_ = op.get_table_row_count();
    spec.output_row_count_ = static_cast<int64_t>(op.get_output_row_count());
    spec.query_range_row_count_ = static_cast<int64_t>(op.get_query_range_row_count());
    spec.index_back_row_count_ = static_cast<int64_t>(op.get_index_back_row_count());
    spec.estimate_method_ = op.get_estimate_method();
    spec.table_name_ = tbl_name;
    spec.index_name_ = index_name;
    // das path not under gi control (TODO: separate gi_above flag from das tsc spec)
    spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
    if (op.is_table_whole_range_scan()) {
      phy_plan_->set_contain_table_scan(true);
    }
    if (OB_NOT_NULL(op.get_table_partition_info())) {
      op.get_table_partition_info()->get_table_location().set_use_das(spec.use_dist_das_);
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
    if (opt_ctx_->is_online_ddl() && stmt::T_INSERT == opt_ctx_->get_session_info()->get_stmt_type()) {
      spec.report_col_checksum_ = true;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(tsc_cg_service_.generate_tsc_ctdef(op, spec.tsc_ctdef_))) {
      LOG_WARN("generate tsc ctdef failed", K(ret));
    } else if (is_oracle_mapping_real_virtual_table(op.get_ref_table_id())) {
      if (OB_FAIL(tsc_cg_service_.generate_agent_vt_access_meta(op, spec))) {
        LOG_WARN("generate virtual agent table access meta failed", K(ret));
      }
    }
    LOG_TRACE("CG index table scan",
              K(spec.tsc_ctdef_.scan_ctdef_.ref_table_id_),
              K(spec.table_loc_id_),
              K(spec.ref_table_id_),
              K(op.get_ref_table_id()),
              K(op.get_index_table_id()),
              K(tbl_name), K(index_name));
  }

  if (OB_SUCC(ret)) {
    if (op.is_sample_scan()
        && op.get_sample_info().method_ == SampleInfo::ROW_SAMPLE) {
      ObRowSampleScanSpec &sample_scan = static_cast<ObRowSampleScanSpec &>(spec);
      sample_scan.set_sample_info(op.get_sample_info());
    }

    if (OB_SUCC(ret) && op.is_sample_scan()
        && op.get_sample_info().method_ == SampleInfo::BLOCK_SAMPLE) {
      ObBlockSampleScanSpec &sample_scan = static_cast<ObBlockSampleScanSpec &>(spec);
      sample_scan.set_sample_info(op.get_sample_info());
    }
  }

  if (OB_SUCC(ret) && spec.report_col_checksum_) {
    spec.ddl_output_cids_.assign(op.get_ddl_output_column_ids());
    for (int64_t i = 0; OB_SUCC(ret) && i < spec.ddl_output_cids_.count(); i++) {
      const ObColumnSchemaV2 *column_schema = NULL;
      if (OB_FAIL(schema_guard->get_column_schema(spec.ref_table_id_,
          spec.ddl_output_cids_.at(i), column_schema))) {
        LOG_WARN("fail to get column schema", K(ret));
      } else if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_COLUMN_NOT_FOUND;
        LOG_WARN("fail to get column schema", K(ret));
      } else if (column_schema->get_meta_type().is_fixed_len_char_type() &&
        column_schema->is_virtual_generated_column()) {
        // add flag in ddl_output_cids_ in this special scene.
        uint64_t VIRTUAL_GEN_FIX_LEN_TAG = 1ULL << 63;
        spec.ddl_output_cids_.at(i) = spec.ddl_output_cids_.at(i) | VIRTUAL_GEN_FIX_LEN_TAG;
      }
    }
  }

  if (OB_SUCC(ret) && 0 != op.get_session_id()) {
    //此时一定是临时表扫描, 记下session_id供计划缓存匹配时使用
    phy_plan_->set_session_id(op.get_session_id());
  }
  if (OB_SUCC(ret)) {
    bool found = false;
    for (int64_t i = 0; i < op.get_output_exprs().count() && !found && OB_SUCC(ret); i++) {
      const ObRawExpr *expr = NULL;
      ObExpr *rt_expr = NULL;
      if (OB_ISNULL(expr = op.get_output_exprs().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("output expr is null", K(ret));
      } else if (expr->get_expr_type() != T_PDML_PARTITION_ID) {
        if (opt_ctx_->is_online_ddl() &&
            stmt::T_INSERT == opt_ctx_->get_session_info()->get_stmt_type() &&
            op.is_table_scan()) {
          if (expr->get_expr_type() == T_REF_COLUMN) {
            const ObColumnRefRawExpr *column_expr = static_cast<const ObColumnRefRawExpr*>(expr);
            if (OB_NOT_NULL(column_expr->get_dependant_expr())
                && column_expr->get_dependant_expr()->get_expr_type() == T_FUN_SYS_SPATIAL_CELLID) {
              spec.set_spatial_ddl(true);
            }
          } else if (expr->get_expr_type() == T_FUN_SYS_SPATIAL_CELLID) {
            spec.set_spatial_ddl(true);
          }
        }
      } else if (OB_FAIL(generate_rt_expr(*expr, rt_expr))) {
        LOG_WARN("generate rt expr failed", K(ret));
      } else {
        spec.pdml_partition_id_ = rt_expr;
        spec.partition_id_calc_type_ = op.get_tablet_id_type();
        found = true;
      }
    }
  }

  if (OB_SUCC(ret) && op.get_table_type() == share::schema::EXTERNAL_TABLE) {
    spec.is_external_table_ = true;
  }

  return ret;
}

int ObStaticEngineCG::generate_param_spec(
  const common::ObIArray<ObExecParamRawExpr *> &param_raw_exprs,
  ObFixedArray<ObDynamicParamSetter, ObIAllocator> &param_setter)
{
  int ret = OB_SUCCESS;
  ObDynamicParamSetter setter;
  OZ(param_setter.init(param_raw_exprs.count()));
  for (int64_t k = 0; OB_SUCC(ret) && k < param_raw_exprs.count(); k++) {
    ObExecParamRawExpr *exec_param = param_raw_exprs.at(k);
    CK (NULL != exec_param);
    CK (NULL != exec_param->get_ref_expr());
    CK (exec_param->get_param_index() >= 0);
    if (OB_SUCC(ret)) {
      setter.param_idx_ = exec_param->get_param_index();
    }
    OZ(generate_rt_expr(*exec_param->get_ref_expr(),
                        *reinterpret_cast<ObExpr **>(&setter.src_)));
    OZ(generate_rt_expr(*exec_param,
                        *const_cast<ObExpr **>(&setter.dst_)));
    OZ(param_setter.push_back(setter));
  }
  return ret;
}
int ObStaticEngineCG::generate_pseudo_column_expr(
  ObLogJoin &op,
  ObNLConnectBySpecBase &spec)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_connect_by_pseudo_columns().count(); ++i) {
    ObRawExpr *expr = op.get_connect_by_pseudo_columns().at(i);
    ObItemType expr_type = expr->get_expr_type();
    OZ(mark_expr_self_produced(expr)); // connect by pseudo column exprs need to set IS_COLUMNLIZED flag
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
int ObStaticEngineCG::need_prior_exprs(
  ObIArray<ObExpr*> &self_output,
  ObIArray<ObExpr*> &left_output,
  bool &need_prior)
{
  int ret = OB_SUCCESS;
  need_prior = false;
  for (int64_t i = 0; !need_prior && i < left_output.count(); ++i) {
    ObExpr *expr = left_output.at(i);
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
// 简单讲就是left和right写入pump的数据是一致的，即依赖的列都需要写入
int ObStaticEngineCG::generate_pump_exprs(ObLogJoin &op, ObNLConnectBySpecBase &spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 8> self_output;
  ObLogicalOperator *left_op = op.get_child(0);
  ObLogicalOperator *right_op = op.get_child(1);
  ObIArray<ObRawExpr *> &left_output = left_op->get_output_exprs();
  ObIArray<ObRawExpr *> &right_output = right_op->get_output_exprs();
  const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt *>(op.get_stmt());
  spec.is_nocycle_ = select_stmt->is_nocycle();
  spec.has_prior_ = select_stmt->has_prior();
  if (OB_ISNULL(left_op) || OB_ISNULL(right_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: child is null", K(ret));
  } else if (OB_UNLIKELY(left_output.count() != right_output.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: left and right output size not same", K(ret));
  } else if (OB_FAIL(generate_pseudo_column_expr(op, spec))) {
    LOG_WARN("failed to generate pseudo column", K(ret));
  } else if (OB_FAIL(spec.connect_by_prior_exprs_.init(left_output.count()))) {
    LOG_WARN("failed to init left pump exprs", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(
      op.get_connect_by_prior_exprs(), spec.connect_by_prior_exprs_))) {
    LOG_WARN("failed to generate prior exprs", K(ret));
  } else if (OB_FAIL(spec.left_prior_exprs_.init(left_output.count()))) {
    LOG_WARN("failed to init left pump exprs", K(ret));
  } else if (OB_FAIL(spec.right_prior_exprs_.init(right_output.count()))) {
    LOG_WARN("failed to init left pump exprs", K(ret));
  } else {
    ObExpr *left_expr = nullptr;
    ObExpr *right_expr = nullptr;
    for (int64_t i = 0; i < left_output.count() && OB_SUCC(ret); ++i) {
      ObRawExpr *left_raw_expr = left_output.at(i);
      ObRawExpr *right_raw_expr = NULL;
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
        LOG_WARN("failed to push back right expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("prior info", K(select_stmt->has_prior()), K(spec.connect_by_prior_exprs_),
        K(op.get_connect_by_prior_exprs()));
      bool need_prior = false;
      int64_t left_prior_cnt = 1 + select_stmt->get_order_item_size();
      if (OB_FAIL(generate_rt_exprs(op.get_output_exprs(), self_output))) {
        LOG_WARN("failed to generate rt exprs", K(ret));
      } else if (OB_FAIL(need_prior_exprs(self_output, spec.left_prior_exprs_, need_prior))) {
        LOG_WARN("failed to calc prior exprs needed", K(ret));
      } else if (FALSE_IT(need_prior = (need_prior
                                      || select_stmt->has_prior()
                                      || spec.connect_by_prior_exprs_.count()))) {
      } else if (OB_FAIL(spec.cur_row_exprs_.init(
            need_prior ? spec.left_prior_exprs_.count() + left_prior_cnt : left_prior_cnt))) {
        LOG_WARN("failed to init cur row exprs", K(ret));
      } else if (need_prior && OB_FAIL(append(spec.cur_row_exprs_, spec.left_prior_exprs_))) {
        LOG_WARN("failed to push back prior exprs", K(ret));
      } else if (nullptr != spec.level_expr_ && PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX == spec.type_
          && OB_FAIL(spec.cur_row_exprs_.push_back(spec.level_expr_))) {
        LOG_WARN("failed to push back prior exprs", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObExpr *expr = nullptr;
      ObSEArray<ObRawExpr *, 8> connect_by_root_exprs;
      OZ(select_stmt->get_connect_by_root_exprs(connect_by_root_exprs));
      OZ(spec.connect_by_root_exprs_.init(connect_by_root_exprs.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_root_exprs.count(); ++i) {
        ObRawExpr *root_expr = connect_by_root_exprs.at(i);
        if (OB_FAIL(generate_rt_expr(*root_expr, expr))) {
          LOG_WARN("failed to generate rt expr", K(ret));
        } else if (OB_FAIL(spec.connect_by_root_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObExpr *expr = nullptr;
      ObSEArray<ObRawExpr *, 8> sys_connect_by_path_exprs;
      OZ(select_stmt->get_sys_connect_by_path_exprs(sys_connect_by_path_exprs));
      OZ(spec.sys_connect_exprs_.init(sys_connect_by_path_exprs.count()));
      for (int64_t i = 0; OB_SUCC(ret) && i <sys_connect_by_path_exprs.count(); ++i) {
        ObRawExpr *sys_expr = sys_connect_by_path_exprs.at(i);
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
        ObExpr *expr = spec.connect_by_prior_exprs_.at(i);
        ObCmpFunc cmp_func;
        if (ob_is_user_defined_sql_type(expr->datum_meta_.type_) || ob_is_user_defined_pl_type(expr->datum_meta_.type_)) {
          // other udt types not supported, xmltype does not have order or map member function
          ret = OB_ERR_NO_ORDER_MAP_SQL;
          LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
        } else {
          cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
                                expr->datum_meta_.type_,
                                expr->datum_meta_.type_,
                                NULL_LAST,//这里null last还是first无所谓
                                expr->datum_meta_.cs_type_,
                                expr->datum_meta_.scale_,
                                lib::is_oracle_mode(),
                                expr->obj_meta_.has_lob_header());
          if (OB_ISNULL(cmp_func.cmp_func_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("cmp_func is null, check datatype is valid", K(ret));
          } else if (OB_FAIL(spec.cmp_funcs_.push_back(cmp_func))) {
            LOG_WARN("failed to push back sort function", K(ret));
          }
        }
      }
    }
  }
  return ret;
}
int ObStaticEngineCG::get_connect_by_copy_expr(ObRawExpr &left_expr,
                                               ObRawExpr *&right_expr,
                                               ObIArray<ObRawExpr *> &right_exprs)
{
  int ret = OB_SUCCESS;
  right_expr = NULL;
  if (OB_LIKELY(left_expr.is_column_ref_expr())) {
    const uint64_t column_id = static_cast<ObColumnRefRawExpr &>(left_expr).get_column_id();
    for (int64_t i = 0; OB_SUCC(ret) && NULL == right_expr && i < right_exprs.count(); ++i) {
      ObRawExpr *cur_expr = right_exprs.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!cur_expr->is_column_ref_expr()) {
        // do nothing
      } else if (column_id == static_cast<ObColumnRefRawExpr *>(cur_expr)->get_column_id()) {
        right_expr = cur_expr;
      }
    }
  }
  return ret;
}
int ObStaticEngineCG::construct_hash_elements_for_connect_by(ObLogJoin &op, ObNLConnectBySpec &spec)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *left_op = NULL;
  ObLogicalOperator *right_op = NULL;
  ObLogPlan *log_plan = NULL;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(log_plan = op.get_plan())
    || OB_ISNULL(session_info = log_plan->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret), K(log_plan));
  } else if (OB_ISNULL(left_op = op.get_child(0)) || OB_ISNULL(right_op = op.get_child(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child op is null", K(ret));
  } else if (OB_FAIL(spec.hash_key_exprs_.init(op.get_other_join_conditions().count()))) {
    LOG_WARN("failed to init hash key exprs", K(ret));
  } else if (OB_FAIL(spec.hash_probe_exprs_.init(op.get_other_join_conditions().count()))) {
    LOG_WARN("failed to init hash probe exprs", K(ret));
  } else {
    const ObRelIds &left_table_set = left_op->get_table_set();
    const ObRelIds &right_table_set = right_op->get_table_set();
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_other_join_conditions().count(); i++) {
      ObRawExpr *other_cond = op.get_other_join_conditions().at(i);
      if (OB_ISNULL(other_cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("other condition is null", K(ret));
      } else if (T_OP_EQ == other_cond->get_expr_type()) {
        bool prior_at_left = false;
        bool can_use_as_key = false;
        if (other_cond->get_param_count() != 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected param count of equal", K(ret), KPC(other_cond));
        } else if (OB_ISNULL(other_cond->get_param_expr(0))
                    || OB_ISNULL(other_cond->get_param_expr(1))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(ret), KPC(other_cond));
        } else {
          const ObRelIds &left_rel_ids = other_cond->get_param_expr(0)->get_relation_ids();
          const ObRelIds &right_rel_ids = other_cond->get_param_expr(1)->get_relation_ids();
          if (left_rel_ids.is_empty() || right_rel_ids.is_empty()) {
          } else if (left_rel_ids.is_subset(left_table_set)
                     && right_rel_ids.is_subset(right_table_set)) {
            can_use_as_key = true;
            prior_at_left = true;
          } else if (left_rel_ids.is_subset(right_table_set)
                     && right_rel_ids.is_subset(left_table_set)) {
            can_use_as_key = true;
          }
          if (can_use_as_key) {
            ObExpr *left_param = NULL;
            ObExpr *right_param = NULL;
            if (OB_FAIL(generate_rt_expr(*other_cond->get_param_expr(0), left_param))) {
              LOG_WARN("generate left expr failed", K(ret));
            } else if (OB_FAIL(generate_rt_expr(*other_cond->get_param_expr(1), right_param))) {
              LOG_WARN("generate right expr failed", K(ret));
            } else if (OB_FAIL(spec.hash_key_exprs_.push_back(prior_at_left ? right_param : left_param))) {
              LOG_WARN("push back hash key expr failed", K(ret));
            } else if (OB_FAIL(spec.hash_probe_exprs_.push_back(prior_at_left ? left_param : right_param))) {
              LOG_WARN("push back hash probe expr failed", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}
int ObStaticEngineCG::generate_spec(ObLogJoin &op, ObNLConnectBySpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<ObRawExpr*> &other_join_conds = op.get_other_join_conditions();
  if (OB_FAIL(generate_param_spec(op.get_nl_params(), spec.rescan_params_))) {
    LOG_WARN("failed to generate parameter", K(ret));
  } else if (OB_FAIL(generate_pump_exprs(op, spec))) {
    LOG_WARN("failed to generate pump exprs", K(ret));
  } else if (OB_FAIL(spec.cond_exprs_.init(other_join_conds.count()))) {
    LOG_WARN("failed to init join conditions", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(other_join_conds, spec.cond_exprs_))) {
    LOG_WARN("failed to generate condition rt exprs", K(ret));
  } else if (OB_FAIL(construct_hash_elements_for_connect_by(op, spec))) {
    LOG_WARN("construct_hash_elements_for_connect_by failed", K(ret));
  }
  return ret;
}
int ObStaticEngineCG::generate_spec(ObLogJoin &op, ObNLConnectByWithIndexSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  const ObIArray<ObRawExpr*> &other_join_conds = op.get_other_join_conditions();
  if (OB_FAIL(generate_param_spec(op.get_nl_params(), spec.rescan_params_))) {
    LOG_WARN("failed to generate parameter", K(ret));
  } else if (OB_FAIL(generate_pump_exprs(op, spec))) {
    LOG_WARN("failed to generate pump exprs", K(ret));
  } else if (OB_FAIL(spec.cond_exprs_.init(other_join_conds.count()))) {
    LOG_WARN("failed to init join conditions", K(ret));
  } else if (OB_FAIL(generate_rt_exprs(other_join_conds, spec.cond_exprs_))) {
    LOG_WARN("failed to generate condition rt exprs", K(ret));
  }
  return ret;
}
int ObStaticEngineCG::generate_spec(ObLogJoin &op, ObHashJoinSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  CK (nullptr != op.get_join_path());
  if (OB_SUCC(ret) && op.get_join_path()->is_naaj_) {
    CK (LEFT_ANTI_JOIN == op.get_join_type() || RIGHT_ANTI_JOIN == op.get_join_type());
    OX (spec.is_naaj_ = op.get_join_path()->is_naaj_);
    OX (spec.is_sna_ = op.get_join_path()->is_sna_);
  }
  spec.is_shared_ht_ = HASH_JOIN == op.get_join_algo()
                    && DIST_BC2HOST_NONE == op.get_join_distributed_method();
  OZ (generate_join_spec(op, spec));
  return ret;
}
int ObStaticEngineCG::generate_spec(ObLogJoin &op,
                                    ObNestedLoopJoinSpec &spec,
                                    const bool in_root_job)
{
  UNUSED(in_root_job);
  return generate_join_spec(op, spec);
}
int ObStaticEngineCG::generate_spec(ObLogJoin &op,
                                    ObMergeJoinSpec &spec,
                                    const bool in_root_job)
{
  UNUSED(in_root_job);
  return generate_join_spec(op, spec);
}
int ObStaticEngineCG::generate_join_spec(ObLogJoin &op, ObJoinSpec &spec)
{
  int ret = OB_SUCCESS;
  bool is_late_mat = (phy_plan_->get_is_late_materialized() || op.is_late_mat());
  phy_plan_->set_is_late_materialized(is_late_mat);
  // print log if possible
  if (OB_NOT_NULL(op.get_stmt())
      && (stmt::T_INSERT == op.get_stmt()->get_stmt_type()
          || stmt::T_UPDATE == op.get_stmt()->get_stmt_type()
          || stmt::T_DELETE == op.get_stmt()->get_stmt_type())
      && true == is_late_mat) {
    LOG_WARN("INSERT, UPDATE or DELETE smt should not be marked as late materialized.",
             K(op.get_stmt()->get_stmt_type()), K(is_late_mat), K(*op.get_stmt()));
  }
  if (op.is_partition_wise()) {
    phy_plan_->set_is_wise_join(op.is_partition_wise()); // set is_wise_join
  }
  // 1. add other join conditions
  const ObIArray<ObRawExpr*> &other_join_conds = op.get_other_join_conditions();

  OZ(spec.other_join_conds_.init(other_join_conds.count()));

  ARRAY_FOREACH(other_join_conds, i) {
    ObRawExpr *raw_expr = other_join_conds.at(i);
    ObExpr *expr = NULL;
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
  } // end for

  spec.join_type_ = op.get_join_type();
  if (MERGE_JOIN == op.get_join_algo()) {
    //A.1. add equaljoin conditions and populate all exprs for left/right child fetcher
    ObMergeJoinSpec &mj_spec = static_cast<ObMergeJoinSpec &>(spec);
    const ObIArray<ObRawExpr*> &equal_join_conds = op.get_equal_join_conditions();
    OZ(mj_spec.equal_cond_infos_.init(equal_join_conds.count()));
    if (OB_ISNULL(mj_spec.get_left())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left is null", K(ret));
    } else if (OB_ISNULL(mj_spec.get_right())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("right is null", K(ret));
    } else if (OB_FAIL(mj_spec.left_child_fetcher_all_exprs_.init(
                 mj_spec.get_left()->output_.count() +
                 equal_join_conds.count()))) {
      LOG_WARN("failed to init left fetcher all exprs", K(ret));
    } else if (OB_FAIL(mj_spec.right_child_fetcher_all_exprs_.init(
                 mj_spec.get_right()->output_.count() +
                 equal_join_conds.count()))) {
      LOG_WARN("failed to init right fetcher all exprs", K(ret));
    } else if (OB_FAIL(
                  append_array_no_dup(mj_spec.left_child_fetcher_all_exprs_,
                                       mj_spec.get_left()->output_))) {
      LOG_WARN("fail to append array no dup for left child", K(ret), K(op));
    } else if (OB_FAIL(
                  append_array_no_dup(mj_spec.right_child_fetcher_all_exprs_,
                                       mj_spec.get_right()->output_))) {
      LOG_WARN("fail to append array no dup for right child", K(ret), K(op));
    }
    ARRAY_FOREACH(equal_join_conds, i) {
      ObMergeJoinSpec::EqualConditionInfo equal_cond_info;
      ObRawExpr *raw_expr = equal_join_conds.at(i);
      CK(OB_NOT_NULL(raw_expr));
      CK(T_OP_EQ == raw_expr->get_expr_type() || T_OP_NSEQ == raw_expr->get_expr_type());
      OZ(generate_rt_expr(*raw_expr, equal_cond_info.expr_));
      CK(OB_NOT_NULL(equal_cond_info.expr_));
      CK(equal_cond_info.expr_->arg_cnt_ == 2)
      CK(OB_NOT_NULL(equal_cond_info.expr_->args_));
      CK(OB_NOT_NULL(equal_cond_info.expr_->args_[0]));
      CK(OB_NOT_NULL(equal_cond_info.expr_->args_[1]));
      if (OB_SUCC(ret)){
        ObDatumMeta &l = equal_cond_info.expr_->args_[0]->datum_meta_;
        ObDatumMeta &r = equal_cond_info.expr_->args_[1]->datum_meta_;
        bool has_lob_header = equal_cond_info.expr_->args_[0]->obj_meta_.has_lob_header() ||
                              equal_cond_info.expr_->args_[1]->obj_meta_.has_lob_header();
        CK(l.cs_type_ == r.cs_type_);
        if (OB_SUCC(ret)) {
          const ObScale scale = ObDatumFuncs::max_scale(l.scale_, r.scale_);
          OZ(calc_equal_cond_opposite(op, *raw_expr, equal_cond_info.is_opposite_));
          if (OB_SUCC(ret)) {
            if (equal_cond_info.is_opposite_) {
              equal_cond_info.ns_cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(r.type_,
                                l.type_, default_null_pos(), r.cs_type_, scale, is_oracle_mode(),
                                has_lob_header);
            } else {
              equal_cond_info.ns_cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(l.type_,
                                r.type_, default_null_pos(), l.cs_type_, scale, is_oracle_mode(),
                                has_lob_header);
            }
          }
          CK(OB_NOT_NULL(equal_cond_info.ns_cmp_func_));
          OZ(mj_spec.equal_cond_infos_.push_back(equal_cond_info));
          // when is_opposite_ is true: left child fetcher accept right
          // arg(args_[1]) and vice versa
          if (OB_SUCC(ret) && OB_FAIL(add_var_to_array_no_dup(mj_spec.left_child_fetcher_all_exprs_,
              !equal_cond_info.is_opposite_ ? equal_cond_info.expr_->args_[0]
                                           : equal_cond_info.expr_->args_[1]))) {
            OB_LOG(WARN, "fail to add_var_to_array_no_dup",  K(ret));
          } else if (OB_SUCC(ret) && OB_FAIL(add_var_to_array_no_dup(mj_spec.right_child_fetcher_all_exprs_,
              !equal_cond_info.is_opposite_ ? equal_cond_info.expr_->args_[1]
                                           : equal_cond_info.expr_->args_[0]))) {
            OB_LOG(WARN, "fail to add_var_to_array_no_dup", K(ret));
          }
          LOG_DEBUG("equijoin condition", K(*raw_expr), K(equal_cond_info),
                   K(equal_cond_info.is_opposite_),
                   KPC(equal_cond_info.expr_->args_[0]),
                   KPC(equal_cond_info.expr_->args_[1]));
        }
      }
    } // end for
    // A.2. add merge directions
    if (OB_SUCC(ret)) {
      const ObIArray<ObOrderDirection> &merge_directions = op.get_merge_directions();
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
  } else if (NESTED_LOOP_JOIN ==  op.get_join_algo()) {  // nested loop join
    if (0 != op.get_equal_join_conditions().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("equal join conditions' count should equal 0", K(ret));
    } else {
      ObBasicNestedLoopJoinSpec &nlj_spec = static_cast<ObBasicNestedLoopJoinSpec &>(spec);
      nlj_spec.enable_gi_partition_pruning_ = op.is_enable_gi_partition_pruning();
      if (nlj_spec.enable_gi_partition_pruning_ && OB_FAIL(do_gi_partition_pruning(op, nlj_spec))) {
        LOG_WARN("fail do gi partition pruning", K(ret));
      } else {
        OZ(generate_param_spec(op.get_nl_params(), nlj_spec.rescan_params_));

        if (OB_SUCC(ret)) {
          // 当nlj条件下推做分布式rescan, 开启px batch rescan
          ObNestedLoopJoinSpec &nlj = static_cast<ObNestedLoopJoinSpec &>(spec);
          if (op.enable_px_batch_rescan()) {
            nlj.enable_px_batch_rescan_ = true;
            nlj.group_size_ = PX_RESCAN_BATCH_ROW_COUNT;
          } else {
            nlj.enable_px_batch_rescan_ = false;
          }
        }
        if (OB_SUCC(ret) && PHY_NESTED_LOOP_JOIN == spec.type_) {
          ObNestedLoopJoinSpec &nlj = static_cast<ObNestedLoopJoinSpec &>(spec);
          bool use_batch_nlj = op.can_use_batch_nlj();
          if (use_batch_nlj) {
            nlj.group_rescan_ = use_batch_nlj;
          }

          if (nlj.is_vectorized()) {
            // populate other cond join info
            const ObIArray<ObExpr *> &conds = spec.other_join_conds_;
            if (OB_FAIL(nlj.left_expr_ids_in_other_cond_.prepare_allocate(conds.count()))) {
              LOG_WARN("Failed to prepare_allocate left_expr_ids_in_other_cond_", K(ret));
            } else {
              ARRAY_FOREACH(conds, i) {
                auto cond = conds.at(i);
                ObSEArray<int, 1> left_expr_ids;
                for (auto l_output_idx = 0;
                     OB_SUCC(ret) && l_output_idx < nlj.get_left()->output_.count();
                     l_output_idx++) {
                  // check if left child expr appears in other_condition
                  bool appears_in_cond = false;
                  if (OB_FAIL(cond->contain_expr(
                          nlj.get_left()->output_.at(l_output_idx), appears_in_cond))) {
                    LOG_WARN("other expr contain calculate failed", K(ret), KPC(cond),
                             K(l_output_idx),
                             KPC(nlj.get_left()->output_.at(l_output_idx)));
                  } else {
                    if (appears_in_cond) {
                      if (OB_FAIL(left_expr_ids.push_back(l_output_idx))) {
                        LOG_WARN("other expr contain", K(ret));
                      }
                    }
                  }
                }
                // Note: no need to call init explicitly as init() is invoked inside assign()
                OZ(nlj.left_expr_ids_in_other_cond_.at(i).assign(left_expr_ids));
              }
            }
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(nlj.left_rescan_params_.init(op.get_above_pushdown_left_params().count()))) {
              LOG_WARN("fail to init fixed array", K(ret));
            } else if (OB_FAIL(nlj.right_rescan_params_.init(op.get_above_pushdown_right_params().count()))) {
              LOG_WARN("fail to init fixed array", K(ret));
            } else if (OB_FAIL(set_batch_exec_param(op.get_nl_params(), nlj_spec.rescan_params_))) {
              LOG_WARN("fail to set batch exec param", K(ret));
            }
            ARRAY_FOREACH(op.get_above_pushdown_left_params(), i) {
              ObExecParamRawExpr* param_expr = op.get_above_pushdown_left_params().at(i);
              if (OB_FAIL(batch_exec_param_caches_.push_back(BatchExecParamCache(param_expr,
                                                                                 &nlj,
                                                                                 true)))) {
                LOG_WARN("fail to push back param expr", K(ret));
              }
            }
            ARRAY_FOREACH(op.get_above_pushdown_right_params(), i) {
              ObExecParamRawExpr* param_expr = op.get_above_pushdown_right_params().at(i);
              if (OB_FAIL(batch_exec_param_caches_.push_back(BatchExecParamCache(param_expr,
                                                                                 &nlj,
                                                                                 false)))) {
                LOG_WARN("fail to push back param expr", K(ret));
              }
            }
          }
        }
      }
    }
  } else if (HASH_JOIN == op.get_join_algo()) {
    ObSEArray<ObExpr*, 4> right_key_exprs;
    ObSEArray<ObHashFunc, 4> right_hash_funcs;
    ObHashJoinSpec &hj_spec = static_cast<ObHashJoinSpec&>(spec);
    if (OB_ISNULL(hj_spec.get_left()) || OB_ISNULL(hj_spec.get_right())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("children of hj is not init",
                            K(ret), KP(hj_spec.get_left()), KP(hj_spec.get_right()));
    } else if (OB_FAIL(hj_spec.equal_join_conds_.init(op.get_equal_join_conditions().count()))) {
      LOG_WARN("failed to init equal join conditions", K(ret));
    } else if (OB_FAIL(generate_rt_exprs(op.get_equal_join_conditions(), hj_spec.equal_join_conds_))) {
      LOG_WARN("failed to generate rt exprs", K(ret));
    } else if (OB_FAIL(hj_spec.all_join_keys_.init(2 * hj_spec.equal_join_conds_.count()))) {
      LOG_WARN("failed to init join keys", K(ret));
    } else if (OB_FAIL(hj_spec.all_hash_funcs_.init(2 * hj_spec.equal_join_conds_.count()))) {
      LOG_WARN("failed to init join keys", K(ret));
    } else {
      hj_spec.can_prob_opt_ = true;
      for (int64_t i = 0; i < hj_spec.equal_join_conds_.count() && OB_SUCC(ret); ++i) {
        ObExpr *expr = hj_spec.equal_join_conds_.at(i);
        ObHashFunc left_hash_func;
        ObHashFunc right_hash_func;
        ObExpr *left_expr = nullptr;
        ObExpr *right_expr = nullptr;
        bool is_opposite = false;
        if (2 != expr->arg_cnt_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: join keys must have 2 arguments", K(ret), K(*expr));
        } else if (OB_FAIL(calc_equal_cond_opposite(
            op, *op.get_equal_join_conditions().at(i), is_opposite))) {
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
            bool is_murmur_hash_v2_ = cur_cluster_version_ >= CLUSTER_VERSION_4_1_0_0;
            left_hash_func.hash_func_ = is_murmur_hash_v2_ ?
                left_expr->basic_funcs_->murmur_hash_v2_ : left_expr->basic_funcs_->murmur_hash_;
            right_hash_func.hash_func_ = is_murmur_hash_v2_ ?
                right_expr->basic_funcs_->murmur_hash_v2_ : right_expr->basic_funcs_->murmur_hash_;
            if (OB_ISNULL(left_hash_func.hash_func_) || OB_ISNULL(right_hash_func.hash_func_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("hash func is null, check datatype is valid", K(ret));
            } else if (OB_FAIL(hj_spec.all_hash_funcs_.push_back(left_hash_func))) {
              LOG_WARN("failed to push back left expr hash func", K(ret));
            } else if (OB_FAIL(right_hash_funcs.push_back(right_hash_func))) {
              LOG_WARN("failed to push back right expr hash func", K(ret));
            }
          }
          if (T_REF_COLUMN != left_expr->type_
              || T_REF_COLUMN != right_expr->type_
              || left_expr->datum_meta_.type_ != right_expr->datum_meta_.type_
              || T_OP_NSEQ == expr->type_
              || !has_exist_in_array(hj_spec.get_left()->output_, left_expr)
              || !has_exist_in_array(hj_spec.get_right()->output_, right_expr)) {
            hj_spec.can_prob_opt_ = false;
          }
        }
      }
      if (hj_spec.can_prob_opt_) {
        if (INNER_JOIN != op.get_join_type()
            || op.get_other_join_conditions().count() > 0) {
          hj_spec.can_prob_opt_ = false;
        }
      }
      if (OB_SUCC(ret)) {
        // 这里暂时不去重，简化后面执行逻辑
        if (OB_FAIL(append(hj_spec.all_join_keys_, right_key_exprs))) {
          LOG_WARN("failed to append join keys", K(ret));
        } else if (OB_FAIL(append(hj_spec.all_hash_funcs_, right_hash_funcs))) {
          LOG_WARN("failed to append join keys", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(hj_spec.is_ns_equal_cond_.init(hj_spec.equal_join_conds_.count()))) {
          LOG_WARN("failed to init ns equal array", K(ret));
        } else {
          // for null safe equal, we can not skip null value during executing
          for (int64_t i = 0; OB_SUCC(ret) && i < hj_spec.equal_join_conds_.count(); ++i) {
            ObExpr *equal_expr = hj_spec.equal_join_conds_.at(i);
            if (OB_ISNULL(equal_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("got null join equal expr", K(ret), K(i));
            } else if (T_OP_NSEQ == equal_expr->type_) {
              OZ (hj_spec.is_ns_equal_cond_.push_back(true));
            } else {
              OZ (hj_spec.is_ns_equal_cond_.push_back(false));
            }
          }
        }
      }
    }
  }
  // level pseudo column as a exec param
/*  if (OB_SUCC(ret) && CONNECT_BY_JOIN == op.get_join_type()) {*/
    //ObNestedLoopConnectBy *nlj_op = static_cast<ObNestedLoopConnectBy*>(phy_op);
    //if (OB_ISNULL(nlj_op)) {
      //ret = OB_ERR_NULL_VALUE;
      //LOG_WARN("nlj_op is null", K(ret));
    //} else if (exec_params.count() == 0) {
      //// Do nothing
    //} else if (exec_params.count() != 1) {
      //// Only one ? expr for all level expr in connect by clause.
      //ret = OB_ERR_UNEXPECTED;
      //LOG_WARN("unexpected exec params count in connect by", K(exec_params.count()), K(ret));
    //} else if (OB_FAIL(nlj_op->init_exec_param_count(exec_params.count()))) {
      //LOG_WARN("fail to init param count", K(ret));
    //} else {
      //ARRAY_FOREACH(exec_params, i) {
        //const std::pair<int64_t, ObRawExpr*> &param_expr = exec_params.at(i);
        //LOG_DEBUG("connect by", K(param_expr.first), K(param_expr.second), K(ret));
        //if (OB_FAIL(nlj_op->add_exec_param(param_expr.first))) {
          //LOG_WARN("failed to add nlj param", K(ret));
        //}
      //}
    //}
  /*}*/
  return ret;
}

int ObStaticEngineCG::do_gi_partition_pruning(
    ObLogJoin &op,
    ObBasicNestedLoopJoinSpec &spec)
{
  int ret = OB_SUCCESS;
  OZ(generate_rt_expr(*op.get_partition_id_expr(), spec.gi_partition_id_expr_));
  return ret;
}
int ObStaticEngineCG::calc_equal_cond_opposite(const ObLogJoin &op,
                                               const ObRawExpr &raw_expr,
                                               bool &is_opposite)
{
  int ret = OB_SUCCESS;
  is_opposite = false;
  const ObLogicalOperator *left_child = NULL;
  const ObLogicalOperator *right_child = NULL;
  const ObRawExpr *lexpr = NULL;
  const ObRawExpr *rexpr = NULL;
  CK(T_OP_EQ == raw_expr.get_expr_type() || T_OP_NSEQ == raw_expr.get_expr_type());
  CK(OB_NOT_NULL(left_child = op.get_child(0)));
  CK(OB_NOT_NULL(right_child = op.get_child(1)));
  CK(OB_NOT_NULL(lexpr = raw_expr.get_param_expr(0)));
  CK(OB_NOT_NULL(rexpr = raw_expr.get_param_expr(1)));
  if (OB_SUCC(ret)) {
    if (lexpr->get_relation_ids().is_subset(left_child->get_table_set())
        && rexpr->get_relation_ids().is_subset(right_child->get_table_set())) {
      is_opposite = false;
    } else if (lexpr->get_relation_ids().is_subset(right_child->get_table_set())
               && rexpr->get_relation_ids().is_subset(left_child->get_table_set())) {
      is_opposite = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid equal condition", K(op), K(raw_expr), K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::set_optimization_info(ObLogTableScan &op, ObTableScanSpec &spec)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(phy_plan_));
  OZ(spec.set_est_row_count_record(op.get_est_row_count_record()));
  if (OB_SUCC(ret)) {
    spec.table_row_count_ = op.get_table_row_count();
    spec.output_row_count_ = static_cast<int64_t>(op.get_output_row_count());
    spec.phy_query_range_row_count_ = static_cast<int64_t>(
        op.get_phy_query_range_row_count());
    spec.query_range_row_count_ = static_cast<int64_t>(op.get_query_range_row_count());
    spec.index_back_row_count_ = static_cast<int64_t>(op.get_index_back_row_count());
  }
  if (OB_NOT_NULL(op.get_table_opt_info())) {
    OZ(spec.set_available_index_name(op.get_table_opt_info()->available_index_name_,
                                     phy_plan_->get_allocator()));
    OZ(spec.set_unstable_index_name(op.get_table_opt_info()->unstable_index_name_,
                                  phy_plan_->get_allocator()));
    OZ(spec.set_pruned_index_name(op.get_table_opt_info()->pruned_index_name_,
                                  phy_plan_->get_allocator()));
  }
  return ret;
}

// copy from ObCodeGeneratorImpl
int ObStaticEngineCG::set_partition_range_info(ObLogTableScan &op, ObTableScanSpec &spec)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = op.get_table_id();
  uint64_t ref_table_id = op.get_location_table_id();
  uint64_t index_id = op.get_index_table_id();
  ObLogPlan *log_plan = op.get_plan();
  const ObDMLStmt *stmt = op.get_stmt();
  const ObTablePartitionInfo *tbl_part_info = op.get_table_partition_info();
  ObSqlSchemaGuard *schema_guard = NULL;
  const ObTableSchema *table_schema = NULL;
  const ObTableSchema *index_schema = NULL;
  ObRawExpr *part_expr = op.get_part_expr();
  ObRawExpr *subpart_expr = op.get_subpart_expr();
  ObSEArray<ObRawExpr *, 2> part_column_exprs;
  ObSEArray<ObRawExpr *, 2> subpart_column_exprs;
  ObSEArray<uint64_t, 2> rowkey_column_ids;
  if (PHY_MULTI_PART_TABLE_SCAN == spec.type_) {
    // do nothing, global index back scan don't has tbl_part_info.
  } else if (OB_ISNULL(log_plan) || OB_ISNULL(stmt) || OB_ISNULL(tbl_part_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(spec), K(log_plan), K(tbl_part_info), K(stmt), K(ret));
  } else if (OB_INVALID_ID == table_id || OB_INVALID_ID == ref_table_id ||
             OB_INVALID_ID == index_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid table id", K(table_id), K(ref_table_id), K(index_id), K(ret));
  } else if (is_virtual_table(ref_table_id)
             || is_inner_table(ref_table_id)
             || is_cte_table(ref_table_id)) {
    /*do nothing*/
  } else if (!stmt->is_select_stmt()
             || tbl_part_info->get_table_location().has_generated_column()) {
    /*do nothing*/
  } else if (OB_ISNULL(schema_guard = log_plan->get_optimizer_context().get_sql_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null schema guard", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, ref_table_id, op.get_stmt(), table_schema))) {
    LOG_WARN("get table schema failed", K(table_id), K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(table_id, index_id, op.get_stmt(), index_schema))) {
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
  } else if (OB_FAIL(mark_expr_self_produced(part_expr))) { // part expr in table scan need to set IS_COLUMNLIZED flag
    LOG_WARN("mark expr self produced failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(part_expr, part_column_exprs))) {
    LOG_WARN("failed to check pure column part expr", K(ret));
  } else if (ObPartitionLevel::PARTITION_LEVEL_TWO == table_schema->get_part_level() &&
             OB_ISNULL(subpart_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null subpart expr", K(ret));
  } else if (NULL != subpart_expr &&
             OB_FAIL(ObRawExprUtils::extract_column_exprs(subpart_expr, subpart_column_exprs))) {
    LOG_WARN("failed to check pure column part expr", K(ret));
  } else if (NULL != subpart_expr
             && OB_FAIL(mark_expr_self_produced(subpart_expr))) { // subpart expr in table scan need to set IS_COLUMNLIZED flag
    LOG_WARN("mark expr self produced failed", K(ret));
  } else {
    bool is_valid = true;
    ObSEArray<int64_t, 4> part_range_pos;
    ObSEArray<int64_t, 4> subpart_range_pos;
    ObSEArray<ObRawExpr *, 4> part_dep_cols;
    ObSEArray<ObRawExpr *, 4> subpart_dep_cols;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < part_column_exprs.count(); i++) {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(part_column_exprs.at(i));
      CK(OB_NOT_NULL(col_expr));
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < rowkey_column_ids.count(); j++) {
        if (col_expr->get_column_id() == rowkey_column_ids.at(j)) {
          is_find = true;
          OZ(part_range_pos.push_back(j));
          OZ(part_dep_cols.push_back(col_expr));
          OZ(mark_expr_self_produced(col_expr)); // part range key expr in table scan need to set IS_COLUMNLIZED flag
        }
      }
      if (!is_find) {
        is_valid = false;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < subpart_column_exprs.count(); i++) {
      bool is_find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !is_find && j < rowkey_column_ids.count(); j++) {
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(subpart_column_exprs.at(i));
        CK(OB_NOT_NULL(col_expr));
        if (col_expr->get_column_id() == rowkey_column_ids.at(j)) {
          is_find = true;
          OZ(subpart_range_pos.push_back(j));
          OZ(subpart_dep_cols.push_back(col_expr));
          OZ(mark_expr_self_produced(col_expr)); // sub part range key expr in table scan need to set IS_COLUMNLIZED flag
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
      LOG_DEBUG("partition range pos", K(table_schema->get_part_level()),
                K(part_range_pos), K(subpart_range_pos), K(ret));
    }
  }
  return ret;
}

// 递归地找出column expr在generated table中对应的基表的column expr
int ObStaticEngineCG::recursive_get_column_expr(const ObColumnRefRawExpr *&column,
                                                const TableItem &table_item)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *stmt = table_item.ref_query_;
  const ObRawExpr *select_expr = NULL;
  if (OB_ISNULL(column) || OB_ISNULL(stmt) || OB_ISNULL(stmt = stmt->get_real_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(column), K(stmt));
  } else {
    const int64_t offset = column->get_column_id() - OB_APP_MIN_COLUMN_ID;
    if (OB_UNLIKELY(offset < 0 || offset >= stmt->get_select_item_size()) ||
        OB_ISNULL(select_expr = stmt->get_select_item(offset).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected select expr", K(ret),
          K(offset), K(stmt->get_select_item_size()), K(select_expr));
    } else if (OB_UNLIKELY(!select_expr->is_column_ref_expr()) && column->is_xml_column()
              && select_expr->get_expr_type() == T_FUN_SYS_MAKEXML
              && OB_NOT_NULL(select_expr->get_param_expr(1))) {
      select_expr = select_expr->get_param_expr(1);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!select_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(*select_expr));
    } else {
      const ObColumnRefRawExpr *inner_column = static_cast<const ObColumnRefRawExpr *>(select_expr);
      const TableItem *table_item = stmt->get_table_item_by_id(inner_column->get_table_id());
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if ((table_item->is_generated_table() || table_item->is_temp_table()) &&
                 OB_FAIL(recursive_get_column_expr(inner_column, *table_item))) {
        LOG_WARN("failed to recursive get column expr", K(ret));
      } else {
        column = inner_column;
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::add_update_set(ObSubPlanFilterSpec &spec)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 8> all_output_exprs;
  const int64_t child_cnt = spec.get_child_cnt();
  ObOpSpec *child_spec = NULL;
  for (int64_t i = 1; OB_SUCC(ret) && i < child_cnt; ++i) {
    if (OB_ISNULL(child_spec = spec.get_children()[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(child_spec));
    } else if (OB_FAIL(append(all_output_exprs, child_spec->output_))) {
      LOG_WARN("failed to append child output.", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(all_output_exprs.count() + 1 < child_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected child output exprs size", K(ret), K(all_output_exprs.count()),
                                                  K(child_cnt));
  } else if (OB_FAIL(spec.update_set_.assign(all_output_exprs))) {
    LOG_WARN("failed to assign update set", K(ret));
  } else {
    LOG_DEBUG("add update set", K(spec.update_set_.count()));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(
    ObLogSubPlanFilter &op, ObSubPlanFilterSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  if (op.is_update_set() && OB_FAIL(add_update_set(spec))) {
    LOG_WARN("failed to add update set", K(ret));
  }
  CK(NULL != op.get_plan() && NULL != op.get_plan()->get_stmt());
  if (OB_SUCC(ret)) {
    const ObIArray<ObExecParamRawExpr *> *exec_params[] = {
      &op.get_exec_params(), &op.get_onetime_exprs()
    };
    ObFixedArray<ObDynamicParamSetter, ObIAllocator> *setters[] = {
      &spec.rescan_params_, &spec.onetime_exprs_
    };
    static_assert(ARRAYSIZEOF(exec_params) == ARRAYSIZEOF(setters),
                  "array count mismatch");
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(exec_params); i++) {
      OZ(generate_param_spec(*exec_params[i], *setters[i]));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(spec.left_rescan_params_.init(op.get_above_pushdown_left_params().count()))) {
      LOG_WARN("fail to init fixed array", K(ret));
    } else if (OB_FAIL(spec.right_rescan_params_.init(op.get_above_pushdown_right_params().count()))) {
      LOG_WARN("fail to init fixed array", K(ret));
    } else if (OB_FAIL(set_batch_exec_param(*exec_params[0], *setters[0]))) {
      LOG_WARN("fail to set batch exec param", K(ret));
    }
    ARRAY_FOREACH(op.get_above_pushdown_left_params(), i) {
      ObExecParamRawExpr* param_expr = op.get_above_pushdown_left_params().at(i);
      if (OB_FAIL(batch_exec_param_caches_.push_back(BatchExecParamCache(param_expr,
                                                                      &spec,
                                                                      true)))) {
        LOG_WARN("fail to push back param expr", K(ret));
      }
    }
    ARRAY_FOREACH(op.get_above_pushdown_right_params(), i) {
      ObExecParamRawExpr* param_expr = op.get_above_pushdown_right_params().at(i);
      if (OB_FAIL(batch_exec_param_caches_.push_back(BatchExecParamCache(param_expr,
                                                                      &spec,
                                                                      false)))) {
        LOG_WARN("fail to push back param expr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    OZ(spec.one_time_idxs_.add_members2(op.get_onetime_idxs()));
    OZ(spec.init_plan_idxs_.add_members2(op.get_initplan_idxs()));
  }

  //set exec_param idx depended
  if (OB_SUCC(ret)) {
    //add all right children there
    int64_t subquery_cnt = spec.get_child_cnt() - 1;
    if (OB_FAIL(spec.exec_param_array_.init(subquery_cnt))) {
      LOG_WARN("failed to init exec param array", K(ret));
    } else {
       ObFixedArray<ObExpr *, ObIAllocator> cache_vec(phy_plan_->get_allocator());
       for (int64_t child_idx = 1; OB_SUCC(ret) && child_idx < spec.get_child_cnt(); ++child_idx) {
         SubPlanInfo *sp_info = nullptr;
         ObLogicalOperator *curr_child = nullptr;
        if (OB_ISNULL(curr_child = op.get_child(child_idx))
              || OB_ISNULL(curr_child->get_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get subplan filter child", K(child_idx), K(ret));
        } else if (OB_FAIL(op.get_plan()->get_subplan(curr_child->get_stmt(), sp_info))) {
          LOG_WARN("failed to get subplan info for this child", K(ret), K(child_idx));
        } else if (OB_ISNULL(sp_info) || OB_ISNULL(sp_info->init_expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sp info is invalid", K(ret), K(sp_info));
        } else {
          cache_vec.reset();
          if (OB_FAIL(cache_vec.init(sp_info->init_expr_->get_param_count()))) {
            LOG_WARN("failed to init tmp_vec", K(ret));
          }
          for (int64_t j = 0; OB_SUCC(ret) && j < sp_info->init_expr_->get_param_count(); ++j) {
            ObExecParamRawExpr *exec_param = sp_info->init_expr_->get_exec_param(j);
            ObExpr *rt_expr = nullptr;
            CK(nullptr != exec_param);
            //OX(rt_expr = ObStaticEngineExprCG::get_rt_expr(*param_expr));
            OZ(generate_rt_expr(*exec_param, rt_expr));
            OZ(cache_vec.push_back(rt_expr));
          }
          if (OB_SUCC(ret)) {
            OZ(spec.exec_param_array_.push_back(cache_vec));
          }
        }
      }
      OX(spec.exec_param_idxs_inited_ = true);
    }
  }

  // set enable px batch rescan infos
  if (OB_SUCC(ret)) {
    if (OB_FAIL(spec.init_px_batch_rescan_flags(spec.get_child_cnt()))) {
      LOG_WARN("fail to init px batch rescan flags", K(ret));
    } else {
      ObIArray<bool> &enable_op_px_batch_flags = op.get_px_batch_rescans();
      ObIArray<bool> &enable_phy_px_batch_flags = spec.enable_px_batch_rescans_;
      if (!enable_op_px_batch_flags.empty() &&
           enable_op_px_batch_flags.count() != spec.get_child_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("batch flag's count is unexpected", K(ret));
      } else {
        for (int i = 0; i < spec.get_child_cnt() && OB_SUCC(ret); ++i) {
          if (enable_op_px_batch_flags.empty()) {
            enable_phy_px_batch_flags.push_back(false);
          } else if (OB_FAIL(enable_phy_px_batch_flags.push_back(
                enable_op_px_batch_flags.at(i)))) {
            LOG_WARN("fail to push back batch flag", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    spec.enable_das_group_rescan_ = op.enable_das_group_rescan();
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(
    ObLogSubPlanScan &op, ObSubPlanScanSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = op.get_child(0);
  CK(NULL != child);
  OZ(spec.projector_.init(op.get_access_exprs().count() * 2));
  FOREACH_CNT_X(e, op.get_access_exprs(), OB_SUCC(ret)) {
    CK(NULL != *e);
    CK((*e)->is_column_ref_expr());
    const ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(*e);
    if (OB_SUCC(ret)) {
      // for generate table column_id is generated by OB_APP_MIN_COLUMN_ID + select_item_index
      int64_t idx = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
      CK(idx >= 0);
      CK(idx < child->get_output_exprs().count());
      CK(NULL != child->get_output_exprs().at(idx));
      if (OB_SUCC(ret)) {
        const ObRawExpr *from = child->get_output_exprs().at(idx);
        ObExpr *rt_expr = NULL;
        ObObjType from_type = from->get_result_type().get_type();
        ObObjType to_type = col_expr->get_result_type().get_type();
        ObCollationType from_coll = from->get_result_type().get_collation_type();
        ObCollationType to_coll = col_expr->get_result_type().get_collation_type();
        if (OB_UNLIKELY(ob_obj_type_class(from_type) != ob_obj_type_class(to_type) ||
                        (ob_is_string_or_lob_type(from_type) && from_coll != to_coll))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected output type of subplan scan", K(ret), K(from->get_result_type()),
                  K(col_expr->get_result_type()));
        }
        OZ(generate_rt_expr(*from, rt_expr));
        OZ(spec.projector_.push_back(rt_expr));
        OZ(generate_rt_expr(*col_expr, rt_expr));
        OZ(spec.projector_.push_back(rt_expr));
        OZ(mark_expr_self_produced(*e)); // table access exprs in convert_subplan_scan need to set IS_COLUMNLIZED flag
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogErrLog &op,
                                    ObErrLogSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  stmt::StmtType stmt_type = stmt::StmtType::T_INSERT;
  UNUSED(in_root_job);
  if (OB_FAIL(dml_cg_service_.generate_err_log_ctdef(op.get_err_log_define(), spec.err_log_ct_def_))) {
    LOG_WARN("fail to cg err_log_ctdef", K(ret));
  } else if (OB_FAIL(op.get_err_log_type(stmt_type))) {
    LOG_WARN("fail get error logging stmt type", K(ret));
  } else {
    ObDASOpType type = DAS_OP_TABLE_INSERT;
    switch(stmt_type) {
    case stmt::StmtType::T_INSERT:
      type = DAS_OP_TABLE_INSERT;
      break;
    case stmt::StmtType::T_UPDATE:
      type = DAS_OP_TABLE_UPDATE;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect stmt type");
      break;
    }
    spec.type_ = type;
  }
  return ret;
}


int ObStaticEngineCG::generate_spec(ObLogTableScan &op, ObRowSampleScanSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_normal_tsc(op, spec));

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan &op, ObBlockSampleScanSpec &spec, const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_normal_tsc(op, spec));

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogTableScan &op, ObTableScanWithIndexBackSpec &spec,
                                    const bool)
{
  int ret = OB_SUCCESS;
  OZ(generate_normal_tsc(op, spec));

  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogDelete &op,
                                    ObPxMultiPartDeleteSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (op.get_num_of_child() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child num is error", K(ret));
  } else if (OB_UNLIKELY(op.get_index_dml_infos().count() != 1) ||
             OB_ISNULL(op.get_index_dml_infos().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of dml index info is error", K(ret));
  } else {
    const IndexDMLInfo &index_dml_info = *op.get_index_dml_infos().at(0);
    phy_plan_->set_use_pdml(true);
    spec.is_returning_ = op.pdml_is_returning();
    spec.set_with_barrier(op.need_barrier());
    spec.is_pdml_index_maintain_ = op.is_index_maintenance();
    spec.is_pdml_update_split_ = op.is_pdml_update_split();

    int64_t partition_expr_idx = OB_INVALID_INDEX;
    if (OB_FAIL(get_pdml_partition_id_column_idx(spec.get_child(0)->output_, partition_expr_idx))) {
      LOG_WARN("failed to get partition id column idx", K(ret));
    } else if (OB_FAIL(dml_cg_service_.generate_delete_ctdef(op, index_dml_info, spec.del_ctdef_))) {
      LOG_WARN("generate delete ctdef failed", K(ret));
    } else {
      spec.row_desc_.set_part_id_index(partition_expr_idx);
    }
    LOG_TRACE("pdml static cg information", K(ret), K(index_dml_info), K(partition_expr_idx));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert &op,
                                    ObPxMultiPartInsertSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_UNLIKELY(op.get_index_dml_infos().count() != 1) ||
      OB_ISNULL(op.get_index_dml_infos().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index dml info is invalid", K(ret), K(op.get_index_dml_infos().count()));
  } else {
    const IndexDMLInfo &index_dml_info = *op.get_index_dml_infos().at(0);
    phy_plan_->set_use_pdml(true);
    spec.is_returning_ = op.pdml_is_returning();
    spec.is_pdml_index_maintain_ = op.is_index_maintenance();
    spec.table_location_uncertain_ = op.is_table_location_uncertain(); // row-movement target table
    spec.is_pdml_update_split_ = op.is_pdml_update_split();
    if (GCONF._ob_enable_direct_load) {
      spec.plan_->set_append_table_id(op.get_append_table_id());
      spec.plan_->set_enable_append(op.get_plan()->get_optimizer_context().get_global_hint().has_append());
    }
    int64_t partition_expr_idx = OB_INVALID_INDEX;
    if (OB_FAIL(get_pdml_partition_id_column_idx(spec.get_child(0)->output_, partition_expr_idx))) {
      LOG_WARN("failed to get partition id column idx", K(ret));
    } else {
      spec.row_desc_.set_part_id_index(partition_expr_idx);
    }
    LOG_TRACE("pdml static cg information", K(ret), K(partition_expr_idx), K(index_dml_info));
    // 处理pdml-insert中的insert_row_exprs
    OZ(dml_cg_service_.generate_insert_ctdef(op, index_dml_info, spec.ins_ctdef_));
    // table columns exprs in dml need to set IS_COLUMNLIZED flag
    OZ(mark_expr_self_produced(index_dml_info.column_exprs_));
    OZ(mark_expr_self_produced(index_dml_info.column_convert_exprs_));
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsert &op, ObPxMultiPartSSTableInsertSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  const ObExecContext *exec_ctx = nullptr;
  ObLogPlan *log_plan = nullptr;
  if (OB_FAIL(generate_spec(op, static_cast<ObPxMultiPartInsertSpec &>(spec), in_root_job))) {
    LOG_WARN("generate multi part sstable insert spec failed", K(ret));
  } else if (OB_ISNULL(log_plan = op.get_plan()) ||
             OB_ISNULL(exec_ctx = log_plan->get_optimizer_context().get_exec_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(log_plan), KP(exec_ctx));
  } else {
    ObSqlCtx *sql_ctx = const_cast<ObExecContext *>(exec_ctx)->get_sql_ctx();
    if (OB_FAIL(generate_rt_expr(*sql_ctx->flashback_query_expr_, spec.flashback_query_expr_))) {
      LOG_WARN("generate rt expr failed", K(ret));
    } else if (log_plan->get_optimizer_context().is_heap_table_ddl()) {
      spec.regenerate_heap_table_pk_ = true;
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogUpdate &op,
                                    ObPxMultiPartUpdateSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_UNLIKELY(op.get_index_dml_infos().count() != 1) ||
      OB_ISNULL(op.get_index_dml_infos().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index dml info is invalid", K(ret), K(op.get_index_dml_infos().count()));
  } else {
    phy_plan_->set_use_pdml(true);
    spec.is_returning_ = op.pdml_is_returning();
    spec.is_pdml_index_maintain_ = op.is_index_maintenance();
    const IndexDMLInfo &index_dml_info = *op.get_index_dml_infos().at(0);
    spec.is_ignore_ = op.is_ignore();
    phy_plan_->set_ignore(op.is_ignore());
    int64_t partition_expr_idx = OB_INVALID_INDEX;
    if (OB_FAIL(get_pdml_partition_id_column_idx(spec.get_child(0)->output_, partition_expr_idx))) {
      LOG_WARN("failed to get partition id column idx", K(ret));
    } else if (OB_FAIL(dml_cg_service_.generate_update_ctdef(op, index_dml_info, spec.upd_ctdef_))) {
      LOG_WARN("generate pdml update ctdef failed", K(ret));
    } else {
      spec.row_desc_.set_part_id_index(partition_expr_idx);
    }
    LOG_TRACE("pdml static cg information", K(ret), K(index_dml_info), K(partition_expr_idx));
    // table columns exprs in dml need to set IS_COLUMNLIZED flag
    OZ(mark_expr_self_produced(index_dml_info.column_exprs_));
  }
  return ret;
}

int ObStaticEngineCG::fill_aggr_infos(ObLogGroupBy &op,
    ObGroupBySpec &spec,
    common::ObIArray<ObExpr *> *group_exprs/*NULL*/,
    common::ObIArray<ObExpr *> *rollup_exprs/*NULL*/,
    common::ObIArray<ObExpr *> *distinct_exprs/*NULL*/)
{
  int ret = OB_SUCCESS;
  CK(NULL != phy_plan_);
  const ObIArray<ObRawExpr*> &aggr_exprs = op.get_aggr_funcs();
  //1.init aggr expr
  ObSEArray<ObExpr *, 8> all_aggr_exprs;
  ARRAY_FOREACH(aggr_exprs, i) {
    ObRawExpr *raw_expr = NULL;
    ObExpr *expr = NULL;
    if (OB_ISNULL(raw_expr = aggr_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw_expr is null ", K(ret), K(expr));
    } else if (OB_UNLIKELY(!raw_expr->has_flag(IS_AGG))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected aggr function", K(ret));
    } else if (OB_FAIL(mark_expr_self_produced(raw_expr))) { // aggr func exprs in group by need to set IS_COLUMNLIZED flag
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
	//   0 - output([ABS(T1.C1(0x7f1aa0c43580))(0x7f1aa0c45850)], [T_FUN_SUM(T1.C2(0x7f1aa0c489e0))(0x7f1aa0c48410)]), filter(nil),
	//       group([ABS(T1.C1(0x7f1aa0c43580))(0x7f1aa0c42a20)]), agg_func([T_FUN_SUM(T1.C2(0x7f1aa0c489e0))(0x7f1aa0c48410)])
	//   1 - output([T1.C1(0x7f1aa0c43580)], [T1.C2(0x7f1aa0c489e0)], [ABS(T1.C1(0x7f1aa0c43580))(0x7f1aa0c42a20)]), filter(nil),
	//       access([T1.C1(0x7f1aa0c43580)], [T1.C2(0x7f1aa0c489e0)]), partitions(p0),
  //
  // The output abs(c1) 0x7f1aa0c45850 is not the group by column (raw expr not the same),
  // (the resolver will correct this in future). We need the mysql non aggregate output
  // feature to evaluate it.
  ObSEArray<ObExpr *, 8> all_non_aggr_exprs;
  common::ObIArray<ObExpr *> &child_output = spec.get_children()[0]->output_;
  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_output_exprs().count(); ++i) {
    ObExpr *expr = NULL;
    const ObRawExpr &raw_expr = *op.get_output_exprs().at(i);
    if (OB_FAIL(generate_rt_expr(raw_expr, expr))) {
      LOG_WARN("failed to generate_rt_expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expr is null ", K(ret), K(expr));
    } else if (OB_FAIL(extract_non_aggr_expr(expr,
                                             &raw_expr,
                                             child_output,
                                             all_aggr_exprs,
                                             group_exprs,
                                             rollup_exprs,
                                             distinct_exprs,
                                             all_non_aggr_exprs))) {
      OB_LOG(WARN, "fail to extract_non_aggr_expr", "count", all_non_aggr_exprs.count(),
             KPC(expr), K(ret));
    } else {
      OB_LOG(DEBUG, "finish extract_non_aggr_expr", KPC(expr), K(raw_expr),  K(child_output),
             K(all_aggr_exprs), KPC(group_exprs), KPC(rollup_exprs), K(all_non_aggr_exprs));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < op.get_filter_exprs().count(); ++i) {
    ObExpr *expr = NULL;
    const ObRawExpr &raw_expr = *op.get_filter_exprs().at(i);
    if (OB_FAIL(generate_rt_expr(raw_expr, expr))) {
      LOG_WARN("failed to generate_rt_expr", K(ret));
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("expr is null ", K(ret), K(expr));
    } else if (OB_FAIL(extract_non_aggr_expr(expr,
                                             &raw_expr,
                                             child_output,
                                             all_aggr_exprs,
                                             group_exprs,
                                             rollup_exprs,
                                             distinct_exprs,
                                             all_non_aggr_exprs))) {
      OB_LOG(WARN, "fail to extract_non_aggr_expr", "count", all_non_aggr_exprs.count(),
             KPC(expr), K(ret));
    } else {
      OB_LOG(DEBUG, "finish extract_non_aggr_expr", KPC(expr), K(raw_expr), K(child_output),
             K(all_aggr_exprs), KPC(group_exprs), KPC(rollup_exprs), K(all_non_aggr_exprs));
    }
  }

  //3.init aggr_infos
  if (OB_SUCC(ret)) {
    if (OB_FAIL(spec.aggr_infos_.prepare_allocate(
        all_aggr_exprs.count() + all_non_aggr_exprs.count()))) {
      OB_LOG(WARN, "fail to prepare_allocate aggr_infos_", K(ret));
    }
  }

  //4.add aggr columns
  spec.support_fast_single_row_agg_ = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < all_aggr_exprs.count(); ++i) {
    ObAggrInfo &aggr_info = spec.aggr_infos_.at(i);
    if (!is_simple_aggr_expr(aggr_exprs.at(i)->get_expr_type())) {
      spec.support_fast_single_row_agg_ = false;
    }
    if (OB_FAIL(fill_aggr_info(*static_cast<ObAggFunRawExpr *>(aggr_exprs.at(i)),
                               *all_aggr_exprs.at(i),
                               aggr_info,
                               group_exprs,
                               rollup_exprs))) {
      LOG_WARN("failed to fill_aggr_info", K(ret));
    }
  }//end of for

  //5. file non_aggr_expr
  for (int64_t i = 0; OB_SUCC(ret) && i < all_non_aggr_exprs.count(); ++i) {
    ObExpr *expr = all_non_aggr_exprs.at(i);
    ObAggrInfo &aggr_info = spec.aggr_infos_.at(all_aggr_exprs.count() + i);
    aggr_info.set_implicit_first_aggr();
    aggr_info.expr_ = expr;
    LOG_TRACE("trace all non aggr exprs", K(*expr), K(all_non_aggr_exprs.count()));
  }
  return ret;
}

int ObStaticEngineCG::fill_aggr_info(ObAggFunRawExpr &raw_expr,
    ObExpr &expr, ObAggrInfo &aggr_info,
    common::ObIArray<ObExpr *> *group_exprs/*NULL*/,
    common::ObIArray<ObExpr *> *rollup_exprs/*NULL*/)
{
  int ret = OB_SUCCESS;
  if (T_FUN_TOP_FRE_HIST == raw_expr.get_expr_type() &&
             OB_FAIL(generate_top_fre_hist_expr_operator(raw_expr, aggr_info))) {
    LOG_WARN("failed to generate_top_fre_hist_expr_operator", K(ret));
  } else if (T_FUN_HYBRID_HIST == raw_expr.get_expr_type() &&
             OB_FAIL(generate_hybrid_hist_expr_operator(raw_expr, aggr_info))) {
    LOG_WARN("failed to generate_top_fre_hist_expr_operator", K(ret));
  } else {
    if (is_oracle_mode()
        && raw_expr.get_expr_type() == T_FUN_ORA_JSON_ARRAYAGG
        && raw_expr.get_real_param_count() > CG_JSON_ARRAYAGG_STRICT) {
      ObRawExpr *format_json_expr = NULL;
      ObRawExpr *absent_on_null_expr = NULL;
      ObRawExpr *returning_type_expr = NULL;
      ObRawExpr *strict_json_expr = NULL;
      aggr_info.format_json_ = (OB_NOT_NULL(format_json_expr = raw_expr.get_param_expr(CG_JSON_ARRAYAGG_FORMAT))
                                && format_json_expr->get_data_type() == ObIntType
                                && static_cast<ObConstRawExpr *>(format_json_expr)->get_value().get_int())
                               ? true
                               : false;
      aggr_info.absent_on_null_ = (OB_NOT_NULL(absent_on_null_expr = raw_expr.get_param_expr(CG_JSON_ARRAYAGG_ON_NULL))
                                   && absent_on_null_expr->get_data_type() == ObIntType
                                   && static_cast<ObConstRawExpr *>(absent_on_null_expr)->get_value().get_int() <= 1)
                                  ? false
                                  : true;
      aggr_info.returning_type_ = (OB_NOT_NULL(returning_type_expr = raw_expr.get_param_expr(CG_JSON_ARRAYAGG_RETURNING))
                                   && returning_type_expr->get_data_type() == ObIntType)
                                  ? static_cast<ObConstRawExpr *>(returning_type_expr)->get_value().get_int()
                                  : INT64_MAX;
      aggr_info.strict_json_ = (OB_NOT_NULL(strict_json_expr = raw_expr.get_param_expr(CG_JSON_ARRAYAGG_STRICT))
                                && strict_json_expr->get_data_type() == ObIntType
                                && static_cast<ObConstRawExpr *>(strict_json_expr)->get_value().get_int())
                               ? true
                               : false;
    }
    if (is_oracle_mode()
        && raw_expr.get_expr_type() == T_FUN_ORA_JSON_OBJECTAGG
        && raw_expr.get_real_param_count() > CG_JSON_OBJECTAGG_UNIQUE_KEYS) {
      ObRawExpr *absent_on_null_expr = NULL;
      ObRawExpr *returning_type_expr = NULL;
      ObRawExpr *unique_keys_expr = NULL;
      ObRawExpr *strict_json_expr = NULL;
      ObRawExpr *format_json_expr = NULL;
      aggr_info.format_json_ = (OB_NOT_NULL(format_json_expr = raw_expr.get_param_expr(CG_JSON_OBJECTAGG_FORMAT))
                                && format_json_expr->get_data_type() == ObIntType
                                && static_cast<ObConstRawExpr *>(format_json_expr)->get_value().get_int())
                               ? true
                               : false;
      aggr_info.absent_on_null_ = (OB_NOT_NULL(absent_on_null_expr = raw_expr.get_param_expr(CG_JSON_OBJECTAGG_ON_NULL))
                                   && absent_on_null_expr->get_data_type() == ObIntType
                                   && static_cast<ObConstRawExpr *>(absent_on_null_expr)->get_value().get_int())
                                  ? false
                                  : true;
      aggr_info.returning_type_ = (OB_NOT_NULL(returning_type_expr = raw_expr.get_param_expr(CG_JSON_OBJECTAGG_RETURNING))
                                   && returning_type_expr->get_data_type() == ObIntType)
                                  ? static_cast<ObConstRawExpr *>(returning_type_expr)->get_value().get_int()
                                  : INT64_MAX;
      aggr_info.strict_json_ = (OB_NOT_NULL(strict_json_expr = raw_expr.get_param_expr(CG_JSON_OBJECTAGG_STRICT))
                                && strict_json_expr->get_data_type() == ObIntType
                                && static_cast<ObConstRawExpr *>(strict_json_expr)->get_value().get_int())
                               ? true
                               : false;
      aggr_info.with_unique_keys_ = (OB_NOT_NULL(unique_keys_expr = raw_expr.get_param_expr(CG_JSON_OBJECTAGG_UNIQUE_KEYS))
                                     && unique_keys_expr->get_data_type() == ObIntType
                                     && static_cast<ObConstRawExpr *>(unique_keys_expr)->get_value().get_int())
                                    ? true
                                    : false;
    }
    const int64_t group_concat_param_count =
        (is_oracle_mode()
         && raw_expr.get_expr_type() == T_FUN_GROUP_CONCAT
         && raw_expr.get_real_param_count() > 1)
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

    //set pl agg udf info
    if (OB_SUCC(ret) && T_FUN_PL_AGG_UDF == raw_expr.get_expr_type()) {
      if (OB_ISNULL(raw_expr.get_pl_agg_udf_expr()) ||
          OB_UNLIKELY(!raw_expr.get_pl_agg_udf_expr()->is_udf_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(raw_expr.get_pl_agg_udf_expr()));
      } else if (OB_FAIL(aggr_info.pl_agg_udf_params_type_.init(group_concat_param_count))) {
        LOG_WARN("failed to init pl_agg_udf_params_type_", K(ret));
      } else {
        ObUDFRawExpr *udf_expr = static_cast<ObUDFRawExpr *>(raw_expr.get_pl_agg_udf_expr());
        aggr_info.pl_agg_udf_type_id_ = udf_expr->get_type_id();
        aggr_info.pl_result_type_ = const_cast<ObExprResType &>(raw_expr.get_result_type());
      }
    }

    // mysql dll udf
    if (OB_SUCC(ret) && T_FUN_AGG_UDF == raw_expr.get_expr_type()) {
      aggr_info.dll_udf_ = OB_NEWx(ObAggDllUdfInfo, aggr_info.alloc_,
                                   (*aggr_info.alloc_), raw_expr.get_expr_type());
      OZ(aggr_info.dll_udf_->from_raw_expr(raw_expr));
    }

    ObSEArray<ObExpr*, 16> all_param_exprs;
    if (OB_SUCC(ret)) {
      const ObOrderDirection order_direction = default_asc_direction();
      const bool is_ascending = is_ascending_direction(order_direction);
      const common::ObCmpNullPos null_pos = ((is_null_first(order_direction) ^ is_ascending)
          ? NULL_LAST : NULL_FIRST);
      for (int64_t i = 0; OB_SUCC(ret) && i < group_concat_param_count; ++i) {
        ObExpr *expr = NULL;
        const ObRawExpr &param_raw_expr = *raw_expr.get_real_param_exprs().at(i);
        if (OB_FAIL(generate_rt_expr(param_raw_expr, expr))) {
          LOG_WARN("failed to generate_rt_expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null ", K(ret), K(expr));
        } else if ((T_FUN_GROUP_CONCAT == raw_expr.get_expr_type() ||
                    T_FUN_KEEP_WM_CONCAT == raw_expr.get_expr_type() ||
                    T_FUN_WM_CONCAT == raw_expr.get_expr_type())
                   && OB_UNLIKELY(!param_raw_expr.get_result_meta().is_string_type())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param_raw_expr is not sting ", K(ret), K(param_raw_expr));
        } else if (OB_FAIL(all_param_exprs.push_back(expr))) {
          LOG_WARN("failed to push_back param_expr", K(ret));
        } else if (T_FUN_PL_AGG_UDF == raw_expr.get_expr_type() &&
                   OB_FAIL(aggr_info.pl_agg_udf_params_type_.push_back(
                                  const_cast<ObExprResType &>(param_raw_expr.get_result_type())))) {
          LOG_WARN("failed to push_back expr type", K(ret));
        } else if (aggr_info.has_distinct_) {
          if (ob_is_user_defined_sql_type(expr->datum_meta_.type_) || ob_is_user_defined_pl_type(expr->datum_meta_.type_)) {
            // other udt types not supported, xmltype does not have order or map member function
            ret = OB_ERR_NO_ORDER_MAP_SQL;
            LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
          } else {
            ObSortFieldCollation field_collation(i, expr->datum_meta_.cs_type_, is_ascending, null_pos);
            ObSortCmpFunc cmp_func;
            cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(expr->datum_meta_.type_,
                                                                    expr->datum_meta_.type_,
                                                                    field_collation.null_pos_,
                                                                    field_collation.cs_type_,
                                                                    expr->datum_meta_.scale_,
                                                                    lib::is_oracle_mode(),
                                                                    expr->obj_meta_.has_lob_header());
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
    }

    if (OB_SUCC(ret) && (T_FUN_GROUP_CONCAT == raw_expr.get_expr_type() ||
                         T_FUN_GROUP_RANK == raw_expr.get_expr_type() ||
                         T_FUN_GROUP_DENSE_RANK == raw_expr.get_expr_type() ||
                         T_FUN_GROUP_PERCENT_RANK == raw_expr.get_expr_type() ||
                         T_FUN_GROUP_CUME_DIST == raw_expr.get_expr_type() ||
                         T_FUN_GROUP_PERCENTILE_CONT == raw_expr.get_expr_type() ||
                         T_FUN_GROUP_PERCENTILE_DISC == raw_expr.get_expr_type() ||
                         T_FUN_MEDIAN == raw_expr.get_expr_type() ||
                         T_FUN_KEEP_SUM == raw_expr.get_expr_type() ||
                         T_FUN_KEEP_MAX == raw_expr.get_expr_type() ||
                         T_FUN_KEEP_MIN == raw_expr.get_expr_type() ||
                         T_FUN_KEEP_COUNT == raw_expr.get_expr_type() ||
                         T_FUN_KEEP_WM_CONCAT == raw_expr.get_expr_type() ||
                         T_FUN_HYBRID_HIST == raw_expr.get_expr_type() ||
                         T_FUN_ORA_JSON_ARRAYAGG == raw_expr.get_expr_type() ||
                         T_FUN_ORA_XMLAGG == raw_expr.get_expr_type())) {
      const ObRawExpr *param_raw_expr = (is_oracle_mode()
          && T_FUN_GROUP_CONCAT == raw_expr.get_expr_type()
          && raw_expr.get_real_param_count() > 1)
          ? raw_expr.get_real_param_exprs().at(raw_expr.get_real_param_count() - 1)
          : raw_expr.get_separator_param_expr();
      if (param_raw_expr != NULL) {
        ObExpr *expr = NULL;
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

      if (OB_SUCC(ret) && !raw_expr.get_order_items().empty()) {
        aggr_info.has_order_by_ = true;
        if (OB_FAIL(fil_sort_info(raw_expr.get_order_items(),
                                  all_param_exprs,
                                  NULL,
                                  aggr_info.sort_collations_,
                                  aggr_info.sort_cmp_funcs_))) {
          LOG_WARN("failed to fil_sort_info", K(ret));
        } else {/*do nothing*/}
      }//order item
    }//group concat

    // The argment of grouping is index in rollup exprs
    if (OB_SUCC(ret) && OB_NOT_NULL(rollup_exprs) &&
        T_FUN_GROUPING == raw_expr.get_expr_type() && 0 < rollup_exprs->count()) {
      bool match = false;
      ObExpr *arg_expr = expr.args_[0];
      for (int64_t expr_idx = 0; !match && expr_idx < group_exprs->count(); expr_idx++) {
        if (arg_expr == group_exprs->at(expr_idx)) {
          match = true;
        }
      }
      for (int64_t expr_idx = 0; !match && expr_idx < rollup_exprs->count(); expr_idx++) {
        if (arg_expr == rollup_exprs->at(expr_idx)) {
          match = true;
          aggr_info.rollup_idx_ = expr_idx + group_exprs->count();
        }
      }
    }

    // The arguments of grouping_id are the indexs in rollup exprs.
    aggr_info.grouping_idxs_.init(expr.arg_cnt_);
    if (OB_SUCC(ret) && OB_NOT_NULL(rollup_exprs) &&
        T_FUN_GROUPING_ID == raw_expr.get_expr_type() && rollup_exprs->count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
        int64_t expr_idx = OB_INVALID_INDEX;
        ObExpr *arg_expr = expr.args_[i];
        if (has_exist_in_array(*group_exprs, arg_expr, &expr_idx)) {
          if (OB_FAIL(aggr_info.grouping_idxs_.push_back(expr_idx))) {
            LOG_WARN("push_back fail", K(ret));
          }
        }
        if (expr_idx == OB_INVALID_INDEX && has_exist_in_array(*rollup_exprs, arg_expr, &expr_idx)) {
          if (OB_FAIL(aggr_info.grouping_idxs_.push_back(group_exprs->count() + expr_idx))) {
            LOG_WARN("push_back fail", K(ret));
          }
        }
      }
    }

    //group_id()
    ObSEArray<int64_t,10> group_id_array;
    if (OB_SUCC(ret) && T_FUN_GROUP_ID == raw_expr.get_expr_type()) {
      if (OB_ISNULL(group_exprs)) {
        ret = OB_ERR_GROUPING_FUNC_WITHOUT_GROUP_BY;
        LOG_WARN("grouping_id shouldn't appear if there were no groupby", K(ret));
      } else if (OB_NOT_NULL(rollup_exprs) && rollup_exprs->count() + group_exprs->count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs->count(); i++) {
          if (has_exist_in_array(*group_exprs, rollup_exprs->at(i))){
            if (OB_FAIL(group_id_array.push_back(group_exprs->count() + i))) {
              LOG_WARN("push_back fail", K(ret));
            }
          }
        }
      }
      aggr_info.group_idxs_.init(group_id_array.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < group_id_array.count(); i++) {
        if (OB_FAIL(aggr_info.group_idxs_.push_back(group_id_array.at(i)))) {
          LOG_WARN("push_back fail", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (all_param_exprs.empty() && T_FUN_COUNT != raw_expr.get_expr_type() &&
          T_FUN_GROUP_ID != raw_expr.get_expr_type()) {
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

int ObStaticEngineCG::extract_non_aggr_expr(ObExpr *input,
    const ObRawExpr *raw_input,
    common::ObIArray<ObExpr *> &exist_in_child,
    common::ObIArray<ObExpr *> &not_exist_in_aggr,
    common::ObIArray<ObExpr *> *not_exist_in_groupby,
    common::ObIArray<ObExpr *> *not_exist_in_rollup,
    common::ObIArray<ObExpr *> *not_exist_in_distinct,
    common::ObIArray<ObExpr *> &output) const
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(check_stack_overflow())) {
    OB_LOG(WARN, "check_stack_overflow failed", "count", output.count(), K(ret), K(lbt()));
  } else if (OB_ISNULL(input)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "input is null", KP(input), K(ret));
  } else if (input->is_const_expr()) {
    // Skip the const expr as implicit first aggr, all const implicit first aggr need add
    // `remove_const` above to calc the result.
  } else if (has_exist_in_array(exist_in_child, input)
             && !has_exist_in_array(not_exist_in_aggr, input)
             && (NULL == not_exist_in_groupby || !has_exist_in_array(*not_exist_in_groupby, input))
             && (NULL == not_exist_in_rollup || !has_exist_in_array(*not_exist_in_rollup, input))) {
    if (OB_FAIL(add_var_to_array_no_dup(output, input))) {
      OB_LOG(WARN, "fail to add_var_to_array_no_dup", "count", output.count(),
             KPC(input), K(ret));
    }
  } else if (NULL != not_exist_in_groupby && has_exist_in_array(*not_exist_in_groupby, input) &&
      nullptr != not_exist_in_distinct && has_exist_in_array(*not_exist_in_distinct, input)) {
    // select /*+ parallel(3) */ c1,count(c3),sum(distinct c1),min(c2) from t1 order by 1,2,3;
    // for three stage, c1 is exists in distinct exprs, then need to calculate the implicit expr
    if (OB_FAIL(add_var_to_array_no_dup(output, input))) {
      OB_LOG(WARN, "fail to add_var_to_array_no_dup", "count", output.count(),
             KPC(input), K(ret));
    } else {
      LOG_DEBUG("debug add distinct expr", K(*input));
    }
  } else if (!has_exist_in_array(not_exist_in_aggr, input)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < input->arg_cnt_; ++i) {
      ObExpr *expr = input->args_[i];
      const ObRawExpr *raw_expr = (raw_input != NULL ? raw_input->get_param_expr(i) : NULL);
      if (OB_FAIL(extract_non_aggr_expr(expr,
                                        raw_expr,
                                        exist_in_child,
                                        not_exist_in_aggr,
                                        not_exist_in_groupby,
                                        not_exist_in_rollup,
                                        not_exist_in_distinct,
                                        output))) {
        OB_LOG(WARN, "fail to extract_non_aggr_expr", "count", output.count(), KPC(expr),
            KPC(raw_expr), K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogLinkScan &op, ObLinkScanSpec &spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  if (OB_FAIL(op.gen_link_stmt_param_infos())) {
    LOG_WARN("failed to generate link stmt", K(ret));
  } else if (OB_FAIL(spec.set_param_infos(op.get_param_infos()))) {
    LOG_WARN("failed to set param infos", K(ret));
  } else if (OB_FAIL(spec.set_stmt_fmt(op.get_stmt_fmt_buf(), op.get_stmt_fmt_len()))) {
    LOG_WARN("failed to set stmt fmt", K(ret));
  } else if (OB_FAIL(spec.select_exprs_.init(op.get_select_exprs().count()))) {
    LOG_WARN("init fixed array failed", K(ret), K(op.get_select_exprs().count()));
  } else if (OB_ISNULL(op.get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (OB_ISNULL(op.get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else {
    spec.has_for_update_ = op.get_plan()->get_stmt()->has_for_update();
    spec.is_reverse_link_ = op.get_reverse_link();
    spec.dblink_id_ = op.get_dblink_id();
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_select_exprs().count(); ++i) {
      ObExpr *rt_expr = nullptr;
      const ObRawExpr* select_expr = op.get_select_exprs().at(i);
      if (OB_ISNULL(select_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(generate_rt_expr(*select_expr, rt_expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (OB_FAIL(spec.select_exprs_.push_back(rt_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogLinkDml &op, ObLinkDmlSpec &spec, const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  if (OB_FAIL(op.gen_link_stmt_param_infos())) {
    LOG_WARN("failed to generate link stmt", K(ret));
  } else if (OB_FAIL(spec.set_param_infos(op.get_param_infos()))) {
    LOG_WARN("failed to set param infos", K(ret));
  } else if (OB_FAIL(spec.set_stmt_fmt(op.get_stmt_fmt_buf(), op.get_stmt_fmt_len()))) {
    LOG_WARN("failed to set stmt fmt", K(ret));
  } else {
    spec.is_reverse_link_ = op.get_reverse_link();
    spec.dblink_id_ = op.get_dblink_id();
    spec.plan_->set_returning(false);
    spec.plan_->need_drive_dml_query_ = true;
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogWindowFunction &op, ObWindowFunctionSpec &spec,
    const bool in_root_job)
{
  UNUSED(in_root_job);
  int ret = OB_SUCCESS;
  ObSEArray<ObExpr*, 16> rd_expr;
  ObSEArray<ObExpr*, 16> all_expr;
  if (OB_UNLIKELY(op.get_num_of_child() != 1 || OB_ISNULL(op.get_child(0)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("wrong number of children", K(ret), K(op.get_num_of_child()));
  }
  if (OB_SUCC(ret) && op.is_range_dist_parallel()) {
    ObSEArray<OrderItem, 8> rd_sort_keys;
    if (OB_FAIL(op.get_rd_sort_keys(rd_sort_keys))) {
      LOG_WARN("Get unexpected null", K(ret));
    } else {
      OZ(fill_sort_info(rd_sort_keys, spec.rd_sort_collations_, rd_expr));
      OZ(fill_sort_funcs(spec.rd_sort_collations_, spec.rd_sort_cmp_funcs_, rd_expr));
      OZ(append(all_expr, rd_expr));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_window_functions_order(op.get_window_exprs()))) {
    LOG_WARN("failed to check window functions order", K(ret));
  } else if (OB_FAIL(spec.wf_infos_.prepare_allocate(op.get_window_exprs().count()))) {
    LOG_WARN("failed to prepare_allocate the window function.", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_expr, spec.get_child()->output_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    spec.single_part_parallel_ = op.is_single_part_parallel();
    spec.range_dist_parallel_ = op.is_range_dist_parallel();
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_window_exprs().count(); ++i) {
      ObWinFunRawExpr *wf_expr = op.get_window_exprs().at(i);
      WinFuncInfo &wf_info = spec.wf_infos_.at(i);
      if (OB_ISNULL(wf_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Get unexpected null", K(ret));
      } else if (op.is_push_down() && op.get_window_exprs().count() != op.get_pushdown_info().count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("count of window_exprs is not equal to count of pushdowns", K(ret),
                 K(op.get_window_exprs().count()), K(op.get_pushdown_info().count()));
      } else if (OB_FAIL(fill_wf_info(
                 all_expr, *wf_expr, wf_info, op.is_push_down() && op.get_pushdown_info().at(i)))) {
        LOG_WARN("failed to generate window function info", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    spec.role_type_ = op.get_role_type();
    if (op.is_push_down()) {
      if (OB_ISNULL(op.get_aggr_status_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr_status_expr is null", K(ret), K(op.get_role_type()));
      } else {
        OZ(generate_rt_expr(*op.get_aggr_status_expr(), spec.wf_aggr_status_expr_));
        OZ(mark_expr_self_produced(op.get_aggr_status_expr()));
        OZ(add_var_to_array_no_dup(all_expr, spec.wf_aggr_status_expr_));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(spec.all_expr_.assign(all_expr))) {
      LOG_WARN("failed to assign", K(ret));
    }
  }
  if (OB_SUCC(ret) && op.is_range_dist_parallel()) {
    // All function in one window function operator are range distributed currently
    OZ(spec.rd_wfs_.init(spec.wf_infos_.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < spec.wf_infos_.count(); i++) {
      OZ(spec.rd_wfs_.push_back(i));
      OZ(rd_expr.push_back(spec.wf_infos_.at(i).expr_));
    }
    OZ(spec.rd_coord_exprs_.assign(rd_expr));
    spec.rd_pby_sort_cnt_ = op.get_rd_pby_sort_cnt();
  }

  LOG_DEBUG("finish generate_spec", K(spec), K(ret));
  return ret;
}

int ObStaticEngineCG::fill_wf_info(ObIArray<ObExpr *> &all_expr,
    ObWinFunRawExpr &win_expr, WinFuncInfo &wf_info, const bool can_push_down)
{
  int ret = OB_SUCCESS;
  ObRawExpr *agg_raw_expr = win_expr.get_agg_expr();
  ObExpr *expr = NULL;
  const ObIArray<ObRawExpr *> &func_params = win_expr.get_func_params();
  if (OB_FAIL(generate_rt_expr(win_expr, expr))) {
    LOG_WARN("failed to generate_rt_expr", K(ret));
  } else if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null ", K(ret), K(expr));
  } else if (OB_FAIL(mark_expr_self_produced(&win_expr))) { // win func exprs need to set IS_COLUMNLIZED flag
    LOG_WARN("failed to add columnized flag", K(ret));
  } else if (OB_FAIL(wf_info.init(func_params.count(),
                                  win_expr.get_partition_exprs().count(),
                                  win_expr.get_order_items().count()))) {
    LOG_WARN("failed to init the func info.", K(ret));
  } else if (NULL != agg_raw_expr && OB_UNLIKELY(!agg_raw_expr->has_flag(IS_AGG))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected aggr function", KPC(agg_raw_expr), K(ret));
  } else {
    if (NULL == agg_raw_expr) {
      //do nothing
    } else if (OB_FAIL(fill_aggr_info(*static_cast<ObAggFunRawExpr *>(agg_raw_expr),
                                      *expr,
                                      wf_info.aggr_info_, nullptr, nullptr))) {
      LOG_WARN("failed to fill_aggr_info", K(ret));
    } else {
      wf_info.aggr_info_.real_aggr_type_ = agg_raw_expr->get_expr_type();
    }

    wf_info.expr_ = expr;
    wf_info.func_type_ = win_expr.get_func_type();
    wf_info.win_type_ = win_expr.get_window_type();
    wf_info.is_ignore_null_ = win_expr.is_ignore_null();
    wf_info.is_from_first_ = win_expr.is_from_first();
    wf_info.can_push_down_ = can_push_down;

    if (OB_SUCC(ret)) {
      switch (wf_info.func_type_)
      {
        case T_FUN_SUM:
        case T_FUN_AVG:
        case T_FUN_COUNT:
          wf_info.remove_type_ = common::REMOVE_STATISTICS;
          break;
        case T_FUN_MAX:
        case T_FUN_MIN:
          wf_info.remove_type_ = common::REMOVE_EXTRENUM;
          break;
        default:
          wf_info.remove_type_ = common::REMOVE_INVALID;
          break;
      }
      // ObFloatTC and ObDoubleTC may cause precision question
      if (common::REMOVE_STATISTICS == wf_info.remove_type_
          && !wf_info.aggr_info_.param_exprs_.empty()) {
        const ObObjTypeClass column_tc =
          ob_obj_type_class(wf_info.aggr_info_.get_first_child_type());
        if (ObFloatTC == column_tc || ObDoubleTC == column_tc) {
          wf_info.remove_type_ = common::REMOVE_INVALID;
        }
      }
    }

    wf_info.upper_.is_preceding_ = win_expr.upper_.is_preceding_;
    wf_info.upper_.is_unbounded_ = BOUND_UNBOUNDED == win_expr.upper_.type_;
    wf_info.upper_.is_nmb_literal_ = win_expr.upper_.is_nmb_literal_;
    wf_info.lower_.is_preceding_ = win_expr.lower_.is_preceding_;
    wf_info.lower_.is_unbounded_ = BOUND_UNBOUNDED == win_expr.lower_.type_;
    wf_info.lower_.is_nmb_literal_ = win_expr.lower_.is_nmb_literal_;

    // add window function params.
    for (int64_t i = 0; OB_SUCC(ret) && i < func_params.count(); ++i) {
      ObRawExpr *raw_expr = func_params.at(i);
      expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw expr is null", K(ret), K(i));
      } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null ", K(ret), K(expr));
      } else if (OB_FAIL(wf_info.param_exprs_.push_back(expr))){
        LOG_WARN("push back sql expr failed", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObRawExpr *raw_expr = win_expr.upper_.interval_expr_;
      expr = NULL;
      if (NULL == raw_expr) {
        //do nothing
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
      ObRawExpr *raw_expr = win_expr.lower_.interval_expr_;
      expr = NULL;
      if (NULL == raw_expr) {
        //do nothing
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
      //do nothing
    } else {
      bool is_asc = win_expr.get_order_items().empty() ? true: win_expr.get_order_items().at(0).is_ascending();
      ObRawExpr *upper_raw_expr = (win_expr.upper_.is_preceding_ ^ is_asc)
          ? win_expr.upper_.exprs_[0] : win_expr.upper_.exprs_[1];
      ObRawExpr *lower_raw_expr = (win_expr.lower_.is_preceding_ ^ is_asc)
          ? win_expr.lower_.exprs_[0] : win_expr.lower_.exprs_[1];
      if (OB_SUCC(ret)) {
        expr = NULL;
        if (NULL == upper_raw_expr) {
          //do nothing
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
          //do nothing
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
      const ObRawExpr *raw_expr = win_expr.get_partition_exprs().at(i);
      expr = NULL;
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw expr is null", K(ret), K(i));
      } else if (OB_FAIL(generate_rt_expr(*raw_expr, expr))) {
        LOG_WARN("failed to generate_rt_expr", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null ", K(ret), K(expr));
      } else if (ob_is_user_defined_sql_type(expr->datum_meta_.type_) || ob_is_user_defined_pl_type(expr->datum_meta_.type_)) {
        // partition by clause not support xmltype
        ret = OB_ERR_NO_ORDER_MAP_SQL;
        LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
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
      const OrderItem &order_item = sort_keys.at(i);
      ObExpr *expr = nullptr;
      int64_t idx = OB_INVALID_INDEX;
      if (OB_FAIL(generate_rt_expr(*order_item.expr_, expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (ob_is_user_defined_sql_type(expr->datum_meta_.type_) || ob_is_user_defined_pl_type(expr->datum_meta_.type_)) {
        // other udt types not supported, xmltype does not have order or map member function
        ret = OB_ERR_NO_ORDER_MAP_SQL;
        LOG_WARN("cannot ORDER objects without MAP or ORDER method", K(ret));
      } else if (sort_exprs != NULL && OB_FAIL(sort_exprs->push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (has_exist_in_array(all_exprs, expr, &idx)) {
        if (OB_UNLIKELY(idx < 0)
            || OB_UNLIKELY(idx >= all_exprs.count())) {
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
                                                                 expr->datum_meta_.scale_,
                                                                 lib::is_oracle_mode(),
                                                                 expr->obj_meta_.has_lob_header());
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

int ObStaticEngineCG::get_pdml_partition_id_column_idx(const ObIArray<ObExpr *> &dml_exprs,
                                                       int64_t &idx)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; i < dml_exprs.count(); i++) {
    const ObExpr *expr = dml_exprs.at(i);
    if (T_PDML_PARTITION_ID == expr->type_) {
      idx = i;
      found = true;
      break;
    }
  }
  if (!found) {
    idx = NO_PARTITION_ID_FLAG; // NO_PARTITION_ID_FLAG = -2
  }
  return ret;
}

int ObStaticEngineCG::generate_top_fre_hist_expr_operator(ObAggFunRawExpr &raw_expr,
                                                          ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_FUN_TOP_FRE_HIST != raw_expr.get_expr_type() ||
                  raw_expr.get_param_count() != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(raw_expr), K(ret));
  } else {
    ObRawExpr *win_raw_expr = raw_expr.get_param_expr(0);
    ObRawExpr *param_raw_expr = raw_expr.get_param_expr(1);
    ObRawExpr *item_raw_expr = raw_expr.get_param_expr(2);
    ObExpr *win_expr = NULL;
    ObExpr *item_expr = NULL;
    raw_expr.get_real_param_exprs_for_update().reset();
    if (OB_ISNULL(win_raw_expr) || OB_ISNULL(param_raw_expr) || OB_ISNULL(item_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(win_raw_expr), K(param_raw_expr), K(item_raw_expr), K(ret));
    } else if (OB_FAIL(generate_rt_expr(*win_raw_expr, win_expr)) ||
               OB_FAIL(generate_rt_expr(*item_raw_expr, item_expr))) {
      LOG_WARN("failed to generate_rt_expr", K(ret), K(*win_raw_expr), K(*item_raw_expr));
    } else if (OB_ISNULL(win_expr) || OB_ISNULL(item_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null ", K(ret), K(win_expr), K(item_expr));
    } else if (OB_UNLIKELY(!win_expr->obj_meta_.is_numeric_type() ||
                           !item_expr->obj_meta_.is_numeric_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expr node is null", K(ret), K(win_expr->obj_meta_), K(item_expr->obj_meta_));
    } else {
      aggr_info.window_size_param_expr_ = win_expr;
      aggr_info.item_size_param_expr_ = item_expr;
      aggr_info.is_need_deserialize_row_ = raw_expr.is_need_deserialize_row();
      if (OB_FAIL(raw_expr.add_real_param_expr(param_raw_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_hybrid_hist_expr_operator(ObAggFunRawExpr &raw_expr,
                                                         ObAggrInfo &aggr_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(T_FUN_HYBRID_HIST != raw_expr.get_expr_type() ||
                  raw_expr.get_param_count() != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid argument", K(raw_expr), K(ret));
  } else {
    ObRawExpr *param_raw_expr = raw_expr.get_param_expr(0);
    ObRawExpr *bucket_num_raw_expr = raw_expr.get_param_expr(1);
    ObExpr *bucket_num_expr = NULL;
    raw_expr.get_real_param_exprs_for_update().reset();
    if ( OB_ISNULL(param_raw_expr) || OB_ISNULL(bucket_num_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(param_raw_expr), K(bucket_num_raw_expr), K(ret));
    } else if (OB_FAIL(generate_rt_expr(*bucket_num_raw_expr, bucket_num_expr))) {
      LOG_WARN("failed to generate_rt_expr", K(ret), K(*bucket_num_raw_expr));
    } else if (OB_ISNULL(bucket_num_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null ", K(ret), K(bucket_num_expr));
    } else if (OB_UNLIKELY(!bucket_num_expr->obj_meta_.is_numeric_type())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("expr node is null", K(ret), K(bucket_num_expr->obj_meta_));
    } else {
      aggr_info.bucket_num_param_expr_ = bucket_num_expr;
      if (OB_FAIL(raw_expr.add_real_param_expr(param_raw_expr))) {
        LOG_WARN("fail to add param expr to agg expr", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogSelectInto &op, ObSelectIntoSpec &spec,
    const bool in_root_job)
{
  UNUSED(in_root_job);
  ObIAllocator &alloc = phy_plan_->get_allocator();
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(1 != op.get_num_of_child())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count of children", K(ret), K(op.get_num_of_child()));
  } else if (OB_FAIL(deep_copy_obj(alloc, op.get_outfile_name(), spec.outfile_name_))) {
    LOG_WARN("fail to set outfile name", K(op.get_outfile_name()), K(ret));
  } else if (OB_FAIL(deep_copy_obj(alloc, op.get_filed_str(), spec.filed_str_))) {
    LOG_WARN("fail to set filed str", K(op.get_filed_str()), K(ret));
  } else if (OB_FAIL(deep_copy_obj(alloc, op.get_line_str(), spec.line_str_))) {
    LOG_WARN("fail to set line str", K(op.get_line_str()), K(ret));
  } else if (OB_FAIL(spec.user_vars_.init(op.get_user_vars().count()))) {
    LOG_WARN("init fixed array failed", K(ret), K(op.get_user_vars().count()));
  } else if (OB_FAIL(spec.select_exprs_.init(op.get_select_exprs().count()))) {
    LOG_WARN("init fixed array failed", K(ret), K(op.get_select_exprs().count()));
  } else {
    ObString var;
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_user_vars().count(); ++i) {
      var.reset();
      if (OB_FAIL(ob_write_string(alloc, op.get_user_vars().at(i), var))) {
        LOG_WARN("fail to deep copy string", K(op.get_user_vars().at(i)), K(ret));
      } else if (OB_FAIL(spec.user_vars_.push_back(var))) {
        LOG_WARN("fail to push back var", K(var), K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_select_exprs().count(); ++i) {
      ObExpr *rt_expr = nullptr;
      const ObRawExpr* select_expr = op.get_select_exprs().at(i);
      if (OB_ISNULL(select_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(generate_rt_expr(*select_expr, rt_expr))) {
        LOG_WARN("failed to generate rt expr", K(ret));
      } else if (OB_FAIL(spec.select_exprs_.push_back(rt_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      spec.into_type_ = op.get_into_type();
      spec.closed_cht_ = op.get_closed_cht();
      spec.is_optional_ = op.get_is_optional();
      spec.plan_->need_drive_dml_query_ = true;
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogFunctionTable &op, ObFunctionTableSpec &spec,
    const bool in_root_job)
{
  UNUSED(in_root_job);
  ObIAllocator &alloc = phy_plan_->get_allocator();
  ObRawExpr *value_raw_expr = nullptr;
  ObExpr *value_expr = nullptr;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get stmt", K(ret));
  } else if (OB_FAIL(spec.column_exprs_.init(op.get_stmt()->get_column_size()))) {
    LOG_WARN("failed to init array", K(ret));
  } else if (OB_UNLIKELY(op.get_num_of_child() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count of children", K(ret), K(op.get_num_of_child()));
  } else if (OB_ISNULL(value_raw_expr = op.get_value_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get value raw expr", K(ret));
  } else if (OB_FAIL(generate_rt_expr(*value_raw_expr, value_expr))) {
    LOG_WARN("failed to generate rt expr", K(ret));
  } else {
    spec.has_correlated_expr_ = value_raw_expr->has_flag(CNT_DYNAMIC_PARAM);
    spec.value_expr_ = value_expr;
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_output_exprs().count(); ++i) {
      if (OB_FAIL(mark_expr_self_produced(op.get_output_exprs().at(i)))) {
        LOG_WARN("failed to mark expr self produced", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_stmt()->get_column_size(); ++i) {
      ObExpr *rt_expr = nullptr;
      const ColumnItem *col_item = op.get_stmt()->get_column_item(i);
      CK (OB_NOT_NULL(col_item));
      CK (OB_NOT_NULL(col_item->expr_));
      if (OB_SUCC(ret)
          && col_item->table_id_ == op.get_table_id()
          && col_item->expr_->is_explicited_reference()) {
        OZ (mark_expr_self_produced(col_item->expr_));
        OZ (generate_rt_expr(*col_item->expr_, rt_expr));
        OZ (spec.column_exprs_.push_back(rt_expr));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogJsonTable &op, ObJsonTableSpec &spec,
    const bool in_root_job)
{
  UNUSED(in_root_job);
  ObIAllocator &alloc = phy_plan_->get_allocator();
  ObRawExpr *value_raw_expr = nullptr;
  ObExpr *value_expr = nullptr;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op.get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get stmt", K(ret));
  } else if (OB_FAIL(spec.column_exprs_.init(op.get_stmt()->get_column_size()))
          || OB_FAIL(spec.emp_default_exprs_.init(op.get_stmt()->get_column_size()))
          || OB_FAIL(spec.err_default_exprs_.init(op.get_stmt()->get_column_size()))
          || OB_FAIL(spec.cols_def_.init(op.get_origin_cols_def().count()))) {
    LOG_WARN("failed to init array", K(ret));
  } else if (OB_UNLIKELY(op.get_num_of_child() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count of children", K(ret), K(op.get_num_of_child()));
  } else if (OB_ISNULL(value_raw_expr = op.get_value_expr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get value raw expr", K(ret));
  } else if (OB_FAIL(generate_rt_expr(*value_raw_expr, value_expr))) {
    LOG_WARN("failed to generate rt expr", K(ret));
  } else {
    spec.has_correlated_expr_ = value_raw_expr->has_flag(CNT_DYNAMIC_PARAM);
    spec.value_expr_ = value_expr;

    if (OB_FAIL(spec.dup_origin_column_defs(op.get_origin_cols_def()))) {
      LOG_WARN("failed to append col define", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_output_exprs().count(); ++i) {
      if (OB_FAIL(mark_expr_self_produced(op.get_output_exprs().at(i)))) {
        LOG_WARN("failed to mark expr self produced", K(ret));
      }
    }

    bool need_set_lob_header = get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0;
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_stmt()->get_column_size(); ++i) {
      ObExpr *rt_expr = nullptr;
      const ColumnItem *col_item = op.get_stmt()->get_column_item(i);
      CK (OB_NOT_NULL(col_item));
      CK (OB_NOT_NULL(col_item->expr_));
      if (OB_SUCC(ret)
          && col_item->table_id_ == op.get_table_id()
          && col_item->expr_->is_explicited_reference()) {
        OZ (mark_expr_self_produced(col_item->expr_));
        OZ (generate_rt_expr(*col_item->expr_, rt_expr));
        if (OB_SUCC(ret) && is_lob_storage(rt_expr->obj_meta_.get_type()) && need_set_lob_header) {
          rt_expr->obj_meta_.set_has_lob_header();
        }

        OZ (spec.column_exprs_.push_back(rt_expr));

        if (OB_FAIL(ret)) {
        } else if (col_item->col_idx_ == common::OB_INVALID_ID
                   || col_item->col_idx_ >= spec.cols_def_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get origin column info", K(ret), K(col_item->col_idx_),
                   K(col_item->column_name_));
        } else {
          ObJtColInfo* col_info = spec.cols_def_.at(col_item->col_idx_);
          col_info->output_column_idx_ = spec.column_exprs_.count() - 1;

          if (OB_NOT_NULL(col_item->default_value_expr_)) {
            ObExpr *err_expr = nullptr;
            OZ (mark_expr_self_produced(col_item->default_value_expr_));
            OZ (generate_rt_expr(*col_item->default_value_expr_, err_expr));
            if (OB_SUCC(ret) && is_lob_storage(err_expr->obj_meta_.get_type()) && need_set_lob_header) {
              err_expr->obj_meta_.set_has_lob_header();
            }
            OX (col_info->error_expr_id_ = spec.err_default_exprs_.count());
            OZ (spec.err_default_exprs_.push_back(err_expr));
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(col_item->default_empty_expr_)) {
            ObExpr *emp_expr = nullptr;
            OZ (mark_expr_self_produced(col_item->default_empty_expr_));
            OZ (generate_rt_expr(*col_item->default_empty_expr_, emp_expr));
            if (OB_SUCC(ret) && is_lob_storage(emp_expr->obj_meta_.get_type()) && need_set_lob_header) {
              emp_expr->obj_meta_.set_has_lob_header();
            }
            OX (col_info->empty_expr_id_ = spec.emp_default_exprs_.count());
            OZ (spec.emp_default_exprs_.push_back(emp_expr));
          }
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogInsertAll &op,
                                    ObTableInsertAllSpec &spec,
                                    const bool in_root_job)
{
  int ret = OB_SUCCESS;
  UNUSED(in_root_job);
  if (OB_FAIL(generate_insert_all_with_das(op, spec))) {
    LOG_WARN("generate insert all with das failed", K(ret));
  } else {
    spec.is_insert_all_with_conditions_ = op.is_multi_conditions_insert();
    spec.is_insert_all_first_ = op.is_multi_insert_first();
  }
  return ret;
}

int ObStaticEngineCG::generate_insert_all_with_das(ObLogInsertAll &op, ObTableInsertAllSpec &spec)
{
  int ret = OB_SUCCESS;
  spec.check_fk_batch_ = true;
  if (OB_ISNULL(op.get_insert_all_table_info()) ||
      OB_ISNULL(phy_plan_) ||
      OB_UNLIKELY(op.get_table_list().count() != op.get_insert_all_table_info()->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(phy_plan_), K(op.get_insert_all_table_info()));
  } else if (OB_FAIL(spec.ins_ctdefs_.allocate_array(phy_plan_->get_allocator(),
                                                     op.get_table_list().count()))) {
    LOG_WARN("allocate insert ctdef array failed", K(ret));
  } else {
    spec.plan_->need_drive_dml_query_ = true;
    spec.use_dist_das_ = op.is_multi_part_dml();
    spec.gi_above_ = op.is_gi_above() && !spec.use_dist_das_;
    //no need, insert all don't occur returning or instead of trigger
    //spec.is_returning_ = op.is_returning();
    //spec.has_instead_of_trigger_ = op.has_instead_of_trigger();
    spec.insert_table_infos_.set_capacity(op.get_table_list().count());
    for (int64_t i = 0; OB_SUCC(ret) && i < op.get_table_list().count(); ++i) {
      InsertAllTableInfo *tbl_info = NULL;
      const ObInsertAllTableInfo* insert_tbl_info = op.get_insert_all_table_info()->at(i);
      const uint64_t loc_table_id = op.get_table_list().at(i);
      ObSEArray<IndexDMLInfo *, 4> index_dml_infos;
      const IndexDMLInfo *primary_dml_info = NULL;
      if (OB_ISNULL(insert_tbl_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null insert all table info", K(ret));
      } else if (OB_ISNULL(primary_dml_info = op.get_primary_dml_info(loc_table_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("primary dml info is null", K(ret), K(loc_table_id));
      } else if (OB_FAIL(op.get_index_dml_infos(loc_table_id, index_dml_infos))) {
        LOG_WARN("failed to get index dml infos", K(ret));
      } else if (OB_FAIL(spec.ins_ctdefs_.at(i).allocate_array(phy_plan_->get_allocator(),
                                                              index_dml_infos.count()))) {
        LOG_WARN("allocate insert ctdef array failed", K(ret), K(index_dml_infos.count()));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < index_dml_infos.count(); ++j) {
        const IndexDMLInfo *index_dml_info = index_dml_infos.at(j);
        ObInsCtDef *ins_ctdef = nullptr;
        if (OB_ISNULL(index_dml_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index dml info", K(ret));
        } else if (OB_FAIL(dml_cg_service_.generate_insert_ctdef(op, *index_dml_info, ins_ctdef))) {
          LOG_WARN("generate insert ctdef failed", K(ret));
        } else {
          spec.ins_ctdefs_.at(i).at(j) = ins_ctdef;
        }
      }
      if (OB_SUCC(ret)) {
        // if (OB_FAIL(spec.index_dml_count_array_.push_back(spec.ins_ctdefs_.count()))) {
        //   LOG_WARN("failed to push back", K(ret));
        if (OB_FAIL(generate_insert_all_table_info(*insert_tbl_info, tbl_info))) {
          LOG_WARN("failed to generate insert all table info", K(ret));
        } else if (OB_FAIL(spec.insert_table_infos_.push_back(tbl_info))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_insert_all_table_info(const ObInsertAllTableInfo &insert_tbl_info,
                                                     InsertAllTableInfo *&tbl_info)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  tbl_info = NULL;
  if (OB_ISNULL(phy_plan_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(phy_plan_));
  } else if (OB_ISNULL(ptr = (phy_plan_->get_allocator().alloc(sizeof(InsertAllTableInfo))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc memory", K(ret), K(ptr));
  } else {
    tbl_info = new(ptr)InsertAllTableInfo(phy_plan_->get_allocator());
    tbl_info->match_conds_idx_ = insert_tbl_info.when_cond_idx_;
    if (insert_tbl_info.when_cond_exprs_.empty()) {
      /*do nothing*/
    } else if (OB_FAIL(generate_rt_exprs(insert_tbl_info.when_cond_exprs_,
                                         tbl_info->match_conds_exprs_))) {
      LOG_WARN("fail to generate rt exprs", K(ret), K(insert_tbl_info.when_cond_exprs_));
    } else {
      LOG_TRACE("Succeed to generate insert all table info", K(insert_tbl_info), K(*tbl_info));
    }
  }
  return ret;
}

int ObStaticEngineCG::generate_spec(ObLogStatCollector &op,
    ObStatCollectorSpec &spec, const bool in_root_job)
{
  int ret = OB_SUCCESS;
  spec.type_ = op.get_stat_collector_type();
  spec.is_none_partition_ = op.get_is_none_partition();
  if (ObStatCollectorType::SAMPLE_SORT == spec.type_) {
    ObIArray<OrderItem> &new_sort_keys = op.get_sort_keys();
    if (OB_FAIL(spec.sort_exprs_.init(new_sort_keys.count()))) {
      LOG_WARN("failed to init all exprs", K(ret));
    } else if (OB_FAIL(fill_sort_info(new_sort_keys,
        spec.sort_collations_, spec.sort_exprs_))) {
      LOG_WARN("failed to sort info", K(ret));
    } else if (OB_FAIL(fill_sort_funcs(spec.sort_collations_,
        spec.sort_cmp_funs_, spec.sort_exprs_))) {
      LOG_WARN("failed to sort funcs", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(spec.type_));
  }
  return ret;
}

int ObStaticEngineCG::set_other_properties(const ObLogPlan &log_plan, ObPhysicalPlan &phy_plan)
{
  int ret = OB_SUCCESS;
  // set params info for plan cache
  ObSchemaGetterGuard *schema_guard = log_plan.get_optimizer_context().get_schema_guard();
  ObSQLSessionInfo *my_session = log_plan.get_optimizer_context().get_session_info();
  ObExecContext *exec_ctx = log_plan.get_optimizer_context().get_exec_ctx();
  ObSqlCtx *sql_ctx;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid exec_ctx_", K(ret));
  } else if (OB_ISNULL(log_plan.get_stmt()) || OB_ISNULL(schema_guard) || OB_ISNULL(my_session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or schema guard is null", K(log_plan.get_stmt()),
             K(schema_guard), K(my_session), K(ret));
  } else if(OB_ISNULL(sql_ctx = exec_ctx->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid sql_ctx", K(ret));
  } else if (OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("plan context is null");
  } else if (OB_ISNULL(exec_ctx->get_stmt_factory())
             || OB_ISNULL(exec_ctx->get_stmt_factory()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid query_ctx", K(ret));
  } else {
    ret = phy_plan.set_params_info(*(log_plan.get_optimizer_context().get_params()));
  }

  if (OB_SUCC(ret)) {
    //set other params
    //set user var assignment property
    phy_plan.set_contains_assignment(log_plan.get_stmt()->is_contains_assignment());
    phy_plan.set_require_local_execution(!log_plan.get_optimizer_context().get_exchange_allocated());
    phy_plan.set_plan_type(log_plan.get_optimizer_context().get_phy_plan_type());
    phy_plan.set_location_type(log_plan.get_optimizer_context().get_location_type());
    phy_plan.set_param_count(log_plan.get_stmt()->get_pre_param_size());
    phy_plan.set_signature(log_plan.get_signature());
    phy_plan.set_plan_hash_value(log_plan.get_signature());
    phy_plan.set_stmt_type(log_plan.get_stmt()->get_stmt_type());
    phy_plan.set_literal_stmt_type(exec_ctx->get_stmt_factory()->get_query_ctx()->get_literal_stmt_type());
    if (exec_ctx->get_stmt_factory()->get_query_ctx()->has_nested_sql()) {
      phy_plan.set_has_nested_sql(exec_ctx->get_stmt_factory()->get_query_ctx()->has_nested_sql());
    }
    if (log_plan.get_stmt()->is_insert_stmt() || log_plan.get_stmt()->is_merge_stmt()) {
      phy_plan.set_autoinc_params(log_plan.get_stmt()->get_autoinc_params());
    }
    phy_plan.set_affected_last_insert_id(log_plan.get_stmt()->get_affected_last_insert_id());
    phy_plan.set_is_contain_virtual_table(log_plan.get_stmt()->get_query_ctx()->is_contain_virtual_table_);
    phy_plan.set_is_contain_inner_table(log_plan.get_stmt()->get_query_ctx()->is_contain_inner_table_);
    phy_plan.set_is_affect_found_row(log_plan.get_stmt()->is_affect_found_rows());
    phy_plan.set_has_top_limit(log_plan.get_stmt()->has_top_limit());
    phy_plan.set_use_px(true);
    phy_plan.set_px_dop(log_plan.get_optimizer_context().get_max_parallel());
    phy_plan.set_expected_worker_count(log_plan.get_optimizer_context().get_expected_worker_count());
    phy_plan.set_minimal_worker_count(log_plan.get_optimizer_context().get_minimal_worker_count());
    phy_plan.set_is_batched_multi_stmt(log_plan.get_optimizer_context().is_batched_multi_stmt());
    phy_plan.set_need_consistent_snapshot(log_plan.need_consistent_read());
    // only if all servers's version >= CLUSTER_VERSION_4_2_0_0
    if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_2_0_0) {
      phy_plan.set_enable_px_fast_reclaim(GCONF._enable_px_fast_reclaim);
    }
    if (OB_FAIL(phy_plan.set_expected_worker_map(log_plan.get_optimizer_context().get_expected_worker_map()))) {
      LOG_WARN("set expected worker map", K(ret));
    } else if (OB_FAIL(phy_plan.set_minimal_worker_map(log_plan.get_optimizer_context().get_minimal_worker_map()))) {
      LOG_WARN("set minimal worker map", K(ret));
    } else {
      if (log_plan.get_optimizer_context().is_online_ddl()) {
        if (log_plan.get_stmt()->get_table_items().count() > 0) {
          const TableItem *insert_table_item = log_plan.get_stmt()->get_table_item(0);
          if (nullptr != insert_table_item) {
            int64_t ddl_execution_id = -1;
            int64_t ddl_task_id = 0;
            const ObOptParamHint *opt_params = &log_plan.get_stmt()->get_query_ctx()->get_global_hint().opt_params_;
            OZ(opt_params->get_integer_opt_param(ObOptParamHint::DDL_EXECUTION_ID, ddl_execution_id));
            OZ(opt_params->get_integer_opt_param(ObOptParamHint::DDL_TASK_ID, ddl_task_id));
            phy_plan.set_ddl_schema_version(insert_table_item->ddl_schema_version_);
            phy_plan.set_ddl_table_id(insert_table_item->ddl_table_id_);
            phy_plan.set_ddl_execution_id(ddl_execution_id);
            phy_plan.set_ddl_task_id(ddl_task_id);
          }
        }
      }
    }
    ObParamOption param_opt = log_plan.get_optimizer_context().get_global_hint().param_option_;
    bool is_exact_mode = my_session->get_enable_exact_mode();
    if (param_opt == ObParamOption::NOT_SPECIFIED) {
      phy_plan.set_need_param(!is_exact_mode);
    } else {
      phy_plan.set_need_param(param_opt==ObParamOption::FORCE);
    }
    phy_plan.tx_id_ = log_plan.get_optimizer_context().get_global_hint().tx_id_;
    phy_plan.tm_sessid_ = log_plan.get_optimizer_context().get_global_hint().tm_sessid_;
  }

  if (OB_SUCC(ret)) {
    bool enable = false;
    if (OB_FAIL(log_plan.check_enable_plan_expiration(enable))) {
      LOG_WARN("failed to check enable plan expiration", K(ret));
    } else if (enable) {
      phy_plan.set_enable_plan_expiration(true);
    }
  }

  // set location cons
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(sql_ctx)) {
      // do nothing
    } else if (OB_FAIL(phy_plan.set_location_constraints(sql_ctx->base_constraints_,
                                                  sql_ctx->strict_constraints_,
                                                  sql_ctx->non_strict_constraints_,
                                                  sql_ctx->dup_table_replica_cons_))) {
        LOG_WARN("failed to set location constraints", K(ret), K(phy_plan),
                 K(sql_ctx->base_constraints_),
                 K(sql_ctx->strict_constraints_),
                 K(sql_ctx->non_strict_constraints_),
                 K(sql_ctx->dup_table_replica_cons_));
    }
  }

  // set schema version and all base table version in phy plan
  if (OB_SUCC(ret)) {
    const ObIArray<ObSchemaObjVersion> *dependency_table = log_plan.get_stmt()->get_global_dependency_table();
    if (OB_ISNULL(dependency_table)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      bool has_dep_table = false;
      for (int64_t i = 0; OB_SUCC(ret) && !has_dep_table && i < dependency_table->count(); i++) {
        if (DEPENDENCY_TABLE == dependency_table->at(i).object_type_) {
          has_dep_table = true;
        }
      }
      LOG_DEBUG("is contain global index or dep base table", K(has_dep_table));
      phy_plan.set_is_dep_base_table(has_dep_table);

      ObArray<uint64_t> gtt_trans_scope_ids;
      ObArray<uint64_t> gtt_session_scope_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < dependency_table->count(); i++) {
        if (DEPENDENCY_TABLE == dependency_table->at(i).object_type_) {
          const ObTableSchema *table_schema = NULL;
          int64_t object_id = dependency_table->at(i).get_object_id();
          if (OB_FAIL(schema_guard->get_table_schema(my_session->get_effective_tenant_id(),
                                                     object_id, table_schema))) {
            LOG_WARN("fail to get table schema", K(ret), K(object_id));
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("fail to get table schema", K(ret), K(object_id));
          } else {
            if (table_schema->is_oracle_trx_tmp_table()) {
             if (OB_FAIL(gtt_trans_scope_ids.push_back(object_id))) {
               LOG_WARN("fail to push back", K(ret));
             }
            } else if (table_schema->is_oracle_sess_tmp_table()) {
              if (OB_FAIL(gtt_session_scope_ids.push_back(object_id))) {
                LOG_WARN("fail to push back", K(ret));
              }
            }
            LOG_DEBUG("plan contain temporary table",
                      "trx level", table_schema->is_oracle_trx_tmp_table(),
                      "session level", table_schema->is_oracle_sess_tmp_table());
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(phy_plan.get_gtt_trans_scope_ids().assign(gtt_trans_scope_ids))) {
          LOG_WARN("fail to assign array", K(ret));
        } else if (OB_FAIL(phy_plan.get_gtt_session_scope_ids().assign(gtt_session_scope_ids))) {
          LOG_WARN("fail to assign array", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      int64_t tenant_schema_version = OB_INVALID_VERSION;
      int64_t sys_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(phy_plan.get_dependency_table().assign(*dependency_table))) {
        LOG_WARN("init dependency table store failed", K(ret));
      } else if (OB_FAIL(schema_guard->get_schema_version(my_session->get_effective_tenant_id(),
                                                        tenant_schema_version))) {
        LOG_WARN("fail to get schema version", K(ret), K(tenant_schema_version));
      } else if (OB_FAIL(schema_guard->get_schema_version(OB_SYS_TENANT_ID,
                                                        sys_schema_version))) {
        LOG_WARN("fail to get schema version", K(ret), K(tenant_schema_version));
      } else {
        phy_plan.set_tenant_schema_version(tenant_schema_version);
        phy_plan.set_sys_schema_version(sys_schema_version);
        plan_ctx->set_tenant_schema_version(tenant_schema_version);
      }
    }
  }

  //set user and system variables
  if (OB_SUCC(ret)) {
    ret = phy_plan.set_vars(log_plan.get_stmt()->get_query_ctx()->variables_);
  }

  if (OB_SUCC(ret) && !log_plan.get_stmt()->is_explain_stmt()) {
    if (OB_FAIL(generate_rt_exprs(log_plan.get_stmt()->get_query_ctx()->var_init_exprs_,
                                  phy_plan.var_init_exprs_))) {
      LOG_WARN("generate var init exprs failed", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    //convert insert row param index map
    stmt::StmtType stmt_type = stmt::T_NONE;
    if (OB_FAIL(log_plan.get_stmt_type(stmt_type))) {
      LOG_WARN("get stmt type of log plan failed", K(ret));
    } else if (IS_INSERT_OR_REPLACE_STMT(stmt_type)) {
      const ObInsertStmt *insert_stmt = static_cast<const ObInsertStmt *>(log_plan.get_stmt());
      if (OB_ISNULL(insert_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(insert_stmt));
      } else if (ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED == phy_plan.get_plan_type() &&
                 !insert_stmt->value_from_select() &&
                 !insert_stmt->has_global_index()) {
        RowParamMap row_params;
        if (OB_FAIL(map_value_param_index(insert_stmt, row_params))) {
          LOG_WARN("failed to map value param index", K(ret));
        } else {
          PhyRowParamMap &phy_row_params = phy_plan.get_row_param_map();
          if (OB_FAIL(phy_row_params.prepare_allocate(row_params.count()))) {
            LOG_WARN("prepare allocate physical row param map failed", K(ret), K(row_params.count()));
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < row_params.count(); ++i) {
            phy_row_params.at(i).set_allocator(&phy_plan.get_allocator());
            if (OB_FAIL(phy_row_params.at(i).init(row_params.at(i).count()))) {
              LOG_WARN("init physical row param map failed", K(ret), K(i));
            } else if (OB_FAIL(phy_row_params.at(i).assign(row_params.at(i)))) {
              LOG_WARN("assign row param array failed", K(ret), K(i));
            }
          }
        }
      }
    }
  }

  //resolve the first array index of array binding, and store in physical plan
  if (OB_SUCC(ret)
      && OB_NOT_NULL(my_session->get_pl_implicit_cursor())
      && my_session->get_pl_implicit_cursor()->get_in_forall() // 仅在FORALL语境中才需要走Array Binding优化
      && (!log_plan.get_optimizer_context().is_batched_multi_stmt())) {
    //batch multi stmt使用了param store的array参数，但是它不是一个array binding优化，因此，这里需要排除掉
    bool is_found = false;
    const ParamStore &param_store = plan_ctx->get_param_store();
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < param_store.count(); ++i) {
      if (param_store.at(i).is_ext_sql_array()) {
        phy_plan.set_first_array_index(i);
        is_found = true;
      }
    }
  }

#ifdef OB_BUILD_TDE_SECURITY
  // set encrypt info in phy plan
  if (OB_SUCC(ret)) {
    const ObIArray<ObSchemaObjVersion> *dependency_table = log_plan.get_stmt()->get_global_dependency_table();
    if (OB_ISNULL(dependency_table)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret));
    } else {
      int64_t table_id = OB_INVALID_ID;
      const share::schema::ObTableSchema *full_table_schema = NULL;
      ObArray<transaction::ObEncryptMetaCache>metas;
      for (int64_t i = 0; OB_SUCC(ret) && i < dependency_table->count(); i++) {
        if (DEPENDENCY_TABLE == dependency_table->at(i).object_type_) {
          full_table_schema = NULL;
          table_id = dependency_table->at(i).get_object_id();
          if (OB_FAIL(schema_guard->get_table_schema(
              MTL_ID(), table_id, full_table_schema))) {
            LOG_WARN("fail to get table schema", K(ret), K(table_id));
          } else if (OB_ISNULL(full_table_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("table is not exist", K(ret), K(full_table_schema));
          } else if (OB_FAIL(init_encrypt_metas(full_table_schema, schema_guard, metas))) {
            LOG_WARN("fail to init encrypt table meta", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && metas.count() > 0) {
        if (OB_FAIL(phy_plan.get_encrypt_meta_array().assign(metas))) {
          LOG_WARN("fail to assign encrypt meta", K(ret));
        }
      }
    }
  }
#endif
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(log_plan.get_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (log_plan.get_stmt()->get_query_ctx()->disable_udf_parallel_) {
      if (log_plan.get_stmt()->is_insert_stmt() ||
          log_plan.get_stmt()->is_update_stmt() ||
          log_plan.get_stmt()->is_delete_stmt() ||
          log_plan.get_stmt()->is_merge_stmt()) {
        //为了支持触发器/UDF支持异常捕获，要求含有pl udf的涉及修改表数据的dml串行执行
        phy_plan_->set_need_serial_exec(true);
      }
      phy_plan_->set_contain_pl_udf_or_trigger(true);
      phy_plan_->set_has_nested_sql(true);
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    phy_plan_->calc_whether_need_trans();
  }
  return ret;
}

// FIXME bin.lb: We should split the big switch case into logical operator class.
int ObStaticEngineCG::get_phy_op_type(ObLogicalOperator &log_op,
                                         ObPhyOperatorType &type,
                                         const bool in_root_job)
{
  int ret = OB_SUCCESS;
  type = PHY_INVALID;
  switch(log_op.get_type()) {
    case log_op_def::LOG_LIMIT: {
      type = PHY_LIMIT;
      break;
    }
    case log_op_def::LOG_GROUP_BY: {
      auto &op = static_cast<ObLogGroupBy&>(log_op);
      switch (op.get_algo()) {
        case MERGE_AGGREGATE:
          type = PHY_MERGE_GROUP_BY;
          break;
        case HASH_AGGREGATE:
          type = PHY_HASH_GROUP_BY;
          break;
        case SCALAR_AGGREGATE:
          type = PHY_SCALAR_AGGREGATE;
          break;
        default:
          break;
      }
      break;
    }
    case log_op_def::LOG_SORT: {
      type = PHY_SORT;
      break;
    }
    case log_op_def::LOG_TABLE_SCAN: {
      auto &op = static_cast<ObLogTableScan&>(log_op);
      if (op.get_contains_fake_cte()) {
        type = PHY_FAKE_CTE_TABLE;
      } else if (op.is_sample_scan()) {
        if (op.get_sample_info().method_ == SampleInfo::ROW_SAMPLE) {
          type = PHY_ROW_SAMPLE_SCAN;
        } else if (op.get_sample_info().method_ == SampleInfo::BLOCK_SAMPLE){
          type = PHY_BLOCK_SAMPLE_SCAN;
        }
      } else if (op.get_is_multi_part_table_scan()) {
        type = PHY_MULTI_PART_TABLE_SCAN;
      } else {
        type = PHY_TABLE_SCAN;
      }
      break;
    }
    case log_op_def::LOG_JOIN: {
      auto &op = static_cast<ObLogJoin&>(log_op);
      switch(op.get_join_algo()) {
        case NESTED_LOOP_JOIN: {
          type = CONNECT_BY_JOIN != op.get_join_type()
             ? PHY_NESTED_LOOP_JOIN
             : (op.get_nl_params().count() > 0
                  ? PHY_NESTED_LOOP_CONNECT_BY_WITH_INDEX
                  : PHY_NESTED_LOOP_CONNECT_BY);
          break;
        }
        case MERGE_JOIN: {
          type = PHY_MERGE_JOIN;
          break;
        }
        case HASH_JOIN: {
          type = PHY_HASH_JOIN;
          break;
        }
        default: {
          break;
        }
      }
      break;
    }
    case log_op_def::LOG_JOIN_FILTER: {
      type = PHY_JOIN_FILTER;
      break;
    }
    case log_op_def::LOG_EXCHANGE: {
      // copy from convert_exchange
      auto &op = static_cast<ObLogExchange&>(log_op);
      if (op.get_plan()->get_optimizer_context().is_batched_multi_stmt()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("batched stmt plan only support executed with DAS");
        LOG_USER_ERROR(OB_NOT_SUPPORTED,
                "batched stmt plan only support executed with DAS."
                "Please contact next layer support to enable DAS configuration");
      } else if (op.is_producer()) {
        if (op.get_is_remote()) {
          type = PHY_DIRECT_TRANSMIT;
        } else if (OB_REPARTITION_NO_REPARTITION != op.get_repartition_type()
              && !op.is_slave_mapping()) {
          type = PHY_PX_REPART_TRANSMIT;
        } else if (ObPQDistributeMethod::LOCAL != op.get_dist_method()) {
          type = PHY_PX_DIST_TRANSMIT;
        } else if (op.get_plan()->get_optimizer_context().is_online_ddl() && ObPQDistributeMethod::PARTITION_RANGE == op.get_dist_method()) {
          type = PHY_PX_REPART_TRANSMIT;
        } else if (OB_REPARTITION_NO_REPARTITION != op.get_repartition_type()
                && !op.is_slave_mapping()) {
          type = PHY_PX_REPART_TRANSMIT;
        } else if (ObPQDistributeMethod::LOCAL != op.get_dist_method()) {
          type = PHY_PX_DIST_TRANSMIT;
        } else {
          // NOTE: 优化器需要和执行器保持一致，既没有分区、又没有HASH、或其它重分区方式时，就使用All To One
          type = PHY_PX_REDUCE_TRANSMIT;
        }
      } else {
        if (op.get_is_remote()) {
          type = PHY_DIRECT_RECEIVE;
        } else if (in_root_job || op.is_rescanable()) {
          if (op.is_task_order()) {
            type = PHY_PX_ORDERED_COORD;
          } else if (op.is_merge_sort()) {
            type = PHY_PX_MERGE_SORT_COORD;
          } else {
            type = PHY_PX_FIFO_COORD;
          }
          if (op.is_sort_local_order()) {
                // root节点不会出现需要local order的exchange-in
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected plan that has merge sort receive "
                         "with local order in root job", K(ret));
          }
        } else if (op.is_merge_sort()) {
          type = PHY_PX_MERGE_SORT_RECEIVE;
        } else {
          type = PHY_PX_FIFO_RECEIVE;
        }
      }
      break;
    }
    case log_op_def::LOG_DISTINCT: {
      auto &op = static_cast<ObLogDistinct&>(log_op);
      if (MERGE_AGGREGATE == op.get_algo()) {
        type = PHY_MERGE_DISTINCT;
      } else if (HASH_AGGREGATE == op.get_algo()) {
        type = PHY_HASH_DISTINCT;
      }
      break;
    }
    case log_op_def::LOG_DELETE: {
      auto &op = static_cast<ObLogDelete&>(log_op);
        if (op.is_pdml()) {
          type = PHY_PX_MULTI_PART_DELETE;
        } else {
          type = PHY_DELETE;
        }
      break;
    }
    case log_op_def::LOG_UPDATE: {
      auto &op = static_cast<ObLogUpdate&>(log_op);
      if (op.is_pdml()) {
        type = PHY_PX_MULTI_PART_UPDATE;
      } else {
        type = PHY_UPDATE;
      }
      break;
    }
    case log_op_def::LOG_FOR_UPD: {
      type = PHY_LOCK;
      break;
    }
    case log_op_def::LOG_INSERT: {
      auto &op = static_cast<ObLogInsert&>(log_op);
      if (op.is_replace()) {
        type = PHY_REPLACE;
      } else if (op.get_insert_up()) {
        type = PHY_INSERT_ON_DUP;
      } else if (op.is_pdml()) {
        if (op.get_plan()->get_optimizer_context().get_session_info()->get_ddl_info().is_ddl()) {
          type = PHY_PX_MULTI_PART_SSTABLE_INSERT;
        } else {
          type = PHY_PX_MULTI_PART_INSERT;
        }
      } else {
        type = PHY_INSERT;
      }
      break;
    }
    case log_op_def::LOG_INSERT_ALL: {
       type = PHY_MULTI_TABLE_INSERT;
       break;
    }
    case log_op_def::LOG_ERR_LOG: {
      type = PHY_ERR_LOG;
      break;
    }
    case log_op_def::LOG_MERGE: {
      type = PHY_MERGE;
      break;
    }
    case log_op_def::LOG_EXPR_VALUES: {
      type = PHY_EXPR_VALUES;
      break;
    }
    case log_op_def::LOG_VALUES: {
      type = PHY_VALUES;
      break;
    }
    case log_op_def::LOG_SET: {
      auto &op = static_cast<ObLogSet&>(log_op);
      switch (op.get_set_op()) {
        case ObSelectStmt::UNION:
          if (op.is_recursive_union()) {
            type = PHY_RECURSIVE_UNION_ALL;
          } else {
            type = (MERGE_SET == op.get_algo() ? PHY_MERGE_UNION : PHY_HASH_UNION);
          }
          break;
        case ObSelectStmt::INTERSECT:
          type = (MERGE_SET == op.get_algo() ? PHY_MERGE_INTERSECT : PHY_HASH_INTERSECT);
          break;
        case ObSelectStmt::EXCEPT:
          type = (MERGE_SET == op.get_algo() ? PHY_MERGE_EXCEPT : PHY_HASH_EXCEPT);
          break;
        default:
          break;
      }
      break;
    }
    case log_op_def::LOG_SUBPLAN_FILTER: {
      type = PHY_SUBPLAN_FILTER;
      break;
    }
    case log_op_def::LOG_SUBPLAN_SCAN: {
      type = PHY_SUBPLAN_SCAN;
      break;
    }
    case log_op_def::LOG_MATERIAL: {
      type = PHY_MATERIAL;
      break;
    }
    case log_op_def::LOG_WINDOW_FUNCTION: {
      type = PHY_WINDOW_FUNCTION;
      break;
    }
    case log_op_def::LOG_SELECT_INTO: {
      type = PHY_SELECT_INTO;
      break;
    }
    case log_op_def::LOG_TOPK: {
      type = PHY_TOPK;
      break;
    }
    case log_op_def::LOG_COUNT: {
      type = PHY_COUNT;
      break;
    }
    case log_op_def::LOG_GRANULE_ITERATOR: {
      type = PHY_GRANULE_ITERATOR;
      break;
    }
    case log_op_def::LOG_SEQUENCE: {
      type = PHY_SEQUENCE;
      break;
    }
    case log_op_def::LOG_FUNCTION_TABLE: {
      type = PHY_FUNCTION_TABLE;
      break;
    }
    case log_op_def::LOG_JSON_TABLE: {
      type = PHY_JSON_TABLE;
      break;
    }
    case log_op_def::LOG_MONITORING_DUMP: {
      type = PHY_MONITORING_DUMP;
      break;
    }
    case log_op_def::LOG_TEMP_TABLE_INSERT: {
      type = PHY_TEMP_TABLE_INSERT;
      break;
    }
    case log_op_def::LOG_TEMP_TABLE_ACCESS: {
      type = PHY_TEMP_TABLE_ACCESS;
      break;
    }
    case log_op_def::LOG_TEMP_TABLE_TRANSFORMATION: {
      type = PHY_TEMP_TABLE_TRANSFORMATION;
      break;
    }
    case log_op_def::LOG_UNPIVOT: {
      type = PHY_UNPIVOT;
      break;
    }
    case log_op_def::LOG_LINK_SCAN: {
      type = PHY_LINK_SCAN;
      break;
    }
    case log_op_def::LOG_LINK_DML: {
      type = PHY_LINK_DML;
      break;
    }
    case log_op_def::LOG_STAT_COLLECTOR: {
      type = PHY_STAT_COLLECTOR;
      break;
    }
    case log_op_def::LOG_OPTIMIZER_STATS_GATHERING: {
      type = PHY_OPTIMIZER_STATS_GATHERING;
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown logical operator", K(log_op.get_type()), K(lbt()));
      break;
  }
  return ret;
}

int ObStaticEngineCG::classify_anti_monotone_filter_exprs(const ObIArray<ObRawExpr*> &input_filters,
                                                             ObIArray<ObRawExpr*> &non_anti_monotone_filters,
                                                             ObIArray<ObRawExpr*> &anti_monotone_filters)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_filters.count(); i++) {
    ObRawExpr *raw_expr = input_filters.at(i);
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (raw_expr->has_flag(CNT_ROWNUM) && !raw_expr->has_flag(CNT_COLUMN) &&
               !raw_expr->has_flag(CNT_SUB_QUERY)) {
      ret = anti_monotone_filters.push_back(raw_expr);
    } else {
      ret = non_anti_monotone_filters.push_back(raw_expr);
    }
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObStaticEngineCG::init_encrypt_metas(
    const share::schema::ObTableSchema *table_schema,
    share::schema::ObSchemaGetterGuard *guard,
    ObIArray<transaction::ObEncryptMetaCache> &meta_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema) || OB_ISNULL(guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is invalid", K(ret));
  } else if (OB_FAIL(init_encrypt_table_meta(table_schema, guard, meta_array))) {
    LOG_WARN("fail to init encrypt_table_meta", KPC(table_schema), K(ret));
  } else if (!table_schema->is_user_table()) {
    /*do nothing*/
  } else {
    ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
    const ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", K(ret));
    }
    for (int i = 0; i < simple_index_infos.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(guard->get_table_schema(
          MTL_ID(),
          simple_index_infos.at(i).table_id_, index_schema))) {
        LOG_WARN("fail to get table schema", K(ret));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("index schema not exist", K(simple_index_infos.at(i).table_id_), K(ret));
      } else if (!index_schema->is_index_local_storage()) {
        // do nothing
      } else if (OB_FAIL(init_encrypt_table_meta(index_schema, guard, meta_array))) {
        LOG_WARN("fail to init encrypt_table_meta", KPC(index_schema), K(ret));
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::init_encrypt_table_meta(
    const share::schema::ObTableSchema *table_schema,
    share::schema::ObSchemaGetterGuard *guard,
    ObIArray<transaction::ObEncryptMetaCache>&meta_array)
{
  int ret = OB_SUCCESS;
  transaction::ObEncryptMetaCache meta_cache;
  char master_key[OB_MAX_MASTER_KEY_LENGTH];
  int64_t master_key_length = 0;
  if (OB_ISNULL(table_schema) || OB_ISNULL(guard)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is invalid", K(ret));
  } else if (!(table_schema->need_encrypt() && table_schema->get_master_key_id() > 0 &&
               table_schema->get_encrypt_key_len() > 0)) {
    // do nothing
  } else if (OB_FAIL(table_schema->get_encryption_id(meta_cache.meta_.encrypt_algorithm_))) {
    LOG_WARN("fail to get encryption id", K(ret));
  } else {
    if (table_schema->is_index_local_storage()) {
      meta_cache.table_id_ = table_schema->get_data_table_id();
      meta_cache.local_index_id_ = table_schema->get_table_id();
    } else {
      meta_cache.table_id_ = table_schema->get_table_id();
    }
    meta_cache.meta_.tenant_id_ = table_schema->get_tenant_id();
    meta_cache.meta_.master_key_version_ = table_schema->get_master_key_id();
    if (OB_FAIL(meta_cache.meta_.encrypted_table_key_.set_content(
        table_schema->get_encrypt_key()))) {
      LOG_WARN("fail to assign encrypt key", K(ret));
    }
    #ifdef ERRSIM
      else if (OB_FAIL(OB_E(EventTable::EN_ENCRYPT_GET_MASTER_KEY_FAILED) OB_SUCCESS)) {
      LOG_WARN("ERRSIM, fail to get master key", K(ret));
    }
    #endif
      else if (OB_FAIL(share::ObMasterKeyGetter::get_master_key(table_schema->get_tenant_id(),
                       table_schema->get_master_key_id(), master_key, OB_MAX_MASTER_KEY_LENGTH,
                       master_key_length))) {
      LOG_WARN("fail to get master key", K(ret));
      // 如果在cg阶段获取主密钥失败了, 有可能是因为RS执行内部sql没有租户资源引起的.
      // 在没有租户资源的情况下获取主密钥, 获取加密租户配置项时会失败
      // 在这种情况下, 我们认为这里是合理的, 缓存中可以不保存主密钥内容
      // cg阶段获取主密钥的任何失败我们可以接受
      // 兜底是执行期再次获取, 再次获取成功了则继续往下走, 失败了则报错出来.
      // 见bug
      ret = OB_SUCCESS;
    } else if (OB_FAIL(meta_cache.meta_.master_key_.set_content(
                                                        ObString(master_key_length, master_key)))) {
      LOG_WARN("fail to assign master_key", K(ret));
    } else if (OB_FAIL(ObEncryptionUtil::decrypt_table_key(meta_cache.meta_))) {
      LOG_WARN("failed to decrypt_table_key", K(ret));
    } else {/*do nothing*/}

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(meta_array.push_back(meta_cache))) {
      LOG_WARN("fail to push back meta array", K(ret));
    }
  }
  return ret;
}
#endif

int ObStaticEngineCG::map_value_param_index(const ObInsertStmt *insert_stmt,
                                            RowParamMap &row_params_map)
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = 0;
  ObArray<ObSEArray<int64_t, 1>> params_row_map;
  if (OB_ISNULL(insert_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert stmt is null", K(ret));
  } else {
    param_cnt = insert_stmt->get_question_marks_count();
    ObSEArray<int64_t, 1> row_indexs;
    if (OB_FAIL(params_row_map.prepare_allocate(param_cnt))) {
      LOG_WARN("reserve row params map failed", K(ret), K(param_cnt));
    } else if (OB_FAIL(row_indexs.push_back(OB_INVALID_INDEX))) {
      LOG_WARN("store row index failed", K(ret));
    }
    //init to OB_INVALID_INDEX
    for (int64_t i = 0; OB_SUCC(ret) && i < params_row_map.count(); ++i) {
      if (OB_FAIL(params_row_map.at(i).assign(row_indexs))) {
        LOG_WARN("init row params map failed", K(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObRawExpr *> &insert_values = insert_stmt->get_values_vector();
    int64_t insert_column_cnt = insert_stmt->get_values_desc().count();
    ObSEArray<int64_t, 1> param_idxs;
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_values.count(); ++i) {
      param_idxs.reset();
      if (OB_FAIL(ObRawExprUtils::extract_param_idxs(insert_values.at(i), param_idxs))) {
        LOG_WARN("extract param idxs failed", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < param_idxs.count(); ++j) {
        if (OB_UNLIKELY(param_idxs.at(j) < 0) || OB_UNLIKELY(param_idxs.at(j) >= param_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param idx is invalid", K(param_idxs.at(j)), K(param_cnt));
        } else if (params_row_map.at(param_idxs.at(j)).count() == 1 && params_row_map.at(param_idxs.at(j)).at(0) == OB_INVALID_INDEX) {
          params_row_map.at(param_idxs.at(j)).at(0) = i / insert_column_cnt;
        } else if (OB_FAIL(add_var_to_array_no_dup(params_row_map.at(param_idxs.at(j)), i / insert_column_cnt))) {
          LOG_WARN("add index no duplicate failed", K(ret));
        }
      }
    }
  }
  //根据得到的param->row的map关系，转换得到row->param的映射关系,由于存在共有的param，不单独属于任何一行表达式，所以将index=0作为公共param的槽位
  //每一行的param从index=1开始存储
  if (OB_SUCC(ret)) {
    if (OB_FAIL(row_params_map.prepare_allocate(insert_stmt->get_insert_row_count() + 1))) {
      LOG_WARN("prepare allocate row params map failed", K(ret), K(insert_stmt->get_insert_row_count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < params_row_map.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < params_row_map.at(i).count(); ++j) {
        if (params_row_map.at(i).at(j) == OB_INVALID_INDEX) {
          //公共参数
          if (OB_FAIL(row_params_map.at(0).push_back(i))) {
            LOG_WARN("add param index to row params map failed", K(ret), K(i), K(j));
          }
        } else {
          //具体行中表达式依赖的param
          if (OB_FAIL(row_params_map.at(params_row_map.at(i).at(j) + 1).push_back(i))) {
            LOG_WARN("add param index to row params map failed", K(ret), K(i), K(j));
          }
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::add_output_datum_check_flag(ObOpSpec &spec)
{
  int ret = OB_SUCCESS;
  // whitelist not to check output datum
  if (!spec.is_vectorized() ||
      IS_PX_TRANSMIT(spec.get_type()) ||
      PHY_MATERIAL == spec.get_type()) {
    // do nothing
  } else if (PHY_UNPIVOT == spec.get_type()) {
    if (OB_ISNULL(spec.get_child(0))) {
      LOG_INFO("invalid argument, fails to enable checking", K(ret));
    } else if (spec.get_child(0)->get_type() != PHY_SUBPLAN_SCAN) {
      LOG_INFO("Unexpected type", K(ret), K(spec.get_child(0)->get_type()));
    } else {
      // Because the Unpivot will affect the output datum of the SubplanScan,
      // which is an by designed case, we need to set the SubplanScan operator
      // to not check the output datum.
      spec.get_child(0)->need_check_output_datum_ = false;
    }
  } else {
    spec.need_check_output_datum_ = true;
  }
  return ret;
}

int ObStaticEngineCG::generate_calc_part_id_expr(const ObRawExpr &src,
                                                 const ObDASTableLocMeta *loc_meta,
                                                 ObExpr *&dst)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_rt_expr(src, dst))) {
    LOG_WARN("generate calc part id expr failed", K(ret));
  } else if (loc_meta != nullptr && !loc_meta->unuse_related_pruning_) {
    //related local index tablet_id pruning only can be used in local plan or remote plan(all operator
    //use the same das context),
    //because the distributed plan will transfer tablet_id through exchange operator,
    //but the related tablet_id map can not be transfered by exchange operator,
    //unused related pruning in distributed plan's dml operator,
    //we will build the related tablet_id map when dml operator be opened in distributed plan
    CalcPartitionBaseInfo *calc_part_info = static_cast<CalcPartitionBaseInfo*>(dst->extra_info_);
    if (OB_FAIL(calc_part_info->related_table_ids_.assign(loc_meta->related_table_ids_))) {
      LOG_WARN("assign related table ids failed", K(ret));
    }
  }
  return ret;
}

int ObStaticEngineCG::check_only_one_unique_key(const ObLogPlan& log_plan,
                                                const ObTableSchema* table_schema,
                                                bool& only_one_unique_key)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard *schema_guard = log_plan.get_optimizer_context().get_schema_guard();
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const ObTableSchema *index_schema = NULL;
  int64_t unique_index_cnt = 0;
  if (OB_ISNULL(schema_guard) || OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(schema_guard), K(table_schema));
  } else {
    if (!table_schema->is_heap_table()) {
      ++unique_index_cnt;
    }
    if (OB_FAIL(table_schema->get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("get simple_index_infos failed", K(ret));
    } else if (simple_index_infos.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); i++) {
        if (OB_FAIL(schema_guard->get_table_schema(MTL_ID(), simple_index_infos.at(i).table_id_, index_schema))) {
          LOG_WARN("fail to get table schema", K(ret), "table_id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get table schema", K(ret), K(index_schema));
        } else if (index_schema->is_unique_index() && !index_schema->is_final_invalid_index()) {
          ++unique_index_cnt;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    only_one_unique_key = (1 == unique_index_cnt);
  }
  return ret;
}

bool ObStaticEngineCG::has_cycle_reference(DASTableIdList &parent_tables, const uint64_t table_id)
{
  bool ret = false;
  if (!parent_tables.empty()) {
    DASTableIdList::iterator iter = parent_tables.begin();
    for (; !ret && iter != parent_tables.end(); iter++) {
      if (*iter == table_id) {
        ret = true;
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::check_fk_nested_dup_del(const uint64_t table_id,
                              const uint64_t root_table_id,
                              DASTableIdList &parent_tables,
                              bool &is_dup)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(parent_tables.push_back(root_table_id))) {
    LOG_WARN("failed to push root_table_id to parent tables list", K(ret), K(root_table_id), K(parent_tables.size()));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, root_table_id, table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id), K(root_table_id));
  } else if (!OB_ISNULL(table_schema)) {
    const common::ObIArray<ObForeignKeyInfo> &foreign_key_infos = table_schema->get_foreign_key_infos();
    for (int64_t i = 0; OB_SUCC(ret) && i < foreign_key_infos.count() && !is_dup; ++i) {
      const ObForeignKeyInfo &fk_info = foreign_key_infos.at(i);
      const uint64_t child_table_id = fk_info.child_table_id_;
      const uint64_t parent_table_id = fk_info.parent_table_id_;
      ObReferenceAction del_act = fk_info.delete_action_;
      if (child_table_id != common::OB_INVALID_ID && del_act == ACTION_CASCADE) {
        if (child_table_id == table_id) {
          is_dup = true;
        } else if (has_cycle_reference(parent_tables, child_table_id)) {
          LOG_DEBUG("This schema has a circular foreign key dependencies");
        } else if (OB_FAIL(SMART_CALL(check_fk_nested_dup_del(table_id, child_table_id, parent_tables, is_dup)))) {
          LOG_WARN("failed deep search nested duplicate delete table", K(ret), K(table_id), K(root_table_id), K(child_table_id));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(parent_tables.pop_back())) {
    LOG_WARN("failed to pop latest table id", K(ret));
  }
  return ret;
}

int ObStaticEngineCG::set_batch_exec_param(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                           const ObFixedArray<ObDynamicParamSetter, ObIAllocator>& setters)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(exec_params.count() != setters.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("nl params should have the same length with rescan params", K(ret));
  }
  ARRAY_FOREACH(exec_params, i) {
    const ObExecParamRawExpr* param_expr = exec_params.at(i);
    const ObDynamicParamSetter& setter = setters.at(i);
    for (int64_t j = batch_exec_param_caches_.count() - 1; OB_SUCC(ret) && j >= 0; --j) {
      const BatchExecParamCache &cache = batch_exec_param_caches_.at(j);
      if (param_expr != cache.expr_) {
      } else if (OB_ISNULL(cache.spec_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (cache.spec_->get_type() == PHY_SUBPLAN_FILTER) {
        ObSubPlanFilterSpec *spf = static_cast<ObSubPlanFilterSpec*>(cache.spec_);
        if (cache.is_left_param_ &&
                    OB_FAIL(spf->left_rescan_params_.push_back(setter))) {
          LOG_WARN("fail to push back left rescan params", K(ret));
        } else if (!cache.is_left_param_ &&
                    OB_FAIL(spf->right_rescan_params_.push_back(setter))) {
          LOG_WARN("fail to push back right rescan params", K(ret));
        } else if (OB_FAIL(batch_exec_param_caches_.remove(j))) {
          LOG_WARN("fail to remove batch nl param caches", K(ret));
        }
      } else if (cache.spec_->get_type() == PHY_NESTED_LOOP_JOIN) {
        ObNestedLoopJoinSpec *nlj = static_cast<ObNestedLoopJoinSpec*>(cache.spec_);
        if (cache.is_left_param_ &&
                    OB_FAIL(nlj->left_rescan_params_.push_back(setter))) {
          LOG_WARN("fail to push back left rescan params", K(ret));
        } else if (!cache.is_left_param_ &&
                    OB_FAIL(nlj->right_rescan_params_.push_back(setter))) {
          LOG_WARN("fail to push back right rescan params", K(ret));
        } else if (OB_FAIL(batch_exec_param_caches_.remove(j))) {
          LOG_WARN("fail to remove batch nl param caches", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObStaticEngineCG::check_window_functions_order(const ObIArray<ObWinFunRawExpr *> &winfunc_exprs)
{
  int ret = OB_SUCCESS;
  int64_t partition_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < winfunc_exprs.count(); ++i) {
    ObWinFunRawExpr * win_expr = winfunc_exprs.at(i);
    if (OB_ISNULL(win_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (i == 0) {
      partition_count = win_expr->get_partition_exprs().count();
    } else if (partition_count < win_expr->get_partition_exprs().count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("earlier partition by exprs must be subsets of the later partition by exprs", K(ret));
    } else {
      partition_count = win_expr->get_partition_exprs().count();
    }
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
