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
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/optimizer/ob_log_merge.h"
#include "sql/ob_phy_table_location.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_merge_log_plan.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace share;
using namespace oceanbase::share::schema;

int ObLogMerge::get_plan_item_info(PlanText &plan_text,
                                   ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogDelUpd::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(print_table_infos(ObString::make_string("columns"),
                                  buf,
                                  buf_len,
                                  pos,
                                  type))) {
      LOG_WARN("failed to print table info", K(ret));
    } else if (NULL == table_partition_info_) {
    } else if(OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else if (OB_FAIL(explain_print_partitions(*table_partition_info_,
                                                buf,
                                                buf_len,
                                                pos))) {
      LOG_WARN("Failed to print partitions");
    } else { }
    if (OB_SUCC(ret) &&
        NULL != get_primary_dml_info() &&
        !get_primary_dml_info()->column_convert_exprs_.empty()) {
      const ObIArray<ObRawExpr *> &column_values = get_primary_dml_info()->column_convert_exprs_;
      if(OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else {
        EXPLAIN_PRINT_EXPRS(column_values, type);
      }
    }
    if (OB_SUCC(ret) && !get_update_infos().empty() &&
        NULL != get_update_infos().at(0)) {
      const ObAssignments &assigns = get_update_infos().at(0)->assignments_;
      if (OB_FAIL(BUF_PRINTF(",\n"))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("      update("))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_FAIL(print_assigns(assigns,
                                       buf,
                                       buf_len,
                                       pos,
                                       type))) {
        LOG_WARN("failed to print assigns", K(ret));
      } else { /* Do nothing */ }
      BUF_PRINTF(")");
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else {
      const ObIArray<ObRawExpr *> &update_conds = get_update_condition();
      const ObIArray<ObRawExpr *> &delete_conds = get_delete_condition();
      const ObIArray<ObRawExpr *> &insert_conds = get_insert_condition();
      EXPLAIN_PRINT_EXPRS(insert_conds, type);

      if (OB_SUCC(ret) && OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }

      EXPLAIN_PRINT_EXPRS(update_conds, type);
      if (OB_SUCC(ret) && OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      }
      EXPLAIN_PRINT_EXPRS(delete_conds, type);
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogMerge::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObMergeLogPlan *merge_log_plan = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(merge_log_plan = dynamic_cast<ObMergeLogPlan*>(get_plan()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(merge_log_plan));
  } else if (NULL != get_sharding()) {
    is_partition_wise_ = true;
  } else if (merge_log_plan->use_pdml() && is_multi_part_dml()) {
    // pdml merge
    strong_sharding_ = get_plan()->get_optimizer_context().get_distributed_sharding();
  } else if (is_multi_part_dml()) {
    strong_sharding_ = get_plan()->get_optimizer_context().get_local_sharding();
  } else if (OB_FAIL(ObLogDelUpd::compute_sharding_info())) {
    LOG_WARN("failed to compute sharding info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogMerge::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogDelUpd::inner_get_op_exprs(all_exprs, true))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else if (OB_FAIL(get_table_columns_exprs(get_update_infos(),
                                             all_exprs,
                                             true))) {
    LOG_WARN("failed to add update dml info exprs", K(ret));
  } else if (OB_FAIL(get_table_columns_exprs(get_delete_infos(),
                                             all_exprs,
                                             true))) {
    LOG_WARN("failed to add delete dml info exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, get_insert_condition()))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, get_update_condition()))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, get_delete_condition()))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogMerge::get_modified_index_id(common::ObIArray<uint64_t> &index_tids)
{
  int ret = OB_SUCCESS;
  const ObMergeStmt *merge_stmt = NULL;
  index_tids.reuse();
  if (OB_ISNULL(merge_stmt = static_cast<const ObMergeStmt *>(get_stmt()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is null", K(ret));
  } else if (merge_stmt->has_insert_clause()) {
    // Reminder, the resolver mock a insert dml info even if the merge stmt does not have a insert clause
    for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
      if (OB_ISNULL(get_index_dml_infos().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (OB_FAIL(index_tids.push_back(get_index_dml_infos().at(i)->ref_table_id_))) {
        LOG_WARN("failed to push back insert index id", K(ret));
      }
    }
  } else if (!get_delete_infos().empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_delete_infos().count(); ++i) {
      if (OB_ISNULL(get_delete_infos().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete dml info is null", K(ret));
      } else if (OB_FAIL(index_tids.push_back(get_delete_infos().at(i)->ref_table_id_))) {
        LOG_WARN("failed to push back delete index id", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_update_infos().count(); ++i) {
      if (OB_ISNULL(get_update_infos().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("update dml info is null", K(ret));
      } else if (OB_FAIL(index_tids.push_back(get_update_infos().at(i)->ref_table_id_))) {
        LOG_WARN("failed to push back update index id", K(ret));
      }
    }
  }
  return ret;
}

int ObLogMerge::assign_dml_infos(const ObIArray<IndexDMLInfo *> &index_insert_infos,
                                 const ObIArray<IndexDMLInfo *> &index_update_infos,
                                 const ObIArray<IndexDMLInfo *> &index_delete_infos)
{
  int ret = OB_SUCCESS;
  uint64_t loc_table_id;
  if (!index_update_infos.empty()) {
    loc_table_id = index_update_infos.at(0)->loc_table_id_;
  } else if (OB_UNLIKELY(index_insert_infos.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert dml info is null", K(ret), K(index_update_infos), K(index_insert_infos));
  } else {
    loc_table_id = index_insert_infos.at(0)->loc_table_id_;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(loc_table_list_.push_back(loc_table_id))) {
      LOG_WARN("failed to push back loc table id", K(ret));
    } else if (OB_FAIL(index_dml_infos_.assign(index_insert_infos))) {
      LOG_WARN("failed to assgin index insert infos", K(ret));
    } else if (OB_FAIL(index_upd_infos_.assign(index_update_infos))) {
      LOG_WARN("failed to assign update index infos", K(ret));
    } else if (OB_FAIL(index_del_infos_.assign(index_delete_infos))) {
      LOG_WARN("failed to assign delete index infos", K(ret));
    }
  }
  return ret;
}

const char *ObLogMerge::get_name() const
{
  const char *ret = "NOT SET";
  if (is_multi_part_dml()) {
    ret = "DISTRIBUTED MERGE";
  } else {
    ret = "MERGE";
  }
  LOG_DEBUG("merge_op get name", K(is_multi_part_dml()));
  return ret;
}

const common::ObIArray<ObRawExpr *>& ObLogMerge::get_insert_condition() const
{
  return static_cast<const ObMergeLogPlan &>(my_dml_plan_).get_insert_condition();
}

const common::ObIArray<ObRawExpr *>& ObLogMerge::get_update_condition() const
{
  return static_cast<const ObMergeLogPlan &>(my_dml_plan_).get_update_condition();
}

const common::ObIArray<ObRawExpr *>& ObLogMerge::get_delete_condition() const
{
  return static_cast<const ObMergeLogPlan &>(my_dml_plan_).get_delete_condition();
}

int ObLogMerge::generate_rowid_expr_for_trigger()
{
  int ret = OB_SUCCESS;
  bool has_trg = false;
  const ObMergeStmt *merge_stmt = NULL;
  uint64_t loc_table_id = OB_INVALID_ID;
  uint64_t ref_table_id = OB_INVALID_ID;

  IndexDMLInfo *insert_info =
      index_dml_infos_.empty() ? NULL : index_dml_infos_.at(0);
  IndexDMLInfo *delete_info =
      index_del_infos_.empty() ? NULL : index_del_infos_.at(0);
  IndexDMLInfo *update_info =
      index_upd_infos_.empty() ? NULL : index_upd_infos_.at(0);

  if (NULL != update_info) {
    ref_table_id = update_info->ref_table_id_;
    loc_table_id = update_info->loc_table_id_;
  } else if (NULL != insert_info) {
    ref_table_id = insert_info->ref_table_id_;
    loc_table_id = insert_info->loc_table_id_;
  }

  if (OB_ISNULL(merge_stmt = static_cast<const ObMergeStmt*>(get_stmt())) ||
      OB_UNLIKELY(OB_INVALID_ID == loc_table_id) ||
      OB_UNLIKELY(OB_INVALID_ID == ref_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table id is invalid", K(ret), K(loc_table_id), K(ref_table_id));
  } else if (has_instead_of_trigger()) {
    // do nothing
  } else if (OB_FAIL(check_has_trigger(ref_table_id, has_trg))) {
    LOG_WARN("failed to check has trg", K(ret));
  }
  // for insert clause
  if (OB_SUCC(ret) && merge_stmt->has_insert_clause() && has_trg) {
    if (OB_ISNULL(insert_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert info is null", K(ret), K(insert_info));
    } else if (OB_FAIL(generate_old_rowid_expr(*insert_info))) {
      LOG_WARN("failed to generate rowid expr", K(ret));
    } else if (OB_FAIL(generate_insert_new_rowid_expr(*insert_info))) {
      LOG_WARN("failed to generate new rowid", K(ret));
    }
  }
  // for update clause
  if (OB_SUCC(ret) && merge_stmt->has_update_clause() && has_trg) {
    if (OB_ISNULL(update_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update info is null", K(ret));
    } else if (OB_FAIL(generate_old_rowid_expr(*update_info))) {
      LOG_WARN("failed to generate rowid expr", K(ret));
    } else if (OB_FAIL(generate_update_new_rowid_expr(*update_info))) {
      LOG_WARN("failed to generate update rowid expr", K(ret));
    } else if (NULL != delete_info) {
      delete_info->old_rowid_expr_ = update_info->new_rowid_expr_;
      delete_info->new_rowid_expr_ = NULL;
    }
  }
  return ret;
}

int ObLogMerge::generate_part_id_expr_for_foreign_key(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  const ObMergeStmt *merge_stmt = static_cast<const ObMergeStmt *>(get_stmt());
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is null", K(ret), K(merge_stmt));
  } else if (merge_stmt->has_insert_clause()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
      IndexDMLInfo *ins_info = get_index_dml_infos().at(i);
      if (OB_ISNULL(ins_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dml info is null", K(ret), K(ins_info));
      } else if (!ins_info->is_primary_index_) {
        // do nothing
      } else if (OB_FAIL(generate_fk_lookup_part_id_expr(*ins_info))) {
        LOG_WARN("failed to generate lookup part expr for foreign key", K(ret));
      } else if (OB_FAIL(convert_insert_new_fk_lookup_part_id_expr(all_exprs,*ins_info))) {
        LOG_WARN("failed to convert lookup part expr for foreign key", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < get_update_infos().count(); ++i) {
    IndexDMLInfo *upd_info = get_update_infos().at(i);
    if (OB_ISNULL(upd_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update info is null", K(ret), K(upd_info));
    } else if (!upd_info->is_primary_index_) {
      // do nothing
    } else if (OB_FAIL(generate_fk_lookup_part_id_expr(*upd_info))) {
      LOG_WARN("failed to generate lookup part expr for foreign key", K(ret));
    } else if (OB_FAIL(convert_update_new_fk_lookup_part_id_expr(all_exprs, *upd_info))) {
      LOG_WARN("failed to convert lookup part expr for foreign key", K(ret));
    }
  }
  return ret;
}

int ObLogMerge::generate_multi_part_partition_id_expr()
{
  int ret = OB_SUCCESS;
  const ObMergeStmt *merge_stmt = static_cast<const ObMergeStmt *>(get_stmt());
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge stmt is null", K(ret), K(merge_stmt));
  } else if (merge_stmt->has_insert_clause()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
      IndexDMLInfo *ins_info = get_index_dml_infos().at(i);
      if (OB_ISNULL(ins_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("insert info is null", K(ret), K(ins_info));
      } else if (OB_FAIL(generate_old_calc_partid_expr(*ins_info))) {
        LOG_WARN("failed to generate calc partition expr", K(ret));
      } else if (OB_FAIL(generate_insert_new_calc_partid_expr(*ins_info))) {
        LOG_WARN("failed to genearte calc partition expr for insert clause", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < get_update_infos().count(); ++i) {
    IndexDMLInfo *upd_info = get_update_infos().at(i);
    if (OB_ISNULL(upd_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("update info is null", K(ret), K(upd_info));
    } else if (OB_FAIL(generate_old_calc_partid_expr(*upd_info))) {
      LOG_WARN("failed to generate calc partition expr for update", K(ret));
    } else if (OB_FAIL(generate_update_new_calc_partid_expr(*upd_info))) {
      LOG_WARN("failed to generate new calc partition id expr", K(ret));
    }
  }
  // Reminder, the calc partition id expr is mocked for the delete clause !
  // You may find that it is exactly the same as the old_part_id_expr of the update_dml_info.
  // But they are calcuated in the different way by the engine.
  // The update_dml_info's old_part_id_expr is calucalted naturally (based on the output of table scan)
  // The delete_dml_info's old_part_id_expr is not calucalted with the output of the table scan
  // Its inputs (those column exprs) are created (or mocked) by the ObTableMergeOp !
  // Hence, the delete old_part_id_expr is different with the update one.
  // We must generate the expr individually !
  for (int64_t i = 0; OB_SUCC(ret) && i < get_delete_infos().count(); ++i) {
    IndexDMLInfo *del_info = get_delete_infos().at(i);
    if (OB_ISNULL(del_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("delete info is null", K(ret), K(del_info));
    } else if (OB_FAIL(generate_old_calc_partid_expr(*del_info))) {
      LOG_WARN("failed to generate calc partition expr for delete", K(ret));
    }
  }
  return ret;
}

bool ObLogMerge::is_insert_dml_info(const IndexDMLInfo *dml_info) const
{
  return ObOptimizerUtil::find_item(get_index_dml_infos(), dml_info);
}

bool ObLogMerge::is_delete_dml_info(const IndexDMLInfo *dml_info) const
{
  return ObOptimizerUtil::find_item(get_delete_infos(), dml_info);
}

int ObLogMerge::gen_location_constraint(void *ctx)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt())
      || OB_ISNULL(query_ctx = get_stmt()->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_plan()), K(get_stmt()), K(query_ctx));
  } else if (OB_FAIL(ObLogicalOperator::gen_location_constraint(ctx))) {
    LOG_WARN("failed to gen location constraint", K(ret));
  } else {
    // constraints for merge partition pruning
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>> &equal_pairs = get_equal_pairs();
    const PreCalcExprExpectResult expect_result = PreCalcExprExpectResult::PRE_CALC_RESULT_TRUE;
    ObRawExpr *equal_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < equal_pairs.count(); ++i) {
      if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(opt_ctx.get_expr_factory(),
                                                              T_OP_EQ,
                                                              equal_pairs.at(i).first,
                                                              equal_pairs.at(i).second,
                                                              equal_expr))) {
        LOG_WARN("failed to build common binary_op_expr");
      } else if (OB_FAIL(equal_expr->formalize(opt_ctx.get_session_info()))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(query_ctx->all_expr_constraints_.push_back(ObExprConstraint(equal_expr, expect_result)))) {
        LOG_WARN("failed to push back expr constraints", K(ret));
      }
    }
  }
  return ret;
}

int ObLogMerge::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogDelUpd::inner_replace_op_exprs(replacer))) {
    LOG_WARN("failed to replace op exprs", K(ret));
  } else if (OB_FAIL(replace_dml_info_exprs(replacer, get_update_infos()))) {
    LOG_WARN("failed to replace update dml info exprs");
  } else if (OB_FAIL(replace_dml_info_exprs(replacer, get_delete_infos()))) {
    LOG_WARN("failed to replace delete dml info exprs");
  } else if (OB_FAIL(replace_exprs_action(replacer,
                        static_cast<ObMergeLogPlan &>(my_dml_plan_).get_insert_condition()))) {
    LOG_WARN("failed to replace insert condition exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer,
                        static_cast<ObMergeLogPlan &>(my_dml_plan_).get_update_condition()))) {
    LOG_WARN("failed to replace update condition exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer,
                        static_cast<ObMergeLogPlan &>(my_dml_plan_).get_delete_condition()))) {
    LOG_WARN("failed to replace delete condition exprs", K(ret));
  }
  return ret;
}
