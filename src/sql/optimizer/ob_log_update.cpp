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
#include "ob_log_update.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int ObLogUpdate::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(tables_assignments_));
  ObLogDelUpd::print_my_plan_annotation(buf, buf_len, pos, type);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF(",\n      "))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("update("))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }
  for (int64_t k = 0; OB_SUCC(ret) && k < tables_assignments_->count(); ++k) {
    const ObTableAssignment& ta = tables_assignments_->at(k);
    int64_t N = ta.assignments_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      OZ(BUF_PRINTF("["));
      CK(OB_NOT_NULL(ta.assignments_.at(i).column_expr_));
      OZ(ta.assignments_.at(i).column_expr_->get_name(buf, buf_len, pos, type));
      OZ(BUF_PRINTF("="));
      CK(OB_NOT_NULL(ta.assignments_.at(i).expr_));
      OZ(ta.assignments_.at(i).expr_->get_name(buf, buf_len, pos, type));
      OZ(BUF_PRINTF("]"));
      if (OB_SUCC(ret) && i < N - 1) {
        OZ(BUF_PRINTF(", "));
      }
    }
    if (OB_SUCC(ret) && k < tables_assignments_->count() - 1) {
      OZ(BUF_PRINTF(", "));
    }
  }
  OZ(BUF_PRINTF(")"));
  /*
  if (OB_SUCC(ret)) {
    if(OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else {
      const ObIArray<ObRawExpr *> &check_cst_filter = *get_check_constraint_exprs();
      EXPLAIN_PRINT_EXPRS(check_cst_filter, type);
    }
  }
  */
  return ret;
}

const char* ObLogUpdate::get_name() const
{
  const char* name = NULL;
  if (is_pdml()) {
    if (is_index_maintenance()) {
      name = "INDEX UPDATE";
    } else {
      name = ObLogDelUpd::get_name();
    }
  } else if (!is_multi_part_dml()) {
    name = ObLogDelUpd::get_name();
  } else {
    name = "MULTI PARTITION UPDATE";
  }
  return name;
}

uint64_t ObLogUpdate::hash(uint64_t seed) const
{
  if (NULL != tables_assignments_) {
    int64_t N = tables_assignments_->count();
    for (int64_t i = 0; i < N; ++i) {
      seed = do_hash(tables_assignments_->at(i), seed);
    }
  }

  seed = ObLogDelUpd::hash(seed);

  return seed;
}

int ObLogUpdate::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(add_table_columns_to_ctx(ctx))) {
    LOG_WARN("failed to table columns", K(ret));
  } else if (OB_FAIL(add_all_table_assignments_to_ctx(tables_assignments_, ctx))) {
    LOG_WARN("faield to add all update exprs", K(ret));
  } else if (is_pdml() && OB_FAIL(alloc_partition_id_expr(ctx))) {
    LOG_WARN("fail generate pseudo partition_id column for update", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
    LOG_WARN("failed to add parent need expr", K(ret));
  } else if (stmt_id_expr_ != nullptr) {
    ExprProducer expr_producer(stmt_id_expr_, id_);
    if (OB_FAIL(ctx.add(expr_producer))) {
      LOG_WARN("failed to push balck raw_expr", K(ret));
    }
  }

  return ret;
}

int ObLogUpdate::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (is_pdml() && is_index_maintenance()) {
    if (OB_FAIL(alloc_shadow_pk_column_for_pdml(ctx))) {
      LOG_WARN("failed alloc generated column for pdml index maintain", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr post for delete", K(ret));
    }
  }
  return ret;
}

int ObLogUpdate::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogDelUpd::check_output_dep_specific(checker))) {
    LOG_WARN("ObLogDelUpd::check_output_dep_specific fails", K(ret));
  } else if (OB_ISNULL(tables_assignments_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tables_assignments_ is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments_->count(); ++i) {
    const ObAssignments& assigns = tables_assignments_->at(i).assignments_;
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      if (OB_ISNULL(assigns.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assigns.at(i).expr_ is null", K(ret), K(i));
      } else if (OB_FAIL(checker.check(*assigns.at(i).expr_))) {
        LOG_WARN("failed to check expr", K(i), K(ret));
      } else {
      }
    }
  }
  if (OB_SUCC(ret) && stmt_id_expr_ != nullptr) {
    if (OB_FAIL(checker.check(*stmt_id_expr_))) {
      LOG_WARN("store stmt_id_expr_ to checker failed", K(ret), KPC(stmt_id_expr_));
    }
  }
  return ret;
}

int ObLogUpdate::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* log_op = NULL;
  if (OB_FAIL(ObLogDelUpd::copy_without_child(log_op))) {
    LOG_WARN("copy without logical delupd failed", K(ret));
  } else {
    ObLogUpdate* log_upd = static_cast<ObLogUpdate*>(log_op);
    log_upd->tables_assignments_ = tables_assignments_;
    out = log_upd;
  }
  return ret;
}

int ObLogUpdate::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  OZ(ObLogDelUpd::allocate_exchange_post(ctx));
  CK(OB_NOT_NULL(get_stmt()));
  CK(OB_NOT_NULL(get_plan()));
  CK(OB_NOT_NULL(get_plan()->get_optimizer_context().get_session_info()));
  if (OB_SUCC(ret) && is_multi_part_dml_ &&
      get_plan()->get_optimizer_context().get_session_info()->use_static_typing_engine()) {
    ObDelUpdStmt* stmt = static_cast<ObDelUpdStmt*>(get_stmt());
    ObIArray<TableColumns>& table_columns = stmt->get_all_table_columns();
    for (int64_t k = 0; k < table_columns.count(); k++) {
      ObIArray<IndexDMLInfo>& index_infos = table_columns.at(k).index_dml_infos_;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
        ObRawExpr* expr = NULL;
        OZ(gen_calc_part_id_expr(index_infos.at(i).loc_table_id_, index_infos.at(i).index_tid_, expr));
        OZ(index_infos.at(i).calc_part_id_exprs_.push_back(expr));
        CK(OB_NOT_NULL(get_plan()));
        CK(OB_NOT_NULL(get_plan()->get_optimizer_context().get_session_info()));
        if (OB_SUCC(ret)) {
          share::schema::ObPartitionLevel part_level = PARTITION_LEVEL_MAX;
          ObRawExpr* part_expr = NULL;
          ObRawExpr* subpart_expr = NULL;
          ObRawExpr* new_part_expr = NULL;
          ObRawExpr* new_subpart_expr = NULL;
          ObRawExprFactory& expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
          if (OB_FAIL(get_part_exprs(index_infos.at(i).loc_table_id_,
                  index_infos.at(i).index_tid_,
                  part_level,
                  part_expr,
                  subpart_expr))) {
            LOG_WARN("fail to get part exprs", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, part_expr, new_part_expr, COPY_REF_DEFAULT))) {
            LOG_WARN("fail to copy part expr", K(ret));
          } else if (OB_FAIL(
                         ObRawExprUtils::copy_expr(expr_factory, subpart_expr, new_subpart_expr, COPY_REF_DEFAULT))) {
            LOG_WARN("fail to copy subpart expr", K(ret));
          } else {
            CK(PARTITION_LEVEL_MAX != part_level);
            for (int64_t assign_idx = 0; OB_SUCC(ret) && assign_idx < index_infos.at(i).assignments_.count();
                 assign_idx++) {
              ObColumnRefRawExpr* col = index_infos.at(i).assignments_.at(assign_idx).column_expr_;
              ObRawExpr* value = index_infos.at(i).assignments_.at(assign_idx).expr_;
              if (PARTITION_LEVEL_ZERO != part_level) {
                if (OB_FAIL(ObRawExprUtils::replace_ref_column(new_part_expr, col, value))) {
                  LOG_WARN("fail to replace ref column", K(ret));
                }
              }
              if (PARTITION_LEVEL_TWO == part_level) {
                if (OB_FAIL(ObRawExprUtils::replace_ref_column(new_subpart_expr, col, value))) {
                  LOG_WARN("fail to replace ref column", K(ret));
                }
              }
            }  // for assignments end
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(ObRawExprUtils::build_calc_part_id_expr(expr_factory,
                    *(get_plan()->get_optimizer_context().get_session_info()),
                    index_infos.at(i).index_tid_,
                    part_level,
                    new_part_expr,
                    new_subpart_expr,
                    expr))) {
              LOG_WARN("fail to build table location expr", K(ret));
            } else {
              OZ(index_infos.at(i).calc_part_id_exprs_.push_back(expr));
            }
          }
        }
      }  // for index info
    }    // for table columns
  }
  return ret;
}

int ObLogUpdate::need_multi_table_dml(AllocExchContext& ctx, ObShardingInfo& sharding_info, bool& is_needed)
{
  int ret = OB_SUCCESS;
  is_needed = false;
  if (is_pdml()) {
    // bypass
  } else if (OB_ISNULL(tables_assignments_) || OB_UNLIKELY(tables_assignments_->empty()) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), KPC_(tables_assignments), K_(my_plan));
  } else if (OB_FAIL(ObLogDelUpd::need_multi_table_dml(ctx, sharding_info, is_needed))) {
    LOG_WARN("check parent multi table dml failed", K(ret));
  } else if (!is_needed) {
    // not multi table update, have no global index in here, so to check if update partition key
    // if the partition key is updated, and partition_cnt > 1,
    // needed multi table dml to handle cross-partition update
    bool is_update_part_key = tables_assignments_->at(0).is_update_part_key_;
    uint64_t ref_table_id = OB_INVALID_ID;
    const ObTableSchema* tbl_schema = NULL;
    ObSchemaGetterGuard* schema_guard = NULL;
    if (OB_FAIL(
            get_update_table(get_stmt(), tables_assignments_->at(0).assignments_.at(0).column_expr_, ref_table_id))) {
      LOG_WARN("failed to get update table", K(ret));
    } else if (OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is null");
    } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, tbl_schema))) {
      LOG_WARN("get table schema failed", K(ret));
    } else if (is_update_part_key && tbl_schema->get_all_part_num() > 1) {
      is_needed = true;
      sharding_info.reset();
    }
  }
  return ret;
}

int ObLogUpdate::get_update_table(ObDMLStmt* stmt, ObColumnRefRawExpr* col, uint64_t& ref_table_id)
{
  int ret = OB_SUCCESS;
  TableItem* table = NULL;
  ObRawExpr* sel_expr = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(col)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(col));
  } else if (OB_ISNULL(table = stmt->get_table_item_by_id(col->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(table));
  } else if (table->is_basic_table()) {
    ref_table_id = table->ref_id_;
  } else if (!table->is_generated_table() || OB_ISNULL(table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is expected to be generated table", K(ret), K(table));
  } else {
    uint64_t sid = col->get_column_id() - OB_APP_MIN_COLUMN_ID;
    ObSelectStmt* view = table->ref_query_;
    if (OB_ISNULL(view) || OB_ISNULL(view = view->get_real_stmt()) ||
        OB_UNLIKELY(sid < 0 || sid >= view->get_select_item_size()) ||
        OB_ISNULL(sel_expr = view->get_select_item(sid).expr_) || OB_UNLIKELY(!sel_expr->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid generated table column", K(ret), K(view), K(sid), K(sel_expr));
    } else if (OB_FAIL(get_update_table(view, static_cast<ObColumnRefRawExpr*>(sel_expr), ref_table_id))) {
      LOG_WARN("failed to get update table", K(ret));
    }
  }
  return ret;
}

int ObLogUpdate::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(ObLogDelUpd::inner_append_not_produced_exprs(raw_exprs));
  if (NULL != lock_row_flag_expr_) {
    OZ(raw_exprs.append(lock_row_flag_expr_));
  }

  return ret;
}

int ObLogUpdate::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else {
    set_op_cost(child->get_card() * UPDATE_ONE_ROW_COST);
    set_cost(child->get_cost() + get_op_cost());
    set_card(child->get_card());
  }
  return ret;
}
