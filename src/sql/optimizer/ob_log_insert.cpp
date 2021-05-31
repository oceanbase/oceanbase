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
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "ob_log_insert.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "sql/ob_phy_table_location.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/engine/expr/ob_expr_column_conv.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace share;
using namespace oceanbase::share::schema;
/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_COLUMN_ITEMS(columns, type)                              \
  {                                                                            \
    if (NULL != columns) {                                                     \
      BUF_PRINTF(#columns "(");                                                \
      int64_t N = columns->count();                                            \
      if (N == 0) {                                                            \
        BUF_PRINTF("nil");                                                     \
      } else {                                                                 \
        for (int64_t i = 0; i < N; i++) {                                      \
          BUF_PRINTF("[");                                                     \
          if (NULL != columns->at(i)) {                                        \
            if (OB_FAIL(columns->at(i)->get_name(buf, buf_len, pos, type))) {} \
          }                                                                    \
          BUF_PRINTF("]");                                                     \
          if (i < N - 1) {                                                     \
            BUF_PRINTF(", ");                                                  \
          }                                                                    \
        }                                                                      \
      }                                                                        \
      BUF_PRINTF(")");                                                         \
    } else {                                                                   \
    }                                                                          \
  }

const char* ObLogInsert::get_name() const
{
  const char* ret = "NOT SET";
  if (is_replace_) {
    if (is_multi_part_dml()) {
      ret = "MULTI TABLE REPLACE";
    } else {
      ret = "REPLACE";
    }
  } else if (!insert_up_) {
    if (is_multi_part_dml()) {
      ret = "MULTI PARTITION INSERT";
    } else if (is_pdml() && is_index_maintenance()) {
      ret = "INDEX INSERT";
    } else {
      ret = "INSERT";
    }
  } else {
    if (is_multi_part_dml()) {
      ret = "MULTI TABLE INSERT_UP";
    } else {
      ret = "INSERT_UP";
    }
  }
  return ret;
}

int ObLogInsert::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUG_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUG_PRINTF fails", K(ret));
  } else { /* Do nothing */
  }
  const ObIArray<TableColumns>* columns = get_all_table_columns();
  if (OB_SUCC(ret)) {
    EXPLAIN_PRINT_COLUMNS(columns, type);
  } else { /* Do nothing */
  }

  // print partitions
  const ObPhyPartitionLocationInfoIArray& partitions =
      table_partition_info_.get_phy_tbl_location_info().get_phy_part_loc_info_list();
  const bool two_level = (schema::PARTITION_LEVEL_TWO == table_partition_info_.get_part_level());
  ObSEArray<int64_t, 128> pids;
  int64_t N = partitions.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    const ObOptPartLoc& part_loc = partitions.at(i).get_partition_location();
    int64_t pid = part_loc.get_partition_id();
    if (OB_FAIL(pids.push_back(pid))) {
      LOG_WARN("failed to add partition id");
    } else {
      std::sort(pids.begin(), pids.end(), compare_partition_id);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(BUF_PRINTF(", "))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else if (OB_FAIL(ObLogicalOperator::explain_print_partitions(pids, two_level, buf, buf_len, pos))) {
      LOG_WARN("Failed to print partitions");
    } else {
    }  // do nothing
  }
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
  if (OB_SUCCESS == ret && (insert_up_ || (log_op_def::LOG_MERGE == type_ && tables_assignments_->count() > 0))) {
    const ObTableAssignment& ta = tables_assignments_->at(0);
    if (OB_FAIL(BUF_PRINTF(",\n"))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("      update("))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else { /* Do nothing */
    }
    int64_t N = ta.assignments_.count();
    if (OB_SUCC(ret)) {
      if (N == 0) {
        if (OB_FAIL(BUF_PRINTF("nil"))) {
          LOG_WARN("BUG_PRINTF fails", K(ret));
        } else { /* Do nothing */
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
          if (OB_FAIL(BUF_PRINTF("["))) {
            LOG_WARN("BUG_PRINTF fails", K(ret));
          } else if (OB_ISNULL(ta.assignments_.at(i).column_expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("variable_ or variable_->expr_ is NULL", K(ret));
          } else if (OB_FAIL(ta.assignments_.at(i).column_expr_->get_name(buf, buf_len, pos, type))) {
            LOG_WARN("get_name fails", K(ret));
          } else if (OB_FAIL(BUF_PRINTF("="))) {
            LOG_WARN("BUG_PRINTF fails", K(ret));
          } else if (OB_ISNULL(ta.assignments_.at(i).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr_ is NULL", K(ret));
          } else if (OB_FAIL(ta.assignments_.at(i).expr_->get_name(buf, buf_len, pos, type))) {
            LOG_WARN("get_name fails", K(ret));
          } else if (OB_FAIL(BUF_PRINTF("]"))) {
            LOG_WARN("BUG_PRINTF fails", K(ret));
          } else { /* Do nothing */
          }
          if (OB_SUCCESS == ret && i < N - 1) {
            if (OB_FAIL(BUF_PRINTF(", "))) {
              LOG_WARN("BUG_PRINTF fails", K(ret));
            } else { /* Do nothing */
            }
          } else { /* Do nothing */
          }
        }
      }
    } else { /* Do nothing */
    }
    BUF_PRINTF(")");
  } else { /* Do nothing */
  }

  if (OB_SUCCESS == ret && is_pdml() && nullptr != get_column_convert_exprs()) {
    ret = BUF_PRINTF(", ");
    const ObIArray<ObRawExpr*>& conv_exprs = *get_column_convert_exprs();
    EXPLAIN_PRINT_EXPRS(conv_exprs, type);
  }
  return ret;
}

uint64_t ObLogInsert::hash(uint64_t seed) const
{
  seed = do_hash(is_replace_, seed);
  seed = do_hash(low_priority_, seed);
  if (NULL != table_columns_) {
    HASH_PTR_ARRAY(*table_columns_, seed);
  }
  if (NULL != value_columns_) {
    HASH_PTR_ARRAY(*value_columns_, seed);
  }
  if (NULL != column_convert_exprs_) {
    HASH_PTR_ARRAY(*column_convert_exprs_, seed);
  }
  if (NULL != primary_keys_) {
    HASH_ARRAY(*primary_keys_, seed);
  }
  if (NULL != part_hint_) {
    seed = do_hash(*part_hint_, seed);
  }
  seed = do_hash(insert_up_, seed);
  seed = do_hash(only_one_unique_key_, seed);
  seed = ObLogDelUpd::hash(seed);

  return seed;
}

int ObLogInsert::extract_value_exprs()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(opt_ctx->get_session_info()) || OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_insert_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
    if (opt_ctx->get_session_info()->use_static_typing_engine() || insert_stmt->is_replace() ||
        insert_stmt->get_insert_up()) {
      if (OB_FAIL(append(value_exprs_, insert_stmt->get_column_conv_functions()))) {
        LOG_WARN("failed to append column conv functions", K(ret));
      }
    } else {
      if (OB_FAIL(add_exprs_without_column_conv(insert_stmt->get_column_conv_functions(), value_exprs_))) {
        LOG_WARN("failed to append output exprs without column conv", K(ret));
      }
    }
  }
  return ret;
}

int ObLogInsert::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (get_insert_up() && OB_FAIL(add_all_table_assignments_to_ctx(tables_assignments_, ctx))) {
    LOG_WARN("add all table assignments to ctx failed", K(ret));
  } else if (is_pdml()) {
    uint64_t producer_id = OB_INVALID_ID;
    if (!is_insert_select() && (NULL != value_columns_) && OB_FAIL(add_exprs_to_ctx(ctx, *value_columns_))) {
      LOG_WARN("failed to add exprs to ctx", K(ret));
    } else if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
      LOG_WARN("failed to get next producer id", K(ret));
    } else if (OB_FAIL(add_exprs_to_ctx_for_pdml(ctx, *column_convert_exprs_, producer_id))) {
      LOG_WARN("failed to add expr to ctx for pdml", K(ret));
    } else if (OB_FAIL(alloc_partition_id_expr(ctx))) {
      LOG_WARN("fail generate pseudo partition_id column for insert", K(ret));
    }
  } else if (OB_FAIL(add_exprs_to_ctx(ctx, get_value_exprs()))) {
    LOG_WARN("failed to add exprs to ctx", K(ret));
  }
  return ret;
}

int ObLogInsert::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* stmt = static_cast<ObInsertStmt*>(get_stmt());
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else if (stmt->is_returning() || is_pdml()) {
    // do nothing
  } else if (OB_FAIL(append(output_exprs_, get_value_exprs()))) {
    LOG_WARN("failed to append value exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
    bool expr_is_required = false;
    const ColumnItem* col_item = stmt->get_column_item(i);
    if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table column is null", K(ret));
    } else if (col_item->table_id_ != stmt->get_insert_table_id() || !col_item->expr_->is_explicited_reference()) {
      // do nothing
    } else if (OB_FAIL(mark_expr_produced(col_item->expr_, branch_id_, id_, ctx, expr_is_required))) {
      LOG_WARN("failed to mark expr produced", K(ret));
    } else if (!expr_is_required || is_plan_root() || !stmt->is_returning()) {
      // do nothing
    } else if (OB_FAIL(output_exprs_.push_back(col_item->expr_))) {
      LOG_WARN("failed to append output exprs", K(ret));
    }
  }

  if (OB_SUCC(ret) && is_pdml() && is_index_maintenance()) {
    // handle shadow pk column
    if (OB_FAIL(alloc_shadow_pk_column_for_pdml(ctx))) {
      LOG_WARN("failed alloc generated column for pdml index maintain", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogDelUpd::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr post", K(ret));
    }
  }

  return ret;
}

int ObLogInsert::add_exprs_without_column_conv(const ObIArray<ObRawExpr*>& src_exprs, ObIArray<ObRawExpr*>& dst_exprs)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(src_exprs, i)
  {
    if (OB_ISNULL(src_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src expr is null");
    } else if (T_FUN_COLUMN_CONV != src_exprs.at(i)->get_expr_type()) {
      if (OB_FAIL(dst_exprs.push_back(src_exprs.at(i)))) {
        LOG_WARN("store src expr failed", K(ret), K(i));
      }
    } else {
      ObRawExpr* origin_expr = src_exprs.at(i)->get_param_expr(ObExprColumnConv::VALUE_EXPR);
      if (ObOptimizerUtil::find_item(dst_exprs, origin_expr)) {
        if (OB_FAIL(dst_exprs.push_back(src_exprs.at(i)))) {
          LOG_WARN("store src expr failed", K(ret));
        }
      } else if (OB_FAIL(dst_exprs.push_back(origin_expr))) {
        LOG_WARN("store src expr failed", K(ret));
      } else {
        LOG_PRINT_EXPR(DEBUG,
            "expr is already produced, added into output expr",
            origin_expr,
            "Operator",
            get_name(),
            "Is Root",
            is_plan_root(),
            "Id",
            id_);
      }
    }
  }
  return ret;
}

int ObLogInsert::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogDelUpd::check_output_dep_specific(checker))) {
    LOG_WARN("ObLogDelUpd::check_output_dep_specific fails", K(ret));
  }
  if (OB_SUCC(ret) && nullptr != tables_assignments_) {
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
  }
  if (OB_SUCC(ret) && NULL != column_convert_exprs_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_convert_exprs_->count(); i++) {
      if (OB_ISNULL(column_convert_exprs_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(checker.check(*column_convert_exprs_->at(i)))) {
        LOG_WARN("failed to check expr", K(i), K(ret));
      } else { /*do nothing*/
      }
    }
  }
  if (OB_SUCC(ret) && NULL != value_columns_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < value_columns_->count(); i++) {
      if (OB_ISNULL(value_columns_->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(checker.check(*value_columns_->at(i)))) {
        LOG_WARN("failed to check expr", K(i), K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogInsert::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt()) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child), K(get_plan()), K(ret));
  } else if (is_pdml()) {
    if (OB_FAIL(allocate_exchange_post_pdml(ctx))) {
      LOG_WARN("failed to allocate pdml exchange", K(ret));
    }
  } else if (OB_FAIL(ObLogDelUpd::allocate_exchange_post(ctx))) {
    LOG_WARN("failed to do allocate exchange post", K(ret));
  } else if (!is_multi_part_dml_ ||
             (is_multi_part_dml_ &&
                 (ctx->exchange_allocated_ || ctx->plan_type_ == AllocExchContext::DistrStat::DISTRIBUTED))) {
    if (OB_FAIL(get_plan()->add_global_table_partition_info(&table_partition_info_))) {
      LOG_WARN("failed to add table partition info", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret) && is_multi_part_dml_) {
    if (!insert_up_) {
      ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
      ObIArray<TableColumns>& table_columns = insert_stmt->get_all_table_columns();
      CK(table_columns.count() == 1);
      ObIArray<IndexDMLInfo>& index_infos = table_columns.at(0).index_dml_infos_;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
        ObRawExpr* expr = NULL;
        if (insert_stmt->is_replace()) {
          if (OB_FAIL(ObLogicalOperator::gen_calc_part_id_expr(
                  index_infos.at(i).loc_table_id_, index_infos.at(i).index_tid_, expr))) {
            LOG_WARN("fail to gen calc part id expr", K(ret));
          }
        } else if (OB_FAIL(
                       gen_calc_part_id_expr(index_infos.at(i).loc_table_id_, index_infos.at(i).index_tid_, expr))) {
          LOG_WARN("fail to gen calc part id expr", K(ret));
        }
        if (OB_SUCC(ret)) {
          if (index_infos.at(i).calc_part_id_exprs_.push_back(expr)) {
            LOG_WARN("fail to push back calc part id expr", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (insert_up_ || log_op_def::LOG_MERGE == type_) {
      ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
      ObIArray<TableColumns>& table_columns = insert_stmt->get_all_table_columns();
      CK(table_columns.count() == 1);
      ObIArray<IndexDMLInfo>& index_infos = table_columns.at(0).index_dml_infos_;
      for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
        ObRawExpr* expr = NULL;
        OZ(ObLogicalOperator::gen_calc_part_id_expr(
            index_infos.at(i).loc_table_id_, index_infos.at(i).index_tid_, expr));
        OZ(index_infos.at(i).calc_part_id_exprs_.push_back(expr));
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
              if (OB_SUCC(ret) && PARTITION_LEVEL_TWO == part_level) {
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
      }
    }
  }

  return ret;
}

int ObLogInsert::gen_calc_part_id_expr(uint64_t table_id, uint64_t ref_table_id, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  expr = NULL;
  schema::ObPartitionLevel part_level = PARTITION_LEVEL_MAX;
  ObSQLSessionInfo* session = NULL;
  ObRawExpr* part_expr = NULL;
  ObRawExpr* subpart_expr = NULL;
  ObRawExpr* new_part_expr = NULL;
  ObRawExpr* new_subpart_expr = NULL;
  if (OB_ISNULL(get_plan()) || OB_INVALID_ID == ref_table_id) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (!session->use_static_typing_engine()) {
    // do nothing
  } else {
    ObRawExprFactory& expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
    if (OB_FAIL(get_part_exprs(table_id, ref_table_id, part_level, part_expr, subpart_expr))) {
      LOG_WARN("fail to get part exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, part_expr, new_part_expr, COPY_REF_DEFAULT))) {
      LOG_WARN("fail to copy part expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, subpart_expr, new_subpart_expr, COPY_REF_DEFAULT))) {
      LOG_WARN("fail to copy subpart expr", K(ret));
    } else {
      CK(OB_NOT_NULL(table_columns_));
      CK(OB_NOT_NULL(column_convert_exprs_));
      CK(PARTITION_LEVEL_MAX != part_level);
      if (column_convert_exprs_->count() == table_columns_->count()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < column_convert_exprs_->count(); i++) {
          if (PARTITION_LEVEL_ZERO != part_level) {
            if (OB_FAIL(ObRawExprUtils::replace_ref_column(
                    new_part_expr, table_columns_->at(i), column_convert_exprs_->at(i)))) {
              LOG_WARN("fail to replace ref column", K(ret));
            }
          }
          if (PARTITION_LEVEL_TWO == part_level) {
            if (OB_FAIL(ObRawExprUtils::replace_ref_column(
                    new_subpart_expr, table_columns_->at(i), column_convert_exprs_->at(i)))) {
              LOG_WARN("fail to replace ref column", K(ret));
            }
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRawExprUtils::build_calc_part_id_expr(
              expr_factory, *session, ref_table_id, part_level, new_part_expr, new_subpart_expr, expr))) {
        LOG_WARN("fail to build table location expr", K(ret));
      }
    }
  }

  return ret;
}

int ObLogInsert::inner_replace_generated_agg_expr(const ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_convert_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(replace_exprs_action(
                 to_replace_exprs, const_cast<common::ObIArray<ObRawExpr*>&>(*column_convert_exprs_)))) {
    LOG_WARN("failed to replace_expr_action", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogInsert::get_join_keys(
    const AllocExchContext& ctx, ObIArray<ObRawExpr*>& target_keys, ObIArray<ObRawExpr*>& source_keys)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> target_exprs;
  ObSEArray<ObRawExpr*, 4> source_exprs;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!get_stmt()->is_insert_stmt()) {
    // do nothing, maybe merge stmt
  } else {
    ObInsertStmt* stmt = static_cast<ObInsertStmt*>(get_stmt());
    if (OB_ISNULL(stmt->get_table_columns())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table columns is null", K(ret));
    } else if (OB_FAIL(append(target_exprs, *(stmt->get_table_columns())))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else if (OB_FAIL(stmt->get_value_exprs(source_exprs))) {
      LOG_WARN("failed to get value exprs", K(ret));
    } else if (OB_FAIL(prune_weak_part_exprs(ctx, target_exprs, source_exprs, target_keys, source_keys))) {
      LOG_WARN("failed to prune weak part exprs", K(ret));
    }
  }
  return ret;
}

int ObLogInsert::generate_sharding_info(ObShardingInfo& target_sharding_info)
{
  int ret = OB_SUCCESS;
  if (NULL == my_plan_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObOptimizerContext* optimizer_context = &my_plan_->get_optimizer_context();
    ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
    ObAddr server = optimizer_context->get_local_server_addr();
    if (OB_FAIL(calculate_table_location())) {
      LOG_WARN("failed to calculate table location", K(ret));
    } else if (OB_FAIL(target_sharding_info.init_partition_info(*optimizer_context,
                   *stmt_,
                   get_loc_table_id(),
                   get_index_tid(),
                   table_partition_info_.get_phy_tbl_location_info_for_update()))) {
      LOG_WARN("set partition key failed", K(ret));
    } else if (OB_FAIL(get_table_partition_info().get_location_type(server, location_type))) {
      LOG_WARN("get location type failed", K(ret));
    } else {
      table_phy_location_type_ = location_type;
      target_sharding_info.set_location_type(location_type);
      LOG_TRACE("succeed to generate target sharding info", K(target_sharding_info), K(table_phy_location_type_));
    }
  }
  return ret;
}

int ObLogInsert::calculate_table_location()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(get_plan()), K(child));
  } else {
    ObSchemaGetterGuard* schema_guard = get_plan()->get_optimizer_context().get_schema_guard();
    ObSqlSchemaGuard* sql_schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard();
    ObIPartitionLocationCache* location_cache = get_plan()->get_optimizer_context().get_location_cache();
    ObDMLStmt* stmt = const_cast<ObDMLStmt*>(get_plan()->get_stmt());
    const ParamStore* params = get_plan()->get_optimizer_context().get_params();
    ObExecContext* exec_ctx = get_plan()->get_optimizer_context().get_exec_ctx();
    const common::ObDataTypeCastParams dtc_params =
        ObBasicSessionInfo::create_dtc_params(get_plan()->get_optimizer_context().get_session_info());
    ObTaskExecutorCtx* task_exec_ctx = get_plan()->get_optimizer_context().get_task_exec_ctx();
    ObOrderDirection direction = UNORDERED;
    if (NULL != child->get_sharding_info().get_phy_table_location_info()) {
      direction = child->get_sharding_info().get_phy_table_location_info()->get_direction();
    }
    // initialized the table location
    if (OB_ISNULL(schema_guard) || OB_ISNULL(sql_schema_guard) || OB_ISNULL(stmt) || OB_ISNULL(exec_ctx) ||
        OB_ISNULL(task_exec_ctx) || OB_ISNULL(params) || OB_ISNULL(location_cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument",
          K(ret),
          K(schema_guard),
          K(sql_schema_guard),
          K(stmt),
          K(exec_ctx),
          K(task_exec_ctx),
          K(params),
          K(location_cache));
    } else if (OB_FAIL(table_partition_info_.init_table_location(*sql_schema_guard,
                   *stmt,
                   exec_ctx->get_my_session(),
                   stmt->get_condition_exprs(),
                   get_loc_table_id(),
                   get_index_tid(),
                   part_hint_,
                   dtc_params,
                   true))) {
      LOG_WARN("Failed to initialize table location", K(ret));
    } else if (OB_FAIL(table_partition_info_.calc_phy_table_loc_and_select_leader(
                   *exec_ctx, schema_guard, *params, *location_cache, dtc_params, get_index_tid(), direction))) {
      LOG_WARN("Failed to calculate table location", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogInsert::need_multi_table_dml(AllocExchContext& ctx, ObShardingInfo& sharding_info, bool& is_needed)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  const ObTableSchema* tbl_schema = NULL;
  ObSchemaGetterGuard* schema_guard = NULL;
  ObShardingInfo target_sharding_info;
  ObSEArray<ObShardingInfo*, 2> input_sharding;
  ObShardingInfo output_sharding;
  ObLogicalOperator* child = NULL;
  bool has_rand_part_key = false;
  bool has_subquery_part_key = false;
  bool trigger_exist = false;
  bool is_match = false;
  ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
  is_needed = false;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(insert_stmt) || OB_ISNULL(child = get_child(first_child)) ||
      OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(child), K(insert_stmt), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(get_index_tid(), tbl_schema))) {
    LOG_WARN("get table schema from schema guard failed", K(ret));
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(insert_stmt));
  }
  if (OB_SUCC(ret) && OB_FAIL(generate_sharding_info(target_sharding_info))) {
    if (ret == OB_NO_PARTITION_FOR_GIVEN_VALUE && trigger_exist) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to generate sharding info", K(ret));
    }
  }
  if (OB_FAIL(ret) || is_needed) {
  } else if (modify_multi_tables()) {
    is_needed = true;
  } else if (is_table_update_part_key() && tbl_schema->get_all_part_num() > 1) {
    is_needed = true;
  } else if (is_table_insert_sequence_part_key()) {
    is_needed = true;
  } else if (OB_FAIL(insert_stmt->part_key_has_rand_value(has_rand_part_key))) {
    LOG_WARN("failed to check whether part key containts random value", K(ret));
  } else if (OB_FAIL(insert_stmt->part_key_has_subquery(has_subquery_part_key))) {
    LOG_WARN("check to check whether part key containts subquery", K(ret));
  } else if (has_rand_part_key || has_subquery_part_key) {
    is_needed = true;
  } else if (get_part_hint() != NULL && insert_stmt->value_from_select()) {
    is_needed = true;
  } else if (target_sharding_info.is_local()) {
    is_needed = false;
    sharding_info.copy_with_part_keys(target_sharding_info);
  } else if (OB_FAIL(input_sharding.push_back(&target_sharding_info)) ||
             OB_FAIL(input_sharding.push_back(&child->get_sharding_info()))) {
    LOG_WARN("failed to push back sharding info", K(ret));
  } else if (ObLogicalOperator::compute_basic_sharding_info(input_sharding, output_sharding, is_basic)) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    is_needed = false;
    sharding_info.copy_with_part_keys(target_sharding_info);
  } else if (target_sharding_info.is_distributed() && child->get_sharding_info().is_match_all() &&
             insert_stmt->with_explicit_autoinc_column() && !is_insert_select()) {
    is_needed = false;
    ctx.plan_type_ = AllocExchContext::DistrStat::DISTRIBUTED;
    sharding_info.copy_with_part_keys(target_sharding_info);
  } else if (OB_FAIL(check_if_match_partition_wise_insert(
                 ctx, target_sharding_info, child->get_sharding_info(), is_match))) {
    LOG_WARN("failed to check partition wise", K(ret));
  } else if (is_match) {
    if (OB_FAIL(check_multi_table_dml_for_nested_execution(is_needed))) {
      LOG_WARN("fail to check nested execution", K(ret));
    } else if (!is_needed && OB_FAIL(check_multi_table_dml_for_px(ctx,
                                 &target_sharding_info,
                                 sharding_info,
                                 target_sharding_info.get_phy_table_location_info(),
                                 is_needed))) {
      LOG_WARN("failed to check multi part table insert for px", K(ret));
    } else {
      LOG_TRACE("multi part table insert for px", K(is_needed), K(target_sharding_info));
    }
  } else {
    is_needed = true;
    if (target_sharding_info.is_remote() && child->get_sharding_info().is_local()) {
      ctx.plan_type_ = AllocExchContext::DistrStat::DISTRIBUTED;
      LOG_TRACE("set multi part insert plan to dist", K(ctx.plan_type_));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed to check whether need multi-table dml for insert operator",
        K(is_match),
        K(is_basic),
        K(is_needed),
        K(target_sharding_info),
        K(sharding_info));
  }
  return ret;
}

int ObLogInsert::check_if_match_partition_wise_insert(const AllocExchContext& ctx,
    const ObShardingInfo& target_sharding_info, const ObShardingInfo& source_sharding_info, bool& is_part_covered)
{
  int ret = OB_SUCCESS;
  is_part_covered = false;
  ObSEArray<ObRawExpr*, 4> target_exprs;
  ObSEArray<ObRawExpr*, 4> source_exprs;
  ObLogicalOperator* first_child = NULL;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan()) ||
      OB_ISNULL(first_child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(get_stmt()), K(get_plan()), K(first_child));
  } else if (!get_stmt()->is_insert_stmt()) {
    // do nothing for merge stmt or px
  } else if (OB_FAIL(get_join_keys(ctx, target_exprs, source_exprs))) {
    LOG_WARN("failed to get join keys", K(ret));
  } else if (OB_FAIL(ObShardingInfo::check_if_match_partition_wise(*get_plan(),
                 first_child->get_ordering_output_equal_sets(),
                 target_exprs,
                 source_exprs,
                 target_sharding_info,
                 source_sharding_info,
                 is_part_covered))) {
    LOG_WARN("failed to check if match partition wise", K(ret));
  }
  return ret;
}

bool ObLogInsert::is_table_update_part_key() const
{
  return tables_assignments_ != NULL && !tables_assignments_->empty() && tables_assignments_->at(0).is_update_part_key_;
}

bool ObLogInsert::is_table_insert_sequence_part_key() const
{
  int ret = OB_SUCCESS;
  bool bret = false;
  if (OB_ISNULL(my_plan_) || OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (stmt_->has_part_key_sequence()) {
    const ObTableSchema* table_schema = NULL;
    uint64_t ref_table_id = get_index_tid();
    ObSchemaGetterGuard* schema_guard = my_plan_->get_optimizer_context().get_schema_guard();
    if (OB_ISNULL(schema_guard)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("failed to get table schema", K(ref_table_id));
    } else if (OB_FAIL(schema_guard->get_table_schema(ref_table_id, table_schema))) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("failed to get table schema", K(ref_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("failed to get table schema", K(ref_table_id));
    } else if (share::schema::PARTITION_LEVEL_ZERO != table_schema->get_part_level()) {
      bret = true;
    }
  }
  return bret;
}

ObPartIdRowMapManager* ObLogInsert::get_part_row_map()
{
  ObPartIdRowMapManager* manager = NULL;
  if (OB_ISNULL(my_plan_)) {
    LOG_WARN("invalid argument", K(my_plan_));
  } else {
    ObExecContext* exec_ctx = my_plan_->get_optimizer_context().get_exec_ctx();
    if (OB_ISNULL(exec_ctx)) {
      LOG_WARN("invalid argument", K(exec_ctx));
    } else {
      manager = &(exec_ctx->get_part_row_manager());
    }
  }
  return manager;
}

int ObLogInsert::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  OZ(ObLogDelUpd::inner_append_not_produced_exprs(raw_exprs));
  CK(OB_NOT_NULL(all_table_columns_));
  CK(all_table_columns_->count() > 0);
  if (OB_SUCC(ret) && is_multi_part_dml()) {
    const ObIArray<IndexDMLInfo>& index_dml_infos = all_table_columns_->at(0).index_dml_infos_;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_infos.count(); ++i) {
      OZ(raw_exprs.append(index_dml_infos.at(i).calc_part_id_exprs_));
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(column_convert_exprs_)) {
    // do nothing
  } else if (OB_FAIL(raw_exprs.append(*column_convert_exprs_))) {
    LOG_WARN("append_array_no_dup failed", K(ret));
  }
  if (nullptr != tables_assignments_ && !tables_assignments_->empty()) {
    const ObTableAssignment& table_assigns = tables_assignments_->at(0);
    ARRAY_FOREACH(table_assigns.assignments_, i)
    {
      const ObAssignment assign = table_assigns.assignments_.at(i);
      OZ(raw_exprs.append(assign.expr_));
      OZ(raw_exprs.append(static_cast<ObRawExpr*>(assign.column_expr_)));
    }
  }
  if (OB_SUCC(ret) && NULL != lock_row_flag_expr_) {
    OZ(raw_exprs.append(lock_row_flag_expr_));
  }

  return ret;
}

int ObLogInsert::allocate_exchange_post_pdml(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  bool is_partition_wise = false;
  ObLogicalOperator* child = NULL;
  ObSEArray<ObRawExpr*, 8> left_keys;
  ObSEArray<ObRawExpr*, 8> right_keys;
  ObShardingInfo output_sharding;
  ObSEArray<ObShardingInfo*, 2> input_sharding;
  EqualSets sharding_input_esets;
  ObInsertStmt* stmt = static_cast<ObInsertStmt*>(get_stmt());

  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(stmt) || OB_ISNULL(table_columns_) ||
      OB_ISNULL(column_convert_exprs_) || OB_ISNULL(all_table_columns_) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child), K(table_columns_), K(get_plan()), K(ret));
  } else if (OB_FAIL(generate_table_sharding_info(
                 get_loc_table_id(), get_index_tid(), part_hint_, get_table_partition_info(), get_sharding_info()))) {
    LOG_WARN("failed to generate sharding info", K(ret));
  } else if (FALSE_IT(get_sharding_info().set_location_type(OB_TBL_LOCATION_DISTRIBUTED))) {
  } else if (FALSE_IT(calc_phy_location_type())) {
    LOG_WARN("fail calc phy location type for insert", K(ret));
  } else if (OB_FAIL(sharding_info_.get_all_partition_keys(left_keys))) {
    LOG_WARN("failed to get all partition keys", K(ret));
  } else if (OB_FAIL(get_right_key(left_keys,
                 *column_convert_exprs_,
                 all_table_columns_->at(0).index_dml_infos_.at(0).column_exprs_,
                 right_keys))) {
    LOG_WARN("failed to get right key", K(ret));
  } else {
    // allocate pkey below insert operator
    ObExchangeInfo exch_info;
    ObRawExprFactory& expr_factory = get_plan()->get_optimizer_context().get_expr_factory();
    if (!left_keys.empty() &&
        OB_FAIL(compute_repartition_func_info(
            sharding_input_esets, right_keys, left_keys, sharding_info_, expr_factory, exch_info))) {
      LOG_WARN("failed to compute repartition func info", K(ret));

    } else if (OB_FAIL(set_hash_dist_column_exprs(exch_info, get_index_tid()))) {
      LOG_WARN("fail set hash dist column exprs", K(ret));
    } else {
      share::schema::ObPartitionLevel part_level = sharding_info_.get_part_level();
      exch_info.slice_count_ = sharding_info_.get_part_cnt();
      if (OB_SUCC(ret)) {
        exch_info.repartition_ref_table_id_ = get_index_tid();
        exch_info.repartition_table_name_ = get_index_table_name();
        ObPQDistributeMethod::Type dist_method = ObPQDistributeMethod::PARTITION_HASH;
        if (share::schema::PARTITION_LEVEL_ONE == part_level) {
          exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL;
          exch_info.dist_method_ = dist_method;
          need_alloc_part_id_expr_ = true;
        } else if (share::schema::PARTITION_LEVEL_TWO == part_level) {
          if (sharding_info_.is_partition_single()) {
            exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB;
          } else if (sharding_info_.is_subpartition_single()) {
            exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST;
          } else {
            exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_TWO_LEVEL;
          }
          exch_info.dist_method_ = dist_method;
          need_alloc_part_id_expr_ = true;
        } else if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
          exch_info.repartition_type_ = OB_REPARTITION_NO_REPARTITION;
          exch_info.dist_method_ = ObPQDistributeMethod::HASH;
          need_alloc_part_id_expr_ = false;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected partition level", K(sharding_info_.get_part_level()));
        }
      }
      if (OB_SUCC(ret) &&
          (part_level == share::schema::PARTITION_LEVEL_ONE || part_level == share::schema::PARTITION_LEVEL_TWO)) {
        ObSQLSessionInfo* session = NULL;
        if (OB_ISNULL(session = get_plan()->get_optimizer_context().get_session_info())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("session is null", K(ret));
        } else if (!session->use_static_typing_engine()) {
          // do nothing
        } else if (OB_FAIL(exch_info.init_calc_part_id_expr(get_plan()->get_optimizer_context()))) {
          LOG_WARN("fail to calc part id expr", K(ret), K(exch_info));
        } else {
          LOG_DEBUG("repart exch info", K(exch_info));
        }
      }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
      LOG_WARN("failed to allocate exchange", K(ret));
    } else if (OB_FAIL(get_plan()->add_global_table_partition_info(&get_table_partition_info()))) {
      LOG_WARN("failed to add global table partition info", K(ret), K(get_name()), K(get_table_partition_info()));
    } else {
      LOG_TRACE("succeed to allocate exchange", K(exch_info), K(left_keys), K(right_keys));
    }
  }
  return ret;
}

int ObLogInsert::set_hash_dist_column_exprs(ObExchangeInfo& exch_info, uint64_t index_tid) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (OB_ISNULL(all_table_columns_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else if (1 != all_table_columns_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table columns count, should have only one table for pdml", K(ret));
  } else {
    auto& infos = all_table_columns_->at(0).index_dml_infos_;
    for (int64_t i = 0; i < infos.count() && OB_SUCC(ret); ++i) {
      const IndexDMLInfo& info = infos.at(i);
      if (index_tid == info.index_tid_) {
        ObSEArray<ObRawExpr*, 8> rowkey_exprs;
        if (info.rowkey_cnt_ > info.column_exprs_.count() || info.rowkey_cnt_ > table_columns_->count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected count", K(info), "table_columns_count", table_columns_->count(), K(ret));
        } else if (column_convert_exprs_->count() != table_columns_->count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected count",
              "convert_cnt",
              column_convert_exprs_->count(),
              "column_cnt",
              table_columns_->count(),
              K(info),
              K(ret));
        }
        for (int64_t k = 0; OB_SUCC(ret) && k < info.rowkey_cnt_; ++k) {
          ObRawExpr* target_expr = info.column_exprs_.at(k);
          for (int64_t i = 0; OB_SUCC(ret) && i < column_convert_exprs_->count(); i++) {
            if (OB_FAIL(ObRawExprUtils::replace_ref_column(
                    target_expr, table_columns_->at(i), column_convert_exprs_->at(i)))) {
              LOG_WARN("fail to replace ref column", K(ret));
            }
          }
          OZ(rowkey_exprs.push_back(target_expr));
        }
        OZ(exch_info.append_hash_dist_expr(rowkey_exprs));
        found = true;
        break;
      }
    }
  }
  return ret;
}

int ObLogInsert::get_right_key(ObIArray<ObRawExpr*>& part_keys, const ObIArray<ObRawExpr*>& child_output_expr,
    const common::ObIArray<ObColumnRefRawExpr*>& columns, ObIArray<ObRawExpr*>& right_keys)
{
  int ret = OB_SUCCESS;
  if (columns.count() != child_output_expr.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr count", K(ret), K(columns), K(child_output_expr));
  }
  for (int64_t i = 0; i < part_keys.count() && OB_SUCC(ret); ++i) {
    if (!part_keys.at(i)->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr type", K(ret));
    } else {
      for (int64_t j = 0; j < columns.count() && OB_SUCC(ret); ++j) {
        if (columns.at(j)->is_column_ref_expr()) {
          ObColumnRefRawExpr* part_expr = static_cast<ObColumnRefRawExpr*>(part_keys.at(i));
          ObColumnRefRawExpr* col_expr = static_cast<ObColumnRefRawExpr*>(columns.at(j));
          if (col_expr->get_column_id() == part_expr->get_column_id() &&
              col_expr->get_table_id() == part_expr->get_table_id()) {
            if (OB_FAIL(right_keys.push_back(child_output_expr.at(j)))) {
              LOG_WARN("failed to push back right key", K(ret));
            }
            LOG_DEBUG("find insert right key", KPC(part_expr), KPC(col_expr));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && right_keys.count() != part_keys.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get right keys", K(ret), K(right_keys), K(part_keys), K(child_output_expr));
  }
  return ret;
}

void ObLogInsert::calc_phy_location_type()
{
  table_phy_location_type_ = get_sharding_info().get_location_type();
}
