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
#include "sql/optimizer/ob_log_del_upd.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/ob_smart_call.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace common;

ObLogDelUpd::ObLogDelUpd(ObLogPlan& plan)
    : ObLogicalOperator(plan),
      all_table_columns_(NULL),
      check_constraint_exprs_(NULL),
      table_columns_(NULL),
      part_hint_(NULL),
      table_partition_info_(plan.get_allocator()),
      stmt_id_expr_(NULL),
      lock_row_flag_expr_(NULL),
      ignore_(false),
      is_returning_(false),
      is_multi_part_dml_(false),
      is_pdml_(false),
      gi_charged_(false),
      is_index_maintenance_(false),
      need_barrier_(false),
      is_first_dml_op_(false),
      table_phy_location_type_(OB_TBL_LOCATION_UNINITIALIZED),
      table_location_uncertain_(false),
      pdml_partition_id_expr_(NULL),
      pdml_is_returning_(false),
      need_alloc_part_id_expr_(false)
{}

int ObLogDelUpd::add_table_columns_to_ctx(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(all_table_columns_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns_->count(); i++) {
      if (OB_UNLIKELY(all_table_columns_->at(i).index_dml_infos_.count() <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected array count", K(ret));
      } else if (OB_FAIL(add_exprs_to_ctx(ctx, all_table_columns_->at(i).index_dml_infos_.at(0).column_exprs_))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogDelUpd::add_all_table_assignments_to_ctx(
    const ObTablesAssignments* tables_assignments, ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  uint64_t producer_id = OB_INVALID_ID;
  if (OB_ISNULL(tables_assignments)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (log_op_def::LOG_UPDATE == get_type() || is_pdml()) {
    if (OB_FAIL(get_next_producer_id(get_child(first_child), producer_id))) {
      LOG_WARN("failed to get next producer id", K(ret));
    }
  } else {
    producer_id = get_operator_id();
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 8> exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_assignments->count(); ++i) {
      const ObAssignments& assigns = tables_assignments->at(i).assignments_;
      for (int64_t j = 0; OB_SUCC(ret) && j < assigns.count(); ++j) {
        if (OB_ISNULL(assigns.at(j).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(exprs.push_back(assigns.at(j).expr_))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_exprs_to_ctx(ctx, exprs, producer_id))) {
        LOG_WARN("failed to add exprs", K(ret));
      }
    }
  }
  return ret;
}

/*
int ObLogDelUpd::set_all_table_columns(const ObIArray<TableColumns> *all_table_columns)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  if (OB_ISNULL(all_table_columns)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("all_table_columns is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns->count(); i++) {
    const ObIArray<IndexDMLInfo> &index_dml_infos = all_table_columns->at(i).index_dml_infos_;
    for (int64_t j = 0; OB_SUCC(ret) && j < index_dml_infos.count(); j++) {
      table_id = index_dml_infos.at(j).index_tid_;
      if (is_link_table_id(table_id)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("write link table is not supported", K(ret), K(table_id));
      }
    }
  }
  if (OB_SUCC(ret)) {
    all_table_columns_ = all_table_columns;
    if (!all_table_columns_->empty() && !all_table_columns_->at(0).index_dml_infos_.empty()) {
      table_columns_ = &(all_table_columns_->at(0).index_dml_infos_.at(0).column_exprs_);
    } else {
      LOG_INFO("empty table_columns",
               "all_table_columns", all_table_columns_->empty(),
               "index_infos cnt", all_table_columns_->empty() ?
                                    0 : all_table_columns_->at(0).index_dml_infos_.count());
    }
  }
  return ret;
}
*/

int ObLogDelUpd::reordering_project_columns()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = get_child(ObLogicalOperator::first_child);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error");
  } else if (OB_FAIL(do_reordering_project_columns(*child))) {
    LOG_WARN("fail reorder child op project columns", K(ret));
  } else if (log_op_def::LOG_GRANULE_ITERATOR == child->get_type()) {
    if (NULL == (child = child->get_child(ObLogicalOperator::first_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret));
    } else if (OB_FAIL(do_reordering_project_columns(*child))) {
      LOG_WARN("fail reorder child op project columns", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::do_reordering_project_columns(ObLogicalOperator& child)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> tmp;
  const ObIArray<TableColumns>* table_columns = get_all_table_columns();
  if (NULL == table_columns) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObArray<ObRawExpr*> tmp_output;
    ObIArray<ObRawExpr*>& child_output = child.get_output_exprs();
    if (OB_FAIL(append(tmp_output, child_output))) {
      LOG_WARN("failed to merge output exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < table_columns->count(); ++i) {
        CK(table_columns->at(i).index_dml_infos_.count() > 0);
        if (OB_SUCC(ret)) {
          const ObIArray<ObColumnRefRawExpr*>& column_exprs = table_columns->at(i).index_dml_infos_.at(0).column_exprs_;
          for (int64_t j = 0; OB_SUCC(ret) && j < column_exprs.count(); ++j) {
            if (NULL == column_exprs.at(j)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid argument", K(ret));
            } else {
              ObRawExpr* target_expr = column_exprs.at(j);
              if (target_expr->is_column_ref_expr()) {
                ObColumnRefRawExpr* column_ref_expr = (ObColumnRefRawExpr*)(target_expr);
                if (column_ref_expr->is_virtual_generated_column() &&
                    !OB_ISNULL(column_ref_expr->get_dependant_expr()) &&
                    column_ref_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
                  continue;
                }
              }
              int64_t idx = OB_INVALID_INDEX;
              if (ObOptimizerUtil::find_item(child_output, target_expr, &idx)) {
                if (OB_INVALID_INDEX == idx) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("failed to get valid index", K(idx));
                } else {
                  ret = tmp.push_back(idx);
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to get all table columns",
                    "operator",
                    get_name(),
                    K(*target_expr),
                    K(child_output),
                    K(ret));
              }
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < child_output.count(); i++) {
          bool found = false;
          for (int64_t j = 0; !found && j < tmp.count(); j++) {
            if (tmp.at(j) == i) {
              found = true;
            }
          }

          if (!found) {
            ret = tmp.push_back(i);
          }
        }
      }

      if (OB_SUCC(ret)) {
        child_output.reset();
        // refill the output according to the order
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp.count(); i++) {
          ret = child_output.push_back(tmp_output.at(tmp.at(i)));
        }
      }
    }
  }
  return ret;
}

int ObLogDelUpd::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  ret = BUF_PRINTF(", ");
  const ObIArray<TableColumns>* table_columns = get_all_table_columns();
  EXPLAIN_PRINT_COLUMNS(table_columns, type);
  if (EXPLAIN_EXTENDED == type && need_barrier()) {
    ret = BUF_PRINTF(", ");
    ret = BUF_PRINTF("with_barrier");
  }
  return ret;
}

int ObLogDelUpd::allocate_exchange_post_pdml(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool need_exchange = false;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(generate_table_sharding_info(
                 get_loc_table_id(), get_index_tid(), part_hint_, get_table_partition_info(), get_sharding_info()))) {
    LOG_WARN("failed to calc sharding info", K(ret));
  } else if (OB_FAIL(check_pdml_need_exchange(ctx, get_sharding_info(), need_exchange))) {
    LOG_WARN("failed to check whether pdml need exchange", K(ret));
  } else if (!need_exchange) {
    ObOptimizerContext& optimizer_context = get_plan()->get_optimizer_context();
    ObSchemaGetterGuard* schema_guard = NULL;
    const ObTableSchema* table_schema = NULL;
    if (OB_ISNULL(schema_guard = optimizer_context.get_schema_guard())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get schama guart", K(ret));
    } else if (OB_FAIL(schema_guard->get_table_schema(get_index_tid(), table_schema))) {
      LOG_WARN("failed to get table schema", K(ret), K(get_index_tid()));
    } else if (table_schema->get_part_level() == PARTITION_LEVEL_ONE ||
               table_schema->get_part_level() == PARTITION_LEVEL_TWO) {
      need_alloc_part_id_expr_ = true;
    }
  } else {
    ObExchangeInfo exch_info;
    exch_info.slice_count_ = sharding_info_.get_part_cnt();
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(exch_info.set_repartition_info(sharding_info_.get_partition_keys(),
                   sharding_info_.get_sub_partition_keys(),
                   sharding_info_.get_partition_func()))) {
      LOG_WARN("failed to set repartition info", K(ret));
    } else if (OB_FAIL(set_hash_dist_column_exprs(exch_info, get_index_tid()))) {
      LOG_WARN("fail get hash dist exprs for pdml pkey-hash", K(ret));
    } else {
      exch_info.repartition_ref_table_id_ = get_index_tid();
      exch_info.repartition_table_name_ = get_index_table_name();
      share::schema::ObPartitionLevel part_level = sharding_info_.get_part_level();
      if (share::schema::PARTITION_LEVEL_ONE == part_level) {
        exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL;
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
        need_alloc_part_id_expr_ = true;
        LOG_TRACE("partition level is one, use pkey reshuffle method");
      } else if (share::schema::PARTITION_LEVEL_TWO == part_level) {
        if (sharding_info_.is_partition_single()) {
          exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL_SUB;
        } else if (sharding_info_.is_subpartition_single()) {
          exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_ONE_LEVEL_FIRST;
        } else {
          exch_info.repartition_type_ = OB_REPARTITION_ONE_SIDE_TWO_LEVEL;
        }
        exch_info.dist_method_ = ObPQDistributeMethod::PARTITION_HASH;
        need_alloc_part_id_expr_ = true;
        LOG_TRACE("partition level is two, use pkey reshuffle method");
      } else if (share::schema::PARTITION_LEVEL_ZERO == part_level) {
        sharding_info_.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
        exch_info.repartition_type_ = OB_REPARTITION_NO_REPARTITION;
        exch_info.dist_method_ = ObPQDistributeMethod::HASH;
        need_alloc_part_id_expr_ = false;
        LOG_TRACE("partition level is zero, use reduce reshuffle method");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected partition level", K(part_level), K(ret));
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

      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
        LOG_WARN("failed to allocate exchange", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_plan()->add_global_table_partition_info(&get_table_partition_info()))) {
      LOG_WARN("failed to add global table partition info", K(ret), K(get_name()), K(get_table_partition_info()));
    } else {
      LOG_TRACE("add table partition info for pdml index", K(get_name()), K(get_table_partition_info()));
    }
  }
  return ret;
}

int ObLogDelUpd::set_hash_dist_column_exprs(ObExchangeInfo& exch_info, uint64_t index_tid) const
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
        if (info.rowkey_cnt_ > info.column_exprs_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected count", K(info), K(ret));
        }
        for (int64_t k = 0; OB_SUCC(ret) && k < info.rowkey_cnt_; ++k) {
          ObRawExpr* target_expr = info.column_exprs_.at(k);
          for (int64_t assign_idx = 0; OB_SUCC(ret) && assign_idx < info.assignments_.count(); ++assign_idx) {
            const ObAssignment& assignment = info.assignments_.at(assign_idx);
            CK(OB_NOT_NULL(assignment.expr_));
            CK(OB_NOT_NULL(assignment.column_expr_));
            if (target_expr == assignment.column_expr_) {
              target_expr = assignment.expr_;
              break;
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
  if (!found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have at least one rowkey column", K(index_tid), K(ret));
  }
  return ret;
}

/* scenarios require/not-require exchange:
 *
 * PDML
 *   EX
 *     PDML
 *
 * EX
 *  PDML
 *   GI
 *    TSC
 *
 * PDML
 *  EX
 *   LOOKUP
 *
 * PDML
 *   EX
 *    EXPRS (insert into select from dual)
 */
int ObLogDelUpd::check_pdml_need_exchange(
    AllocExchContext* ctx, ObShardingInfo& target_sharding_info, bool& need_exchange)
{
  int ret = OB_SUCCESS;
  bool is_physical_equal = false;
  bool access_same_table = false;
  ObLogicalOperator* child = get_child(first_child);
  need_exchange = false;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(child), K(ret));
  } else if (log_op_def::LOG_DELETE == child->get_type() || log_op_def::LOG_UPDATE == child->get_type() ||
             log_op_def::LOG_INSERT == child->get_type()) {
    need_exchange = true;
  } else if (FALSE_IT(access_same_table =
                          (child->get_sharding_info().get_ref_table_id() == target_sharding_info.get_ref_table_id()))) {
  } else if (access_same_table) {
    // for intra partition parallel
    LOG_DEBUG("no exch, intra partition parallel", K(child->get_sharding_info()), K(target_sharding_info));
    need_exchange = false;
    ret = target_sharding_info.copy_with_part_keys(child->get_sharding_info());
  } else {
    need_exchange = true;
  }
  return ret;
}

int ObLogDelUpd::allocate_exchange_post_non_pdml(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool need_multi_dml = false;
  ObLogicalOperator* child = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt()) ||
      OB_ISNULL(get_plan()->get_optimizer_context().get_session_info())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ctx), K(child), K(get_plan()), K(ret));
  } else if (OB_FAIL(need_multi_table_dml(*ctx, sharding_info_, need_multi_dml))) {
    LOG_WARN("check need multi table dml failed", K(ret));
  } else if (need_multi_dml) {
    set_is_multi_part_dml(true);
    sharding_info_.set_location_type(OB_TBL_LOCATION_LOCAL);
    get_plan()->set_location_type(OB_PHY_PLAN_UNCERTAIN);
    ObOptimizerContext& opt_ctx = get_plan()->get_optimizer_context();
    if (child->get_sharding_info().is_sharding()) {
      ObExchangeInfo exch_info;
      if (OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
        LOG_WARN("allocate exchange failed", K(ret));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && opt_ctx.get_session_info()->use_static_typing_engine() &&
        (log_op_def::LOG_DELETE == get_type())) {
      ObDelUpdStmt* del_upd_stmt = static_cast<ObDelUpdStmt*>(get_stmt());
      ObIArray<TableColumns>& table_columns = del_upd_stmt->get_all_table_columns();
      for (int64_t k = 0; OB_SUCC(ret) && k < table_columns.count(); k++) {
        ObIArray<IndexDMLInfo>& index_infos = table_columns.at(k).index_dml_infos_;
        for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); ++i) {
          ObRawExpr* expr = NULL;
          if (OB_FAIL(gen_calc_part_id_expr(index_infos.at(i).loc_table_id_, index_infos.at(i).index_tid_, expr))) {
            LOG_WARN("fail to gen calc part id expr", K(ret));
          } else if (index_infos.at(i).calc_part_id_exprs_.push_back(expr)) {
            LOG_WARN("fail to push back calc part id expr", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret) && opt_ctx.is_batched_multi_stmt()) {
      // for multi part dml with batched multi stmt, data comes from remote server,
      // but implicit cursor was calculated in multi update operator,
      // so we must establish the correspondence between the row and the stmt_id
      // so we use the stmt_id_expr to project stmt_id to the row
      ObSysFunRawExpr* stmt_id_expr = NULL;
      if (OB_FAIL(opt_ctx.get_expr_factory().create_raw_expr(T_FUN_SYS_STMT_ID, stmt_id_expr))) {
        LOG_WARN("create stmt id expr failed", K(ret));
      } else if (OB_FAIL(stmt_id_expr->formalize(opt_ctx.get_session_info()))) {
        LOG_WARN("formalize stmt id expr failed", K(ret));
      } else if (OB_ISNULL(stmt_id_expr->get_op())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get expr operator of stmt_id_expr failed", K(ret));
      } else {
        stmt_id_expr->set_func_name(stmt_id_expr->get_op()->get_name());
        stmt_id_expr_ = stmt_id_expr;
      }
    }
  } else if (sharding_info_.is_local() && child->get_sharding_info().is_remote_or_distribute()) {
    ObExchangeInfo exch_info;
    if (OB_FAIL(child->allocate_exchange(ctx, exch_info))) {
      LOG_WARN("allocate exchange failed", K(ret));
    } else { /*do nothing*/
    }
  } else {
    is_partition_wise_ = (NULL != sharding_info_.get_phy_table_location_info());
  }
  return ret;
}

bool ObLogDelUpd::modify_multi_tables() const
{
  bool bret = false;
  if (OB_ISNULL(all_table_columns_) || all_table_columns_->empty()) {
    // do nothing
  } else if (all_table_columns_->count() > 1) {
    bret = true;
  } else if (all_table_columns_->at(0).index_dml_infos_.count() > 1) {
    bret = true;
  }
  return bret;
}

uint64_t ObLogDelUpd::get_target_table_id() const
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  if (OB_ISNULL(all_table_columns_) || OB_UNLIKELY(all_table_columns_->empty()) ||
      OB_UNLIKELY(all_table_columns_->at(0).index_dml_infos_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are null", K(ret), K(all_table_columns_));
  } else {
    table_id = all_table_columns_->at(0).index_dml_infos_.at(0).loc_table_id_;
  }
  return table_id;
}

int ObLogDelUpd::need_multi_table_dml(AllocExchContext& ctx, ObShardingInfo& sharding_info, bool& is_needed)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* child = NULL;
  uint64_t table_id = OB_INVALID_ID;
  ObShardingInfo* source_sharding = NULL;
  bool is_match = false;
  is_needed = false;
  const ObPhyTableLocationInfo* phy_table_loc_info;
  if (OB_ISNULL(my_plan_) || OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(my_plan_), K(child), K(ret));
  } else if (modify_multi_tables()) {
    is_needed = true;
    LOG_DEBUG("modify multi tables, needs multi table dml");
  } else if (ObPhyPlanType::OB_PHY_PLAN_UNCERTAIN == my_plan_->get_location_type()) {
    is_needed = true;
    LOG_DEBUG("uncertain location type, needs multi table dml");
  } else if (OB_INVALID_ID == (table_id = get_target_table_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table id", K(ret), KPC_(all_table_columns));
  } else if (OB_FAIL(get_source_sharding_info(child, table_id, source_sharding, phy_table_loc_info))) {
    LOG_WARN("failed to get dml table sharding info", K(ret), K(table_id), K(source_sharding));
  } else if (NULL == source_sharding) {
    // for insert into stmt
    if (log_op_def::LOG_INSERT == get_type()) {
      is_needed = false;
      LOG_DEBUG("insert without source sharding, no need multi table dml");
    } else {
      is_needed = true;
      LOG_DEBUG("dml without source sharding, need multi table dml");
    }
  } else if (source_sharding->is_local()) {
    is_needed = false;
    sharding_info.copy_with_part_keys(*source_sharding);
    LOG_DEBUG("source sharding is local, no need multi table dml");
  } else if (source_sharding->is_remote()) {
    if (ctx.exchange_allocated_) {
      is_needed = true;
      LOG_DEBUG("source sharding is remote, and exchange allocated, need multi table dml");
    } else {
      is_needed = false;
      sharding_info.copy_with_part_keys(*source_sharding);
      LOG_DEBUG("source sharding is remote, no need multi table dml");
    }
  } else if (source_sharding->is_distributed()) {
    if (OB_FAIL(check_multi_table_dml_for_nested_execution(is_needed))) {
      LOG_WARN("fail to check nested execution", K(ret));
    } else if (!is_needed && OB_FAIL(check_multi_table_dml_for_px(
                                 ctx, source_sharding, sharding_info, phy_table_loc_info, is_needed))) {
      LOG_WARN("failed to check multi part table dml for px", K(ret));
    } else {
      LOG_TRACE("use px, source sharding is distributed, check if need multi table dml",
          K(is_needed),
          K(source_sharding),
          K(sharding_info));
    }
  }

  if (OB_SUCC(ret) && source_sharding != NULL) {
    LOG_TRACE("succeed to check whether need multi-table dml",
        K(is_needed),
        K(is_match),
        K(*source_sharding),
        K(ctx.exchange_allocated_),
        K(has_global_index()),
        K(my_plan_->get_location_type()));
  } else if (OB_SUCC(ret) && source_sharding == NULL) {
    LOG_TRACE("succeed to check whether need multi-table dml",
        K(is_needed),
        K(is_match),
        K(source_sharding),
        K(ctx.exchange_allocated_),
        K(has_global_index()),
        K(my_plan_->get_location_type()));
  }
  return ret;
}

int ObLogDelUpd::check_multi_table_dml_for_px(AllocExchContext& ctx, ObShardingInfo* source_sharding,
    ObShardingInfo& sharding_info, const ObPhyTableLocationInfo* phy_table_locaion_info, bool& is_needed)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_table_locaion_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy_table_locaion_info is null", K(ret));
  } else if (phy_table_locaion_info->get_partition_cnt() > 1) {
    LOG_TRACE("multi partition for px dml");
    if (ctx.exchange_allocated_) {
      is_needed = true;
    } else {
      is_needed = false;
      sharding_info.copy_with_part_keys(*source_sharding);
    }
  } else if (phy_table_locaion_info->get_partition_cnt() <= 1) {
    LOG_TRACE("single partition for px dml");
    ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
    const ObAddr& server = my_plan_->get_optimizer_context().get_local_server_addr();
    if (phy_table_locaion_info->get_phy_part_loc_info_list().count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the count of partition is error", K(ret));
    } else if (OB_FAIL(ObTableLocation::get_location_type(
                   server, phy_table_locaion_info->get_phy_part_loc_info_list(), location_type))) {
      LOG_WARN("failed get location type", K(ret));
    } else if (location_type == OB_TBL_LOCATION_LOCAL) {
      LOG_TRACE("single partition use px dml, the dml location type is local");
      is_needed = false;
      sharding_info.copy_with_part_keys(*source_sharding);
      sharding_info.set_location_type(location_type);
    } else if (location_type == OB_TBL_LOCATION_REMOTE) {
      LOG_TRACE("single partition use px dml, the dml location type is remote");
      is_needed = true;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location type is error", K(ret), K(location_type));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part cnt is not correct", K(ret), K(source_sharding->get_part_cnt()));
  }

  return ret;
}

int ObLogDelUpd::get_source_sharding_info(ObLogicalOperator* child_op, uint64_t source_table_id,
    ObShardingInfo*& sharding_info, const ObPhyTableLocationInfo*& phy_table_locaion_info)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  sharding_info = NULL;
  if (OB_ISNULL(child_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (child_op->is_table_scan()) {
    ObLogTableScan* table_scan = static_cast<ObLogTableScan*>(child_op);
    if (table_scan->get_table_id() == source_table_id && !table_scan->get_is_index_global()) {
      sharding_info = &table_scan->get_sharding_info();
      phy_table_locaion_info = &(table_scan->get_table_partition_info_for_update()->get_phy_tbl_location_info());
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && NULL == sharding_info && i < child_op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(get_source_sharding_info(
              child_op->get_child(i), source_table_id, sharding_info, phy_table_locaion_info)))) {
        LOG_WARN("get source sharding info recursive failed", K(ret), K(source_table_id));
      }
    }
  }
  return ret;
}

int ObLogDelUpd::check_match_source_sharding_info(ObLogicalOperator* child_op, uint64_t source_table_id, bool& is_match)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(child_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret));
  } else if (NULL != child_op->get_sharding_info().get_phy_table_location_info() &&
             source_table_id == child_op->get_sharding_info().get_phy_table_location_info()->get_table_location_key()) {
    is_match = true;
  } else if (child_op->is_partition_wise()) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < child_op->get_num_of_child(); i++) {
      if (OB_FAIL(check_match_source_sharding_info(child_op->get_child(i), source_table_id, is_match))) {
        LOG_WARN("failed to check match source sharding info", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    is_match = false;
  }
  return ret;
}

uint64_t ObLogDelUpd::hash(uint64_t seed) const
{
  if (NULL != all_table_columns_) {
    int64_t N = all_table_columns_->count();
    for (int64_t i = 0; i < N; i++) {
      seed = do_hash(all_table_columns_->at(i), seed);
    }
  }
  if (NULL != check_constraint_exprs_) {
    seed = ObOptimizerUtil::hash_exprs(seed, *check_constraint_exprs_);
  }
  seed = do_hash(ignore_, seed);
  seed = do_hash(is_returning_, seed);
  seed = do_hash(is_multi_part_dml_, seed);
  seed = do_hash(is_pdml_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogDelUpd::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("clone op failed", K(ret));
  } else {
    ObLogDelUpd* del_upd = static_cast<ObLogDelUpd*>(op);
    del_upd->all_table_columns_ = all_table_columns_;
    del_upd->ignore_ = ignore_;
    out = del_upd;
  }
  return ret;
}

int ObLogDelUpd::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  if (all_table_columns_ != NULL) {
    int64_t N = all_table_columns_->count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      const ObIArray<ObColumnRefRawExpr*>& columns = all_table_columns_->at(i).index_dml_infos_.at(0).column_exprs_;
      int64_t M = columns.count();
      for (int64_t j = 0; OB_SUCC(ret) && j < M; ++j) {
        if (OB_ISNULL(columns.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("columns->at(j) contains null", K(ret), K(j));
        } else if (OB_FAIL(checker.check(*static_cast<ObRawExpr*>(columns.at(j))))) {
          LOG_WARN("failed to check column expr", K(ret), K(j));
        } else {
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_pdml()) {
      if (need_alloc_part_id_expr_) {
        if (OB_ISNULL(pdml_partition_id_expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the pdml partition id expr is null", K(ret), K(is_pdml()));
        } else if (checker.check(*pdml_partition_id_expr_)) {
          LOG_WARN("failed to check pdml_partition_id_expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogDelUpd::check_rowkey_distinct()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> rowkey_exprs;
  ObDelUpdStmt* delupd_stmt = static_cast<ObDelUpdStmt*>(get_stmt());
  if (OB_ISNULL(all_table_columns_) || OB_ISNULL(delupd_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(all_table_columns_), K(delupd_stmt));
  } else if (!delupd_stmt->dml_source_from_join()) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns_->count(); ++i) {
      IndexDMLInfo& primary_dml_info = const_cast<IndexDMLInfo&>(all_table_columns_->at(i).index_dml_infos_.at(0));
      bool is_unique = false;
      rowkey_exprs.reuse();
      for (int64_t k = 0; OB_SUCC(ret) && k < primary_dml_info.rowkey_cnt_; ++k) {
        if (OB_FAIL(rowkey_exprs.push_back(primary_dml_info.column_exprs_.at(k)))) {
          LOG_WARN("store rowkey expr failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(rowkey_exprs,
                get_fd_item_set(),
                get_ordering_output_equal_sets(),
                get_output_const_exprs(),
                is_unique))) {
          LOG_WARN("check dml is order unique failed", K(ret));
        } else if (is_unique) {
          primary_dml_info.distinct_algo_ = T_DISTINCT_NONE;
        } else {
          primary_dml_info.distinct_algo_ = T_HASH_DISTINCT;
        }
      }
    }
  }
  return ret;
}

int ObLogDelUpd::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  if (NULL != check_constraint_exprs_) {
    OZ(raw_exprs.append(*check_constraint_exprs_));
  }
  if (OB_SUCC(ret) && is_multi_part_dml()) {
    CK(OB_NOT_NULL(all_table_columns_));
    for (int64_t i = 0; OB_SUCC(ret) && i < all_table_columns_->count(); ++i) {
      const ObIArray<IndexDMLInfo>& index_dml_infos = all_table_columns_->at(i).index_dml_infos_;
      for (int64_t info_idx = 0; OB_SUCC(ret) && info_idx < index_dml_infos.count(); ++info_idx) {
        const IndexDMLInfo& index_dml_info = index_dml_infos.at(info_idx);
        OZ(raw_exprs.append(index_dml_info.calc_part_id_exprs_));
        for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < index_dml_info.column_exprs_.count(); ++col_idx) {
          ObColumnRefRawExpr* col_expr = index_dml_info.column_exprs_.at(col_idx);
          CK(OB_NOT_NULL(col_expr));
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (is_shadow_column(col_expr->get_column_id())) {
            OZ(raw_exprs.append(col_expr->get_dependant_expr()));
          } else {
            OZ(raw_exprs.append(static_cast<ObRawExpr*>(col_expr)));
          }
        }
        for (int64_t assign_idx = 0; OB_SUCC(ret) && assign_idx < index_dml_info.assignments_.count(); ++assign_idx) {
          const ObAssignment& assignment = index_dml_info.assignments_.at(assign_idx);
          CK(OB_NOT_NULL(assignment.expr_));
          CK(OB_NOT_NULL(assignment.column_expr_));
          OZ(raw_exprs.append(assignment.expr_));
          OZ(raw_exprs.append(static_cast<ObRawExpr*>(assignment.column_expr_)));
        }  // for assignments end
      }    // for index infos end
    }      // for all table end
  }

  return ret;
}

int ObLogDelUpd::generate_table_sharding_info(uint64_t loc_table_id, uint64_t ref_table_id, const ObPartHint* part_hint,
    ObTablePartitionInfo& table_partition_info, ObShardingInfo& sharding_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(get_plan()), K(get_stmt()), K(ret));
  } else {
    ObOptimizerContext& optimizer_context = get_plan()->get_optimizer_context();
    ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
    ObAddr& server = optimizer_context.get_local_server_addr();
    if (OB_FAIL(calculate_table_location(loc_table_id, ref_table_id, part_hint, table_partition_info))) {
      LOG_WARN("calculate table location failed", K(ret));
    } else if (OB_FAIL(table_partition_info.get_location_type(server, location_type))) {
      LOG_WARN("get location type failed", K(ret));
    } else if (OB_FAIL(sharding_info.init_partition_info(optimizer_context,
                   *stmt_,
                   loc_table_id,
                   ref_table_id,
                   table_partition_info.get_phy_tbl_location_info_for_update()))) {
      LOG_WARN("set partition key failed", K(ret));
    } else {
      sharding_info.set_location_type(OB_TBL_LOCATION_DISTRIBUTED);
      // record the real location type for PlanCache constraint
      table_phy_location_type_ = location_type;
      LOG_DEBUG("generate dml op sharding info",
          K(sharding_info),
          K(loc_table_id),
          K(ref_table_id),
          K(location_type),
          K(table_partition_info));
    }
  }
  return ret;
}

int ObLogDelUpd::allocate_granule_post(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  bool is_partition_wise_state = ctx.is_in_partition_wise_state();
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (get_plan()->get_optimizer_context().is_batched_multi_stmt() &&
             ((OB_PHY_PLAN_LOCAL == get_plan()->get_phy_plan_type() && !is_multi_part_dml()) ||
                 OB_PHY_PLAN_REMOTE == get_plan()->get_phy_plan_type())) {
    ctx.alloc_gi_ = true;
  } else if (OB_FAIL(pw_allocate_granule_post(ctx))) {
    LOG_WARN("failed to allocate pw gi post", K(ret));
  } else {
    if (is_partition_wise_state && ctx.is_op_set_pw(this)) {
      if (get_type() == log_op_def::LOG_UPDATE ||   // UPDATE: UPDATE, UPDATE RETURNING
          get_type() == log_op_def::LOG_DELETE ||   // DELETE: DELETE, DELETE RETURNING
          get_type() == log_op_def::LOG_INSERT ||   // INSERT: INSERT, INSERT UPDATE, REPLACE
          get_type() == log_op_def::LOG_MERGE ||    // MERGE: MERGE INTO
          get_type() == log_op_def::LOG_FOR_UPD) {  // FOR UPDATE: FOR UPDATE
        ObSEArray<ObLogicalOperator*, 2> tsc_ops;
        if (OB_FAIL(find_all_tsc(tsc_ops, this))) {
          LOG_WARN("failed to find all tsc", K(ret));
        } else if (tsc_ops.count() < 1) {
          // do nothing
          set_gi_above(true);
        } else {
          ARRAY_FOREACH(tsc_ops, idx)
          {
            ObLogTableScan* tsc_op = static_cast<ObLogTableScan*>(tsc_ops.at(idx));
            tsc_op->set_gi_above(true);
          }
          set_gi_above(true);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op type doesn't support gi", K(ret), K(get_name()));
      }
    } else {
      LOG_TRACE("not allocate dml gi for px", K(ret), K(ctx), K(ctx.is_op_set_pw(this)));
    }
  }
  return ret;
}

int ObLogDelUpd::allocate_granule_pre(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (get_plan()->get_optimizer_context().is_batched_multi_stmt() &&
             (OB_PHY_PLAN_LOCAL == get_plan()->get_phy_plan_type() ||
                 OB_PHY_PLAN_REMOTE == get_plan()->get_phy_plan_type())) {
    if (!is_multi_part_dml()) {
      ctx.set_in_partition_wise_state(this);
    }
  } else {
    ret = pw_allocate_granule_pre(ctx);
  }
  return ret;
}

int ObLogDelUpd::find_all_tsc(ObIArray<ObLogicalOperator*>& tsc_ops, ObLogicalOperator* root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the log op is null", K(ret));
  } else if (log_op_def::LOG_EXCHANGE != get_type()) {
    for (int64_t i = 0; i < root->get_num_of_child(); i++) {
      ObLogicalOperator* child = root->get_child(i);
      if (OB_FAIL(find_all_tsc(tsc_ops, child))) {
        LOG_WARN("failed to find all tsc", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && log_op_def::LOG_TABLE_SCAN == root->get_type()) {
    if (OB_FAIL(tsc_ops.push_back(root))) {
      LOG_WARN("failed to push back tsc ops", K(ret));
    }
  }
  return ret;
}

int ObLogDelUpd::calculate_table_location(uint64_t loc_table_id, uint64_t ref_table_id, const ObPartHint* part_hint,
    ObTablePartitionInfo& table_partition_info)
{
  int ret = OB_SUCCESS;

  if (NULL == my_plan_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObSchemaGetterGuard* schema_guard = my_plan_->get_optimizer_context().get_schema_guard();
    ObSqlSchemaGuard* sql_schema_guard = my_plan_->get_optimizer_context().get_sql_schema_guard();
    ObIPartitionLocationCache* location_cache = my_plan_->get_optimizer_context().get_location_cache();
    ObDMLStmt* stmt = const_cast<ObDMLStmt*>(my_plan_->get_stmt());
    const ParamStore* params = my_plan_->get_optimizer_context().get_params();
    ObExecContext* exec_ctx = my_plan_->get_optimizer_context().get_exec_ctx();
    const common::ObDataTypeCastParams dtc_params =
        ObBasicSessionInfo::create_dtc_params(my_plan_->get_optimizer_context().get_session_info());
    ObTaskExecutorCtx* task_exec_ctx = my_plan_->get_optimizer_context().get_task_exec_ctx();
    // initialized the table location
    if (OB_ISNULL(schema_guard) || OB_ISNULL(sql_schema_guard) || OB_ISNULL(stmt) || OB_ISNULL(exec_ctx) ||
        OB_ISNULL(task_exec_ctx) || OB_ISNULL(params) || OB_ISNULL(location_cache)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument",
          K(ret),
          K(schema_guard),
          KP(sql_schema_guard),
          K(stmt),
          K(exec_ctx),
          K(task_exec_ctx),
          K(params),
          K(location_cache));
    } else if (OB_FAIL(table_partition_info.init_table_location(*sql_schema_guard,
                   *stmt,
                   exec_ctx->get_my_session(),
                   is_first_dml_op_ ? stmt->get_condition_exprs() : get_filter_exprs(),
                   loc_table_id,
                   ref_table_id,
                   part_hint,
                   dtc_params,
                   true))) {
      LOG_WARN("Failed to initialize table location", K(ret));
    } else if (OB_FAIL(table_partition_info.calc_phy_table_loc_and_select_leader(
                   *exec_ctx, schema_guard, *params, *location_cache, dtc_params, ref_table_id, UNORDERED))) {
      LOG_WARN("Failed to calculate table location", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogDelUpd::alloc_partition_id_expr(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (need_alloc_part_id_expr_) {
    uint64_t table_id = OB_INVALID_ID;
    ObPseudoColumnRawExpr* partition_id_expr = nullptr;
    if (OB_FAIL(get_modify_table_id(table_id))) {
      LOG_WARN("failed to get modify table id", K(ret));
    } else if (OB_FAIL(ObLogicalOperator::alloc_partition_id_expr(table_id, ctx, partition_id_expr))) {
      LOG_WARN("fail allocate part id expr", K(table_id), K(ret));
    } else {
      pdml_partition_id_expr_ = partition_id_expr;
    }
  }
  return ret;
}

int ObLogDelUpd::alloc_shadow_pk_column_for_pdml(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; i < ctx.expr_producers_.count() && OB_SUCC(ret) && !found; i++) {
    ExprProducer expr_producer = ctx.expr_producers_.at(i);
    if (expr_producer.consumer_id_ == id_ && expr_producer.expr_->is_column_ref_expr()) {
      ObColumnRefRawExpr* column_ref_expr = (ObColumnRefRawExpr*)(expr_producer.expr_);
      if (column_ref_expr->is_virtual_generated_column() && !OB_ISNULL(column_ref_expr->get_dependant_expr()) &&
          column_ref_expr->get_dependant_expr()->get_expr_type() == T_OP_SHADOW_UK_PROJECT) {
        if (OB_FAIL(mark_expr_produced(column_ref_expr, branch_id_, id_, ctx, found))) {
          LOG_WARN("failed to mark expr produce", K(ret), K(id_), K(get_name()), K(*column_ref_expr));
        } else if (!found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("find error with mark expr produce for pdml", K(ret), K(id_), K(get_name()));
        } else {
          LOG_TRACE("the generated column expr is producing, and not add to output exprs", K(id_), K(get_name()));
        }
      }
    }
  }
  return ret;
}

int ObLogDelUpd::get_modify_table_id(uint64_t& table_id) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(all_table_columns_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all table columns is null", K(ret));
  } else if (all_table_columns_->count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of all table column is not 1", K(all_table_columns_->count()));
  } else if (all_table_columns_->at(0).index_dml_infos_.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of index dml info is not 1", K(ret), K(*all_table_columns_));
  } else {
    table_id = all_table_columns_->at(0).index_dml_infos_.at(0).index_tid_;
  }
  return ret;
}

// table id for location lookup.
uint64_t ObLogDelUpd::get_loc_table_id() const
{
  uint64_t tid = common::OB_INVALID_ID;
  if (all_table_columns_ != NULL && all_table_columns_->count() >= 1) {
    if (all_table_columns_->at(0).index_dml_infos_.count() >= 1) {
      tid = all_table_columns_->at(0).index_dml_infos_.at(0).loc_table_id_;
    }
  }
  return tid;
}
uint64_t ObLogDelUpd::get_index_tid() const
{
  uint64_t index_tid = common::OB_INVALID_ID;
  if (all_table_columns_ != NULL && all_table_columns_->count() >= 1) {
    if (all_table_columns_->at(0).index_dml_infos_.count() >= 1) {
      index_tid = all_table_columns_->at(0).index_dml_infos_.at(0).index_tid_;
    }
  }
  return index_tid;
}
uint64_t ObLogDelUpd::get_table_id() const
{
  uint64_t table_id = common::OB_INVALID_ID;
  if (all_table_columns_ != NULL && all_table_columns_->count() >= 1) {
    if (all_table_columns_->at(0).index_dml_infos_.count() >= 1) {
      table_id = all_table_columns_->at(0).index_dml_infos_.at(0).table_id_;
    }
  }
  return table_id;
}

const ObString ObLogDelUpd::get_index_table_name() const
{
  ObString index_table_name;
  if (all_table_columns_ != NULL && all_table_columns_->count() >= 1) {
    if (all_table_columns_->at(0).index_dml_infos_.count() >= 1) {
      index_table_name = all_table_columns_->at(0).index_dml_infos_.at(0).index_name_;
    }
  }
  return index_table_name;
}

int ObLogDelUpd::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_pdml()) {
    LOG_TRACE("allocate exchange for pdml");
    ret = allocate_exchange_post_pdml(ctx);
  } else {
    LOG_TRACE("allocate exchange for non-pdml");
    ret = allocate_exchange_post_non_pdml(ctx);
  }
  return ret;
}

int ObLogDelUpd::add_exprs_to_ctx_for_pdml(
    ObAllocExprContext& ctx, const ObIArray<ObRawExpr*>& input_exprs, uint64_t producer_id)
{
  int ret = OB_SUCCESS;
  ObRawExpr* cur_expr = NULL;
  ExprProducer* expr_producer = NULL;
  // set consumer id and producer id for pdml exprs
  for (int64_t i = 0; OB_SUCC(ret) && i < input_exprs.count(); ++i) {
    if (OB_ISNULL(cur_expr = input_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column convert exprs", K(ret));
    } else if (OB_FAIL(ctx.find(cur_expr, expr_producer))) {
      LOG_WARN("failed to find expr producer", K(ret));
    } else if (NULL != expr_producer) {
      // update producer id
      expr_producer->producer_id_ = producer_id;
      LOG_TRACE("succeed to update column convert expr producer id",
          K(cur_expr),
          K(*cur_expr),
          K(producer_id),
          K(id_),
          KPC(expr_producer),
          K(get_name()));
    } else {
      ExprProducer new_producer(cur_expr, id_, producer_id);
      if (OB_FAIL(ctx.add(new_producer))) {
        LOG_WARN("failed to push back expr producer", K(ret));
      } else {
        LOG_TRACE(
            "succeed to add exprs", K(cur_expr), K(*cur_expr), K(producer_id), K(id_), K(new_producer), K(get_name()));
      }
    }
  }
  return ret;
}

int ObLogDelUpd::check_multi_table_dml_for_nested_execution(bool& is_needed)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard* schema_guard = NULL;
  const ObTableSchema* tbl_schema = NULL;
  if (OB_ISNULL(all_table_columns_)) {
    // for update do nothing
  } else if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(get_plan()));
  } else if (get_plan()->get_stmt()->get_query_ctx()->has_pl_udf_) {
    is_needed = true;
  } else if (OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null");
  } else if (OB_FAIL(schema_guard->get_table_schema(get_index_tid(), tbl_schema))) {
    LOG_WARN("get table schema failed", K(ret));
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (tbl_schema->get_foreign_key_infos().count() > 0) {
    is_needed = true;
  }
  return ret;
}
