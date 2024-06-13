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
#include "sql/optimizer/ob_log_for_update.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_table_scan.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

ObLogForUpdate::ObLogForUpdate(ObLogPlan &plan)
  : ObLogicalOperator(plan),
    skip_locked_(false),
    is_multi_part_dml_(false),
    gi_charged_(false),
    wait_ts_(-1),
    lock_rownum_(NULL),
    index_dml_info_()
{}

const char* ObLogForUpdate::get_name() const
{
  const char *ret = "NOT SET";
  if (is_multi_part_dml()) {
    ret = "DISTRIBUTED FOR UPDATE";
  } else {
    ret = "FOR UPDATE";
  }
  return ret;
}

int ObLogForUpdate::get_plan_item_info(PlanText &plan_text,
                                       ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(BUF_PRINTF("lock tables"))) {
      LOG_WARN("BUF_PRINTF fails", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info_.count(); ++i) {
      TableItem *table = NULL;
      if (OB_ISNULL(index_dml_info_.at(i)) ||
          OB_ISNULL(table = stmt->get_table_item_by_id(index_dml_info_.at(i)->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret), K(index_dml_info_.at(i)), K(table));
      } else if (table->is_generated_table()) {
        // For hierarchical query, we only lock table in right view which is mocked by transform preprocess,
        // the name of right view is meaningless, hence we only print the base table name here.
        if (OB_ISNULL(table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ref query is NULL", K(ret));
        } else if (stmt->is_hierarchical_query()) {
          if (OB_ISNULL(table = table->ref_query_->get_table_item_by_id(index_dml_info_.at(i)->loc_table_id_))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table item is NULL", K(ret));
          }
        } else if (table->ref_query_->is_hierarchical_query()) {
          // for split hierarchical query
          TableItem *base_table = NULL;
          for (int j = 0; OB_SUCC(ret) && j < table->ref_query_->get_table_size(); ++j) {
            TableItem *mocked_join_table = table->ref_query_->get_table_item(j);
            if(OB_ISNULL(mocked_join_table)
               || OB_UNLIKELY(!mocked_join_table->is_generated_table())
               || OB_ISNULL(mocked_join_table->ref_query_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected mocked join table", K(ret), KPC(mocked_join_table));
            } else {
              base_table = mocked_join_table->ref_query_->get_table_item_by_id(index_dml_info_.at(i)->loc_table_id_);
              if (base_table != NULL) {
                break;
              }
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(base_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("base table is not find", K(ret));
          } else {
            table = base_table;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not a hierarchical query", K(ret), KPC(stmt));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(BUF_PRINTF("%c%.*s%c",
                                  i == 0 ? '(' : ' ',
                                  table->get_table_name().length(),
                                  table->get_table_name().ptr(),
                                  i == index_dml_info_.count() - 1 ? ')' : ','))) {
        LOG_WARN("failed to print lock table name", K(ret));
      }
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogForUpdate::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(ObLogicalOperator::compute_sharding_info())) {
    LOG_WARN("failed to compute sharding info", K(ret));
  } else if (OB_ISNULL(get_sharding())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    is_partition_wise_ = !is_multi_part_dml_ && !child->is_exchange_allocated() &&
        get_sharding()->is_distributed() &&
        NULL != get_sharding()->get_phy_table_location_info();
  }
  return ret;
}

int ObLogForUpdate::allocate_granule_pre(AllocGIContext &ctx)
{
  return pw_allocate_granule_pre(ctx);
}

int ObLogForUpdate::allocate_granule_post(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  bool is_partition_wise_state = ctx.is_in_partition_wise_state();
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(pw_allocate_granule_post(ctx))) { // 分配完GI以后，会对ctx的状态进行清理
    LOG_WARN("failed to allocate pw gi post", K(ret));
  } else {
    if (is_partition_wise_state && ctx.is_op_set_pw(this)) {
      ObSEArray<ObLogicalOperator *, 2> tsc_ops;
      if (OB_FAIL(find_all_tsc(tsc_ops, this))) {
        LOG_WARN("failed to find all tsc", K(ret));
      } else if (tsc_ops.count() < 1){
        // do nothing
        set_gi_above(true);
      } else {
        // tsc op与当前dml算子都需要set gi above
        ARRAY_FOREACH(tsc_ops, idx) {
          ObLogTableScan *tsc_op = static_cast<ObLogTableScan*>(tsc_ops.at(idx));
          tsc_op->set_gi_above(true);
        }
        set_gi_above(true);
      }
    } else {
      LOG_TRACE("not allocate dml gi for px",
        K(ret), K(ctx), K(ctx.is_op_set_pw(this)));
    }
  }
  return ret;
}

int ObLogForUpdate::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (is_multi_part_dml() && OB_FAIL(generate_multi_part_partition_id_expr())) {
    LOG_WARN("failed to generate update expr", K(ret));
  } else if (OB_FAIL(get_for_update_dependant_exprs(all_exprs))) {
    LOG_WARN("failed to get for update dependant exprs", K(ret));
  } else if (NULL != lock_rownum_ && OB_FAIL(all_exprs.push_back(lock_rownum_))) {
    LOG_WARN("failed to push back exprs", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogForUpdate::generate_multi_part_partition_id_expr()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info_.count(); ++i) {
    ObRawExpr *part_expr = NULL;
    if (OB_ISNULL(index_dml_info_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(get_plan()->gen_calc_part_id_expr(index_dml_info_.at(i)->loc_table_id_,
                                                         index_dml_info_.at(i)->ref_table_id_,
                                                         CALC_PARTITION_TABLET_ID,
                                                         part_expr))) {
      LOG_WARN("failed to gen calc part id expr", K(ret));
    } else if (OB_ISNULL(part_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      index_dml_info_.at(i)->old_part_id_expr_ = part_expr;
    }
  }
  return ret;
}

int ObLogForUpdate::get_for_update_dependant_exprs(ObIArray<ObRawExpr*> &dep_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const TableItem *table_item = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info_.count(); i++) {
      if (OB_ISNULL(index_dml_info_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (OB_FAIL(append(dep_exprs, index_dml_info_.at(i)->column_exprs_))) {
        LOG_WARN("failed to append rowkey expr", K(ret));
      } else if (!is_multi_part_dml()) {
        /*do nothing*/
      } else if (NULL != index_dml_info_.at(i)->old_part_id_expr_ &&
                 OB_FAIL(dep_exprs.push_back(index_dml_info_.at(i)->old_part_id_expr_))) {
        LOG_WARN("failed to push back old partition id expr", K(ret));
      } else { /*do nothing*/ }
    }
    // mark expr reference
    for (int64_t i = 0; OB_SUCC(ret) && i < dep_exprs.count(); i++) {
      if (OB_ISNULL(dep_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        dep_exprs.at(i)->set_explicited_reference();
      }
    }
  }
  return ret;
}

int ObLogForUpdate::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *first_child = NULL;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(first_child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("first child is null", K(ret));
  } else {
    // todo: refine for update cost
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    set_op_cost(ObOptEstCost::cost_get_rows(first_child->get_card(), opt_ctx));
    set_cost(first_child->get_cost() + op_cost_);
    set_card(first_child->get_card());
  }
  return ret;
}

int ObLogForUpdate::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Child is null", K(ret));
  } else if (OB_FAIL(set_op_ordering(child->get_op_ordering()))) {
    LOG_WARN("Failed to set op ordering", K(ret));
  }
  return ret;
}

int ObLogForUpdate::get_table_columns(const uint64_t table_id,
                                      ObIArray<ObColumnRefRawExpr *> &table_cols) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(get_stmt()->get_column_exprs(table_id, table_cols)))  {
    LOG_WARN("failed to get column exprs", K(ret));
  }
  for (int64_t i = table_cols.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(table_cols.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table column is null", K(ret), K(table_cols.at(i)));
    } else if (!IS_SHADOW_COLUMN(table_cols.at(i)->get_column_id())) {
      // do nothing
    } else if (OB_FAIL(table_cols.remove(i))) {
      LOG_WARN("failed to remove column expr", K(ret));
    }
  }
  return ret;
}

int ObLogForUpdate::get_rowkey_exprs(const uint64_t table_id,
                                     ObIArray<ObColumnRefRawExpr *> &rowkey) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info_.count(); ++i) {
    if (OB_ISNULL(index_dml_info_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index dml info is null", K(ret));
    } else if (index_dml_info_.at(i)->table_id_ != table_id) {
      // do nothing
    } else if (OB_FAIL(append(rowkey, index_dml_info_.at(i)->column_exprs_))) {
      LOG_WARN("failed to append rowkey expr", K(ret));
    } else {
      break;
    }
  }
  return ret;
}

int ObLogForUpdate::is_rowkey_nullable(const uint64_t table_id, bool &is_nullable) const
{
  int ret = OB_SUCCESS;
  is_nullable = false;
  if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(get_stmt(),
                                                     table_id,
                                                     is_nullable))) {
    LOG_WARN("failed to check is table on null side", K(ret));
  }
  return ret;
}

bool ObLogForUpdate::is_multi_table_skip_locked()
{
  bool is_multi_table_skip_locked = false;
  if (index_dml_info_.count() > 1 && is_skip_locked()) {
    is_multi_table_skip_locked = true;
  }
  return is_multi_table_skip_locked;
}

int ObLogForUpdate::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dml_info_.count(); ++i) {
    IndexDMLInfo *dml_info = index_dml_info_.at(i);
    if (OB_ISNULL(dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml info is null", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, dml_info->ck_cst_exprs_))) {
      LOG_WARN("failed to replace exprs", K(ret));
    } else if (NULL != dml_info->new_part_id_expr_ &&
        OB_FAIL(replace_expr_action(replacer, dml_info->new_part_id_expr_))) {
      LOG_WARN("failed to replace new parititon id expr", K(ret));
    } else if (NULL != dml_info->old_part_id_expr_ &&
        OB_FAIL(replace_expr_action(replacer, dml_info->old_part_id_expr_))) {
      LOG_WARN("failed to replace old parititon id expr", K(ret));
    } else if (NULL != dml_info->old_rowid_expr_ &&
        OB_FAIL(replace_expr_action(replacer, dml_info->old_rowid_expr_))) {
      LOG_WARN("failed to replace old rowid expr", K(ret));
    } else if (NULL != dml_info->new_rowid_expr_ &&
        OB_FAIL(replace_expr_action(replacer, dml_info->new_rowid_expr_))) {
      LOG_WARN("failed to replace new rowid expr", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, dml_info->column_convert_exprs_))) {
      LOG_WARN("failed to replace exprs", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, dml_info->column_old_values_exprs_))) {
      LOG_WARN("failed to replace column old values exprs ", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < dml_info->assignments_.count(); ++j) {
      if (OB_FAIL(replace_expr_action(replacer, dml_info->assignments_.at(j).expr_))) {
        LOG_WARN("failed to replace expr", K(ret));
      }
    }
  }
  return ret;
}
