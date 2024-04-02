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
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "ob_log_insert.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_se_array_iterator.h"
#include "sql/ob_phy_table_location.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/ob_sql_utils.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_select_log_plan.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_insert_log_plan.h"
#include "common/ob_smart_call.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace share;
using namespace oceanbase::share::schema;
/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_COLUMN_ITEMS(columns, type)                                  \
  {                                                                                \
    if (NULL != columns) {                                                         \
      BUF_PRINTF(#columns"(");                                                     \
      int64_t N = columns->count();                                                \
      if (N == 0) {                                                                \
        BUF_PRINTF("nil");                                                         \
      } else {                                                                     \
        for (int64_t i = 0; i < N; i++) {                                          \
          BUF_PRINTF("[");                                                         \
          if (NULL != columns->at(i)) {                                            \
            if (OB_FAIL(columns->at(i)->get_name(buf, buf_len, pos, type))) {      \
            }                                                                      \
          }                                                                        \
          BUF_PRINTF("]");                                                         \
          if (i < N - 1) {                                                         \
            BUF_PRINTF(", ");                                                      \
          }                                                                        \
        }                                                                          \
      }                                                                            \
      BUF_PRINTF(")");                                                             \
    } else {                                                                       \
    }                                                                              \
  }

const char* ObLogInsert::get_name() const
{
  const char *ret = "NOT SET";
  if (is_replace_) {
    if (is_multi_part_dml()) {
      ret = "DISTRIBUTED REPLACE";
    } else {
      ret = "REPLACE";
    }
  } else if (!insert_up_) {
    if (is_multi_part_dml()) {
      ret = "DISTRIBUTED INSERT";
    } else if (is_pdml() && is_index_maintenance()) {
      ret = "INDEX INSERT";
    } else {
      ret = "INSERT";
    }
  } else {
    if (is_multi_part_dml()) {
      ret = "DISTRIBUTED INSERT_UP";
    } else {
      ret = "INSERT_UP";
    }
  }
  return ret;
}

int ObLogInsert::get_plan_item_info(PlanText &plan_text,
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
    } else if (NULL != table_partition_info_) {
      if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_FAIL(explain_print_partitions(*table_partition_info_,
                                                  buf,
                                                  buf_len,
                                                  pos))) {
        LOG_WARN("Failed to print partitions");
      } else { }//do nothing
    }
    // print column convert exprs
    if (OB_SUCC(ret) && !get_index_dml_infos().empty() && NULL != get_index_dml_infos().at(0)) {
      const ObIArray<ObRawExpr *> &column_values = get_index_dml_infos().at(0)->column_convert_exprs_;
      if(OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else {
        EXPLAIN_PRINT_EXPRS(column_values, type);
      }
    }

    if (OB_SUCC(ret) && insert_up_ ) {
      const IndexDMLInfo *table_insert_info = get_insert_up_index_dml_infos().at(0);
      if (OB_ISNULL(table_insert_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index dml info is null", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(",\n"))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("      update("))) {
        LOG_WARN("BUG_PRINTF fails", K(ret));
      } else if (OB_FAIL(print_assigns(table_insert_info->assignments_,
                                       buf,
                                       buf_len,
                                       pos,
                                       type))) {
        LOG_WARN("failed to print assigns", K(ret));
      } else { /* Do nothing */ }
      BUF_PRINTF(")");
    } else { /* Do nothing */ }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

uint64_t ObLogInsert::hash(uint64_t seed) const
{
  seed = do_hash(is_replace_, seed);
  seed = do_hash(insert_up_, seed);
  seed = ObLogDelUpd::hash(seed);

  return seed;
}

int ObLogInsert::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  // TODO @yibo split log insert and log insert all
  } else if (get_stmt()->is_insert_stmt() && OB_FAIL(get_constraint_info_exprs(all_exprs))) {
    LOG_WARN("failed to add duplicate key checker exprs to ctx", K(ret));
  } else if (OB_FAIL(ObLogDelUpd::inner_get_op_exprs(all_exprs, false))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else if (is_replace() && OB_FAIL(get_table_columns_exprs(get_replace_index_dml_infos(), 
                                                             all_exprs,
                                                             true))) {
    LOG_WARN("failed to add table columns to ctx", K(ret));
  } else if (get_insert_up() && OB_FAIL(get_table_columns_exprs(get_insert_up_index_dml_infos(),
                                                                all_exprs,
                                                                true))) {
    LOG_WARN("failed to add table columns to ctx", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogInsert::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (NULL != get_sharding()) {
    //do nothing
  } else if (is_multi_part_dml()) {
    strong_sharding_ = get_plan()->get_optimizer_context().get_local_sharding();
  } else if (OB_FAIL(ObLogDelUpd::compute_sharding_info())) {
    LOG_WARN("failed to compute sharding info", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogInsert::compute_plan_type()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(first_child)) || OB_ISNULL(get_plan())
      || OB_ISNULL(get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(get_plan()), K(ret));
  } else if (OB_FAIL(ObLogDelUpd::compute_plan_type())) { 
    LOG_WARN("failed to compute plan type", K(ret));
  } else if (is_multi_part_dml() && log_op_def::LOG_INSERT_ALL != get_type() &&
             ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED != phy_plan_type_ &&
             !get_plan()->get_stmt()->has_instead_of_trigger() &&
             is_insert_select()) {
    // 包含instead of trigger的view是没有table_partition_info_的,不需要走下面的逻辑
    ObTableLocationType location_type = OB_TBL_LOCATION_UNINITIALIZED;
    ObAddr &server = get_plan()->get_optimizer_context().get_local_server_addr();
    if (OB_ISNULL(table_partition_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(table_partition_info_->get_location_type(server, location_type))) {
      LOG_WARN("get location type failed", K(ret));
    } else if (child->is_local() && ObTableLocationType::OB_TBL_LOCATION_REMOTE == location_type  ) {
      // 特殊insert case处理：insert table是remote，child对应的是local，需要将计划设置为dist plan
      phy_plan_type_ = ObPhyPlanType::OB_PHY_PLAN_DISTRIBUTED;
      exchange_allocated_ = true;
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogInsert::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else {
    double op_cost = 0.0;
    if (OB_FAIL(inner_est_cost(child->get_card(), op_cost))) {
      LOG_WARN("failed to get insert cost", K(ret));
    } else {
      set_op_cost(op_cost);
      set_cost(child->get_cost() + get_op_cost());
      set_card(child->get_card());
    }
  }
  return ret;
}

int ObLogInsert::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  if (OB_ISNULL(child = get_child(ObLogicalOperator::first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else {
    double child_card = child->get_card();
    double child_cost = child->get_cost();
    if (OB_FAIL(SMART_CALL(child->re_est_cost(param, child_card, child_cost)))) {
      LOG_WARN("failed to re est exchange cost", K(ret));
    } else if (OB_FAIL(inner_est_cost(child_card, op_cost))) {
      LOG_WARN("failed to get insert cost", K(ret));
    } else {
      cost = child_cost + op_cost;
      card = child_card;
    }
  }
  return ret;
}

int ObLogInsert::inner_est_cost(double child_card, double &op_cost)
{
  int ret = OB_SUCCESS;
  ObDelUpCostInfo cost_info(0,0,0);
  cost_info.affect_rows_ = child_card;
  cost_info.index_count_ = get_index_dml_infos().count();
  if (0 == cost_info.index_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected index count", K(ret), K(get_index_dml_infos()));
  } else {
  const IndexDMLInfo *insert_dml_info = get_index_dml_infos().at(0);
  if (OB_ISNULL(insert_dml_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (get_insert_up()) {
    if (get_insert_up_index_dml_infos().empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert_up_index_dml_infos is empty", K(ret));
    } else {
      const IndexDMLInfo *upd_pri_dml_info = get_insert_up_index_dml_infos().at(0);
      if (OB_ISNULL(upd_pri_dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ins_pri_dml_info or upd_pri_dml_info is null", K(ret));
      } else {
        cost_info.constraint_count_ = insert_dml_info->ck_cst_exprs_.count()
                                      + upd_pri_dml_info->ck_cst_exprs_.count();
      }
    }
  } else {
    cost_info.constraint_count_ = insert_dml_info->ck_cst_exprs_.count();
  }
  }
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    if (OB_FAIL(ObOptEstCost::cost_insert(cost_info, 
                                          op_cost, 
                                          opt_ctx))) {
      LOG_WARN("failed to get insert cost", K(ret));
    }
  }
  return ret;
}

int ObLogInsert::get_constraint_info_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_insert_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (NULL != constraint_infos_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < constraint_infos_->count(); ++i) {
      const ObIArray<ObColumnRefRawExpr*> &constraint_columns =
          constraint_infos_->at(i).constraint_columns_;
      temp_exprs.reuse();
      if (OB_FAIL(append(temp_exprs, constraint_columns))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(all_exprs, temp_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObLogInsert::generate_rowid_expr_for_trigger()
{
  int ret = OB_SUCCESS;
  // oracle mode does not have replace or insert on duplicate
  if (lib::is_oracle_mode() && !has_instead_of_trigger()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
      bool has_trg = false;
      IndexDMLInfo *dml_info = get_index_dml_infos().at(i);
      if (OB_ISNULL(dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dml info is null", K(ret), K(dml_info));
      } else if (!dml_info->is_primary_index_) {
        // do nothing
      } else if (OB_FAIL(check_has_trigger(dml_info->ref_table_id_, has_trg))) {
        LOG_WARN("failed to check has trigger", K(ret));
      } else if (!has_trg) {
        // do nothing
      } else if (OB_FAIL(generate_old_rowid_expr(*dml_info))) {
        LOG_WARN("failed to generate rowid expr", K(ret));
      } else if (OB_FAIL(generate_insert_new_rowid_expr(*dml_info))) {
        LOG_WARN("failed to generate new rowid expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogInsert::generate_part_id_expr_for_foreign_key(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
    IndexDMLInfo *dml_info = get_index_dml_infos().at(i);
    if (OB_ISNULL(dml_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dml info is null", K(ret), K(dml_info));
    } else if (!dml_info->is_primary_index_) {
      // do nothing
    } else if (OB_FAIL(generate_fk_lookup_part_id_expr(*dml_info))) {
      LOG_WARN("failed to generate lookup part expr for foreign key", K(ret));
    } else if (OB_FAIL(convert_insert_new_fk_lookup_part_id_expr(all_exprs, *dml_info))) {
      LOG_WARN("failed to convert lookup part expr for foreign key", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (is_replace()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_replace_index_dml_infos().count(); ++i) {
      IndexDMLInfo *dml_info = get_replace_index_dml_infos().at(i);
      if (OB_ISNULL(dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dml info is null", K(ret), K(dml_info));
      } else if (!dml_info->is_primary_index_) {
        // do nothing
      } else if (OB_FAIL(generate_fk_lookup_part_id_expr(*dml_info))) {
        LOG_WARN("failed to generate lookup part expr for foreign key", K(ret));
      } else if (OB_FAIL(convert_update_new_fk_lookup_part_id_expr(all_exprs, *dml_info))) {
        LOG_WARN("failed to convert lookup part expr for foreign key", K(ret));
      }
    }
  } else if (get_insert_up()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_insert_up_index_dml_infos().count(); ++i) {
      IndexDMLInfo *dml_info = get_insert_up_index_dml_infos().at(i);
      if (OB_ISNULL(dml_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("dml info is null", K(ret), K(dml_info));
      } else if (!dml_info->is_primary_index_) {
        // do nothing
      } else if (OB_FAIL(generate_fk_lookup_part_id_expr(*dml_info))) {
        LOG_WARN("failed to generate lookup part expr for foreign key", K(ret));
      } else if (OB_FAIL(convert_update_new_fk_lookup_part_id_expr(all_exprs, *dml_info))) {
        LOG_WARN("failed to convert lookup part expr for foreign key", K(ret));
      }
    }
  }

  return ret;
}

int ObLogInsert::generate_multi_part_partition_id_expr()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < get_index_dml_infos().count(); ++i) {
    if (OB_ISNULL(get_index_dml_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index dml info is null", K(ret));
    } else if (OB_FAIL(generate_old_calc_partid_expr(*get_index_dml_infos().at(i)))) {
      LOG_WARN("failed to generate calc partid expr", K(ret));
    } else if (OB_FAIL(generate_insert_new_calc_partid_expr(*get_index_dml_infos().at(i)))) {
      LOG_WARN("failed to generate new calc partid expr", K(ret));
    } else { /*do nothing*/ }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (is_replace()) {
    // delete in replace only old part id expr is required
    for (int64_t i = 0; OB_SUCC(ret) && i < get_replace_index_dml_infos().count(); ++i) {
      IndexDMLInfo *index_info = get_replace_index_dml_infos().at(i);
      ObSqlSchemaGuard *schema_guard = NULL;
      const ObTableSchema *table_schema = NULL;
      bool is_heap_table = false;
      ObArray<ObRawExpr *> column_exprs;
      if (OB_ISNULL(get_plan()) ||
          OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(index_info->ref_table_id_, table_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (table_schema != NULL && FALSE_IT(is_heap_table = table_schema->is_heap_table())) {
        // do nothing.
      } else {
        // When lookup_part_id_expr is a virtual generated column,
        // the table with the primary key needs to be replaced,
        // and the table without the primary key does not need to be replaced
        ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
        if (OB_ISNULL(index_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("replace dml info is null", K(ret));
        } else if (OB_FAIL(generate_old_calc_partid_expr(*index_info))) {
          LOG_WARN("failed to generate calc partid expr", K(ret));
        } else if (!is_heap_table && OB_FAIL(ObLogTableScan::replace_gen_column(get_plan(),
                                            index_info->old_part_id_expr_,
                                            index_info->lookup_part_id_expr_))){
          LOG_WARN("failed to replace expr", K(ret));
        } else if (is_heap_table) {
          if (OB_FAIL(generate_lookup_part_id_expr(*index_info))) {
            LOG_WARN("failed to generate lookup part id expr", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(
                    index_info->lookup_part_id_expr_, column_exprs))) {
            LOG_WARN("failed to extract column exprs", K(ret));
          } else if (OB_FAIL(copier.add_skipped_expr(column_exprs))) {
            LOG_WARN("failed to add skipped exprs", K(ret));
          } else if (OB_FAIL(copier.copy(index_info->lookup_part_id_expr_,
                                        index_info->lookup_part_id_expr_))) {
            LOG_WARN("failed to copy lookup part id expr", K(ret));
          }
        }
      }
    }
  } else if (get_insert_up()) {
    // generate the calc_part_id_expr_ of update clause
    for (int64_t i = 0; OB_SUCC(ret) && i < get_insert_up_index_dml_infos().count(); ++i) {
      IndexDMLInfo *dml_info = get_insert_up_index_dml_infos().at(i);
      ObSqlSchemaGuard *schema_guard = NULL;
      const ObTableSchema *table_schema = NULL;
      bool is_heap_table = false;
      ObArray<ObRawExpr *> column_exprs;
      if (OB_ISNULL(get_plan()) ||
          OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_sql_schema_guard())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(schema_guard->get_table_schema(dml_info->ref_table_id_, table_schema))) {
        LOG_WARN("failed to get table schema", K(ret));
      } else if (table_schema != NULL && FALSE_IT(is_heap_table = table_schema->is_heap_table())) {
        // do nothing.
      } else {
        // When lookup_part_id_expr is a virtual generated column,
        // the table with the primary key needs to be replaced,
        // and the table without the primary key does not need to be replaced
        ObRawExprCopier copier(get_plan()->get_optimizer_context().get_expr_factory());
        // insert on duplicate update stmt
        if (OB_ISNULL(dml_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insert_up dml info is null", K(ret));
        } else if (OB_FAIL(generate_old_calc_partid_expr(*dml_info))) {
          LOG_WARN("fail to generate calc partid expr", K(ret));
        } else if (OB_FAIL(generate_update_new_calc_partid_expr(*dml_info))) {
          LOG_WARN("failed to generate update new part id expr", K(ret));
        } else if (!is_heap_table && OB_FAIL(ObLogTableScan::replace_gen_column(get_plan(),
                                            dml_info->old_part_id_expr_,
                                            dml_info->lookup_part_id_expr_))){
          LOG_WARN("failed to replace expr", K(ret));
        } else if (is_heap_table) {
          if (OB_FAIL(generate_lookup_part_id_expr(*dml_info))) {
            LOG_WARN("failed to generate lookup part id expr", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(
                    dml_info->lookup_part_id_expr_, column_exprs))) {
            LOG_WARN("failed to extract column exprs", K(ret));
          } else if (OB_FAIL(copier.add_skipped_expr(column_exprs))) {
            LOG_WARN("failed to add skipped exprs", K(ret));
          } else if (OB_FAIL(copier.copy(dml_info->lookup_part_id_expr_,
                            dml_info->lookup_part_id_expr_))) {
            LOG_WARN("failed to copy lookup part id expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogInsert::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogDelUpd::inner_replace_op_exprs(replacer))) {
    LOG_WARN("failed to replace op exprs", K(ret));
  } else if (is_replace() &&
             OB_FAIL(replace_dml_info_exprs(replacer, get_replace_index_dml_infos()))) {
    LOG_WARN("failed to replace dml info exprs", K(ret));
  } else if (get_insert_up() &&
             OB_FAIL(replace_dml_info_exprs(replacer, get_insert_up_index_dml_infos()))) {
    LOG_WARN("failed to replace dml info exprs", K(ret));
  } else if (NULL != constraint_infos_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < constraint_infos_->count(); ++i) {
      const ObIArray<ObColumnRefRawExpr*> &constraint_columns =
          constraint_infos_->at(i).constraint_columns_;
      for (int64_t i = 0; OB_SUCC(ret) && i < constraint_columns.count(); ++i) {
        ObColumnRefRawExpr *expr = constraint_columns.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (expr->is_virtual_generated_column()) {
          ObRawExpr *&dependant_expr = static_cast<ObColumnRefRawExpr *>(
                                      expr)->get_dependant_expr();
          if (OB_FAIL(replace_expr_action(replacer, dependant_expr))) {
            LOG_WARN("failed to push back generate replace pair", K(ret));
          }
        }
      }
    }
  }
  return ret;
}