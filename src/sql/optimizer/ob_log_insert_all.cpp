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
#include "ob_log_insert_all.h"
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

const char* ObLogInsertAll::get_name() const
{
  const char* ret = "MULTI TABLE INSERT";
  return ret;
}

int ObLogInsertAll::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
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

  // print multi table info
  if (OB_FAIL(ret)) {
  } else if (is_multi_conditions_insert()) {
    if (is_multi_insert_first() && OB_FAIL(BUF_PRINTF(", multi_table_first(%s),\n      ", "true"))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else if (!is_multi_insert_first() && OB_FAIL(BUF_PRINTF(",\n      "))) {
      LOG_WARN("BUG_PRINTF fails", K(ret));
    } else if (get_multi_column_convert_exprs() != NULL && get_multi_insert_table_info() != NULL) {
      if (OB_ISNULL(get_stmt()) ||
          OB_UNLIKELY(get_multi_insert_table_info()->count() != get_multi_column_convert_exprs()->count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null",
            K(get_multi_insert_table_info()->count()),
            K(get_stmt()),
            K(get_multi_column_convert_exprs()->count()),
            K(ret));
      } else {
        int64_t pre_idx = -1;
        for (int64_t i = 0; OB_SUCC(ret) && i < get_multi_insert_table_info()->count(); ++i) {
          TableItem* table_item = get_stmt()->get_table_item_by_id(get_multi_insert_table_info()->at(i).table_id_);
          if (OB_ISNULL(table_item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(table_item), K(ret));
          } else if (get_multi_insert_table_info()->at(i).when_conds_idx_ == pre_idx) {
            BUF_PRINTF(",\"%.*s\"", table_item->table_name_.length(), table_item->table_name_.ptr());
          } else if (get_multi_insert_table_info()->at(i).when_conds_idx_ != -1) {  // when
            if (pre_idx != -1) {
              BUF_PRINTF("}; ");
            }
            const ObIArray<ObRawExpr*>& when_exprs = get_multi_insert_table_info()->at(i).when_conds_expr_;
            EXPLAIN_PRINT_EXPRS(when_exprs, type);
            BUF_PRINTF(" then: table_list{");
            BUF_PRINTF("\"%.*s\"", table_item->table_name_.length(), table_item->table_name_.ptr());
            pre_idx = get_multi_insert_table_info()->at(i).when_conds_idx_;
          } else {  // else
            BUF_PRINTF("}; ELSE: table_list{");
            BUF_PRINTF("\"%.*s\"", table_item->table_name_.length(), table_item->table_name_.ptr());
            pre_idx = get_multi_insert_table_info()->at(i).when_conds_idx_;
          }
        }
        if (OB_SUCC(ret)) {
          BUF_PRINTF("},\n      ");
          for (int64_t i = 0; OB_SUCC(ret) && i < get_multi_column_convert_exprs()->count(); ++i) {
            const ObIArray<ObRawExpr*>& conv_exprs = get_multi_column_convert_exprs()->at(i);
            EXPLAIN_PRINT_EXPRS(conv_exprs, type);
            if (i < get_multi_column_convert_exprs()->count() - 1) {
              BUF_PRINTF(",\n      ");
            }
          }
        }
      }
    }
  } else if (OB_FAIL(BUF_PRINTF(",\n      "))) {
    LOG_WARN("BUG_PRINTF fails", K(ret));
  } else if (get_multi_column_convert_exprs() != NULL) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_multi_column_convert_exprs()->count(); ++i) {
      const ObIArray<ObRawExpr*>& conv_exprs = get_multi_column_convert_exprs()->at(i);
      EXPLAIN_PRINT_EXPRS(conv_exprs, type);
      if (i < get_multi_column_convert_exprs()->count() - 1) {
        BUF_PRINTF(",\n      ");
      }
    }
  }
  return ret;
}

int ObLogInsertAll::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(multi_value_columns_) || OB_ISNULL(multi_column_convert_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null", K(multi_value_columns_), K(multi_column_convert_exprs_), K(ret));
  } else if (OB_FAIL(ObLogInsert::check_output_dep_specific(checker))) {
    LOG_WARN("ObLogDelUpd::check_output_dep_specific fails", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_value_exprs_.count(); ++i) {
      const ObIArray<ObRawExpr*>& value_exprs = multi_value_exprs_.at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < value_exprs.count(); j++) {
        if (OB_ISNULL(value_exprs.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*value_exprs.at(j)))) {
          LOG_WARN("failed to check expr", K(j), K(ret));
        } else { /*do nothing*/
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_value_columns_->count(); ++i) {
      const ObIArray<ObColumnRefRawExpr*>& value_columns = multi_value_columns_->at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < value_columns.count(); j++) {
        if (OB_ISNULL(value_columns.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*value_columns.at(j)))) {
          LOG_WARN("failed to check expr", K(j), K(ret));
        } else { /*do nothing*/
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_column_convert_exprs_->count(); ++i) {
      const ObIArray<ObRawExpr*>& convert_exprs = multi_column_convert_exprs_->at(i);
      for (int64_t j = 0; OB_SUCC(ret) && j < convert_exprs.count(); j++) {
        if (OB_ISNULL(convert_exprs.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(checker.check(*convert_exprs.at(j)))) {
          LOG_WARN("failed to check expr", K(j), K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret) && is_multi_conditions_insert_ && NULL != multi_insert_table_info_) {
      int64_t pre_idx = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_table_info_->count(); i++) {
        if (multi_insert_table_info_->at(i).when_conds_idx_ != pre_idx &&
            multi_insert_table_info_->at(i).when_conds_idx_ != -1) {
          pre_idx = multi_insert_table_info_->at(i).when_conds_idx_;
          for (int64_t j = 0; OB_SUCC(ret) && j < multi_insert_table_info_->at(i).when_conds_expr_.count(); j++) {
            if (OB_ISNULL(multi_insert_table_info_->at(i).when_conds_expr_.at(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get unexpected null", K(ret));
            } else if (OB_FAIL(checker.check(*multi_insert_table_info_->at(i).when_conds_expr_.at(j)))) {
              LOG_WARN("failed to check expr", K(i), K(j), K(ret));
            } else { /*do nothing*/
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLogInsertAll::extract_value_exprs()
{
  int ret = OB_SUCCESS;
  ObOptimizerContext* opt_ctx = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context()) ||
      OB_ISNULL(opt_ctx->get_session_info()) || OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_insert_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    const ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
    for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_multi_insert_col_conv_funcs().count(); ++i) {
      RawExprArray expr_without_col_conv;
      RawExprArray column_conv_functions = insert_stmt->get_multi_insert_col_conv_funcs().at(i);
      if (opt_ctx->get_session_info()->use_static_typing_engine()) {
        if (OB_FAIL(multi_value_exprs_.push_back(column_conv_functions))) {
          LOG_WARN("failed to push back column conv functions", K(ret));
        } else { /*do nothing*/
        }
      } else if (OB_FAIL(multi_value_exprs_.push_back(column_conv_functions))) {
        LOG_WARN("failed to push back column conv functions", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogInsertAll::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> new_value_exprs;
  ObSEArray<ObRawExpr*, 4> new_insert_cond_exprs;
  // Since multi-table insertion may involve the constant value insertion of many tables, such as
  // 500 tables, this time will cause the generation plan to be too slow,so unnecessary constant expr
  // needs to be removed, Avoid too deep and time-consuming recursion.
  for (int64_t i = 0; OB_SUCC(ret) && i < get_multi_value_exprs().count(); ++i) {
    if (OB_FAIL(remove_const_expr(get_multi_value_exprs().at(i), new_value_exprs))) {
      LOG_WARN("failed to remove const expr", K(ret));
    } else if (OB_FAIL(add_exprs_to_ctx(ctx, new_value_exprs))) {
      LOG_WARN("failed to add exprs to ctx", K(ret));
    } else {
      new_value_exprs.reset();
    }
  }
  if (OB_SUCC(ret) && is_multi_conditions_insert_ && OB_NOT_NULL(multi_insert_table_info_)) {
    int64_t pre_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_insert_table_info_->count(); ++i) {
      if (multi_insert_table_info_->at(i).when_conds_idx_ != pre_idx &&
          multi_insert_table_info_->at(i).when_conds_idx_ != -1) {
        if (OB_FAIL(remove_const_expr(multi_insert_table_info_->at(i).when_conds_expr_, new_insert_cond_exprs))) {
          LOG_WARN("failed to remove const expr", K(ret));
        } else if (OB_FAIL(add_exprs_to_ctx(ctx, new_insert_cond_exprs))) {
          LOG_WARN("failed to add exprs to ctx", K(ret));
        } else {
          pre_idx = multi_insert_table_info_->at(i).when_conds_idx_;
          new_insert_cond_exprs.reset();
        }
      }
    }
  }
  return ret;
}

int ObLogInsertAll::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObInsertStmt* stmt = static_cast<ObInsertStmt*>(get_stmt());
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      bool expr_is_required = false;
      bool is_true = false;
      const ColumnItem* col_item = stmt->get_column_item(i);
      if (OB_ISNULL(col_item) || OB_ISNULL(col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table column is null", K(ret));
      } else if (OB_FAIL(is_insert_table_id(col_item->table_id_, is_true))) {
        LOG_WARN("failed to is insert table id", K(ret));
      } else if (!is_true || !col_item->expr_->is_explicited_reference()) {
        // do nothing
      } else if (OB_FAIL(mark_expr_produced(col_item->expr_, branch_id_, id_, ctx, expr_is_required))) {
        LOG_WARN("failed to mark expr produced", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObLogDelUpd::allocate_expr_post(ctx))) {
        LOG_WARN("failed to allocate expr post", K(ret));
      }
    }
  }

  return ret;
}

int ObLogInsertAll::need_multi_table_dml(AllocExchContext& ctx, ObShardingInfo& sharding_info, bool& is_needed)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* tbl_schema = NULL;
  ObShardingInfo target_sharding_info;
  ObSchemaGetterGuard* schema_guard = NULL;
  ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
  is_needed = false;
  // When there is a row-before insert trigger, the original inserted data is allowed to be invalid
  //(which does not correspond to any partition), so the trigger check operation is brought to the forefront.
  if (OB_ISNULL(get_plan()) || OB_ISNULL(schema_guard = get_plan()->get_optimizer_context().get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(get_plan()), K(schema_guard));
  } else if (OB_FAIL(schema_guard->get_table_schema(get_index_tid(), tbl_schema))) {
    LOG_WARN("get table schema from schema guard failed", K(ret));
  } else if (OB_ISNULL(tbl_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(insert_stmt));
  } else if (OB_FAIL(generate_sharding_info(target_sharding_info))) {
    LOG_WARN("failed to generate sharding info", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (target_sharding_info.is_distributed()) {
    is_needed = true;
    ctx.plan_type_ = AllocExchContext::DistrStat::DISTRIBUTED;
    LOG_TRACE("set multi part insert plan to dist", K(ctx.plan_type_));
  } else {
    is_needed = true;
  }
  UNUSED(sharding_info);
  return ret;
}

int ObLogInsertAll::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(get_plan()), K(ret));
  } else if (OB_FAIL(ObLogDelUpd::allocate_exchange_post(ctx))) {
    LOG_WARN("failed to do allocate exchange post", K(ret));
  } else if (ctx->exchange_allocated_ || ctx->plan_type_ == AllocExchContext::DistrStat::DISTRIBUTED) {
    // The LOCAL type multi part insert plan does not add the table partition info corresponding to the insert table
    if (OB_FAIL(get_plan()->add_global_table_partition_info(&table_partition_info_))) {
      LOG_WARN("failed to add table partition info", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
    ObIArray<TableColumns>& table_columns = insert_stmt->get_all_table_columns();
    for (int64_t i = 1; OB_SUCC(ret) && i < table_columns.count(); ++i) {
      ObIArray<IndexDMLInfo>& index_infos = table_columns.at(i).index_dml_infos_;
      for (int64_t j = 0; OB_SUCC(ret) && j < index_infos.count(); ++j) {
        ObRawExpr* expr = NULL;
        if (OB_FAIL(gen_calc_part_id_expr(index_infos.at(j).loc_table_id_, index_infos.at(j).index_tid_, expr))) {
          LOG_WARN("fail to gen calc part id expr", K(ret));
        } else if (index_infos.at(j).calc_part_id_exprs_.push_back(expr)) {
          LOG_WARN("fail to push back calc part id expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogInsertAll::is_insert_table_id(uint64_t table_id, bool& is_true) const
{
  is_true = false;
  int ret = OB_SUCCESS;
  const ObInsertStmt* stmt = static_cast<const ObInsertStmt*>(get_stmt());
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else if (stmt->is_multi_insert_stmt()) {
    // The last table of multi table insert stmt is subquery
    for (int64_t i = 0; OB_SUCC(ret) && !is_true && i < stmt->get_table_items().count() - 1; ++i) {
      TableItem* table_item = stmt->get_table_items().at(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(table_item));
      } else if (table_id == table_item->table_id_) {
        is_true = true;
      } else {
        /*do nothing*/
      }
    }
  } else {
    is_true = (table_id == stmt->get_insert_table_id());
  }
  return ret;
}

int ObLogInsertAll::get_part_hint(const ObPartHint*& part_hint, const uint64_t table_id) const
{
  int ret = OB_SUCCESS;
  bool found = false;
  part_hint = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < part_hints_.count(); i++) {
    if (OB_ISNULL(part_hints_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(part_hints_.at(i)));
    } else if (table_id == part_hints_.at(i)->table_id_) {
      part_hint = part_hints_.at(i);
      found = true;
    }
  }
  return ret;
}

int ObLogInsertAll::inner_replace_generated_agg_expr(
    const ObIArray<std::pair<ObRawExpr*, ObRawExpr*>>& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(multi_column_convert_exprs_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < multi_column_convert_exprs_->count(); ++i) {
      if (OB_FAIL(
              replace_exprs_action(to_replace_exprs, const_cast<RawExprArray&>(multi_column_convert_exprs_->at(i))))) {
        LOG_WARN("failed to replace_expr_action", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogInsertAll::remove_const_expr(const ObIArray<ObRawExpr*>& old_exprs, ObIArray<ObRawExpr*>& new_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < old_exprs.count(); ++i) {
    if (OB_ISNULL(old_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null");
    } else if (old_exprs.at(i)->has_const_or_const_expr_flag()) {
      /*do nothing */
    } else if (OB_FAIL(new_exprs.push_back(old_exprs.at(i)))) {
      LOG_WARN("failed to push back old exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}
