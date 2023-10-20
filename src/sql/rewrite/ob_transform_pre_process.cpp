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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_pre_process.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "share/ob_unit_getter.h"
#include "share/schema/ob_column_schema.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/expr/ob_expr_arg_case.h"
#include "sql/engine/expr/ob_expr_xmlparse.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/config/ob_server_config.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/dml/ob_merge_resolver.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/rewrite/ob_expand_aggregate_utils.h"
#include "pl/ob_pl_stmt.h"
#include "pl/ob_pl_resolver.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "sql/resolver/dml/ob_insert_all_stmt.h"
#include "sql/engine/expr/ob_expr_align_date4cmp.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
using namespace common;
namespace sql
{
int ObTransformPreProcess::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(ObTransformUtils::right_join_to_left(stmt))) {
    LOG_WARN("failed to transform right join as left", K(ret));
  } else if (parent_stmts.empty() && lib::is_oracle_mode() &&
              OB_FAIL(formalize_limit_expr(*stmt))) {
    LOG_WARN("formalize stmt fialed", K(ret));
  } else {
    if (OB_SUCC(ret) && parent_stmts.empty()) {
      if (OB_FAIL(expand_correlated_cte(stmt, is_happened))) {
        LOG_WARN("failed to expand correlated cte", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("expand correlated cte", is_happened);
        LOG_TRACE("succeed to expand correlated cte", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_udt_columns(parent_stmts, stmt, is_happened))) {
        LOG_WARN("failed to transform for transform for cast multiset", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform for udt columns", is_happened);
        LOG_TRACE("succeed to transform for udt columns", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_cast_multiset_for_stmt(stmt, is_happened))) {
        LOG_WARN("failed to transform for transform for cast multiset", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform for cast multiset", is_happened);
        LOG_TRACE("succeed to transform for cast multiset", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_all_rowkey_columns_to_stmt(stmt, is_happened))) {
        LOG_WARN("faield to add all rowkey columns", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("add all rowkey columns:", is_happened);
        LOG_TRACE("succeed to add all rowkey columns", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(eliminate_having(stmt, is_happened))) {
        LOG_WARN("failed to elinimate having", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("eliminating having statement:", is_happened);
        LOG_TRACE("succeed to eliminating having statement", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transformer_rowid_expr(stmt, is_happened))) {
        LOG_WARN("failed to transform rowid expr", K(ret));
      } else if (OB_FAIL(add_rowid_constraint(*stmt))) {
        LOG_WARN("add rowid constraint failed", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform rowid expr:", is_happened);
        LOG_TRACE("succeed to transform rowid expr", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(replace_func_is_serving_tenant(stmt, is_happened))) {
        LOG_WARN("failed to replace function is_serving_tenant", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("replace is_serving_tenant function:", is_happened);
        LOG_TRACE("succeed to replace function", K(is_happened));
      }
    }
#ifdef OB_BUILD_LABEL_SECURITY
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_label_se_table(stmt, is_happened))) {
        LOG_WARN("failed to transform for label security table", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform for label security table:", is_happened);
        LOG_TRACE("succeed to transform for label security table", K(is_happened), K(ret));
      }
    }
#endif
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_rls_table(stmt, is_happened))) {
        LOG_WARN("failed to transform for rls table", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform for rls table", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_merge_into(stmt, is_happened))) {
        LOG_WARN("failed to transform for merge into", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform for merge into:", is_happened);
        LOG_TRACE("succeed to transform for merge into", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_temporary_table(stmt, is_happened))) {
        LOG_WARN("failed to transform for temporary table", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform for temporary table:", is_happened);
        LOG_TRACE("succeed to transform for temporary table", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_outerjoin_exprs(stmt, is_happened))) {
        LOG_WARN("failed to transform outer join exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform outer join exprs:", is_happened);
        LOG_TRACE("succeed to transform outer join exprs", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_batch_stmt(stmt, is_happened))) {
        LOG_WARN("failed to transform for batch update", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform for batch stmt:", is_happened);
        LOG_TRACE("succeed to transform for batch stmt", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_exprs(stmt, is_happened))) {
        LOG_WARN("transform exprs failed", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform exprs:", is_happened);
        LOG_TRACE("success to transform exprs", K(is_happened));
      }
    }
    /*transform_for_nested_aggregate、transformer_aggr_expr两个函数强依赖，必须保证两者改写顺序*/
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_nested_aggregate(stmt, is_happened))) {
        LOG_WARN("failed to transform for nested aggregate.", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform for nested aggregate:", is_happened);
        LOG_TRACE("succeed to transform for nested aggregate", K(is_happened), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(transformer_aggr_expr(stmt, is_happened))) {
          LOG_WARN("failed to transform aggr expr", K(ret));
        } else {
          trans_happened |= is_happened;
          OPT_TRACE("transform aggr expr:", is_happened);
          LOG_TRACE("succeed to transform aggr expr", K(is_happened), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_full_outer_join(stmt, is_happened))) {
        LOG_WARN("failed to transform full outer join", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform full outer join:", is_happened);
        LOG_TRACE("succeed to transform full outer join", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_hierarchical_query(stmt, is_happened))) {
        LOG_WARN("failed to transform for hierarchical query", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform hierarchical query:", is_happened);
        LOG_TRACE("succeed to transform hierarchical query", K(is_happened));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_rownum_as_limit_offset(parent_stmts, stmt, is_happened))) {
        LOG_WARN("failed to transform rownum as limit", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform rownum as limit:", is_happened);
        LOG_TRACE("succeed to transform rownum as limit", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_groupingsets_rollup_cube(stmt, is_happened))) {
        LOG_WARN("failed to transform for transform for grouping sets, rollup and cube.", K(ret));
      } else {
        trans_happened |= is_happened;
        OPT_TRACE("transform for grouping sets and multi rollup:", is_happened);
        LOG_TRACE("succeed to transform for grouping sets and multi rollup",K(is_happened), K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_rollup_exprs(stmt, is_happened))) {
        LOG_WARN("failed to transform rollup exprs", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform rollup exprs",  K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_last_insert_id(stmt, is_happened))) {
        LOG_WARN("failed to transform for last_insert_id.", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform for last_insert_id.",K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("transform pre process succ", K(*stmt));
     if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      //} else if (OB_FAIL(stmt->formalize_stmt_expr_reference())) {
      //  LOG_WARN("failed to formalize stmt reference", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                          const int64_t current_level,
                                          const ObDMLStmt &stmt,
                                          bool &need_trans)
{
  UNUSED(parent_stmts);
  UNUSED(current_level);
  UNUSED(stmt);
  need_trans = true;
  return OB_SUCCESS;
}

int ObTransformPreProcess::add_all_rowkey_columns_to_stmt(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(stmt), K(ctx_));
  } else {
    ObIArray<TableItem*> &tables = stmt->get_table_items();
    TableItem *table_item = NULL;
    const ObTableSchema *table_schema = NULL;
    ObSEArray<ColumnItem, 16> column_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      if (OB_ISNULL(table_item = tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null", K(ret), K(table_item));
      } else if (!table_item->is_basic_table()) {
        /* do nothing */
      } else if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(),
                                                                 table_item->ref_id_,
                                                                 table_schema,
                                                                 table_item->is_link_table()))) {
        LOG_WARN("table schema not found", K(table_schema));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid table schema", K(table_schema));
      } else if (OB_FAIL(add_all_rowkey_columns_to_stmt(*table_schema, *table_item,
                                                        *ctx_->expr_factory_,
                                                        *stmt,
                                                        column_items))) {
        LOG_WARN("add all rowkey exprs failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && !column_items.empty()) {
      ObIArray<ColumnItem> &orign_column_items = stmt->get_column_items();
      for (int i = 0; OB_SUCC(ret) && i < orign_column_items.count(); ++i) {
        if (OB_ISNULL(orign_column_items.at(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(orign_column_items.at(i).expr_));
        } else if (!orign_column_items.at(i).expr_->is_rowkey_column() &&
                   OB_FAIL(column_items.push_back(orign_column_items.at(i)))) {
          LOG_WARN("failed to push back column item", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(stmt->get_column_items().assign(column_items))) {
          LOG_WARN("failed to assign column items", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::add_all_rowkey_columns_to_stmt(const ObTableSchema &table_schema,
                                                          const TableItem &table_item,
                                                          ObRawExprFactory &expr_factory,
                                                          ObDMLStmt &stmt,
                                                          ObIArray<ColumnItem> &column_items)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = table_schema.get_rowkey_info();
  const ObColumnSchemaV2 *column_schema = NULL;
  uint64_t column_id = OB_INVALID_ID;
  ColumnItem *exists_col_item = NULL;
  ObColumnRefRawExpr *rowkey = NULL;
  for (int col_idx = 0; OB_SUCC(ret) && col_idx < rowkey_info.get_size(); ++col_idx) {
    if (OB_FAIL(rowkey_info.get_column_id(col_idx, column_id))) {
      LOG_WARN("Failed to get column id", K(ret));
    } else if (NULL != (exists_col_item = stmt.get_column_item_by_id(table_item.table_id_,
                                                                     column_id))) {
      if (OB_FAIL(column_items.push_back(*exists_col_item))) {
        LOG_WARN("failed to push back column item", K(ret));
      }
    } else if (OB_MOCK_LINK_TABLE_PK_COLUMN_ID == column_id && table_item.is_link_table()) {
      continue;
    } else if (OB_ISNULL(column_schema = (table_schema.get_column_schema(column_id)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get column schema", K(column_id), K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(expr_factory, *column_schema, rowkey))) {
      LOG_WARN("build column expr failed", K(ret));
    } else if (OB_ISNULL(rowkey)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to create raw expr for dummy output", K(ret));
    } else {
      ColumnItem column_item;
      rowkey->set_ref_id(table_item.table_id_, column_schema->get_column_id());
      rowkey->set_column_attr(table_item.get_table_name(), column_schema->get_column_name_str());
      rowkey->set_database_name(table_item.database_name_);
      if (!table_item.alias_name_.empty()) {
        rowkey->set_table_alias_name();
      }
      column_item.table_id_ = rowkey->get_table_id();
      column_item.column_id_ = rowkey->get_column_id();
      column_item.base_tid_ = table_item.ref_id_;
      column_item.base_cid_ = rowkey->get_column_id();
      column_item.column_name_ = rowkey->get_column_name();
      column_item.set_default_value(column_schema->get_cur_default_value());
      column_item.expr_ = rowkey;
      if (OB_FAIL(stmt.add_column_item(column_item))) {
        LOG_WARN("add column item to stmt failed", K(ret));
      } else if (OB_FAIL(column_items.push_back(column_item))) {
        LOG_WARN("failed to push back column item", K(ret));
      } else if (FALSE_IT(rowkey->clear_explicited_referece())) {
      } else if (OB_FAIL(rowkey->formalize(NULL))) {
        LOG_WARN("formalize rowkey failed", K(ret));
      } else if (OB_FAIL(rowkey->pull_relation_id())) {
        LOG_WARN("failed to pullup relation ids", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::formalize_limit_expr(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;

  ObRawExpr *limit_expr = stmt.get_limit_expr();
  ObRawExpr *offset_expr = stmt.get_offset_expr();
  ObRawExpr *percent_expr = stmt.get_limit_percent_expr();
  ObSEArray<ObRawExpr *, 4> params;
  bool transed_zero = false;
  ObConstRawExpr *one_expr = NULL;
  ObConstRawExpr *zero_expr = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) || OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())
      || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (limit_expr != NULL) {
    bool is_null_value = false;
    int64_t limit_value = 0;
    ObRawExpr *cmp_expr = NULL;
    if (OB_FAIL(ObTransformUtils::get_expr_int_value(limit_expr,
                                                  &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
                                                  ctx_->exec_ctx_,
                                                  ctx_->allocator_,
                                                  limit_value,
                                                  is_null_value))) {
      LOG_WARN("failed to get_expr_int_value", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                            ObIntType,
                                                            1,
                                                            one_expr))) {
      LOG_WARN("build expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                            ObIntType,
                                                            0,
                                                            zero_expr))) {
      LOG_WARN("build expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_, ctx_->session_info_, T_OP_GE,
                                                            cmp_expr, limit_expr, one_expr))) {
      LOG_WARN("create_double_op_expr_failed", K(ret));

    } else if (!is_null_value && limit_value >= 1) {
      if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, cmp_expr, true))) {
        LOG_WARN("add param failed", K(ret));
      }
    } else {
      stmt.set_limit_offset(zero_expr, NULL);
      transed_zero = true;
      if (OB_FAIL(params.push_back(cmp_expr))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  } else if (percent_expr != NULL) {
    bool is_null_value = false;
    double limit_value = 0;
    ObRawExpr *cmp_expr = NULL;
    ObConstRawExpr *double_zero_expr = NULL;
    if (OB_FAIL(ObTransformUtils::get_percentage_value(percent_expr,
                                                       &stmt,
                                                       &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
                                                       ctx_->exec_ctx_,
                                                       ctx_->allocator_,
                                                       limit_value,
                                                       is_null_value))) {
      LOG_WARN("failed to get_percentage_value", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_double_expr(*ctx_->expr_factory_,
                                                               ObDoubleType,
                                                               0,
                                                               double_zero_expr))) {
      LOG_WARN("build expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                            ObIntType,
                                                            0,
                                                            zero_expr))) {
      LOG_WARN("build expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_, ctx_->session_info_, T_OP_GT,
                                                             cmp_expr, percent_expr, double_zero_expr))) {
      LOG_WARN("create_double_op_expr_failed", K(ret));
    } else if (!is_null_value && limit_value > 0) {
      if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, cmp_expr, true))) {
        LOG_WARN("add param failed", K(ret));
      }
    } else {
      stmt.set_limit_percent_expr(NULL);
      stmt.set_limit_offset(zero_expr, NULL);
      transed_zero = true;
      if (OB_FAIL(params.push_back(cmp_expr))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (offset_expr != NULL) {
    bool is_null_value = false;
    int64_t offset_value = 0;
    ObRawExpr *cmp_expr = NULL;
    if (OB_FAIL(ObTransformUtils::get_expr_int_value(offset_expr,
                                                  &ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store(),
                                                  ctx_->exec_ctx_,
                                                  ctx_->allocator_,
                                                  offset_value,
                                                  is_null_value))) {
      LOG_WARN("failed to get_expr_int_value", K(ret));
    } else if (zero_expr == NULL && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                            ObIntType,
                                                            0,
                                                            zero_expr))) {
      LOG_WARN("build expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_, ctx_->session_info_, T_OP_GE,
                                                              cmp_expr, offset_expr, zero_expr))) {
      LOG_WARN("create_double_op_expr_failed", K(ret));
    } else if (offset_value >= 0) {
      if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, cmp_expr, true))) {
        LOG_WARN("add param failed", K(ret));
      }
    } else if (is_null_value) {
      stmt.set_limit_offset(zero_expr, NULL);
      transed_zero = true;
      ObRawExpr *new_cond = NULL;
      if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_, offset_expr,
                                                     true, new_cond))) {
        LOG_WARN("build expr failed", K(ret));
      } else if (OB_FAIL(params.push_back(new_cond))) {
        LOG_WARN("push back failed", K(ret));
      }
    } else {
      stmt.set_limit_offset(stmt.get_limit_expr(), zero_expr);
      transed_zero = true;
      if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, cmp_expr, false))) {
        LOG_WARN("add param failed", K(ret));
      }
    }
  }


  if (OB_SUCC(ret) && params.count() > 0 && transed_zero) {
    ObRawExpr* and_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_, params, and_expr))) {
      LOG_WARN("build and expr failed", K(ret));
    } else  if (OB_FAIL(and_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("formalize failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, and_expr, false))) {
      LOG_WARN("add param failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    ObSEArray<ObSelectStmt *, 2> child_stmts;
    if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
      LOG_WARN("get child_stmts failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(formalize_limit_expr(*child_stmts.at(i))))) {
        LOG_WARN("formalize limit expr failed", K(ret));
      }
    }
  }
  return ret;
}

/* @brief,ObTransformPreProcess::release_single_item_groupingsets
 * if grouping sets has one item only, then release grouping sets.
 * eg:
 * group by grouping sets(c1) ---> group by c1
 * group by grouping sets(rollup(c1, c2)) ---> group by rollup(c1, c2)
 * group by grouping sets(cube(c1, c2)) ---> group by cube(c1, c2)
 */
int ObTransformPreProcess::remove_single_item_groupingsets(ObSelectStmt &stmt,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObIArray<ObGroupingSetsItem> &grouping_sets_items = stmt.get_grouping_sets_items();
  typedef ObSEArray<ObGroupingSetsItem, 4> GroupingSetsArray;
  SMART_VAR(GroupingSetsArray, tmp_grouping_sets_items) {
    for (int64_t i = 0; OB_SUCC(ret) && i < grouping_sets_items.count(); ++i) {
      bool is_released = true;
      int64_t grouping_sets_expr_cnt = grouping_sets_items.at(i).grouping_sets_exprs_.count();
      int64_t rollup_cnt = grouping_sets_items.at(i).rollup_items_.count();
      int64_t cube_cnt = grouping_sets_items.at(i).cube_items_.count();
      if (grouping_sets_expr_cnt == 1 && rollup_cnt == 0 && cube_cnt == 0) {
        if (OB_FAIL(append(stmt.get_group_exprs(),
                           grouping_sets_items.at(i).grouping_sets_exprs_.at(0).groupby_exprs_))) {
          LOG_WARN("failed to append group exprs", K(ret));
        }
      } else if (grouping_sets_expr_cnt == 0 && rollup_cnt == 1 && cube_cnt == 0) {
        if (OB_FAIL(stmt.add_rollup_item(grouping_sets_items.at(i).rollup_items_.at(0)))) {
          LOG_WARN("failed to add multi rollup item", K(ret));
        }
      } else if (grouping_sets_expr_cnt == 0 && rollup_cnt == 0 && cube_cnt == 1) {
        if (OB_FAIL(stmt.add_cube_item(grouping_sets_items.at(i).cube_items_.at(0)))) {
          LOG_WARN("failed to add multi cube item", K(ret));
        }
      } else {
        is_released = false;
      }
      if (OB_SUCC(ret) && !is_released &&
          OB_FAIL(tmp_grouping_sets_items.push_back(grouping_sets_items.at(i)))) {
        LOG_WARN("failed to push back item", K(ret));
      }
    }
    if (OB_SUCC(ret) && grouping_sets_items.count() != tmp_grouping_sets_items.count()) {
      grouping_sets_items.reset();
      bool is_happened = false;
      if (OB_ISNULL(ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), KP(ctx_));
      } else if (!stmt.has_group_by() && OB_FAIL(ObTransformUtils::set_limit_expr(&stmt, ctx_))) {
        LOG_WARN("add limit expr failed", K(ret));
      } else if (!stmt.has_group_by() && OB_FAIL(eliminate_having(&stmt, is_happened))) {
        LOG_WARN("failed to eliminate having", K(ret));
      } else if (OB_FAIL(grouping_sets_items.assign(tmp_grouping_sets_items))) {
        LOG_WARN("failed to assign stmt's grouping sets items", K(ret));
      } else {
        trans_happened = true;
        LOG_TRACE("eliminate having", K(is_happened));
      }
    }
  }
  return ret;
}

/* @brief,ObTransformPreProcess::try_convert_rollup
 * if stmt doesn't have rollup_exprs, then convert rollup_item which has the most count of exprs to
 * rollup_exprs
 * eg:
 * rollup_exprs = [], rollup_items = [[c1, c2], [(c1, c2), c1, c3, c4], [c1, c2, c3]]
 * --->
 * rollup_exprs = [c1, c2, c3], rollup_items = [[c1, c2], [(c1, c2), c1, c3, c4]]
 */
/*
 * group_id:
 *   (1). all deal in pre_process
 * grouping/grouping_id:
 *   (2). if single rollup and rollup_exprs are one item like 'group by rollup(c1, c2)', deal functions in engine.
 *   (3). else deal functions in pre_process.
 */
int ObTransformPreProcess::try_convert_rollup(ObDMLStmt *&stmt, ObSelectStmt *select_stmt)
{
  int ret = OB_SUCCESS;
  bool can_conv_rollup = false;
  bool has_group_id_func = false;
  bool has_grouping_func = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), KP(stmt), KP(select_stmt));
  } else if (select_stmt->get_rollup_exprs().empty() && !select_stmt->get_rollup_items().empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && !has_group_id_func && i < select_stmt->get_aggr_item_size(); ++i) {
      ObRawExpr *expr = select_stmt->get_aggr_items().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected NULL ptr", K(ret));
      } else if (T_FUN_GROUP_ID == expr->get_expr_type()) {
        has_group_id_func = true;
      } else if (T_FUN_GROUPING_ID == expr->get_expr_type() ||
                 T_FUN_GROUPING == expr->get_expr_type()) {
        has_grouping_func = true;
      }
    }
    if (OB_SUCC(ret) && !has_group_id_func) {
      if (!has_grouping_func) {
        can_conv_rollup = true;
      } else if (select_stmt->get_rollup_items_size() == 1 &&
                 select_stmt->get_grouping_sets_items_size() == 0 &&
                 select_stmt->get_cube_items_size() == 0) {
        can_conv_rollup = true;
      }
    }
    if (OB_SUCC(ret) && can_conv_rollup) {
      int64_t idx = -1;
      int64_t cnt = 0;
      for (int64_t i = 0; i < select_stmt->get_rollup_items_size(); ++i) {
        bool can_conv_this_rollup = true;
        const ObIArray<ObGroupbyExpr> &rollup_list_exprs =
                                          select_stmt->get_rollup_items().at(i).rollup_list_exprs_;
        for (int64_t j = 0; can_conv_this_rollup && j < rollup_list_exprs.count(); ++j) {
          if (rollup_list_exprs.at(j).groupby_exprs_.count() != 1) {
            can_conv_this_rollup = false;
          }
        }
        if (can_conv_this_rollup && rollup_list_exprs.count() > cnt) {
          cnt = rollup_list_exprs.count();
          idx = i;
        }
      }
      if (idx >= 0) {
        const ObIArray<ObGroupbyExpr> &rollup_list_exprs =
                                        select_stmt->get_rollup_items().at(idx).rollup_list_exprs_;
        for (int64_t i = 0; OB_SUCC(ret) && i < rollup_list_exprs.count(); ++i) {
          if (OB_FAIL(select_stmt->get_rollup_exprs().push_back(
                                                  rollup_list_exprs.at(i).groupby_exprs_.at(0)))) {
            LOG_WARN("failed to push back exprs", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          select_stmt->get_rollup_items().remove(idx);
          stmt = select_stmt;
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::create_cte_for_groupby_items(ObSelectStmt &stmt)
{
  int ret = OB_SUCCESS;
  bool can_pre_aggregate = false;
  bool is_correlated = false;
  ObSelectStmt *view_stmt = NULL;
  if (OB_FAIL(check_pre_aggregate(stmt, can_pre_aggregate))) {
    LOG_WARN("fialed to check pre aggregate", K(ret));
  } else if (!can_pre_aggregate) {
    if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, &stmt, view_stmt))) {
      LOG_WARN("failed to create simple view", K(ret), K(stmt));
    }
  } else if (OB_FAIL(ObTransformUtils::create_view_with_pre_aggregate(&stmt, view_stmt, ctx_))) {
    LOG_WARN("failed to create view with pushdown groupby", K(ret), K(stmt));
  } else { /* do nothing */ }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("view stmt is null", K(ret), K(view_stmt));
  } else if (OB_FAIL(is_subquery_correlated(view_stmt, is_correlated))) {
    LOG_WARN("failed to check subquery correlated", K(ret));
  } else if (is_correlated) {
    ret = OB_NOT_SUPPORTED; // TODO GROUPING_SETS
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "correlated temp table now");
    LOG_WARN("correlated temp table is not support now!", K(ret));
  } else if (OB_FAIL(add_generated_table_as_temp_table(ctx_, &stmt))) {
    LOG_WARN("failed to add generated table as temp table", K(ret));
  } else { /* do nothing */ }
  return ret;
}

/*
 * select c1, sum(c2) from t1 group by grouping sets(c1, c2) having sum(c2) > 2 order by c1;
 * --->(pull cte)
 * with cte as : select c1, c2, sum(c2) from t1 group by c1, c2
 * select cte.c1, cte.c3 from cte group by grouping sets(cte.c1, cte.c2)
 *   having cte.c3 > 2 order by cte.c1;
 * --->(create_view_with_groupby_items)
 * 1.
 * with view as : select cte.c1, cte.c3 from cte group by grouping sets(cte.c1, cte.c2)
 *                  having cte.c3 > 2;
 * select v.c1, v.c2 from v order by v.c1;
 * 2.
 * view select cte.c1, cte.c3 from cte group by grouping sets(cte.c1, cte.c2)
 *                  having cte.c3 > 2
 */
int ObTransformPreProcess::expand_stmt_groupby_items(ObSelectStmt &stmt)
{
  int ret = OB_SUCCESS;
  TableItem *view_table_item = NULL;
  if (OB_FAIL(ObTransformUtils::create_view_with_groupby_items(&stmt, view_table_item, ctx_))) {
    LOG_WARN("failed to create simple view", K(ret));
  } else if (OB_ISNULL(view_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create view with groupby", K(ret), K(view_table_item));
  } else if (OB_FAIL(create_set_view_stmt(&stmt, view_table_item))) {
    LOG_WARN("failed to create grouping sets view.", K(ret));
  } else { /* do nothing */ }
  return ret;
}

/*@brief,ObTransformPreProcess::transform_groupingsets_rollup_cube
 *
 * as following steps to transform grouping sets stmt to set stmt:
 * 1. create cte for "grouping sets/rollup/cube"
 *    (1). Creating spj stmt from origin stmt with considering whether do pre_aggregation;
 *    (2). According to the spj stmt separated in the previous step, creating temp table and add
 *         it to origin stmt
 * 2. According to "grouping sets/rollup/cube" and stmt created in the previous
 *    step,creating set stmt
 * 3. Merging set stmt created in the previous step and origin stmt into transform stmt
 *
 * tranform stmt with
 * grouping sets or multi rollup to equal set stmt.
 * for grouping sets stmt:
 *  1.select c1,c2 from t1 group by grouping sets(c1,c2);
 * <==>
 *  select c1, NULL from t1 group by c1
 *union all
 *  select NULL, c2 from t1 group by c1;
 *
 *  2.select c1,c2,c3,c4 from t1 group by grouping sets(c1,c2), grouping sets(c3,c4);
 *<==>
 *  select c1,NULL,c3,NULL from t1 group by c1,c3
 * union all
 *  select c1,NULL,NULL,c4 from t1 group by c1,c4
 * union all
 *  select NULL,c2,c3,NULL from t1 group by c2,c3
 * union all
 *  select NULL,c2,NULL,c4 from t1 group by c2,c4;
 *
 *  as above, {c1,c2} * {c3,c4}  ==> Cartesian product
 *
 * for multi rollup stmt:
 * 1.select c1,c2,c3,sum(c3) from t1 group by rollup((c1,c2),c3);
 * <==>
 *  select c1,c2,c3,sum(c3) from t1 group by c1,c2,c3
 * union all
 *  select c1,c2,NULL,sum(c3) from t1 group by c1,c2
 * union all
 *  select NULL,NULL,NULL,sum(c3) from t1;
 *
 * 2.select c1,c2,c3,sum(c3) from t1 group by rollup (c1,c2),rollup(c3);
 * <==>
 *  select c1,c2,c3,sum(c3) from t1 group by c1,c2,c3
 * union all
 *  select c1,c2,NULL,sum(c3) from t1 group by c1,c2
 * union all
 *  select c1,NULL,c3,sum(c3) from t1 group by c1,c3
 * union all
 *  select c1,NULL,NULL,sum(c3) from t1 group by c1
 * union all
 *  select NULL,NULL,c3,sum(c3) from t1 group by c3
 * union all
 *  select NULL,NULL,NULL,sum(c3) from t1;
 *
 *  as above, {(c1,c2),(c1),(NULL)} * {(c3),(NULL)}  ==> Cartesian product
 */
int ObTransformPreProcess::transform_groupingsets_rollup_cube(ObDMLStmt *&stmt,
                                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null.", K(ret));
  } else if (!stmt->is_select_stmt()) {
    /* do nothing.*/
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
  } else if (OB_FAIL(remove_single_item_groupingsets(*select_stmt, trans_happened))) {
    LOG_WARN("failed to transform single item grouping sets", K(ret));
  } else if (OB_FAIL(try_convert_rollup(stmt, select_stmt))) {
    LOG_WARN("failed to convert multi rollup", K(ret));
  } else if (select_stmt->get_grouping_sets_items().count() == 0 &&
             select_stmt->get_rollup_items().count() == 0 &&
             select_stmt->get_cube_items().count() == 0) {
    // deal select item like 'c1 - 1'
  } else if (OB_FAIL(ObTransformUtils::replace_stmt_expr_with_groupby_exprs(select_stmt, ctx_))) {
    LOG_WARN("replace stmt expr with groupby exprs failed", K(ret));
  } else if (OB_FAIL(create_cte_for_groupby_items(*select_stmt))) {
    LOG_WARN("failed to create spj view.", K(ret));
  } else if (OB_FAIL(expand_stmt_groupby_items(*select_stmt))) {
    LOG_WARN("failed to expand grouping_sets_items/rollup_items/cube_items", K(ret));
  } else {
    trans_happened = true;
  }
  if (OB_SUCC(ret) && trans_happened) {
    stmt = select_stmt;
    LOG_TRACE("succeed to transform transform for grouping sets, rollup or cube", K(*stmt));
  }
  return ret;
}

int ObTransformPreProcess::is_subquery_correlated(const ObSelectStmt *stmt,
                                                  bool &is_correlated)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<uint64_t> param_set;
  if (OB_FAIL(param_set.create(128, "PreProcess", "HashNode"))) {
    LOG_WARN("failed to create param set", K(ret));
  } else if (OB_FAIL(is_subquery_correlated(stmt, param_set, is_correlated))) {
    LOG_WARN("failed to check is subquery correlated", K(ret));
  }

  if (param_set.created()) {
    int tmp_ret = param_set.destroy();
    if (OB_SUCC(ret) && OB_FAIL(tmp_ret)) {
      LOG_WARN("failed to destory param set", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::is_subquery_correlated(const ObSelectStmt *stmt,
                                                  hash::ObHashSet<uint64_t> &param_set,
                                                  bool &is_correlated)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> relation_exprs;
  ObArray<ObSelectStmt *> child_stmts;
  if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(has_new_exec_param(relation_exprs.at(i), param_set, is_correlated))) {
      LOG_WARN("failed to check has new exec param", K(ret));
    }
  }
  if (OB_SUCC(ret) && !is_correlated && OB_FAIL(add_exec_params(*stmt, param_set))) {
    LOG_WARN("failed to add exec params", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < child_stmts.count(); ++i) {
    if (OB_FAIL(SMART_CALL(is_subquery_correlated(child_stmts.at(i),
                                                  param_set,
                                                  is_correlated)))) {
      LOG_WARN("failed to check is subquery correlated", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::add_exec_params(const ObSelectStmt &stmt,
                                           hash::ObHashSet<uint64_t> &param_set)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_subquery_expr_size(); ++i) {
    ObQueryRefRawExpr *query_ref = stmt.get_subquery_exprs().at(i);
    if (OB_ISNULL(query_ref)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("query_ref is null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < query_ref->get_exec_params().count(); ++j) {
      ObExecParamRawExpr *exec_param = query_ref->get_exec_param(j);
      uint64_t key = reinterpret_cast<uint64_t>(exec_param);
      if (OB_FAIL(param_set.set_refactored(key))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to add expr into set", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTransformPreProcess::has_new_exec_param(const ObRawExpr *expr,
                                              const hash::ObHashSet<uint64_t> &param_set,
                                              bool &has_new)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_exec_param_expr()) {
    uint64_t key = reinterpret_cast<uint64_t>(expr);
    int tmp_ret = param_set.exist_refactored(key);
    if (OB_HASH_NOT_EXIST == tmp_ret) {
      has_new = true;
    } else if (OB_UNLIKELY(OB_HASH_EXIST != tmp_ret)) {
      ret = tmp_ret;
      LOG_WARN("failed to check hash set", K(ret), K(ret));
    }
  } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
    for (int64_t i = 0; OB_SUCC(ret) && !has_new && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(has_new_exec_param(expr->get_param_expr(i),
                                                param_set,
                                                has_new)))) {
        LOG_WARN("failed to check has new exec param", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::add_generated_table_as_temp_table(ObTransformerCtx *ctx,
                                                             ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(stmt), K(ret));
  } else if (OB_UNLIKELY(1 != stmt->get_table_items().count()) ||
             OB_ISNULL(table_item = stmt->get_table_item(0)) ||
             OB_UNLIKELY(!table_item->is_generated_table()) ||
             OB_ISNULL(table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(stmt->get_table_items().count()), K(table_item), K(ret));
  } else if (OB_FALSE_IT(table_item->alias_name_ = table_item->table_name_)) {
  } else if (OB_FAIL(stmt->generate_view_name(*ctx->allocator_, table_item->table_name_, true))) {
    LOG_WARN("failed to generate view name", K(ret));
  } else {
    table_item->type_ = TableItem::TEMP_TABLE;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_items().count(); i++) {
      if (OB_ISNULL(stmt->get_column_item(i)) || OB_ISNULL(stmt->get_column_item(i)->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        stmt->get_column_item(i)->expr_->set_table_name(table_item->alias_name_);
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::calc_grouping_in_grouping_sets(const ObIArray<ObRawExpr*> &groupby_exprs,
                                                          const ObIArray<ObRawExpr*> &rollup_exprs,
                                                          ObAggFunRawExpr *expr,
                                                          ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  ObConstRawExpr *one_expr = NULL;
  ObSysFunRawExpr *cast_expr = NULL;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_) ||
      expr->get_expr_type() != T_FUN_GROUPING) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr or wrong type", K(ret), K(expr), K(ctx_));
  } else if (OB_UNLIKELY(expr->get_param_count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(*expr));
  } else if (ObOptimizerUtil::find_item(groupby_exprs, expr->get_param_expr(0)) ||
             ObOptimizerUtil::find_item(rollup_exprs, expr->get_param_expr(0))) {
    /*do nothing*/
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                          ObIntType,
                                                          1,
                                                          one_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_,
                                                      one_expr,
                                                      expr->get_result_type(),
                                                      cast_expr,
                                                      ctx_->session_info_))) {
    LOG_WARN("create cast expr failed", K(ret));
  } else if (OB_FAIL(cast_expr->add_flag(IS_INNER_ADDED_EXPR))) {
    LOG_WARN("failed to add flag", K(ret));
  } else {
    new_expr = cast_expr;
  }
  return ret;
}

/*grouping sets is tranformed into union all, so  the grouping_id function must finish in transformer. 
 *grouping_id function in stmt without rollup is done here. 
 *select grouping_id(c1) from t1 group by grouping sets(c1,c2) will be transformed to 
 *select {new_value1} from t1 group by c1 UNION ALL select {new_value2} from t1 group by c2.
 * new value is a const value we have calculated in the following function.
 * here new_value1 is 0, and new_value2 is 1. So the stmt is:
 * select 0 from t1 group by c1 UNION ALL select 1 from t1 group by c2;
 */
int ObTransformPreProcess::calc_grouping_id_in_grouping_sets(
                                                          const ObIArray<ObRawExpr*> &groupby_exprs,
                                                          const ObIArray<ObRawExpr*> &rollup_exprs,
                                                          ObAggFunRawExpr *expr,
                                                          ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  uint64_t new_value = 0;
  ObConstRawExpr *int_expr = NULL;
  ObSysFunRawExpr *cast_expr = NULL;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_) ||
      expr->get_expr_type() != T_FUN_GROUPING_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(ret), K(expr), K(ctx_));
  } else if (OB_UNLIKELY(expr->get_param_count() == 0)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid number of argument", K(ret));
  } else if (rollup_exprs.count() != 0) {
    // do nothing. for stmt that has rollup, the grouping_id is done in executor.
  } else {
    for (int64_t expr_idx = 0; expr_idx < expr->get_param_count(); expr_idx++) {
      if (ObOptimizerUtil::find_item(groupby_exprs, expr->get_param_expr(expr_idx))) {
        new_value = new_value << 1;
      } else {
        new_value = new_value << 1;
        new_value++;
      }
    }
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                     ObIntType,
                                                     new_value,
                                                     int_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_,
                                                        int_expr,
                                                        expr->get_result_type(),
                                                        cast_expr,
                                                        ctx_->session_info_))) {
      LOG_WARN("create cast expr failed", K(ret));
    } else if (OB_FAIL(cast_expr->add_flag(IS_INNER_ADDED_EXPR))) {
      LOG_WARN("failed to add flag", K(ret));
    } else {
      new_expr = cast_expr;
    }
  }
  return ret;
}

/* since OB's grouping sets will be expanded to UNION ALL. We have to calculate the group_id() here
 * rather than the executor.
 * select group_id() from t1 group by grouping sets (c1,c2); will first be transformed to 
 * select group_id() from t1 group by c1 UNION ALL select group_id() from t1 group by c2;
 * and than be transformed to 
 * select {duplicated_groupby_num1} from t1 group by c1 UNION ALL select {duplicated_groupby_num2} from t1 group by c2;
 * duplicated_groupby_num is a const value that we have calculated in the following function
 * 
 * @brief
 *  @param groupby_exprs_list: store all previous sub stmt's groupby exprs.
 *  @param origin_groupby_num: groupby_exprs'number in origin stmt.
 * 
 * we traversal the groupby_exprs_list from 0 to cur_index to find out how many sub stmts have same groupby_exprs will
 * current sub stmt. 
 */
int ObTransformPreProcess::calc_group_id_in_grouping_sets(
                                                  const ObIArray<ObRawExpr*> &groupby_exprs,
                                                  const ObIArray<ObRawExpr*> &rollup_exprs,
                                                  const ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                                  const int64_t cur_index,
                                                  const int64_t origin_groupby_num,
                                                  ObAggFunRawExpr *expr,
                                                  ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  int64_t duplicated_groupby_num = 0;
  ObConstRawExpr *int_expr = NULL;
  ObSysFunRawExpr *cast_expr = NULL;
  const ObIArray<ObRawExpr*> &now_groupby_exprs = groupby_exprs_list.at(cur_index).groupby_exprs_;
  if (OB_ISNULL(expr) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_) ||
      expr->get_expr_type() != T_FUN_GROUP_ID) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(ret), K(expr), K(ctx_));
  } else if (OB_UNLIKELY(expr->get_param_count() != 0)) {
    ret = OB_INVALID_ARGUMENT_NUM;
    LOG_WARN("invalid number of argument", K(ret));
  } else if (rollup_exprs.count() != 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "use group_id coexisting grouping sets, rollup or cube");
    LOG_WARN("group_id doesn't support coexisting grouping sets and rollup", K(ret));
  } else {
    for (int64_t i = 0; i < cur_index; ++i) {
      bool match = true;
      const ObIArray<ObRawExpr*> &origin_groupby_exprs = groupby_exprs_list.at(i).groupby_exprs_;
      for(int64_t j = origin_groupby_num; match && j < now_groupby_exprs.count(); ++j) {
        if (!ObOptimizerUtil::find_item(origin_groupby_exprs, now_groupby_exprs.at(j))) {
          match = false;
        }
      }
      for (int64_t j = 0; match && j< origin_groupby_exprs.count(); ++j) {
        if (!ObOptimizerUtil::find_item(now_groupby_exprs, origin_groupby_exprs.at(j))) {
          match = false;
        }
      }
      if (match) {
        //if match inc the duplciated count.
        duplicated_groupby_num++;
      }
    }
    // the group id is duplicated_groupby_num;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                     ObIntType,
                                                     duplicated_groupby_num,
                                                     int_expr))) {
      LOG_WARN("failed to build const int expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_,
                                                        int_expr,
                                                        expr->get_result_type(),
                                                        cast_expr,
                                                        ctx_->session_info_))) {
      LOG_WARN("create cast expr failed", K(ret));
    } else if (OB_FAIL(cast_expr->add_flag(IS_INNER_ADDED_EXPR))) {
      LOG_WARN("failed to add flag", K(ret));
    } else {
      new_expr = cast_expr;
    }
  }
  return ret;
}

int ObTransformPreProcess::calc_group_type_aggr_func(const ObIArray<ObRawExpr*> &groupby_exprs,
                                                     const ObIArray<ObRawExpr*> &rollup_exprs,
                                                     const ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                                     const int64_t cur_index,
                                                     const int64_t origin_groupby_num,
                                                     ObAggFunRawExpr *expr,
                                                     ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. unexpected NULL", K(ret));
  } else if (T_FUN_GROUPING == expr->get_expr_type() &&
             OB_FAIL(calc_grouping_in_grouping_sets(groupby_exprs, rollup_exprs, expr, new_expr))) {
    LOG_WARN("failed to calculate grouping in grouping sets", K(ret), K(*expr));
  } else if (T_FUN_GROUPING_ID == expr->get_expr_type() &&
             OB_FAIL(calc_grouping_id_in_grouping_sets(groupby_exprs, rollup_exprs, expr,
                                                       new_expr))) {
    LOG_WARN("failed to calculate grouping_id in grouping sets", K(ret), K(*expr));
  } else if (T_FUN_GROUP_ID == expr->get_expr_type() &&
             OB_FAIL(calc_group_id_in_grouping_sets(groupby_exprs, rollup_exprs, groupby_exprs_list,
                                                    cur_index, origin_groupby_num, expr,
                                                    new_expr))) {
    LOG_WARN("failed to calculate grouping in grouping sets", K(ret), K(*expr));
  }
  return ret;
}

/*@brief, ObTransformPreProcess::convert_select_having_in_groupby_stmt, according to oracle action:
 * if the expr not in group by expr except in aggr, the expr will replaced with NULL; eg:
 *  select c1, c2, max(c1), max(c2) from t1 group by grouping sets(c1,c2) having c1 > 1 or c2 > 1 or sum(c1) > 2 or sum(c2) > 2;
 * <==>
 *  select c1, NULL, max(c1), max(c1) from t1 group by c1 having c1 > 1 or NULL > 1 or sum(c1) > 2 or sum(c2) > 2
 * union all
 *  select NULL, c2, max(c1), max(c1) from t1 group by c2 having NULL > 1 or c2 > 1 or sum(c1) > 2 or sum(c2) > 2;
 *
 *  select nvl(c1,1),c3 from t1 group by grouping sets(nvl(c1,1),c3);
 * <==>
 *  select nvl(c1,1), NULL from t1 group by nvl(c1,1)
 * union all
 *  select NULL, c3 from t1 group by c3;
 *
 * select nvl(c1,1) + c3 from t1 group by grouping sets(nvl(c1,1),c3);
 *
 */
int ObTransformPreProcess::convert_select_having_in_groupby_stmt(
                                                  const ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                                  const ObIArray<ObGroupbyExpr> &rollup_exprs_list,
                                                  const ObIArray<ObRawExpr*> &groupby_exprs,
                                                  const int64_t cur_index,
                                                  const ObSelectStmt &origin_stmt,
                                                  ObSelectStmt &substmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  ObSEArray<ObAggFunRawExpr*, 4> tmp_agg_exprs;
  ObSEArray<ObRawExpr*, 4> cur_groupby_exprs;
  ObSEArray<ObRawExpr*, 4> other_groupby_exprs;
  // 1. build cur_groupby_exprs and other_groupby_exprs
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. get unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(append_array_no_dup(cur_groupby_exprs, substmt.get_group_exprs()))) {
    LOG_WARN("failed to append groupby exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(cur_groupby_exprs, substmt.get_rollup_exprs()))) {
    LOG_WARN("failed to append groupby exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < groupby_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = groupby_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed. get unexpected NULL", K(ret));
    } else if (ObOptimizerUtil::find_item(cur_groupby_exprs, expr)) {
      /* do nothing */
    } else if (OB_FAIL(add_var_to_array_no_dup(other_groupby_exprs, expr))) {
      LOG_WARN("failed to add var into other groupby exprs", K(ret));
    }
  }
  // 2. try convert other_groupby_exprs to null and convert group_type _aggr
  if (OB_SUCC(ret)) {
    ObRawExpr *null_expr = NULL;
    ObSysFunRawExpr *cast_expr = NULL;
    ObRawExpr *new_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < other_groupby_exprs.count(); ++i) {
      ObRawExpr *expr = other_groupby_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed. get unexpected null", K(ret), K(expr));
      } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, null_expr))) {
        LOG_WARN("failed build null exprs.", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_,
                                                          null_expr,
                                                          expr->get_result_type(),
                                                          cast_expr,
                                                          ctx_->session_info_))) {
        LOG_WARN("create cast expr failed", K(ret));
      } else if (OB_FAIL(cast_expr->add_flag(IS_INNER_ADDED_EXPR))) {
        LOG_WARN("failed to add flag", K(ret));
      } else if (OB_FAIL(old_exprs.push_back(expr))) {
        LOG_WARN("failed to push back into old_exprs", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(cast_expr))) {
        LOG_WARN("failed to push back into new_exprs", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < substmt.get_aggr_item_size(); ++i) {
      ObAggFunRawExpr *agg_expr = substmt.get_aggr_item(i);
      if (OB_ISNULL(agg_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed. get unexpected null", K(ret), K(agg_expr));
      } else if (OB_FAIL(calc_group_type_aggr_func(substmt.get_group_exprs(),
                                                  substmt.get_rollup_exprs(),
                                                  groupby_exprs_list,
                                                  cur_index,
                                                  origin_stmt.get_group_expr_size(),
                                                  agg_expr,
                                                  new_expr))) {
        LOG_WARN("failed to replace group type aggr func", K(ret), K(*agg_expr));
      } else if (OB_ISNULL(new_expr)) {
        if (OB_FAIL(tmp_agg_exprs.push_back(agg_expr))) {
          LOG_WARN("failed to remove item", K(ret), K(*agg_expr));
        }
      } else if (OB_FAIL(old_exprs.push_back(agg_expr))) {
        LOG_WARN("failed to push back into old_exprs", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(new_expr))) {
        LOG_WARN("failed to push back into new_exprs", K(ret));
      }
    }
  }
  // 3. do the replacement
  if (OB_SUCC(ret)) {
    ObStmtExprReplacer replacer;
    replacer.remove_all();
    replacer.set_recursive(false);
    replacer.add_scope(SCOPE_SELECT);
    replacer.add_scope(SCOPE_HAVING);
    ObSEArray<ObRawExpr*, 4> skip_exprs;
    if (OB_FAIL(substmt.get_aggr_items().assign(tmp_agg_exprs))) {
      LOG_WARN("failed to assign array", K(ret));
    } else if (OB_FAIL(append(skip_exprs, cur_groupby_exprs))) {
      LOG_WARN("failed to append into skip_exprs", K(ret));
    } else if (OB_FAIL(append(skip_exprs, substmt.get_aggr_items()))) {
      LOG_WARN("failed to append into skip_exprs", K(ret));
    } else if (OB_FAIL(replacer.add_replace_exprs(old_exprs, new_exprs, &skip_exprs))) {
      LOG_WARN("failed to add replace exprs", K(ret));
    } else if (OB_FAIL(substmt.iterate_stmt_expr(replacer))) {
      LOG_WARN("failed to iterate stmt expr", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::create_groupby_substmt(const ObIArray<ObRawExpr*> &origin_groupby_exprs,
                                                  const ObIArray<ObRawExpr*> &origin_rollup_exprs,
                                                  ObSelectStmt &origin_stmt,
                                                  ObSelectStmt *&sub_stmt,
                                                  ObIArray<ObRawExpr*> &substmt_exprs)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *stmt_copy = NULL;
  ObSEArray<ObRawExpr*, 4> origin_exprs;
  ObStmtExprGetter origin_visitor;
  origin_visitor.remove_all();
  origin_visitor.add_scope(SCOPE_GROUPBY);
  origin_visitor.set_recursive(false);
  ObStmtExprGetter substmt_visitor;
  substmt_visitor.remove_all();
  substmt_visitor.add_scope(SCOPE_GROUPBY);
  substmt_visitor.set_recursive(false);
  int64_t idx = -1;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), KP(ctx_));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                      *ctx_->expr_factory_,
                                                      &origin_stmt,
                                                      stmt_copy))) {
    LOG_WARN("failed to deep copy from stmt.", K(ret));
  } else if (OB_ISNULL(sub_stmt = static_cast<ObSelectStmt*>(stmt_copy))) {
    LOG_WARN("failed. get unexpected NULL", K(ret));
  } else if (OB_FAIL(origin_stmt.get_relation_exprs(origin_exprs, origin_visitor))) {
    LOG_WARN("failed to get relation exprs on origin stmt", K(ret));
  } else if (OB_FAIL(sub_stmt->get_relation_exprs(substmt_exprs, substmt_visitor))) {
    LOG_WARN("failed to get relation exprs on substmt", K(ret));
  } else {
    sub_stmt->get_grouping_sets_items().reset();
    sub_stmt->get_rollup_items().reset();
    sub_stmt->get_cube_items().reset();
    sub_stmt->get_rollup_exprs().reset();
    sub_stmt->get_group_exprs().reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_groupby_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (!ObOptimizerUtil::find_item(origin_exprs, origin_groupby_exprs.at(i), &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed. origin exprs should have origin_groupby_exprs.at(i)", K(ret));
    } else if (OB_ISNULL(expr = substmt_exprs.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed. get unexpected NULL", K(ret));
    } else if (OB_FAIL(sub_stmt->get_group_exprs().push_back(expr))) {
      LOG_WARN("failed to push back to groupby_exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_rollup_exprs.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (!ObOptimizerUtil::find_item(origin_exprs, origin_rollup_exprs.at(i), &idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed. origin exprs should have origin_rollup_exprs.at(i)", K(ret));
    } else if (OB_ISNULL(expr = substmt_exprs.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed. get unexpected NULL", K(ret));
    } else if (OB_FAIL(sub_stmt->get_rollup_exprs().push_back(expr))) {
      LOG_WARN("failed to push back to rollup_exprs", K(ret));
    }
  }
  /* case
   * select c1, 1 from t1 group by grouping sets(c1, ()) having sum(c1) > 0
   * --->
   * select c1, 1 from t1 group by c1 having sum(c1) > 0
   * union all
   * select NULL, 1 from t1 group by 1 having sum(c1) > 0;
   */
  if (OB_SUCC(ret) && sub_stmt->get_group_exprs().empty() && sub_stmt->get_rollup_exprs().empty()) {
    ObConstRawExpr *one_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                     ObNumberType,
                                                     1,
                                                     one_expr))) {
      LOG_WARN("build expr failed", K(ret));
    } else if (OB_FAIL(sub_stmt->get_group_exprs().push_back(one_expr))) {
      LOG_WARN("failed to push back into groupby exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::create_child_stmts_for_groupby_sets(ObSelectStmt *origin_stmt,
                                                               ObIArray<ObSelectStmt*> &child_stmts)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGroupbyExpr, 4> groupby_exprs_list;
  ObSEArray<ObGroupbyExpr, 4> rollup_exprs_list;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), KP(origin_stmt), KP(ctx_));
  } else if (OB_FAIL(get_groupby_and_rollup_exprs_list(origin_stmt, groupby_exprs_list, rollup_exprs_list))) {
    LOG_WARN("failed to get groupby and rollup exprs list", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < groupby_exprs_list.count(); ++i) {
      ObSelectStmt *groupby_stmt = NULL;
      ObSEArray<ObRawExpr*, 4> groupby_exprs;
      if (OB_FAIL(create_groupby_substmt(groupby_exprs_list.at(i).groupby_exprs_,
                                         rollup_exprs_list.at(i).groupby_exprs_,
                                         *origin_stmt,
                                         groupby_stmt,
                                         groupby_exprs))) {
        LOG_WARN("failed to init groupby stmt", K(ret));
      } else if (OB_FAIL(convert_select_having_in_groupby_stmt(groupby_exprs_list,
                                                               rollup_exprs_list,
                                                               groupby_exprs,
                                                               i,
                                                               *origin_stmt,
                                                               *groupby_stmt))) {
        LOG_WARN("failed to convert select list and having from grouping sets.", K(ret));
      } else if (OB_FAIL(groupby_stmt->recursive_adjust_statement_id(ctx_->allocator_,
                                                                     ctx_->src_hash_val_,
                                                                     i + 1))) {
        LOG_WARN("failed to recursive adjust statement id", K(ret));
      } else if (OB_FAIL(groupby_stmt->update_stmt_table_id(*origin_stmt))) {
        LOG_WARN("failed to update stmt table id.", K(ret));
      } else if (OB_FAIL(groupby_stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalized stmt.", K(ret));
      } else if (OB_FAIL(child_stmts.push_back(groupby_stmt))) {
        LOG_WARN("failed to push back child stmt");
      }
    }
  }
  return ret;
}

/* as following to create set stmt:
* 1. get total group by stmt count(grouping sets + multi rollup + multi cube)
* 2. deep copy origin stmt;
* 3. generate group by exprs and add stmt;
* 4. deal with stmt other attribute: select list、aggr、column and so on;
* 5. create set stmt.
*/
int ObTransformPreProcess::create_set_view_stmt(ObSelectStmt *stmt, TableItem *view_table_item)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  ObSelectStmt *parent_stmt = NULL;
  ObSelectStmt *set_view_stmt = NULL;
  if (OB_ISNULL(stmt)||
      OB_ISNULL(view_table_item) ||
      OB_ISNULL(parent_stmt = view_table_item->ref_query_) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("origin stmt is null", K(ret));
  } else if (OB_FAIL(create_child_stmts_for_groupby_sets(parent_stmt, child_stmts))) {
    LOG_WARN("failed to create child stmts for groupby set stmt", K(ret), K(*parent_stmt));
  } else if (OB_FAIL(ObTransformUtils::create_set_stmt(ctx_, ObSelectStmt::UNION, false,
                                                       child_stmts, set_view_stmt))) {
    LOG_WARN("failed to create set stmt", K(ret));
  } else if (OB_ISNULL(set_view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(set_view_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  // it is to rename the select items by recreating the select items with same select exprs
  } else if (FALSE_IT(view_table_item->ref_query_ = set_view_stmt)) {
  } else if (OB_FAIL(ObTransformUtils::refresh_select_items_name(*ctx_->allocator_,
                                                                 set_view_stmt))) {
    LOG_WARN("failed to refresh select items name", K(ret));
  } else if (OB_FAIL(ObTransformUtils::refresh_column_items_name(stmt,
                                                                 view_table_item->table_id_))) {
    LOG_WARN("failed to refresh column items name", K(ret));
  }
  return ret;
}

/*
 *  just do combination bewteen grouping sets
 *  group by grouping sets(c1, c2), grouping sets(rollup(c3), cube(c4)) --->
 *  group by c1, rollup(c3) union all
 *  group by c2, rollup(c3) union all
 *  group by c1, cube(c4) union all
 *  group by c2, cube(c4)
*/
int ObTransformPreProcess::simple_expand_grouping_sets_items(
                                           ObIArray<ObGroupingSetsItem> &grouping_sets_items,
                                           ObIArray<ObGroupingSetsItem> &simple_grouping_sets_items)
{
  int ret = OB_SUCCESS;
  if (grouping_sets_items.empty()) {
    /* do nothing */
  } else {
    int total_cnt = 1;
    for (int64_t i = 0; i < grouping_sets_items.count(); ++i) {
      total_cnt = total_cnt * (grouping_sets_items.at(i).grouping_sets_exprs_.count() +
                               grouping_sets_items.at(i).rollup_items_.count() +
                               grouping_sets_items.at(i).cube_items_.count());
    }
    if (OB_FAIL(simple_grouping_sets_items.prepare_allocate(total_cnt))) {
      LOG_WARN("failed to prepare allocate", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
        int64_t bit_count = i;
        int64_t index = 0;
        for (int64_t j = 0; OB_SUCC(ret) && j < grouping_sets_items.count(); ++j) {
          ObGroupingSetsItem &item = grouping_sets_items.at(j);
          int64_t item_count = item.grouping_sets_exprs_.count() + item.rollup_items_.count() +
                               item.cube_items_.count();
          index = bit_count % (item_count);
          bit_count = bit_count / (item_count);
          if (index < item.grouping_sets_exprs_.count()) {
            if (OB_FAIL(simple_grouping_sets_items.at(i).grouping_sets_exprs_.push_back(
                        item.grouping_sets_exprs_.at(index)))) {
              LOG_WARN("failed to push back item", K(ret));
            }
          } else if (index >= item.grouping_sets_exprs_.count() &&
                     index < item.grouping_sets_exprs_.count() + item.rollup_items_.count()) {
            if (OB_FAIL(simple_grouping_sets_items.at(i).rollup_items_.push_back(
                        item.rollup_items_.at(index - item.grouping_sets_exprs_.count())))) {
              LOG_WARN("failed to push back item", K(ret));
            }
          } else if (index >= item.grouping_sets_exprs_.count() + item.rollup_items_.count() &&
                     index < item.grouping_sets_exprs_.count() + item.rollup_items_.count() +
                             item.cube_items_.count()) {
            if (OB_FAIL(simple_grouping_sets_items.at(i).cube_items_.push_back(
                        item.cube_items_.at(index - item.grouping_sets_exprs_.count() -
                                            item.rollup_items_.count())))) {
              LOG_WARN("failed to push back item", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(index), K(item.grouping_sets_exprs_.count()),
                     K(item.rollup_items_.count()), K(item.cube_items_.count()));
          }
        }
      }
    }
  }
  return ret;
}

/*
 * 1. cube(c1, c2, c3) ---> rollup(c1) X rollup(c2) X rollup(c3)
*/
int ObTransformPreProcess::expand_cube_item(ObCubeItem &cube_item,
                                            common::ObIArray<ObRollupItem> &rollup_items) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cube_item.cube_list_exprs_.count(); ++i) {
    ObRollupItem rollup_item;
    if (OB_FAIL(rollup_item.rollup_list_exprs_.push_back(cube_item.cube_list_exprs_.at(i)))) {
      LOG_WARN("failed to append item", K(ret));
    } else if (OB_FAIL(rollup_items.push_back(rollup_item))) {
      LOG_WARN("failed to push back item", K(ret));
    }
  }
  return ret;
}

/*
 * 1. cube(c1, c2, c3), cube(c1, c2) ---> rollup(c1), rollup(c2), rollup(c3), rollup(c1), rollup(c2)
*/
int ObTransformPreProcess::expand_cube_items(common::ObIArray<ObCubeItem> &cube_items,
                                             common::ObIArray<ObRollupItem> &rollup_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cube_items.count(); ++i) {
    ObCubeItem &cube_item = cube_items.at(i);
    if (OB_FAIL(expand_cube_item(cube_item, rollup_items))) {
      LOG_WARN("failed expand multi cube item", K(ret), K(cube_items), K(i));
    }
  }
  return ret;
}

int ObTransformPreProcess::get_groupby_exprs_list(ObIArray<ObGroupingSetsItem> &grouping_sets_items,
                                                  ObIArray<ObRollupItem> &rollup_items,
                                                  ObIArray<ObCubeItem> &cube_items,
                                                  ObIArray<ObGroupbyExpr> &groupby_exprs_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGroupbyExpr, 4> grouping_sets_exprs_list;
  ObSEArray<ObGroupbyExpr, 4> rollup_exprs_list;
  ObSEArray<ObGroupbyExpr, 4> tmp_groupby_exprs_list;
  ObSEArray<ObRollupItem, 4> tmp_rollup_items;
  ObSEArray<ObGroupbyExpr, 4> tmp_rollup_exprs_list;
  if (OB_FAIL(expand_grouping_sets_items(grouping_sets_items, grouping_sets_exprs_list))) {
    LOG_WARN("failed to expand grouping sets items", K(ret));
  } else if (OB_FAIL(expand_rollup_items(rollup_items, rollup_exprs_list))) {
    LOG_WARN("failed to expand grouping sets items", K(ret));
  } else if (OB_FAIL(combination_two_rollup_list(grouping_sets_exprs_list,
                                                 rollup_exprs_list,
                                                 tmp_groupby_exprs_list))) {
    LOG_WARN("failed to expand grouping sets items", K(ret));
  } else if (OB_FAIL(expand_cube_items(cube_items, tmp_rollup_items))){
    LOG_WARN("failed to expand multi cube items", K(ret));
  } else if (OB_FAIL(expand_rollup_items(tmp_rollup_items, tmp_rollup_exprs_list))) {
    LOG_WARN("failed to expand grouping sets items", K(ret));
  } else if (OB_FAIL(combination_two_rollup_list(tmp_groupby_exprs_list,
                                                 tmp_rollup_exprs_list,
                                                 groupby_exprs_list))) {
    LOG_WARN("failed to expand grouping sets items", K(ret));
  } else {
    LOG_TRACE("succeed to get groupby exprs list", K(grouping_sets_exprs_list),
                                                   K(rollup_exprs_list),
                                                   K(cube_items),
                                                   K(groupby_exprs_list));
  }
  return ret;
}

/*
 * transform_grouping_sets_to_rollup_exprs should do things like
 * origin_groupby_exprs_list = [(c1, c2, c3), (c1, c2), c1]
 * --->
 * rollup(c2, c3), c1
 * but not support now
 */
int ObTransformPreProcess::transform_grouping_sets_to_rollup_exprs(ObSelectStmt &origin_stmt,
                                                ObIArray<ObGroupbyExpr> &origin_groupby_exprs_list,
                                                ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                                ObIArray<ObGroupbyExpr> &rollup_exprs_list)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr*> &groupby_exprs = origin_stmt.get_group_exprs();
  ObIArray<ObRawExpr*> &rollup_exprs = origin_stmt.get_rollup_exprs();
  /* TODO GROUPING_SETS:
   * 1. Do transform grouping sets to rollup in the future.
   * 2. And what is more, if has_group_id = true, remember to handle it.
   */
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_groupby_exprs_list.count(); ++i) {
    ObGroupbyExpr groupby_expr;
    ObGroupbyExpr rollup_expr;
    if (OB_FAIL(groupby_exprs_list.push_back(groupby_expr))) {
      LOG_WARN("failed to push_back groupby_exprs_list", K(ret));
    } else if (OB_FAIL(rollup_exprs_list.push_back(rollup_expr))) {
      LOG_WARN("failed to push_back rollup_exprs_list", K(ret));
    } else if (OB_FAIL(append_array_no_dup(groupby_exprs_list.at(i).groupby_exprs_, groupby_exprs))) {
      LOG_WARN("failed to append array no dup", K(ret));
    } else if (OB_FAIL(append_array_no_dup(groupby_exprs_list.at(i).groupby_exprs_,
                                            origin_groupby_exprs_list.at(i).groupby_exprs_))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(append(rollup_exprs_list.at(i).groupby_exprs_, rollup_exprs))) {
      LOG_WARN("failed to append", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::get_groupby_and_rollup_exprs_list(ObSelectStmt *origin_stmt,
                                                        ObIArray<ObGroupbyExpr> &groupby_exprs_list,
                                                        ObIArray<ObGroupbyExpr> &rollup_exprs_list)
{
  int ret = OB_SUCCESS;
  ObIArray<ObGroupingSetsItem> &grouping_sets_items = origin_stmt->get_grouping_sets_items();
  ObIArray<ObRollupItem> &rollup_items = origin_stmt->get_rollup_items();
  ObIArray<ObCubeItem> &cube_items = origin_stmt->get_cube_items();
  ObSEArray<ObGroupbyExpr, 4> temp_groupby_exprs_list;
  if (OB_ISNULL(origin_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("origin stmt is NULL", K(ret), K(origin_stmt));
  } else if (OB_FAIL(get_groupby_exprs_list(grouping_sets_items,
                                            rollup_items,
                                            cube_items,
                                            temp_groupby_exprs_list))) {
    LOG_WARN("failed to get group by exprs list", K(ret));
  } else if (temp_groupby_exprs_list.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("temp_groupby_exprs_list is empty", K(ret));
  } else if (OB_FAIL(transform_grouping_sets_to_rollup_exprs(*origin_stmt,
                                                             temp_groupby_exprs_list,
                                                             groupby_exprs_list,
                                                             rollup_exprs_list))) {
    LOG_WARN("failed to transform_grouping_sets_to_rollup_exprs", K(ret));
  } else if (groupby_exprs_list.count() != rollup_exprs_list.count() ||
             groupby_exprs_list.count() != temp_groupby_exprs_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("groupby_exprs_list is not same size to rollup_exprs_list or temp_groupby_exprs_list",
             K(ret), K(temp_groupby_exprs_list), K(groupby_exprs_list), K(rollup_exprs_list));
  } else { /* do nothing */ }
  return ret;
}
/*@brief,ObTransformPreProcess::expand_grouping_sets_items,Creating a complete group exprs. such as:
 * select c1,c2,c3,c4 from t1 group by grouping sets(c1,c2), grouping sets(c3,c4);
 * <==>
 * select c1,NULL,c3,NULL from t1 group by c1,c3
 * union all
 * select c1,NULL,NULL,c4 from t1 group by c1,c4
 * union all
 * select NULL,c2,c3,NULL from t1 group by c2,c3
 * union all
 * select NULL,c2,NULL,c4 from t1 group by c2,c4;
 * as above example, multi grouping sets is actually grouping sets cartesian product.
 */
int ObTransformPreProcess::expand_grouping_sets_items(
                                                  ObIArray<ObGroupingSetsItem> &grouping_sets_items,
                                                  ObIArray<ObGroupbyExpr> &grouping_sets_exprs)
{
  int ret = OB_SUCCESS;
  typedef ObSEArray<ObGroupingSetsItem, 4> GroupingSetsArray;
  SMART_VAR(GroupingSetsArray, tmp_grouping_sets_items) {
    if (OB_FAIL(simple_expand_grouping_sets_items(grouping_sets_items, tmp_grouping_sets_items))) {
      LOG_WARN("failed to simple expand grouping_sets_items", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_grouping_sets_items.count(); ++i) {
        ObGroupbyExpr groupby_item;
        for (int64_t j = 0; OB_SUCC(ret) && j < tmp_grouping_sets_items.at(i).grouping_sets_exprs_.count(); ++j) {
          if (OB_FAIL(append(groupby_item.groupby_exprs_,
                        tmp_grouping_sets_items.at(i).grouping_sets_exprs_.at(j).groupby_exprs_))) {
            LOG_WARN("failed to append exprs", K(ret));
          } else {/*do nothing*/}
        }
        // multi cube -> multi rollup
        if (OB_SUCC(ret) && tmp_grouping_sets_items.at(i).cube_items_.count() > 0) {
          ObSEArray<ObRollupItem, 4> tmp_rollup_items;
          if (OB_FAIL(expand_cube_items(tmp_grouping_sets_items.at(i).cube_items_,
                                        tmp_rollup_items))) {
            LOG_WARN("failed to expand multi rollup items", K(ret));
          } else if (OB_FAIL(append(tmp_grouping_sets_items.at(i).rollup_items_,
                                    tmp_rollup_items))) {
            LOG_WARN("failed to append tmp multi rollup items", K(ret));
          } else {/* do nothing */}
        }
        if (OB_SUCC(ret)) {
          if (tmp_grouping_sets_items.at(i).rollup_items_.count() > 0) {
            ObSEArray<ObGroupbyExpr, 4> groupby_exprs_list;
            if (OB_FAIL(expand_rollup_items(tmp_grouping_sets_items.at(i).rollup_items_,
                                            groupby_exprs_list))) {
              LOG_WARN("failed to expand multi rollup items", K(ret));
            } else {
              for (int64_t k = 0; OB_SUCC(ret) && k < groupby_exprs_list.count(); ++k) {
                if (OB_FAIL(append(groupby_exprs_list.at(k).groupby_exprs_,
                                   groupby_item.groupby_exprs_))) {
                  LOG_WARN("failed to append exprs", K(ret));
                } else if (OB_FAIL(grouping_sets_exprs.push_back(groupby_exprs_list.at(k)))) {
                  LOG_WARN("failed to push back groupby exprs", K(ret));
                } else {/*do nothing*/}
              }
            }
          } else if (OB_FAIL(grouping_sets_exprs.push_back(groupby_item))) {
            LOG_WARN("failed to push back item", K(ret));
          } else {/*do nothing*/}
        }
      }
    }
  }
  return ret;
}

/*
rollup(c1), rollup(c2) --->
[(), c1, c2, (c1, c2)]
*/
int ObTransformPreProcess::expand_rollup_items(ObIArray<ObRollupItem> &rollup_items,
                                               ObIArray<ObGroupbyExpr> &rollup_list_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGroupbyExpr, 4> result_rollup_list_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < rollup_items.count(); ++i) {
    ObRollupItem &rollup_item = rollup_items.at(i);
    ObSEArray<ObGroupbyExpr, 4> tmp_rollup_list_exprs;
    ObGroupbyExpr empty_item;
    if (OB_FAIL(tmp_rollup_list_exprs.push_back(empty_item))) {
      LOG_WARN("failed to push back item", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < rollup_item.rollup_list_exprs_.count(); ++j) {
        ObGroupbyExpr item;
        if (OB_FAIL(append(item.groupby_exprs_, tmp_rollup_list_exprs.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(append(item.groupby_exprs_,
                                  rollup_item.rollup_list_exprs_.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(tmp_rollup_list_exprs.push_back(item))) {
          LOG_WARN("failed to push back item", K(ret));
        } else {/*do nothing*/}
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(combination_two_rollup_list(result_rollup_list_exprs,
                                                tmp_rollup_list_exprs,
                                                rollup_list_exprs))) {
          LOG_WARN("failed to combination two rollup list", K(ret));
        } else if (OB_FAIL(result_rollup_list_exprs.assign(rollup_list_exprs))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::combination_two_rollup_list(ObIArray<ObGroupbyExpr> &rollup_list_exprs1,
                                                       ObIArray<ObGroupbyExpr> &rollup_list_exprs2,
                                                       ObIArray<ObGroupbyExpr> &rollup_list_exprs)
{
  int ret = OB_SUCCESS;
  rollup_list_exprs.reset();
  if (rollup_list_exprs1.count() == 0 && rollup_list_exprs2.count() > 0) {
    if (OB_FAIL(append(rollup_list_exprs, rollup_list_exprs2))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else {/*do nothing*/}
  } else if (rollup_list_exprs1.count() > 0 && rollup_list_exprs2.count() == 0) {
    if (OB_FAIL(append(rollup_list_exprs, rollup_list_exprs1))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else {/*do nothing*/}
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rollup_list_exprs1.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs2.count(); ++j) {
        ObGroupbyExpr item;
        if (OB_FAIL(append(item.groupby_exprs_,
                          rollup_list_exprs1.at(i).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(append(item.groupby_exprs_,
                                  rollup_list_exprs2.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(rollup_list_exprs.push_back(item))) {
          LOG_WARN("failed to push back item", K(ret));
        } else {/*do nothing*/}
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_hierarchical_query(ObDMLStmt *stmt,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(ret));
  } else if (stmt->is_hierarchical_query()) {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    ObSelectStmt *hierarchical_stmt = NULL;
    TableItem *table = NULL;
    //connect by视图分离
    if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(try_split_hierarchical_query(*select_stmt, hierarchical_stmt))) {
      LOG_WARN("failed to try split hierarchical query", K(ret));
    } else if (OB_ISNULL(hierarchical_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null stmt", K(ret));
    } else if (OB_FAIL(create_and_mock_join_view(*hierarchical_stmt))) {
      LOG_WARN("failed to transform from item", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::try_split_hierarchical_query(ObSelectStmt &stmt,
                                                        ObSelectStmt *&hierarchical_stmt)
{
  int ret = OB_SUCCESS;
  bool need_split = false;
  TableItem *table = NULL;
  if (OB_FAIL(check_need_split_hierarchical_query(stmt, need_split))) {
    LOG_WARN("failed to check need split hierarchical query", K(ret));
  } else if (!need_split) {
    hierarchical_stmt = &stmt;
  } else if (OB_FAIL(create_connect_by_view(stmt))) {
    LOG_WARN("failed to create connect by view", K(ret));
  } else if (OB_UNLIKELY(stmt.get_table_size() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect table size after create connect by view", K(stmt.get_table_size()), K(ret));
  } else if (OB_ISNULL(table = stmt.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_UNLIKELY(!table->is_generated_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect generated table after create connect by view", K(ret));
  } else if (OB_ISNULL(table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ref query", K(ret));
  } else {
    hierarchical_stmt = table->ref_query_;
  }
  return ret;
}

int ObTransformPreProcess::check_need_split_hierarchical_query(ObSelectStmt &stmt, bool &need_split)
{
  int ret = OB_SUCCESS;
  need_split = false;
  if (stmt.has_subquery()
      || (stmt.has_order_by() && !stmt.is_order_siblings())
      || stmt.has_distinct()
      || stmt.has_group_by()
      || stmt.has_rollup()
      || stmt.has_window_function()
      || stmt.has_limit()
      || stmt.is_contains_assignment()
      || stmt.has_sequence()
      || stmt.has_ora_rowscn()) {
    need_split = true;
  } else {
    ObSEArray<ObRawExpr *, 8> join_conds;
    ObSEArray<ObRawExpr *, 8> other_conds;
    if (OB_FAIL(classify_join_conds(stmt, join_conds, other_conds))) {
      LOG_WARN("failed to classify join conditions", K(ret));
    } else if (!other_conds.empty()) {
      need_split = true;
    }
  }
  return ret;
}

class ReplaceRownumRowidExpr : public ObIRawExprReplacer
{
public:
  ReplaceRownumRowidExpr(ObSelectStmt *stmt) : view_stmt_(stmt) {}
  virtual int generate_new_expr(ObRawExprFactory &expr_factory,
                                ObRawExpr *old_expr,
                                ObRawExpr *&new_expr)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(old_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old expr is null", K(ret));
    } else if (old_expr->has_flag(IS_ROWNUM)) {
      if (OB_FAIL(ObRawExprUtils::build_rownum_expr(expr_factory, new_expr))) {
        LOG_WARN("failed to build rownum expr", K(ret));
      } else if (OB_ISNULL(new_expr) || OB_ISNULL(view_stmt_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to build new expr", K(ret), K(new_expr), K(view_stmt_));
      } else if (OB_FAIL(view_stmt_->get_pseudo_column_like_exprs().push_back(new_expr))) {
        LOG_WARN("failed to add rownum expr into view stmt", K(ret));
      }
    } else if (old_expr->get_expr_type() == T_FUN_SYS_CALC_UROWID) {
      if (OB_FAIL(ObRawExprCopier::copy_expr_node(expr_factory,
                                                  old_expr,
                                                  new_expr))) {
        LOG_WARN("failed to copy calc urowid expr", K(ret));
      } else if (OB_ISNULL(new_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to build new calc rowid expr", K(ret));
      }
    }
    return ret;
  }
  ObSelectStmt *view_stmt_;
};

int ObTransformPreProcess::create_connect_by_view(ObSelectStmt &stmt)
{
  int ret = OB_SUCCESS;
  //把层次查询相关的运算全都封装在view内部
  ObSelectStmt* view_stmt = NULL;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  //connect by view需要投影出来的表达式
  ObSEArray<ObRawExpr *, 4> select_list;
  //LEVEL、CONNECT_BY_ISLEAF、CONNECT_BY_ISCYCLE
  //CONNECT_BY_ROOT、PRIOR、CONNECT_BY_PATH表达式
  ObSEArray<ObRawExpr *, 4> connect_by_related_exprs;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt_factory), K(expr_factory),
                                    K(stmt), K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(view_stmt->ObDMLStmt::assign(stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(view_stmt->adjust_statement_id(ctx_->allocator_,
                                                    ctx_->src_qb_name_,
                                                    ctx_->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    view_stmt->set_stmt_type(stmt::T_SELECT);
    /*in:
    ObDMLStmt::assign won't assign has_prior/order_siblings/is_hierarchical_query. 
    So we need to set it manually.
    */
   view_stmt->set_has_prior(stmt.has_prior());
   view_stmt->set_order_siblings(stmt.is_order_siblings());
   view_stmt->set_hierarchical_query(stmt.is_hierarchical_query());
  // 1. handle table, columns, from
  // dml_stmt: from table, semi table, joined table
    stmt.get_semi_infos().reuse();
    stmt.get_column_items().reuse();
    stmt.get_part_exprs().reset();
    stmt.get_check_constraint_items().reset();
    stmt.set_hierarchical_query(false);
    //prior相关表达式放在视图内计算
    stmt.set_has_prior(false);
    if (stmt.is_order_siblings()) {
      //order by siblings需要放到视图内计算
      stmt.get_order_items().reset();
      stmt.set_order_siblings(false);
    } else {
      //普通order by需要在层次查询完再计算
    }
  }
  // 2. handle where conditions
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr *, 8> join_conds;
    ObSEArray<ObRawExpr *, 8> other_conds;
    if (OB_FAIL(classify_join_conds(stmt, join_conds, other_conds))) {
      LOG_WARN("failed to classify join conditions", K(ret));
    } else if (OB_FAIL(stmt.get_condition_exprs().assign(other_conds))) {
      LOG_WARN("failed to assign rownum conditions", K(ret));
    } else if (OB_FAIL(view_stmt->get_condition_exprs().assign(join_conds))) {
      LOG_WARN("failed to assign normal conditions", K(ret));
    } else {
      stmt.reset_table_items();
      stmt.get_joined_tables().reuse();
      stmt.clear_from_items();
      if (OB_FAIL(stmt.rebuild_tables_hash())) {
        LOG_WARN("failed to rebuild tables hash", K(ret));
      }
    }
  }
  // 3. handle clauses processed by the upper_stmt
  if (OB_SUCC(ret)) {
    // consider following parts:
    // select: group-by, rollup, select subquery, window function, distinct, sequence,
    //         order by, limit, select into, connect by
    if (!view_stmt->is_order_siblings()) {
      //普通的order by需要在上层stmt计算
      view_stmt->get_order_items().reset();
    } else {
      //sibling order在层次查询内部计算
    }
    view_stmt->set_limit_offset(NULL, NULL);
    view_stmt->set_limit_percent_expr(NULL);
    view_stmt->set_fetch_with_ties(false);
    view_stmt->set_has_fetch(false);
    view_stmt->clear_sequence();
    view_stmt->set_select_into(NULL);
    view_stmt->get_pseudo_column_like_exprs().reset();
    //填充层次查询信息
    view_stmt->set_hierarchical_query(true);
    view_stmt->set_nocycle(stmt.is_nocycle());
    ObRawExprCopier copier(*expr_factory);
    ReplaceRownumRowidExpr replacer(view_stmt);
    if (OB_FAIL(copier.copy_on_replace(stmt.get_start_with_exprs(),
                                       view_stmt->get_start_with_exprs(),
                                       &replacer))) {
      LOG_WARN("failed to generate start with expr", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(stmt.get_connect_by_exprs(),
                                              view_stmt->get_connect_by_exprs(),
                                              &replacer))) {
      LOG_WARN("failed to copy and replace connect by expr", K(ret));
    } else {
      stmt.get_start_with_exprs().reset();
      stmt.get_connect_by_exprs().reset();
    }
  }
  // 4. 处理 connect by related expr
  if (OB_SUCC(ret)) {
    //提取所有层次查询相关表达式，在层次查询内计算，然后投影出来
    if (OB_FAIL(extract_connect_by_related_exprs(stmt, connect_by_related_exprs))) {
      LOG_WARN("failed to extract connect by related exprs", K(ret));
    }
  }
  // 5. finish creating the child stmts
  if (OB_SUCC(ret)) {
    // create select list
    ObSEArray<ObRawExpr *, 4> columns;
    ObSqlBitSet<> from_tables;
    ObSEArray<ObRawExpr*, 16> shared_exprs;
    if (OB_FAIL(view_stmt->get_from_tables(from_tables))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(view_stmt->get_column_exprs(columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*view_stmt,
                                                             columns,
                                                             from_tables,
                                                             select_list))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(append(select_list, connect_by_related_exprs))) {
      LOG_WARN("failed to append connect by related exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                            select_list,
                                                            view_stmt))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_shared_expr(&stmt,
                                                             view_stmt,
                                                             shared_exprs))) {
      LOG_WARN("failed to extract shared expr", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(shared_exprs, select_list))) {
      LOG_WARN("failed to remove duplicate exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                            shared_exprs,
                                                            view_stmt))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (view_stmt->get_select_item_size() == 0 &&
                OB_FAIL(ObTransformUtils::create_dummy_select_item(*view_stmt, ctx_))) {
      LOG_WARN("failed to create dummy select item", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*view_stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    }
  }
  // 6. link upper stmt and view stmt
  TableItem *table_item = NULL;
  ObSEArray<ObRawExpr *, 4> columns;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_subquery_exprs(),
                                             view_stmt->get_subquery_exprs()))) {
      LOG_WARN("failed to remove subqueries", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_,
                                                            &stmt,
                                                            view_stmt,
                                                            table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                                 *table_item,
                                                                 &stmt,
                                                                 columns))) {
      LOG_WARN("failed to create columns for view", K(ret));
    }
  }
  // 7. formalize
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr *, 4> view_output;
    ObSEArray<ObRawExpr *, 4> view_columns;
    if (OB_FAIL(stmt.get_view_output(*table_item, view_output, view_columns))) {
      LOG_WARN("failed to get view output", K(ret));
    } else if (OB_FAIL(stmt.replace_relation_exprs(view_output, view_columns))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else if (OB_FAIL(stmt.formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

/**
 * @brief create_and_mock_join_view
 * 层次查询会把所有的连接运算封装到视图内部计算，
 * 处理时会生成两个相同的视图，第一个做个root节点
 * 生成子计划，第二个作为子节点的生成子计划，在这里
 * start with会预处理放入第一个视图内，其他表达式
 * 引用第二个视图
 */
int ObTransformPreProcess::create_and_mock_join_view(ObSelectStmt &stmt)
{
  int ret = OB_SUCCESS;
  bool has_for_update = false;
  ObSelectStmt* left_view_stmt = NULL;
  ObSelectStmt* right_view_stmt = NULL;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObRawExpr *, 4> select_list;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt_factory), K(expr_factory),
                                    K(stmt), K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(left_view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(right_view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(left_view_stmt->ObDMLStmt::assign(stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(left_view_stmt->adjust_statement_id(ctx_->allocator_,
                                                         ctx_->src_qb_name_,
                                                         ctx_->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    has_for_update = stmt.has_for_update();
    left_view_stmt->set_stmt_type(stmt::T_SELECT);
    left_view_stmt->set_has_prior(stmt.has_prior());
    left_view_stmt->set_order_siblings(stmt.is_order_siblings());
    left_view_stmt->set_hierarchical_query(stmt.is_hierarchical_query());
  // 1. handle table, columns, from
  // dml_stmt: from table, semi table, joined table
    stmt.reset_table_items();
    stmt.get_joined_tables().reuse();
    stmt.get_semi_infos().reuse();
    stmt.get_column_items().reuse();
    stmt.clear_from_items();
    stmt.get_part_exprs().reset();
    stmt.get_check_constraint_items().reset();
    if (OB_FAIL(stmt.rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash", K(ret));
    }
  }
  // 2. handle where conditions
  if (OB_SUCC(ret)) {
    ObRawExprCopier copier(*expr_factory);
    ReplaceRownumRowidExpr replacer(left_view_stmt);
    if (OB_FAIL(left_view_stmt->get_condition_exprs().assign(stmt.get_condition_exprs()))) {
      LOG_WARN("failed to assign conditions", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(stmt.get_start_with_exprs(),
                                              left_view_stmt->get_start_with_exprs(),
                                              &replacer))) {
      LOG_WARN("failed to copy on replace start with exprs", K(ret));
    } else {
      stmt.get_condition_exprs().reset();
      stmt.get_start_with_exprs().reset();
    }
  }
  // 3. handle clauses processed by the upper_stmt
  if (OB_SUCC(ret)) {
    // consider following parts:
    // select: group-by, rollup, select subquery, window function, distinct, sequence,
    //         order by, limit, select into, start with, connect by
    left_view_stmt->get_order_items().reset();
    left_view_stmt->set_limit_offset(NULL, NULL);
    left_view_stmt->set_limit_percent_expr(NULL);
    left_view_stmt->set_fetch_with_ties(false);
    left_view_stmt->set_has_fetch(false);
    left_view_stmt->clear_sequence();
    left_view_stmt->set_select_into(NULL);
    left_view_stmt->get_pseudo_column_like_exprs().reset();
    left_view_stmt->get_connect_by_exprs().reset();
    left_view_stmt->set_hierarchical_query(false);
    left_view_stmt->set_has_prior(false);
    left_view_stmt->set_order_siblings(false);
  }
  // 4. finish creating the left child stmts
  if (OB_SUCC(ret)) {
    // create select list
    ObSEArray<ObRawExpr *, 4> columns;
    ObSEArray<ObQueryRefRawExpr *, 4> query_refs;
    ObSqlBitSet<> from_tables;
    ObSEArray<ObRawExpr*, 16> shared_exprs;
    if (OB_FAIL(left_view_stmt->get_from_tables(from_tables))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(left_view_stmt->get_column_exprs(columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*left_view_stmt,
                                                             columns,
                                                             from_tables,
                                                             select_list))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_shared_expr(&stmt,
                                                             left_view_stmt,
                                                             shared_exprs))) {
      LOG_WARN("failed to extract shared expr", K(ret));
    } else if (OB_FAIL(append_array_no_dup(select_list, shared_exprs))) {
      LOG_WARN("failed to append shared exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                            select_list,
                                                            left_view_stmt))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (left_view_stmt->get_select_item_size() == 0 &&
              OB_FAIL(ObTransformUtils::create_dummy_select_item(*left_view_stmt, ctx_))) {
      LOG_WARN("failed to create dummy select item", K(ret));
    } else if (OB_FAIL(left_view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    }
  }
  // 5. copy left stmt to right stmt
  if (OB_SUCC(ret)) {
    int64_t sub_num = 1;
    if (OB_FAIL(right_view_stmt->deep_copy(*stmt_factory,
                                            *expr_factory,
                                            *left_view_stmt))) {
      LOG_WARN("failed to copy stmt", K(ret));
    } else if (OB_FAIL(right_view_stmt->recursive_adjust_statement_id(ctx_->allocator_,
                                                                      ctx_->src_hash_val_,
                                                                      sub_num))) {
      LOG_WARN("failed to adjust statement id", K(ret));
    } else if (OB_FAIL(right_view_stmt->update_stmt_table_id(*left_view_stmt))) {
      LOG_WARN("failed to update stmt table id", K(ret));
    } else {
      right_view_stmt->get_start_with_exprs().reset();
    }
  }
  // 6. link upper stmt and view stmt
  TableItem *left_table_item = NULL;
  ObSEArray<ObRawExpr *, 4> left_columns;
  TableItem *right_table_item = NULL;
  ObSEArray<ObRawExpr *, 4> right_columns;
  if (OB_SUCC(ret)) {
    //为左右视图创建table item
    if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_,
                                                     &stmt,
                                                     left_view_stmt,
                                                     left_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(left_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                                 *left_table_item,
                                                                 &stmt,
                                                                 left_columns))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_,
                                                            &stmt,
                                                            right_view_stmt,
                                                            right_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(right_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                                 *right_table_item,
                                                                 &stmt,
                                                                 right_columns))) {
      LOG_WARN("failed to create columns for copy stmt", K(ret));
    }
  }
  // 7. 预处理start with exprs，放置到第一个视图的where condition
  //调整subquery exprs
  if (OB_SUCC(ret)) {
    if (OB_FAIL(left_view_stmt->add_condition_exprs(left_view_stmt->get_start_with_exprs()))) {
      LOG_WARN("failed to push start with exprs to view", K(ret));
    } else if (OB_FAIL(left_view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(right_view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*left_view_stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*right_view_stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else {
      left_view_stmt->get_start_with_exprs().reset();
    }
  }
  // 8. 如果stmt有for update标记，需要把标记打在connect by的右侧表上，
  // 同时清除左侧表上的for update标记
  if (OB_SUCC(ret) && has_for_update) {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_view_stmt->get_table_size(); ++i) {
      TableItem *table = left_view_stmt->get_table_item(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else {
        table->for_update_ = false;
      }
    }
  }

  // 9. replace prior exprs to left table column
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr *, 4> connect_by_prior_exprs;
    ObSEArray<ObRawExpr *, 4> prior_exprs;
    ObSEArray<ObRawExpr *, 4> converted_exprs;
    ObSEArray<ObRawExpr *, 16> relation_exprs;
    if (OB_FAIL(get_prior_exprs(stmt.get_connect_by_exprs(), connect_by_prior_exprs))) {
      LOG_WARN("failed to get prior exprs", K(ret));
    } else if (OB_FAIL(modify_prior_exprs(*ctx_->expr_factory_,
                                          stmt,
                                          select_list,
                                          right_columns,
                                          connect_by_prior_exprs,
                                          converted_exprs,
                                          false))) {
      LOG_WARN("failed to modify prior exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < converted_exprs.count(); ++i) {
      ObRawExpr *expr = converted_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (OB_FAIL(stmt.add_connect_by_prior_expr(expr))) {
        LOG_WARN("failed to add connect by prior expr", K(ret));
      }
    }
    converted_exprs.reuse();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(stmt.get_relation_exprs(relation_exprs))) {
      LOG_WARN("get stmt relation exprs fail", K(ret));
    } else if (OB_FAIL(get_prior_exprs(relation_exprs, prior_exprs))) {
      LOG_WARN("failed to get prior exprs", K(ret));
    } else if (OB_FAIL(modify_prior_exprs(*ctx_->expr_factory_,
                                          stmt,
                                          select_list,
                                          left_columns,
                                          prior_exprs,
                                          converted_exprs,
                                          true))) {
      LOG_WARN("failed to modify prior exprs", K(ret));
    } else if (OB_FAIL(stmt.replace_relation_exprs(prior_exprs, converted_exprs))) {
      LOG_WARN("failed to replace stmt expr", K(ret));
    }
  }

  // 10. create connect by joined table
  if (OB_SUCC(ret)) {
    TableItem *joined_table = NULL;
    stmt.get_joined_tables().reset();
    if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_, stmt, CONNECT_BY_JOIN,
                                                       left_table_item, right_table_item,
                                                       stmt.get_connect_by_exprs(),
                                                       joined_table,
                                                       true))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(joined_table->table_id_, true))) {
      LOG_WARN("failed to add from item", K(ret));
    } else {
      stmt.clear_connect_by_exprs();
    }
  }

  // 11. replace expr in parent stmt and formalize
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_subquery_exprs(),
                                             left_view_stmt->get_subquery_exprs()))) {
      LOG_WARN("failed to remove subqueries", K(ret));
    } else if (!select_list.empty()
               && OB_FAIL(stmt.replace_relation_exprs(select_list, right_columns))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else if (OB_FAIL(stmt.formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(stmt.formalize_stmt_expr_reference())) {
      LOG_WARN("failed to formalize stmt expr reference", K(ret));
    }
  }
  // 12. ignore for temp table optimization
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_->temp_table_ignore_stmts_.push_back(left_view_stmt))) {
      LOG_WARN("failed to push back stmt", K(ret));
    } else if (OB_FAIL(ctx_->temp_table_ignore_stmts_.push_back(right_view_stmt))) {
      LOG_WARN("failed to push back stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::classify_join_conds(ObSelectStmt &stmt,
                                              ObIArray<ObRawExpr *> &normal_join_conds,
                                              ObIArray<ObRawExpr *> &other_conds)
{
  int ret = OB_SUCCESS;
  //将where condition分成普通连接条件和其他条件
  //CONNECT_BY_PATH不会出现在where condition中
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_size(); ++i) {
    ObRawExpr *cond_expr = NULL;
    bool in_from_item = false;
    if (OB_ISNULL(cond_expr = stmt.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cond_expr->has_flag(CNT_LEVEL) ||
              cond_expr->has_flag(CNT_CONNECT_BY_ISLEAF) ||
              cond_expr->has_flag(CNT_CONNECT_BY_ISCYCLE) ||
              cond_expr->has_flag(CNT_CONNECT_BY_ROOT) ||
              cond_expr->has_flag(CNT_PRIOR) ||
              cond_expr->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(other_conds.push_back(cond_expr))) {
        LOG_WARN("failed to add condition expr into upper stmt", K(ret));
      }
    } else if (OB_FAIL(is_cond_in_one_from_item(stmt, cond_expr, in_from_item))) {
      LOG_WARN("failed to check cond is in from item", K(ret));
    } else if (!in_from_item) {
      //如果下推的join condition与select expr共享了sub_query expr
      //需要将subquery下推到view，然后构造select expr吐出
      if (OB_FAIL(normal_join_conds.push_back(cond_expr))) {
        LOG_WARN("failed to add condition expr into join view stmt", K(ret));
      }
    } else {
      //from item filter
      if (OB_FAIL(other_conds.push_back(cond_expr))) {
        LOG_WARN("failed to add condition expr into upper stmt", K(ret));
      }
    }
  }
  return ret;
}

/**
 * 例如select * from t1 lef join t2 on xxx , t3
 *        where t1.c1 = t2.c1 and t2.c2 = t3.c2
 * t1.c1 = t2.c1来自一个from item，t2.c2 = t3.c2来自两个from item
 * 所以t1.c1 = t2.c1应该在层次查询之后做，t2.c2 = t3.c2在层次查询之前做。
 */
int ObTransformPreProcess::is_cond_in_one_from_item(ObSelectStmt &stmt,
                                                    ObRawExpr *expr,
                                                    bool &in_from_item)
{
  int ret = OB_SUCCESS;
  TableItem *table = NULL;
  ObSqlBitSet<> table_ids;
  in_from_item = false;
  if (OB_ISNULL(expr)) {
    ret =  OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->get_relation_ids().is_empty()) {
    in_from_item = true;
  }
  int64_t N = stmt.get_from_item_size();
  for (int64_t i = 0; OB_SUCC(ret) && !in_from_item && i < N; ++i) {
    table_ids.reuse();
    FromItem &item = stmt.get_from_items().at(i);
    if (item.is_joined_) {
      table = stmt.get_joined_table(item.table_id_);
    } else {
      table = stmt.get_table_item_by_id(item.table_id_);
    }
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (OB_FAIL(stmt.get_table_rel_ids(table->is_joined_table() ?
                                              *(static_cast<JoinedTable*>(table)) :
                                              *table,
                                              table_ids))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    } else if (expr->get_relation_ids().is_subset(table_ids)) {
      in_from_item = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::extract_connect_by_related_exprs(ObSelectStmt &stmt,
                                                            ObIArray<ObRawExpr*> &special_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> relation_exprs;
  if (OB_FAIL(stmt.get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to relation exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); i++) {
      ObRawExpr *expr = relation_exprs.at(i);
      if (OB_FAIL(extract_connect_by_related_exprs(expr, special_exprs))) {
        LOG_WARN("failed to extract connect by related exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::extract_connect_by_related_exprs(ObRawExpr *expr,
                                                            ObIArray<ObRawExpr*> &special_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (T_LEVEL == expr->get_expr_type() ||
             T_CONNECT_BY_ISLEAF == expr->get_expr_type() ||
             T_CONNECT_BY_ISCYCLE == expr->get_expr_type() ||
             T_OP_CONNECT_BY_ROOT == expr->get_expr_type() ||
             T_OP_PRIOR == expr->get_expr_type() ||
             T_FUN_SYS_CONNECT_BY_PATH == expr->get_expr_type()) {
    if (ObOptimizerUtil::find_item(special_exprs, expr)) {
      //do nothing
    } else if (OB_FAIL(special_exprs.push_back(expr))) {
      LOG_WARN("failed to push back connect by expr", K(ret));
    }
  } else if (expr->has_flag(CNT_LEVEL) ||
             expr->has_flag(CNT_CONNECT_BY_ISLEAF) ||
             expr->has_flag(CNT_CONNECT_BY_ISCYCLE) ||
             expr->has_flag(CNT_CONNECT_BY_ROOT) ||
             expr->has_flag(CNT_PRIOR) ||
             expr->has_flag(CNT_SYS_CONNECT_BY_PATH)) {
    int64_t N = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(extract_connect_by_related_exprs(expr->get_param_expr(i), special_exprs))) {
        LOG_WARN("failed to extract connect by eprs", K(ret));
      }
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObTransformPreProcess::get_prior_exprs(ObIArray<ObRawExpr *> &exprs,
                                           ObIArray<ObRawExpr *> &prior_exprs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_FAIL(get_prior_exprs(exprs.at(i), prior_exprs))) {
      LOG_WARN("failed to get prior exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::get_prior_exprs(ObRawExpr *expr,
                                           ObIArray<ObRawExpr *> &prior_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (T_OP_PRIOR == expr->get_expr_type()) {
    if (OB_FAIL(add_var_to_array_no_dup(prior_exprs, expr))) {
      LOG_WARN("failed to add prior exprs", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_prior_exprs(expr->get_param_expr(i), prior_exprs)))) {
        LOG_WARN("failed to get prior exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::modify_prior_exprs(ObRawExprFactory &expr_factory,
                                              ObSelectStmt &stmt,
                                              const ObIArray<ObRawExpr *> &parent_columns,
                                              const ObIArray<ObRawExpr *> &left_columns,
                                              const ObIArray<ObRawExpr *> &prior_exprs,
                                              ObIArray<ObRawExpr *> &convert_exprs,
                                              const bool only_columns)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(expr_factory);
  if (!parent_columns.empty()
      && OB_FAIL(copier.add_replaced_expr(parent_columns, left_columns))) {
    LOG_WARN("failed to add replaced expr", K(ret));
  }
  for (int64_t i = 0; i < prior_exprs.count() && OB_SUCC(ret); i++) {
    ObRawExpr *raw_expr = prior_exprs.at(i);
    ObRawExpr *new_expr = NULL;
    if (OB_ISNULL(raw_expr) || OB_UNLIKELY(T_OP_PRIOR != raw_expr->get_expr_type())
        || OB_UNLIKELY(1 != raw_expr->get_param_count())
        || OB_ISNULL(raw_expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expr", KPC(raw_expr), K(ret));
    } else if (only_columns && !raw_expr->has_flag(CNT_COLUMN)) {
      // do nothing
    } else if (OB_FAIL(copier.copy_on_replace(raw_expr->get_param_expr(0), new_expr))) {
      LOG_WARN("failed to copy on replace expr", K(ret));
    } else {
      raw_expr = new_expr;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(convert_exprs.push_back(raw_expr))) {
      LOG_WARN("failed to push back new prior param expr", K(ret));
    }
  }
  return ret;
}

uint64_t ObTransformPreProcess::get_real_tid(uint64_t tid, ObSelectStmt &stmt)
{
  uint64_t real_tid = OB_INVALID_ID;
  for (int64_t i = 0; i < stmt.get_table_size(); i ++) {
    TableItem *table_item = stmt.get_table_item(i);
    if (tid == table_item->table_id_) {
      real_tid = table_item->ref_id_;
      break;
    }
  }
  return real_tid;
}

int ObTransformPreProcess::eliminate_having(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL pointer", K(stmt), K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else {
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    if (!select_stmt->has_group_by() && !select_stmt->get_having_exprs().empty()) {
      if (OB_FAIL(append(select_stmt->get_condition_exprs(), select_stmt->get_having_exprs()))) {
        LOG_WARN("failed to append condition exprs", K(ret));
      } else {
        select_stmt->get_having_exprs().reset();
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_func_is_serving_tenant(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter or data member", K(ret), K(stmt), K(ctx_));
  } else if (OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else {
    common::ObIArray<ObRawExpr*> &cond_exprs = stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
      if (OB_ISNULL(cond_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cond expr is NULL", K(ret), K(i), K(cond_exprs));
      } else if (OB_FAIL(recursive_replace_func_is_serving_tenant(*stmt, cond_exprs.at(i), trans_happened))) { // 此处必须直接传cond_exprs.at(i)，因为可能需要修改它的值
        LOG_WARN("fail to recursive replace functino is_serving_tenant",
                        K(ret), K(i), K(*cond_exprs.at(i)));
      } else if (OB_ISNULL(cond_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null pointer", K(ret));
      } else if (OB_FAIL(cond_exprs.at(i)->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret), K(i), K(*cond_exprs.at(i)));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->pull_all_expr_relation_id())) {
        LOG_WARN("pull stmt all expr relation ids failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::recursive_replace_func_is_serving_tenant(ObDMLStmt &stmt,
                                                                    ObRawExpr *&cond_expr,
                                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(cond_expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cond_expr is NULL", K(cond_expr), K_(ctx));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_expr->get_param_count(); ++i) {
      // 此处必须直接传cond_expr->get_param_expr(i)，因为可能需要修改它的值
      if (OB_FAIL(SMART_CALL(recursive_replace_func_is_serving_tenant(stmt,
                                                                      cond_expr->get_param_expr(i),
                                                                      trans_happened)))) {
        LOG_WARN("fail to recursive replace_func_is_serving_tenant", K(ret));
      }
    }
    // 如果是函数is_serving_tenant并且tenant_id为常量表达式，则改写为(svr_ip, svr_port) in ((ip1,
    // port1), (ip2, port2), ...)的形式
    // 如果当前租户是系统租户，直接返回true
    if (OB_SUCC(ret) && T_FUN_IS_SERVING_TENANT == cond_expr->get_expr_type()) {
      int64_t tenant_id_int64 = -1;
      if (OB_UNLIKELY(3 != cond_expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("T_FUN_IS_SERVING_TENANT must has 3 params",
                        K(ret), K(cond_expr->get_param_count()), K(*cond_expr));
      } else if (OB_ISNULL(cond_expr->get_param_expr(0))
                 || OB_ISNULL(cond_expr->get_param_expr(1))
                 || OB_ISNULL(cond_expr->get_param_expr(2))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("T_FUN_IS_SERVING_TENANT has a null param",
                        K(ret), K(cond_expr->get_param_expr(0)),
                        K(cond_expr->get_param_expr(1)),
                        K(cond_expr->get_param_expr(2)), K(*cond_expr));
      } else if (!cond_expr->get_param_expr(2)->is_static_scalar_const_expr()) {
      } else if (OB_ISNULL(ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx_ is NULL", K(ret));
      } else if (OB_ISNULL(ctx_->exec_ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument",
                        K(ret), K(ctx_->exec_ctx_), K(ctx_->session_info_), K(ctx_->allocator_));
      } else if (OB_FAIL(calc_const_raw_expr_and_get_int(stmt,
                                                         cond_expr->get_param_expr(2),
                                                         *ctx_->exec_ctx_,
                                                         ctx_->session_info_,
                                                         *ctx_->allocator_,
                                                         tenant_id_int64))) {
        LOG_WARN("fail to calc tenant id", K(ret), K(*cond_expr->get_param_expr(2)));
      } else if (OB_UNLIKELY(tenant_id_int64 <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tenant id is <= 0", K(ret), K(tenant_id_int64));
      } else {
        uint64_t tenant_id = static_cast<uint64_t>(tenant_id_int64);
        // if current tenant is sys, return true directly
        if (OB_SYS_TENANT_ID == tenant_id) {
          ObConstRawExpr *true_expr = NULL;
          if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_VARCHAR, true_expr))) {
            LOG_WARN("create const expr failed", K(ret));
          } else if (OB_ISNULL(true_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("true expr is NULL", K(ret));
          } else {
            ObObj true_obj;
            true_obj.set_bool(true);
            true_expr->set_value(true_obj);
            cond_expr = true_expr;
            trans_happened = true;
          }
        } else {
          ObUnitInfoGetter ui_getter;
          ObArray<ObAddr> servers;
          if (OB_ISNULL(ctx_->exec_ctx_->get_sql_proxy())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sql proxy from exec_ctx_ is NULL", K(ret));
          } else if (OB_FAIL(ui_getter.init(*ctx_->exec_ctx_->get_sql_proxy(), &GCONF))) {
            LOG_WARN("fail to init ObUnitInfoGetter", K(ret));
          } else if (OB_FAIL(ui_getter.get_tenant_servers(tenant_id, servers))) {
            LOG_WARN("fail to get servers of a tenant", K(ret));
          } else if (0 == servers.count()) {
            // 没找到该tenant_id对应的observer，可能该tenant_id是非法的，为了能通过query
            // range，将这里改成where false的形式，这样虽然优化器会返回所有partition，但是
            // ObPhyOperator中会处理好false的条件，不会进行多余的查询
            ObConstRawExpr *false_expr = NULL;
            if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_VARCHAR, false_expr))) {
              LOG_WARN("create varchar expr failed", K(ret));
            } else if (OB_ISNULL(false_expr)){
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("false expr is NULL", K(ret));
            } else {
              ObObj false_obj;
              false_obj.set_bool(false);
              false_expr->set_value(false_obj);
              cond_expr = false_expr;
              trans_happened = true;
            }
          } else {
            ObOpRawExpr *in_op = NULL;
            ObOpRawExpr *left_row_op = NULL;
            ObOpRawExpr *right_row_op = NULL;
            if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_IN, in_op))) {
              LOG_WARN("create in operator expr", K(ret));
            } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ROW, left_row_op))) {
              LOG_WARN("create left row operator failed", K(ret));
            } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ROW, right_row_op))) {
              LOG_WARN("create right row op failed", K(ret));
            } else if (OB_ISNULL(in_op) || OB_ISNULL(left_row_op) || OB_ISNULL(right_row_op)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("operator is null", K(in_op), K(left_row_op), K(right_row_op));
            } else {/*do nothing*/}

            for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
              ObAddr server = servers.at(i);
              ObOpRawExpr *row_op = NULL;
              ObConstRawExpr *ip_expr = NULL;
              ObConstRawExpr *port_expr = NULL;
              char *ip_buf = NULL;
              ObObj ip_obj;
              ObObj port_obj;
              if (OB_UNLIKELY(NULL == (ip_buf = static_cast<char*>(ctx_->allocator_->alloc(OB_MAX_SERVER_ADDR_SIZE))))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_ERROR("fail to alloc ip str buffer", K(ret), LITERAL_K(OB_MAX_SERVER_ADDR_SIZE));
              } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ROW, row_op))) {
                LOG_WARN("create row operator expr failed", K(ret));
              } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_VARCHAR, ip_expr))) {
                LOG_WARN("create ip operator expr failed", K(ret));
              } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_INT, port_expr))) {
                LOG_WARN("create port expr failed", K(ret));
              } else if (OB_UNLIKELY(!server.ip_to_string(ip_buf, OB_MAX_SERVER_ADDR_SIZE))) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("convert server addr to ip failed", K(ret), K(i), K(server));
              } else if (OB_ISNULL(row_op) || OB_ISNULL(ip_expr) || OB_ISNULL(port_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("expr is null", K(row_op), K(ip_expr), K(port_expr));
              } else {
                ip_obj.set_varchar(ObString(ip_buf));
                ip_obj.set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                ip_obj.set_collation_level(CS_LEVEL_SYSCONST);
                port_obj.set_int(server.get_port());
                ip_expr->set_value(ip_obj);
                port_expr->set_value(port_obj);
                if (OB_FAIL(row_op->set_param_exprs(ip_expr, port_expr))) {
                  LOG_WARN("fail to set param expr", K(ret));
                } else if (OB_FAIL(right_row_op->add_param_expr(row_op))) {
                  LOG_WARN("fail to add param expr", K(ret));
                } else {/*do nothing*/}
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(left_row_op->set_param_exprs(cond_expr->get_param_expr(0), cond_expr->get_param_expr(1)))) {
                LOG_WARN("fail to set param expr", K(ret));
              } else if (OB_FAIL(in_op->set_param_exprs(left_row_op, right_row_op))) {
                LOG_WARN("fail to set param expr", K(ret));
              } else {
                cond_expr = in_op;
                trans_happened = true;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::calc_const_raw_expr_and_get_int(const ObStmt &stmt,
                                                           ObRawExpr *const_expr,
                                                           ObExecContext &exec_ctx,
                                                           ObSQLSessionInfo *session,
                                                           ObIAllocator &allocator,
                                                           int64_t &result)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *sql_proxy = NULL;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObObj result_int_obj;
  bool got_result = false;
  if (OB_ISNULL(const_expr) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr or session is NULL", KP(session), KP(const_expr), K(ret));
  } else if (OB_UNLIKELY(!const_expr->is_static_scalar_const_expr())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is not const expr", K(ret), K(*const_expr));
  } else if (OB_ISNULL(sql_proxy = exec_ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is NULL", K(ret));
  } else if (OB_ISNULL(plan_ctx = exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is NULL", K(ret));
  } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                               const_expr,
                                                               result_int_obj,
                                                               got_result,
                                                               exec_ctx.get_allocator(),
                                                               false))) {
    LOG_WARN("failed to calc const or calculable expr", K(ret));
  } else if (OB_UNLIKELY(false == ob_is_integer_type(result_int_obj.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant id result must integer", K(ret), K(result_int_obj));
  } else {
    result = result_int_obj.get_int();
  }
  return ret;
}

/**
 * 1) select * from temp_t1 x, t2 y, temp_t3 z where ... ==>
 *    select * from temp_t1 x, t2 y, temp_t3 z where ... AND x.__session_id = xxx and z.__session_id = xxx;
 * 2) update temp_t set c1 = 1 where ... ==>
 *    update temp_t set c1 = 1 where ... AND __session_id = xxx;
 * 3) delete from temp_t where ... ==>
 *    delete from temp_t where ... AND __session_id = xxx;
 */
int ObTransformPreProcess::transform_for_temporary_table(ObDMLStmt *&stmt,
                                                         bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt or ctx", K(ret), K(stmt), K(ctx_));
  } else if (OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (/*false == ctx_->session_info_->is_inner() //内部session不附加条件, 给定的sql已经考虑过了*/
             ObSQLSessionInfo::USER_SESSION == ctx_->session_info_->get_session_type()) {
    common::ObArray<TableItem*> table_item_list;
    int64_t num_from_items = stmt->get_from_item_size();
    //1, collect all table item
    for(int64_t i = 0; OB_SUCC(ret) && i < num_from_items; ++i) {
      const FromItem &from_item = stmt->get_from_item(i);
      if (from_item.is_joined_) {
        JoinedTable *joined_table_item = stmt->get_joined_table(from_item.table_id_);
        if (OB_FAIL(collect_all_tableitem(stmt, joined_table_item, table_item_list))) {
          LOG_WARN("failed to collect table item", K(ret));
        }
      } else {
        TableItem *table_item = NULL;
        table_item = stmt->get_table_item_by_id(from_item.table_id_);
        if (OB_FAIL(collect_all_tableitem(stmt, table_item, table_item_list))) {
          LOG_WARN("failed to collect table item", K(ret));
        }
      }
    }
    //2, for each temporary table item, add session_id = xxx filter
    for (int64 i = 0; OB_SUCC(ret) && i < table_item_list.count(); ++i) {
      TableItem *table_item = table_item_list.at(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (table_item->is_basic_table()) {
        uint64_t table_ref_id = table_item->ref_id_;
        const ObTableSchema *table_schema = NULL;
        if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx_ or schema_cheker_ is NULL", K(ret), K(ctx_), K(ctx_->schema_checker_));
        } else if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(), table_ref_id, table_schema, table_item->is_link_table()))) {
          LOG_WARN("failed to get table schema", K(table_ref_id), K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table should not be null", K(table_ref_id));
        } else if (table_schema->is_oracle_tmp_table()) {
          // 单表查询时直接添加 filter; 多表时创建 view 并在内部添加 filter
          TableItem *view_table = NULL;
          ObSelectStmt *ref_query = NULL;
          TableItem *child_table = NULL;
          if (stmt->is_single_table_stmt()) {
            if (OB_FAIL(add_filter_for_temporary_table(*stmt, *table_item, table_schema->is_oracle_trx_tmp_table()))) {
              LOG_WARN("add filter for temporary table failed", K(ret));
            } else {
              trans_happened = true;
            }
          } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                                       stmt,
                                                                       view_table,
                                                                       table_item))) {
            LOG_WARN("failed to create empty view table", K(ret));
          } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                                  stmt,
                                                                  view_table,
                                                                  table_item))) {
            LOG_WARN("failed to create inline view", K(ret));
          } else if (!view_table->is_generated_table()
                     || OB_ISNULL(ref_query = view_table->ref_query_)
                     || !ref_query->is_single_table_stmt()
                     || OB_ISNULL(child_table = ref_query->get_table_item(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected view table", K(ret), K(*view_table));
          } else if (OB_FAIL(add_filter_for_temporary_table(*ref_query, *child_table, table_schema->is_oracle_trx_tmp_table()))) {
            LOG_WARN("add filter for temporary table failed", K(ret));
          } else {
            trans_happened = true;
          }
        }
      }
    }
    ObInsertStmt *ins_stmt = dynamic_cast<ObInsertStmt*>(stmt);
    if (OB_SUCC(ret) && NULL != ins_stmt) {
      // value exprs for insert stmt is not included in relation expr array.
      ObIArray<ObRawExpr*> &value_vectors = ins_stmt->get_values_vector();
      for (int64_t i = 0; OB_SUCC(ret) && i < value_vectors.count(); ++i) {
        ObRawExpr *expr = value_vectors.at(i);
        bool is_happened = false;
        if (OB_FAIL(transform_expr(*ctx_->expr_factory_,
                                   *ctx_->session_info_,
                                   expr,
                                   is_happened))) {
          LOG_WARN("transform expr failed", K(ret));
        } else if(OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret));
        } else {
          value_vectors.at(i) = expr;
          trans_happened |= is_happened;
        }
      }
    }
  }
  return ret;
}

//为stmt->where添加session_id = xxx
int ObTransformPreProcess::add_filter_for_temporary_table(ObDMLStmt &stmt,
                                                          const TableItem &table_item,
                                                          bool is_trans_scope_temp_table)
{
  int ret = OB_SUCCESS;
  ObRawExpr *equal_expr = NULL;
  ObConstRawExpr *temp_table_type = NULL;
  ObColumnRefRawExpr *expr_col = NULL;
  ObSysFunRawExpr *expr_temp_table_ssid = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("some parameter is NULL", K(ret), K(ctx_));
  }

  //build column expr
  if (OB_SUCC(ret)) {
    ColumnItem *exist_col_item = NULL;
    if (NULL != (exist_col_item = stmt.get_column_item(table_item.table_id_, OB_HIDDEN_SESSION_ID_COLUMN_NAME))) {
      expr_col = exist_col_item->expr_;
    } else {
      ColumnItem column_item;
      ObExprResType result_type;
      result_type.set_int();
      if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_REF_COLUMN, expr_col))) {
        LOG_WARN("fail to create raw expr", K(ret));
      } else if (OB_ISNULL(expr_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr_col is null", K(expr_col), K(ret));
      } else {
        expr_col->set_table_name(table_item.get_table_name());
        expr_col->set_column_name(OB_HIDDEN_SESSION_ID_COLUMN_NAME);
        expr_col->set_ref_id(table_item.table_id_, OB_HIDDEN_SESSION_ID_COLUMN_ID);
        expr_col->set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        expr_col->set_collation_level(CS_LEVEL_IMPLICIT);
        expr_col->set_result_type(result_type);
        column_item.expr_ = expr_col;
        column_item.table_id_ = expr_col->get_table_id();
        column_item.column_id_ = expr_col->get_column_id();
        column_item.column_name_ = expr_col->get_column_name();
        if (OB_FAIL(expr_col->add_relation_id(stmt.get_table_bit_index(table_item.table_id_)))) {
          LOG_WARN("failed to add relation id", K(ret));
        } else if (OB_FAIL(stmt.add_column_item(column_item))) {
          LOG_WARN("add column item to stmt failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_INT, temp_table_type))) {
      LOG_WARN("create const raw expr failed", K(ret));
    } else if (OB_ISNULL(temp_table_type)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr temp table ssid", K(ret));
    } else {
      ObObj val;
      val.set_int(is_trans_scope_temp_table ? 1 : 0);
      temp_table_type->set_value(val);
    }
  }

  //build session id expr
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_FUN_GET_TEMP_TABLE_SESSID, expr_temp_table_ssid))) {
      LOG_WARN("create const raw expr failed", K(ret));
    } else if (OB_ISNULL(expr_temp_table_ssid)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr temp table ssid", K(ret));
    } else {
      expr_temp_table_ssid->set_func_name(N_TEMP_TABLE_SSID);
      if (OB_FAIL(expr_temp_table_ssid->add_param_expr(temp_table_type))) {
        LOG_WARN("fail to add param expr", K(ret));
      }
    }
  }

  //build equal expr
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::create_equal_expr(*(ctx_->expr_factory_),
                                                  ctx_->session_info_,
                                                  expr_temp_table_ssid,
                                                  expr_col,
                                                  equal_expr))) {
      LOG_WARN("Creation of equal expr for outer stmt fails", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(equal_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("fail to formalize expr", K(ret));
    } else if (OB_FAIL(stmt.get_condition_exprs().push_back(equal_expr))) {
      LOG_WARN("failed to push back new filter", K(ret));
    } else {
      LOG_TRACE("add new filter succeed", K(stmt.get_condition_exprs()), K(*equal_expr));
    }
  }
  return ret;
}

//递归收集from_item中所有TableItem, 用于后续的查询改写
int ObTransformPreProcess::collect_all_tableitem(ObDMLStmt *stmt,
                                                 TableItem *table_item,
                                                 common::ObArray<TableItem*> &table_item_list)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else {
    if (table_item->is_joined_table()) {
      JoinedTable *joined_table_item = static_cast<JoinedTable *>(table_item);
      if (OB_FAIL(SMART_CALL(collect_all_tableitem(stmt, joined_table_item->left_table_,
                                                   table_item_list)))) {
        LOG_WARN("failed to collect temp table item", K(ret));
      } else if (OB_FAIL(SMART_CALL(collect_all_tableitem(stmt, joined_table_item->right_table_,
                                                          table_item_list)))) {
        LOG_WARN("failed to collect temp table item", K(ret));
      }
    } else if (table_item->is_basic_table() &&
               OB_FAIL(add_var_to_array_no_dup(table_item_list, table_item))) {
      LOG_WARN("failed to push table item", K(ret));
    }
  }
  return ret;
}

#ifdef OB_BUILD_LABEL_SECURITY
/**
 * 假如t1的安全列是 c_label，对应的安全策略是 policy_name, 会在下面三种语句添加filter
 * 1) select * from t1 where  ..
 * 2) update t1 set c1 = 1 where ..
 * 3) delete from t1 where ..
 *
 * filter: 0 == ols_label_value_cmp_le(ols_session_label(policy_name), c_label)
 *
 */
int ObTransformPreProcess::transform_for_label_se_table(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret), K(stmt), K(ctx_), K(ctx_->schema_checker_));
  } else if (OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (!ctx_->session_info_->is_inner()) { //内部session不用条件进行过滤
    common::ObArray<TableItem*> table_item_list;
    int64_t num_from_items = stmt->get_from_item_size();
    //1, collect all table item
    for(int64_t i = 0; OB_SUCC(ret) && i < num_from_items; ++i) {
      const FromItem &from_item = stmt->get_from_item(i);
      if (from_item.is_joined_) {
        JoinedTable *joined_table_item = stmt->get_joined_table(from_item.table_id_);
        if (OB_FAIL(collect_all_tableitem(stmt, joined_table_item, table_item_list))) {
          LOG_WARN("failed to collect table item", K(ret));
        }
      } else {
        TableItem *table_item = NULL;
        table_item = stmt->get_table_item_by_id(from_item.table_id_);
        if (OB_FAIL(collect_all_tableitem(stmt, table_item, table_item_list))) {
          LOG_WARN("failed to collect table item", K(ret));
          LOG_WARN("failed to collect table item", K(ret), K(*stmt));
        }
      }
    }

    //2, for each label security table item, add label column filter
    for (int64_t i = 0; OB_SUCC(ret) && i < table_item_list.count(); ++i) {
      TableItem *table_item = table_item_list.at(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (table_item->is_basic_table() || table_item->is_link_table()) {
        uint64_t table_ref_id = table_item->ref_id_;
        const ObTableSchema *table_schema = NULL;
        if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(), table_ref_id, table_schema, table_item->is_link_table()))) {
          LOG_WARN("failed to get table schema", K(table_ref_id), K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table should not be null", K(table_ref_id));
        } else if (table_schema->has_label_se_column()) {
          const ObIArray<uint64_t> &label_se_column_ids = table_schema->get_label_se_column_ids();
          for (int64_t j = 0; OB_SUCC(ret) && j < label_se_column_ids.count(); ++j) {
            const ObColumnSchemaV2 *column_schema = NULL;
            ObString policy_name;
            if (OB_ISNULL(column_schema = table_schema->get_column_schema(label_se_column_ids.at(j)))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema not exist", K(ret), K(label_se_column_ids.at(j)));
            } else if (OB_FAIL(ctx_->schema_checker_->get_label_se_policy_name_by_column_name(
                                 ctx_->session_info_->get_effective_tenant_id(),
                                 column_schema->get_column_name_str(),
                                 policy_name))) {
              LOG_WARN("fail to get policy name", K(ret));
            } else if (OB_FAIL(add_filter_for_label_se_table(*stmt, *table_item, policy_name, *column_schema))) {
              LOG_WARN("add filter for temporary table failed", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            trans_happened = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::add_filter_for_label_se_table(ObDMLStmt &stmt,
                                                         const TableItem &table_item,
                                                         const ObString &policy_name,
                                                         const ObColumnSchemaV2 &column_schema)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;

  if (OB_ISNULL(ctx_)
      || OB_ISNULL(session_info = ctx_->session_info_)
      || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("some parameter is NULL", K(ret), K(ctx_), K(expr_factory), K(session_info));
  }

  ObConstRawExpr *policy_name_expr = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*expr_factory,
                                                        ObVarcharType,
                                                        policy_name,
                                                        ObCharset::get_default_collation(ObCharset::get_default_charset()),
                                                        policy_name_expr))) {
      LOG_WARN("fail to build expr", K(ret));
    }
  }

  ObSysFunRawExpr *session_label_expr = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_LABEL_SE_SESSION_LABEL, session_label_expr))) {
      LOG_WARN("create expr failed", K(ret));
    } else {
      ObString func_name = ObString::make_string(N_OLS_SESSION_LABEL);
      session_label_expr->set_func_name(func_name);
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(session_label_expr->add_param_expr(policy_name_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      }
    }
  }

  ObColumnRefRawExpr *col_expr = NULL;
  if (OB_SUCC(ret)) {
    ColumnItem *col_item = NULL;
    if (NULL != (col_item = stmt.get_column_item_by_id(table_item.table_id_, column_schema.get_column_id()))) {
      col_expr = col_item->expr_;
      col_expr->set_explicited_reference();
    } else {
      if (OB_FAIL(ObRawExprUtils::build_column_expr(*expr_factory, column_schema, col_expr))) {
        LOG_WARN("fail to build column expr", K(ret));
      } else {
        col_expr->set_table_id(table_item.table_id_);
        col_expr->set_table_name(table_item.table_name_);
        col_expr->set_explicited_reference();
        ColumnItem column_item;
        column_item.expr_ = col_expr;
        column_item.table_id_ = col_expr->get_table_id();
        column_item.column_id_ = col_expr->get_column_id();
        column_item.column_name_ = col_expr->get_column_name();
        if (OB_FAIL(stmt.add_column_item(column_item))) {
          LOG_WARN("add column item to stmt failed", K(ret));
        }
      }
    }
  }
  ObSysFunRawExpr *label_cmp_expr = NULL;
  if (OB_SUCC(ret)) {
    ObString func_name = ObString::make_string(N_OLS_LABEL_VALUE_CMP_LE);

    if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_LABEL_SE_LABEL_VALUE_CMP_LE, label_cmp_expr))) {
      LOG_WARN("create expr failed", K(ret));
    } else {
      label_cmp_expr->set_func_name(func_name);
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(label_cmp_expr->add_param_expr(col_expr))) {
        LOG_WARN("fail to add param expr", K(ret));
      } else if (OB_FAIL(label_cmp_expr->add_param_expr(session_label_expr))) {
        LOG_WARN("fail to add parm expr", K(ret));
      }
    }
  }
  ObConstRawExpr *zero_int_expr = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*expr_factory, ObIntType, 0, zero_int_expr))) {
      LOG_WARN("fail to build expr", K(ret));
    }
  }
  ObRawExpr *equal_expr = NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::create_equal_expr(*expr_factory,
                                                  session_info,
                                                  label_cmp_expr,
                                                  zero_int_expr,
                                                  equal_expr))) {
      LOG_WARN("fail to create equal expr", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt.get_condition_exprs().push_back(equal_expr))) {
      LOG_WARN("failed to push back new filter", K(ret));
    } else {
      LOG_TRACE("add new filter succeed", K(stmt.get_condition_exprs()), K(*equal_expr));
    }
  }
  return ret;
}
#endif

/**
 * ObTransformPreProcess::transform_for_rls_table
 * generate predicate from rls policy on table and add them to the stmt
 * 1) add filter for select/update/delete
 *    select * from t1
 * => select * from t1 where ...
 *    update t1 set c1 = 1
 * => update t1 set c1 = 1 where ...
 *    delete from t1
 * => delete from t1 where ...
 * 2) if column-masking is enable, transform select item instead of add filter for select
 *    select c1 from t1
 * => select (case when ... then c1 else null end) c1 from t1
 * 3) if update_check is set, then add check constraint instead of adding filter for insert/update
 *    insert into t1 values(...)
 *    update t1 set c1 = 1
 */
int ObTransformPreProcess::transform_for_rls_table(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool exempt_rls_policy = false;
  bool contain_rls_table = false;
  ObSQLSessionInfo *session_info = NULL;
  ObSchemaChecker *schema_checker = NULL;
  trans_happened = false;
  if (OB_ISNULL(stmt)
      || OB_ISNULL(ctx_)
      || OB_ISNULL(schema_checker = ctx_->schema_checker_)
      || OB_ISNULL(session_info = ctx_->session_info_)
      || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret), K(stmt), K(ctx_), K(ret));
  } else if (OB_FAIL(check_exempt_rls_policy(exempt_rls_policy))) {
    LOG_WARN("failed to check exempt_rls_policy for rls", K(ret));
  } else {
    ObSEArray<TableItem*, 4> table_item_list;
    if (OB_FAIL(table_item_list.assign(stmt->get_table_items()))) {
      LOG_WARN("failed to assign table item list");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_item_list.count(); ++i) {
      TableItem *table_item = table_item_list.at(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if ((table_item->is_basic_table() &&
                  !table_item->is_system_table_ &&
                  !table_item->is_link_table()) ||
                 table_item->is_view_table_) {
        uint64_t table_ref_id = table_item->ref_id_;
        const ObTableSchema *table_schema = NULL;
        if (OB_FAIL(schema_checker->get_table_schema(session_info->get_effective_tenant_id(),
                                                     table_ref_id, table_schema))) {
          LOG_WARN("failed to get table schema", K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is null", K(table_ref_id), K(ret));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < table_schema->get_rls_policy_ids().count(); ++j) {
          const ObRlsPolicySchema *policy_schema = NULL;
          if (OB_FAIL(schema_checker->get_schema_guard()->get_rls_policy_schema_by_id(
                session_info->get_effective_tenant_id(),
                table_schema->get_rls_policy_ids().at(j),
                policy_schema))) {
            LOG_WARN("failed to get rls policy schema", K(ret));
          } else if (OB_ISNULL(policy_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("policy schema is null");
          } else if (!policy_schema->get_enable_flag()) {
            // do nothing
          } else if (FALSE_IT(contain_rls_table = true)) {
          } else if (exempt_rls_policy) {
            // do nothing
          } else if (OB_FAIL(transform_for_single_rls_policy(*stmt, *table_item, *policy_schema, trans_happened))) {
            LOG_WARN("failed to transform for single rls policy");
          }
        }
      }
    }
    if (OB_SUCC(ret) && contain_rls_table) {
      ObPCPrivInfo priv_info;
      priv_info.sys_priv_ = PRIV_ID_EXEMPT_ACCESS_POLICY;
      priv_info.has_privilege_ = exempt_rls_policy;
      if (OB_FAIL(stmt->get_query_ctx()->all_priv_constraints_.push_back(priv_info))) {
        LOG_WARN("failed to add rls policy constraint", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_exempt_rls_policy(bool &exempt_rls_policy)
{
  int ret = OB_SUCCESS;
  exempt_rls_policy = false;
  ObSQLSessionInfo *session_info = NULL;
  ObSchemaChecker *schema_checker = NULL;
  if (OB_ISNULL(ctx_)
      || OB_ISNULL(session_info = ctx_->session_info_)
      || OB_ISNULL(schema_checker = ctx_->schema_checker_)
      || OB_ISNULL(schema_checker->get_schema_guard())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ctx_), K(ret));
  } else if (!session_info->is_user_session() || session_info->is_tenant_changed()) {
    exempt_rls_policy = true;
  } else if (OB_FAIL(ObOraSysChecker::check_ora_user_sys_priv(*schema_checker->get_schema_guard(),
                                                        session_info->get_effective_tenant_id(),
                                                        session_info->get_priv_user_id(),
                                                        session_info->get_database_name(),
                                                        PRIV_ID_EXEMPT_ACCESS_POLICY,
                                                        session_info->get_enable_role_array()))) {
    if (OB_ERR_NO_PRIVILEGE == ret) {
      ret = OB_SUCCESS;
      LOG_TRACE("lack EXEMPT ACCESS POLICY priv, need trans");
    } else {
      LOG_WARN("failed to check ora user sys priv", K(ret));
    }
  } else {
    exempt_rls_policy = true;
  }
  return ret;
}

int ObTransformPreProcess::check_need_transform_column_level(ObDMLStmt &stmt,
                                                             const TableItem &table_item,
                                                             const ObRlsPolicySchema &policy_schema,
                                                             bool &need_trans)
{
  int ret = OB_SUCCESS;
  need_trans = false;
  if (!stmt.is_select_stmt() || !policy_schema.is_column_level_policy()) {
    // do nothing
  } else {
    const int64_t sec_column_count = policy_schema.get_sec_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && !need_trans && i < sec_column_count; ++i) {
      const ObRlsSecColumnSchema* rls_column = policy_schema.get_sec_column_by_idx(i);
      if (OB_ISNULL(rls_column)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null rls column", K(ret));
      } else if (NULL != stmt.get_column_item_by_id(table_item.table_id_,
                                                    rls_column->get_column_id())) {
        need_trans = true;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_single_rls_policy(ObDMLStmt &stmt,
                                                           TableItem &table_item,
                                                           const ObRlsPolicySchema &policy_schema,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObString predicate_str;
  ObRawExpr *expr = NULL;
  ObRawExpr *policy_expr = NULL;
  ObSEArray<ObQualifiedName, 1> columns;
  bool need_trans = true;
  if (OB_FAIL(calc_policy_function(stmt, table_item, policy_schema, policy_expr, predicate_str))) {
    LOG_WARN("failed to calc policy function", K(table_item), K(policy_schema), K(ret));
  } else if (OB_FAIL(build_policy_predicate_expr(predicate_str, table_item, columns, expr))) {
    LOG_WARN("failed to build predicate expr", K(predicate_str), K(ret));
  } else {
    switch (stmt.get_stmt_type()) {
    case stmt::T_SELECT: {
      bool need_filter = false;
      bool need_replace = false;
      if (!policy_schema.has_stmt_type_flag(RLS_POLICY_SELECT_FLAG)) {
        // do nothing
      } else if (!policy_schema.is_column_level_policy()) {
        need_filter = true;
      } else {
        if (check_need_transform_column_level(stmt, table_item, policy_schema, need_trans)) {
          LOG_WARN("failed to check need trans for column level policy", K(ret));
        } else if (!need_trans) {
          // do nothing
        } else if (policy_schema.has_stmt_type_flag(RLS_POLICY_SEC_ALL_ROWS_FLAG)) {
          need_replace = true;
        } else {
          need_filter = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (need_filter) {
          if (OB_FAIL(add_filter_for_rls_select(stmt, table_item, columns, expr))) {
            LOG_WARN("failed to add filter for rls table", K(ret));
          } else {
            trans_happened = true;
          }
        } else if (need_replace) {
          if (OB_FAIL(replace_expr_for_rls(stmt, table_item, policy_schema, columns, expr))) {
            LOG_WARN("failed to add modify expr for rls table", K(ret));
          } else {
            trans_happened = true;
          }
        }
      }
      break;
    }
    case stmt::T_DELETE: {
      if (!policy_schema.has_stmt_type_flag(RLS_POLICY_DELETE_FLAG)) {
        // do nothing
      } else if (OB_FAIL(add_filter_for_rls(stmt, table_item, columns, expr))) {
        LOG_WARN("failed to add filter for rls table", K(ret));
      } else {
        trans_happened = true;
      }
      break;
    }
    case stmt::T_UPDATE: {
      if (!policy_schema.has_stmt_type_flag(RLS_POLICY_UPDATE_FLAG)) {
        // do nothing
      } else if (OB_FAIL(add_filter_for_rls(stmt, table_item, columns, expr))) {
        LOG_WARN("failed to add filter for rls table", K(ret));
      } else if (policy_schema.get_check_opt()) {
        if (OB_FAIL(add_constraint_for_rls(stmt, table_item, columns, expr))) {
          LOG_WARN("failed to add filter for rls table", K(ret));
        } else {
          trans_happened = true;
        }
      } else {
        trans_happened = true;
      }
      break;
    }
    case stmt::T_INSERT:
    case stmt::T_INSERT_ALL: {
      if (!policy_schema.has_stmt_type_flag(RLS_POLICY_INSERT_FLAG)) {
        // do nothing
      } else if (OB_FAIL(add_constraint_for_rls(stmt, table_item, columns, expr))) {
        LOG_WARN("failed to add filter for rls table", K(ret));
      } else {
        trans_happened = true;
      }
      break;
    }
    case stmt::T_MERGE: {
      int64_t merge_flag = RLS_POLICY_INSERT_FLAG | RLS_POLICY_UPDATE_FLAG | RLS_POLICY_DELETE_FLAG;
      if (!policy_schema.has_partial_stmt_type_flag(merge_flag)) {
        // do nothing
      } else if (!policy_schema.has_stmt_type_flag(merge_flag)) {
        ret = OB_ERR_MERGE_INTO_WITH_POLICY;
        LOG_WARN("rls policy does not contain all flag of insert, update and delete", K(ret));
      } else if (OB_FAIL(add_filter_for_rls_merge(stmt, table_item, columns, expr))) {
        LOG_WARN("failed to add filter for rls table", K(ret));
      } else if (OB_FAIL(add_constraint_for_rls(stmt, table_item, columns, expr))) {
        LOG_WARN("failed to add filter for rls table", K(ret));
      } else {
        trans_happened = true;
      }
      break;
    }
    default:
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported stmt type", K(stmt.get_stmt_type()), K(ret));
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (!(policy_schema.has_stmt_type_flag(RLS_POLICY_STATIC_FLAG)
        || policy_schema.has_stmt_type_flag(RLS_POLICY_SHARE_STATIC_FLAG))
        && need_trans) {
      if (OB_FAIL(add_rls_policy_constraint(policy_expr, predicate_str))) {
        LOG_WARN("failed to add rls policy constraint", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::calc_policy_function(ObDMLStmt &stmt,
                                                const TableItem &table_item,
                                                const ObRlsPolicySchema &policy_schema,
                                                ObRawExpr *&policy_expr,
                                                ObString &predicate_str)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObSchemaChecker *schema_checker = NULL;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(ctx_)
      || OB_ISNULL(session_info = ctx_->session_info_)
      || OB_ISNULL(schema_checker = ctx_->schema_checker_)
      || OB_ISNULL(schema_checker->get_schema_guard())
      || OB_ISNULL(expr_factory = ctx_->expr_factory_)
      || OB_ISNULL(ctx_->exec_ctx_)
      || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ctx_), K(ret));
  } else if (OB_ISNULL(ctx_->exec_ctx_->get_sql_proxy())
             || OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ret));
  } else {
    ObConstRawExpr *object_schema_expr = NULL;
    ObConstRawExpr *object_name_expr = NULL;
    ObUDFRawExpr *udf_expr = NULL;
    ObUDFInfo udf_info;
    ObResolverParams params;
    ObObj result_obj;
    udf_info.udf_database_ = policy_schema.get_policy_function_schema();
    udf_info.udf_package_ = policy_schema.get_policy_package_name();
    udf_info.udf_name_ = policy_schema.get_policy_function_name();
    udf_info.udf_param_num_ = 2; // database_name and table_name
    params.schema_checker_ = schema_checker;
    params.session_info_ = session_info;
    params.allocator_ = ctx_->allocator_;
    params.expr_factory_ = expr_factory;
    params.sql_proxy_ = ctx_->exec_ctx_->get_sql_proxy();
    if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*expr_factory,
        ObVarcharType,
        table_item.database_name_,
        ObCharset::get_default_collation(ObCharset::get_default_charset()),
        object_schema_expr))) {
      LOG_WARN("failed to create object schema expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*expr_factory,
        ObVarcharType,
        table_item.get_table_name(),
        ObCharset::get_default_collation(ObCharset::get_default_charset()),
        object_name_expr))) {
      LOG_WARN("failed to create object name expr", K(ret));
    } else if (OB_FAIL(expr_factory->create_raw_expr(T_FUN_UDF, udf_expr))) {
      LOG_WARN("failed to create raw expr", K(ret));
    } else if (OB_FAIL(udf_expr->add_param_expr(object_schema_expr))) {
      LOG_WARN("failed to add param expr", K(object_schema_expr), K(ret));
    } else if (OB_FAIL(udf_expr->add_param_expr(object_name_expr))) {
      LOG_WARN("failed to add param expr", K(object_name_expr), K(ret));
    } else if (FALSE_IT(udf_info.ref_expr_ = udf_expr)) {
    } else if (OB_FAIL(ObRawExprUtils::resolve_udf_info(
        *params.allocator_, *params.expr_factory_, *params.session_info_, *params.schema_checker_->get_schema_guard(), udf_info))) {
      LOG_WARN("failed to init udf_info", K(udf_info), K(ret));
    } else if (OB_FAIL(udf_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize", K(ret));
    } else if (OB_FAIL(ObSQLUtils::calc_const_expr(*ctx_->exec_ctx_, udf_expr, result_obj,
                *ctx_->allocator_, ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store()))){
      LOG_WARN("faild to calc const epxr", K(ret));
    } else if (OB_UNLIKELY(!result_obj.is_varchar())) {
      ret = OB_ERR_POLICY_PREDICATE;
      LOG_WARN("invalid policy function result", K(result_obj), K(ret));
    } else if (OB_FAIL(result_obj.get_string(predicate_str))) {
      LOG_WARN("failed to get string", K(ret));
    } else {
      policy_expr = udf_expr;
    }
    if (OB_SUCC(ret) && udf_expr->need_add_dependency()) {
      ObSchemaObjVersion udf_version;
      OZ (udf_expr->get_schema_object_version(udf_version));
      OZ (stmt.add_global_dependency_table(udf_version));
    }
  }
  return ret;
}

int ObTransformPreProcess::build_policy_predicate_expr(const common::ObString &predicate_str,
                                                       const TableItem &table_item,
                                                       ObIArray<ObQualifiedName> &columns,
                                                       ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObRawExprFactory *expr_factory = NULL;
  const ParseNode *node = NULL;
  const ObColumnSchemaV2 *col_schema = NULL;
  if (OB_ISNULL(ctx_)
      || OB_ISNULL(session_info = ctx_->session_info_)
      || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null unexpected", K(ctx_), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_rls_predicate_expr(predicate_str, *expr_factory,
      *session_info, columns, expr))) {
    LOG_WARN("failed to build rls predicate exr", K(predicate_str));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName &q_name = columns.at(i);
    if (q_name.is_sys_func()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column expr", K(q_name), K(ret));
    } else if (OB_UNLIKELY(!q_name.database_name_.empty()
                           && 0 != q_name.database_name_.compare(table_item.database_name_))) {
      ret = OB_ERR_POLICY_FUNCTION;
      LOG_WARN("database name not match", K(q_name), K(table_item.database_name_), K(ret));
    } else if (OB_UNLIKELY(!q_name.tbl_name_.empty()
                           && 0 != q_name.tbl_name_.compare(table_item.table_name_))) {
      ret = OB_ERR_POLICY_FUNCTION;
      LOG_WARN("table name not match", K(q_name), K(table_item.table_name_), K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::add_rls_policy_constraint(const ObRawExpr *expr,
                                                     const common::ObString &predicate_str)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *str_expr = NULL;
  ObRawExpr *eq_expr = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*ctx_->expr_factory_,
                                ObVarcharType,
                                predicate_str,
                                ObCharset::get_default_collation(ObCharset::get_default_charset()),
                                str_expr))) {
    LOG_WARN("failed to build const string expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_,
                                                       ctx_->session_info_,
                                                       expr,
                                                       str_expr,
                                                       eq_expr))) {
    LOG_WARN("failed to create equal expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, eq_expr, true, true))) {
    LOG_WARN("failed to add bool constraint", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::build_rls_filter_expr(ObDMLStmt &stmt,
                                                 const TableItem &table_item,
                                                 const ObIArray<ObQualifiedName> &columns,
                                                 ObRawExpr *predicate_expr,
                                                 ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = NULL;
  if (OB_ISNULL(predicate_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ctx_->schema_checker_->get_table_schema(
      ctx_->session_info_->get_effective_tenant_id(), table_item.ref_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(table_item.ref_id_), K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table should not be null", K(table_item.ref_id_));
  } else {
    ObRawExprCopier copier(*ctx_->expr_factory_);
    for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
      const ObQualifiedName &q_name = columns.at(i);
      ColumnItem *col_item = NULL;
      ObColumnRefRawExpr *col_expr = NULL;
      const ObColumnSchemaV2 *column_schema = NULL;
      if (NULL != (col_item = stmt.get_column_item(table_item.table_id_, q_name.col_name_))) {
        col_expr = col_item->expr_;
      } else if (OB_ISNULL(column_schema = table_schema->get_column_schema(q_name.col_name_))) {
        ret = OB_ERR_POLICY_PREDICATE;
        LOG_WARN("not found column in table", K(q_name), K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_column_expr(*ctx_->expr_factory_, *column_schema,
                                                          col_expr))) {
        LOG_WARN("failed to build column expr", K(ret));
      } else {
        col_expr->set_table_name(table_item.table_name_);
        col_expr->set_table_id(table_item.table_id_);
        ColumnItem column_item;
        column_item.expr_ = col_expr;
        column_item.table_id_ = col_expr->get_table_id();
        column_item.column_id_ = col_expr->get_column_id();
        column_item.column_name_ = col_expr->get_column_name();
        if (OB_FAIL(stmt.add_column_item(column_item))) {
          LOG_WARN("add column item to stmt failed", K(ret), K(column_item));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(copier.add_replaced_expr(q_name.ref_expr_, col_expr))) {
        LOG_WARN("failed to add expr", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(copier.copy_on_replace(predicate_expr, expr))) {
      LOG_WARN("failed to copy on replace expr", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::build_rls_constraint_expr(const ObDmlTableInfo &table_info,
                                                     const ObIArray<ObQualifiedName> &columns,
                                                     ObRawExpr *predicate_expr,
                                                     ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExprCopier copier(*ctx_->expr_factory_);
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    const ObQualifiedName &q_name = columns.at(i);
    bool found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < table_info.column_exprs_.count(); ++j) {
      ObColumnRefRawExpr *col = table_info.column_exprs_.at(j);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(j), K(ret));
      } else if (0 != q_name.col_name_.compare(col->get_column_name())) {
        // do nothing
      } else if (OB_FAIL(copier.add_replaced_expr(q_name.ref_expr_, col))) {
        LOG_WARN("failed to add replaced expr", K(ret));
      } else {
        found = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!found)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column not found in column_conv exprs", K(q_name), K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(copier.copy_on_replace(predicate_expr, expr))) {
    LOG_WARN("failed to copy on replace expr", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::add_filter_for_rls_select(ObDMLStmt &stmt,
                                                     TableItem &table_item,
                                                     const ObIArray<ObQualifiedName> &columns,
                                                     ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  TableItem *view_table = NULL;
  ObSelectStmt *ref_query = NULL;
  TableItem *child_table = NULL;
  if (stmt.is_single_table_stmt()) {
    if (OB_FAIL(add_filter_for_rls(stmt, table_item, columns, expr))) {
      LOG_WARN("failed to add filter for rls table", K(ret));
    }
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               &stmt,
                                                               view_table,
                                                               &table_item))) {
    LOG_WARN("failed to create empty view table", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          &stmt,
                                                          view_table,
                                                          &table_item))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (!view_table->is_generated_table()
              || OB_ISNULL(ref_query = view_table->ref_query_)
              || !ref_query->is_single_table_stmt()
              || OB_ISNULL(child_table = ref_query->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected view table", K(ret), K(*view_table));
  } else if (OB_FAIL(add_filter_for_rls(*ref_query, *child_table, columns, expr))) {
    LOG_WARN("failed to add filter for rls table", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::add_filter_for_rls_merge(ObDMLStmt &stmt,
                                                    const TableItem &table_item,
                                                    const ObIArray<ObQualifiedName> &columns,
                                                    ObRawExpr *predicate_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  if (OB_UNLIKELY(!stmt.is_merge_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add rls constraint for invalid stmt type", K(stmt.get_stmt_type()), K(ret));
  } else {
    ObMergeStmt *merge_stmt = static_cast<ObMergeStmt *>(&stmt);
    if (OB_FAIL(build_rls_filter_expr(stmt, table_item, columns, predicate_expr, expr))) {
      LOG_WARN("failed to build filter expr", K(ret));
    } else if (OB_FAIL(merge_stmt->get_match_condition_exprs().push_back(expr))) {
      LOG_WARN("failed to add new condition", K(ret));
    } else {
      LOG_TRACE("add new filter succeed", K(merge_stmt->get_match_condition_exprs()), K(*expr));
    }
  }
  return ret;
}

int ObTransformPreProcess::add_filter_for_rls(ObDMLStmt &stmt,
                                              const TableItem &table_item,
                                              const ObIArray<ObQualifiedName> &columns,
                                              ObRawExpr *predicate_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  if (OB_FAIL(build_rls_filter_expr(stmt, table_item, columns, predicate_expr, expr))) {
    LOG_WARN("failed to build filter expr", K(ret));
  } else if (OB_FAIL(stmt.add_condition_expr(expr))) {
    LOG_WARN("failed to add new condition", K(ret));
  } else {
    LOG_TRACE("add new filter succeed", K(stmt.get_condition_exprs()), K(*expr));
  }
  return ret;
}

int ObTransformPreProcess::add_constraint_for_rls(ObDMLStmt &stmt,
                                                  const TableItem &table_item,
                                                  const ObIArray<ObQualifiedName> &columns,
                                                  ObRawExpr *predicate_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDmlTableInfo*, 2> table_infos;
  bool found = false;
  if (OB_ISNULL(predicate_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_UNLIKELY(!stmt.is_dml_write_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add rls constraint for invalid stmt type", K(stmt.get_stmt_type()), K(ret));
  } else if (OB_FAIL(static_cast<ObDelUpdStmt &>(stmt).get_dml_table_infos(table_infos))) {
    LOG_WARN("failed to get dml table info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
    ObDmlTableInfo* table_info = table_infos.at(i);
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(table_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null table_info", K(ret));
    } else if (table_info->loc_table_id_ != table_item.table_id_) {
      // do nothing
    } else if (OB_FAIL(build_rls_constraint_expr(*table_info, columns, predicate_expr, expr))) {
      LOG_WARN("failed to build rls constraint expr", K(ret));
    } else if (OB_FAIL(table_info->check_constraint_exprs_.push_back(expr))) {
      LOG_WARN("failed to add check constrinat", K(ret));
    } else {
      found = true;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!found)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item not found", K(table_item), K(ret));
  }

  return ret;
}

int ObTransformPreProcess::replace_expr_for_rls(ObDMLStmt &stmt,
                                                const TableItem &table_item,
                                                const ObRlsPolicySchema &policy_schema,
                                                const ObIArray<ObQualifiedName> &columns,
                                                ObRawExpr *predicate_expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  ObSEArray<ObRawExprPointer, 16> relation_expr_pointers;
  ObSEArray<ObRawExpr *, 1> old_exprs;
  ObSEArray<ObRawExpr *, 1> new_exprs;
  const int64_t sec_column_count = policy_schema.get_sec_column_count();
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(build_rls_filter_expr(stmt, table_item, columns, predicate_expr, expr))) {
    LOG_WARN("failed to build filter expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sec_column_count; ++i) {
    const ObRlsSecColumnSchema* rls_column = policy_schema.get_sec_column_by_idx(i);
    ColumnItem *column_item = NULL;
    if (OB_ISNULL(rls_column)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null rls column", K(ret));
    } else if (NULL != (column_item = stmt.get_column_item_by_id(table_item.table_id_,
                                                                 rls_column->get_column_id()))) {
      ObRawExpr *old_expr = column_item->expr_;
      ObRawExpr *new_expr = NULL;
      ObRawExpr *when_expr = NULL;
      ObRawExpr *null_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, null_expr))) {
        LOG_WARN("failed to build null expr", K(ret));
      } else if (FALSE_IT(when_expr = expr)) {
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(*ctx_->expr_factory_,
                                                              when_expr,
                                                              old_expr,
                                                              null_expr,
                                                              new_expr))) {
        LOG_WARN("failed to build case when expr", K(ret));
      } else if (OB_FAIL(old_exprs.push_back(old_expr)) || OB_FAIL(new_exprs.push_back(new_expr))) {
        LOG_WARN("failed to push expr", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(stmt.replace_relation_exprs(old_exprs, new_exprs))) {
    LOG_WARN("stmt replace inner expr failed", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::get_first_level_output_exprs(
    ObSelectStmt *sub_stmt,
    common::ObIArray<ObRawExpr*>& inner_aggr_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else {
    common::ObIArray<ObAggFunRawExpr*> &aggr_exprs = sub_stmt->get_aggr_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_exprs.count(); i++) {
      if (OB_ISNULL(aggr_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr of aggr expr is null", K(ret));
      } else if (aggr_exprs.at(i)->is_nested_aggr()) {
        /*do nothing.*/
      } else {
        int64_t N = aggr_exprs.at(i)->get_param_count();
        for (int64_t j = 0; OB_SUCC(ret) && j < N; ++j) {
          if (OB_ISNULL(aggr_exprs.at(i)->get_param_expr(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (aggr_exprs.at(i)->get_param_expr(j)->is_const_expr()) {
            //do nothing
          } else if (OB_FAIL(add_var_to_array_no_dup(inner_aggr_exprs,
                                                     aggr_exprs.at(i)->get_param_expr(j)))) {
            LOG_WARN("failed to to add var to array no dup.", K(ret));
          } else { /*do nothing.*/ }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::generate_child_level_aggr_stmt(ObSelectStmt *select_stmt,
                                                          ObSelectStmt *&sub_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> complex_aggr_exprs;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt<ObSelectStmt>(sub_stmt))) {
    LOG_WARN("failed to create stmt.", K(ret));
  } else if (FALSE_IT(sub_stmt->set_query_ctx(select_stmt->get_query_ctx()))) {
  } else if (OB_FAIL(sub_stmt->deep_copy(*ctx_->stmt_factory_,
                                         *ctx_->expr_factory_,
                                         *select_stmt))) {
    LOG_WARN("failed to deep copy from nested stmt.", K(ret));
  } else if (OB_FAIL(sub_stmt->adjust_statement_id(ctx_->allocator_,
                                                   ctx_->src_qb_name_,
                                                   ctx_->src_hash_val_))) {
    LOG_WARN("failed to recursive adjust statement id", K(ret));
  } else if (OB_FAIL(get_first_level_output_exprs(sub_stmt,
                                                  complex_aggr_exprs))) {
    LOG_WARN("failed to extract levels aggr.", K(ret));
  } else {
    sub_stmt->get_aggr_items().reset();
    sub_stmt->get_select_items().reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sub_stmt->get_having_exprs().count(); ++i) {
    if (OB_FAIL(ObTransformUtils::extract_aggr_expr(sub_stmt->get_having_exprs().at(i),
                                                    sub_stmt->get_aggr_items()))) {
      LOG_WARN("failed to extract aggr exprs.", K(ret));
    } else { /*do nothing.*/ }
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < complex_aggr_exprs.count(); ++j) {
    if (OB_FAIL(ObTransformUtils::extract_aggr_expr(complex_aggr_exprs.at(j),
                                                    sub_stmt->get_aggr_items()))) {
      LOG_WARN("failed to extract aggr exprs.", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                            complex_aggr_exprs.at(j),
                                                            sub_stmt))) {
      LOG_WARN("failed to push back into select item array.", K(ret));
    } else { /*do nothing.*/ }
  }

  if (OB_SUCC(ret)) {
    //try transform_groupingsets_rollup_cube:
    //  SELECT count(sum(c1)) FROM t1 GROUP BY GROUPING sets(c1, c2);
    bool is_happened = false;
    ObDMLStmt *dml_stmt = static_cast<ObDMLStmt *>(sub_stmt);
    if (OB_FAIL(transform_groupingsets_rollup_cube(dml_stmt, is_happened))) {
      LOG_WARN("failed to transform for transform for grouping sets, rollup and cube.", K(ret));
    } else if (is_happened) {
      sub_stmt = static_cast<ObSelectStmt *>(dml_stmt);
      LOG_TRACE("succeed to transform for grouping sets and rollup",K(is_happened), K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObTransformPreProcess::remove_nested_aggr_exprs(ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    ObSEArray<ObAggFunRawExpr *, 4> aggr_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_aggr_items().count(); i++) {
      if (OB_ISNULL(stmt->get_aggr_items().at(i))) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("expr of aggr expr is null", K(ret));
      } else if (stmt->get_aggr_items().at(i)->is_nested_aggr()) {
       /*do nothing.*/
      } else if (OB_FAIL(aggr_exprs.push_back(stmt->get_aggr_items().at(i)))) {
       LOG_WARN("failed to assign to inner aggr exprs.", K(ret));
      } else { /*do nothing.*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->get_aggr_items().assign(aggr_exprs))) {
        LOG_WARN("failed to extract second aggr items.", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::construct_column_items_from_exprs(
    const ObIArray<ObRawExpr*> &column_exprs,
    ObIArray<ColumnItem> &column_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_exprs.count(); ++i) {
    ColumnItem column_item;
    ObColumnRefRawExpr* expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(i));
    column_item.expr_ = expr;
    column_item.table_id_ = expr->get_table_id();
    column_item.column_id_ = expr->get_column_id();
    column_item.column_name_ = expr->get_expr_name();
    if (OB_FAIL(column_items.push_back(column_item))) {
      LOG_WARN("failed to push back into temp column items.", K(ret));
    } else { /*do nothing.*/ }
  }
  return ret;
}

int ObTransformPreProcess::generate_parent_level_aggr_stmt(ObSelectStmt *&select_stmt,
                                                           ObSelectStmt *sub_stmt)
{
  int ret = OB_SUCCESS;
  TableItem *view_table_item = NULL;
  ObSEArray<ObRawExpr *, 4> old_exprs;
  ObSEArray<ObRawExpr *, 4> new_exprs;
  ObSEArray<ColumnItem, 4> column_items;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else {
    select_stmt->get_table_items().reset();
    select_stmt->get_joined_tables().reset();
    select_stmt->get_from_items().reset();
    select_stmt->get_order_items().reset();
    select_stmt->get_column_items().reset();
    select_stmt->get_condition_exprs().reset();
    select_stmt->get_part_exprs().reset();
    select_stmt->get_check_constraint_items().reset();
    select_stmt->get_group_exprs().reset();
    select_stmt->get_rollup_exprs().reset();
    select_stmt->get_grouping_sets_items().reset();
    select_stmt->get_rollup_items().reset();
    select_stmt->get_cube_items().reset();
    select_stmt->get_having_exprs().reset();
    if (OB_FAIL(get_first_level_output_exprs(select_stmt,
                                             old_exprs))) {
      LOG_WARN("failed to get column exprs from stmt from.", K(ret));
    } else if (OB_FAIL(remove_nested_aggr_exprs(select_stmt))) {
      LOG_WARN("failed to extract second aggr items.", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_,
                                                            select_stmt,
                                                            sub_stmt,
                                                            view_table_item))) {
      LOG_WARN("failed to add new table item.", K(ret));
    } else if (OB_FAIL(select_stmt->add_from_item(view_table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                                 *view_table_item,
                                                                 select_stmt,
                                                                 new_exprs))) {
      LOG_WARN("failed to get select exprs from grouping sets view.", K(ret));
    } else if (OB_FAIL(construct_column_items_from_exprs(new_exprs, column_items))) {
      LOG_WARN("failed to construct column items from exprs.", K(ret));
    } else if (OB_FAIL(replace_group_id_in_stmt(select_stmt))) {
      LOG_WARN("fail to replace group_id in nested aggr");
    } else if (OB_FAIL(select_stmt->replace_relation_exprs(old_exprs, new_exprs))) {
      LOG_WARN("failed to replace inner stmt exprs.", K(ret));
    } else if (OB_FAIL(select_stmt->get_column_items().assign(column_items))) {
      LOG_WARN("failed to assign column items.", K(ret));
    } else if (OB_FAIL(select_stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash.", K(ret));
    } else if (OB_FAIL(select_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column items rel id.", K(ret));
    } else if (OB_FAIL(select_stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalized stmt.", K(ret));
    } else { /*do nothing.*/ }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_nested_aggregate(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(ctx_));
  } else if (!stmt->is_select_stmt()) {
    /*do nothing.*/
  } else {
    ObSelectStmt *sub_stmt = NULL;
    ObSelectStmt *select_stmt = static_cast<ObSelectStmt *>(stmt);
    /**
     * 本函数将含有嵌套聚合的stmt改写成两层stmt
     * select sum(b), max(sum(b)) from t1 group by b;
     * 以上sql可以改写成
     * select sum(v.b), max(v.sum_b)
     * from (
     *      select b, sum(b) as sum_b
     *      from t1
     *      group by b
     *      ) v
     * 其中generate_child_level_aggr_stmt函数生成视图v
     * generate_parent_level_aggr_stmt生成外部stmt
     */
    if (!select_stmt->contain_nested_aggr()) {
      /*do nothing.*/
    } else if (OB_FAIL(generate_child_level_aggr_stmt(select_stmt,
                                                      sub_stmt))) {
      LOG_WARN("failed to generate first level aggr stmt.", K(ret));
    } else if (OB_FAIL(generate_parent_level_aggr_stmt(select_stmt,
                                                       sub_stmt))) {
      LOG_WARN("failed to generate nested aggr stmt.", K(ret));
    } else if (OB_FAIL(select_stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt.", K(ret));
    } else {
      trans_happened = true;
      stmt = select_stmt;
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_group_id_in_stmt(ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    // replace group_id in select list;
    ObIArray<SelectItem> &sel_items = stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_items.count(); i++) {
      if (OB_ISNULL(sel_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr of select item shouldn't be null", K(ret));
      } else if (OB_FAIL(replace_group_id_in_expr_recursive(sel_items.at(i).expr_))) {
        LOG_WARN("fail to replace expr in selet items", K(ret));
      }
    }
    // replace group_id in having exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_having_expr_size(); i++) {
      if (OB_ISNULL(stmt->get_having_exprs().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("having expr shouldn't be null", K(ret));
      } else if (OB_FAIL(replace_group_id_in_expr_recursive(stmt->get_having_exprs().at(i)))) {
        LOG_WARN("fail to replace expr in having exprs", K(ret));
      }
    }
    // replace group_id in order by exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_order_item_size(); i++) {
      if (OB_ISNULL(stmt->get_order_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("having expr shouldn't be null", K(ret));
      } else if (OB_FAIL(replace_group_id_in_expr_recursive(stmt->get_order_item(i).expr_))) {
        LOG_WARN("fail to replace expr in having exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_group_id_in_expr_recursive(ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", K(ret));
  } else {
    if (expr->is_aggr_expr()) {
      if (expr->get_expr_type() == T_FUN_GROUP_ID) {
        // replace group id with 0;
        ObConstRawExpr *c_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                         ObIntType,
                                                         0,
                                                         c_expr))) {
          LOG_WARN("fail to build const expr", K(ret));
        } else if (OB_ISNULL(c_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("c_expr is null", K(ret));
        } else {
          expr = c_expr;
        }
      }
    } else {
      // recursive check hierarchical expr(exclude aggr_expr);
      ObRawExpr *tmp_expr = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
        if (OB_ISNULL(tmp_expr = expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get param expr is null", K(ret));
        } else if (OB_FAIL(replace_group_id_in_expr_recursive(tmp_expr))) {
          LOG_WARN("fail to replace group id in expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_merge_into(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->get_stmt_type() != stmt::T_MERGE) {
    /*do nothing*/
  } else if (OB_ISNULL(stmt->get_query_ctx()) ||
             OB_UNLIKELY(stmt->get_table_size() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(stmt), K(ret));
  } else {
    const int64_t TARGET_TABLE_IDX = 0;
    const int64_t SOURCE_TABLE_IDX = 1;
    TableItem *new_table = NULL;
    ObMergeStmt *merge_stmt = static_cast<ObMergeStmt*>(stmt);
    TableItem *target_table = stmt->get_table_item(TARGET_TABLE_IDX);
    TableItem *source_table = stmt->get_table_item(SOURCE_TABLE_IDX);
    bool is_valid = false;
    if (OB_ISNULL(target_table) || OB_ISNULL(source_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table item", K(target_table), K(source_table), K(ret));
    } else if (OB_FAIL(check_can_transform_insert_only_merge_into(merge_stmt,
                                                                  is_valid))) {
      LOG_WARN("failed to check can transform insert only merge into", K(ret));
    } else if (is_valid) {
      ObDMLStmt* insert_stmt = NULL;
      if (OB_FAIL(transform_insert_only_merge_into(stmt, insert_stmt))) {
        LOG_WARN("failed to transform for insert only merge into", K(ret));
      } else {
        stmt = insert_stmt;
        trans_happened = true;
      }
    } else if (merge_stmt->has_update_clause() && !merge_stmt->has_insert_clause()) {
      if (OB_FAIL(transform_update_only_merge_into(stmt))) {
        LOG_WARN("failed to transform for update only merge into", K(ret));
      } else {
        trans_happened = true;
      }
    } else if (ObTransformUtils::add_new_joined_table(ctx_,
                                                      *merge_stmt,
                                                      LEFT_OUTER_JOIN,
                                                      source_table,
                                                      target_table,
                                                      merge_stmt->get_match_condition_exprs(),
                                                      new_table,
                                                      true)) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (OB_ISNULL(new_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new join table is NULL", K(ret));
    } else if (OB_FAIL(merge_stmt->add_from_item(new_table->table_id_,
                                                 new_table->is_joined_table()))) {
      LOG_WARN("fail to add from item", K(new_table), K(ret));
    } else if (OB_FAIL(transform_merge_into_subquery(merge_stmt))) {
      LOG_WARN("failed to transform merge into subquery", K(ret));
    } else {
      merge_stmt->get_match_condition_exprs().reset();
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_merge_into_subquery(ObMergeStmt *merge_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObRawExpr *matched_expr = NULL;
  ObRawExpr *not_matched_expr = NULL;
  bool update_has_subquery = false;
  bool insert_has_subquery = false;
  ObSEArray<ObRawExpr*, 8> condition_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> target_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> delete_subquery_exprs;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(merge_stmt));
  } else if (!merge_stmt->has_subquery()) {
    // do nothing
  } else if (OB_FAIL(create_matched_expr(*merge_stmt, matched_expr, not_matched_expr))) {
    LOG_WARN("failed to build matched expr", K(ret));
  } else if (OB_FAIL(get_update_insert_condition_subquery(merge_stmt,
                                                          matched_expr,
                                                          not_matched_expr,
                                                          update_has_subquery,
                                                          insert_has_subquery,
                                                          condition_subquery_exprs))) {
    LOG_WARN("failed to allocate update insert condition subquery", K(ret));
  } else if (OB_FAIL(get_update_insert_target_subquery(merge_stmt,
                                                       matched_expr,
                                                       not_matched_expr,
                                                       update_has_subquery,
                                                       insert_has_subquery,
                                                       target_subquery_exprs))) {
    LOG_WARN("failed to allocate update insert target subquery", K(ret));
  } else if (OB_FAIL(get_delete_condition_subquery(merge_stmt,
                                                   matched_expr,
                                                   update_has_subquery,
                                                   delete_subquery_exprs))) {
    LOG_WARN("failed to allocate delete condition subquery", K(ret));
  } else if (OB_FAIL(merge_stmt->formalize_stmt_expr_reference())) {
    LOG_WARN("failed to formalize stmt expr reference", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::create_matched_expr(ObMergeStmt &stmt,
                                                ObRawExpr *&matched_flag,
                                                ObRawExpr *&not_matched_flag)
{
  int ret = OB_SUCCESS;
  ObMergeTableInfo& table_info = stmt.get_merge_table_info();
  ObRawExpr *not_null_expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_info.column_exprs_.count(); ++i) {
    ObColumnRefRawExpr *column = NULL;
    if (OB_ISNULL(column = table_info.column_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else if (column->is_not_null_for_read()) {
      not_null_expr = column;
      break;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("context is invalid", K(ret), K(ctx_));
    } else if (OB_ISNULL(not_null_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to find not null expr from merge into target table", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(
                         *ctx_->expr_factory_,
                         not_null_expr,
                         true,
                         matched_flag))) {
      LOG_WARN("failed to build is not null expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(
                         *ctx_->expr_factory_,
                         not_null_expr,
                         false,
                         not_matched_flag))) {
      LOG_WARN("failed to build is null expr", K(ret));
    } else if (OB_FAIL(matched_flag->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize matched expr", K(ret));
    } else if (OB_FAIL(not_matched_flag->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize not matched flag", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::get_update_insert_condition_subquery(ObMergeStmt *merge_stmt,
                                                                ObRawExpr *matched_expr,
                                                                ObRawExpr *not_matched_expr,
                                                                bool &update_has_subquery,
                                                                bool &insert_has_subquery,
                                                                ObIArray<ObRawExpr*> &new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  bool where_has_subquery = false;
  update_has_subquery = false;
  insert_has_subquery = false;
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(merge_stmt), K(ctx_));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(merge_stmt->get_update_condition_exprs(),
                                                                  update_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(merge_stmt->get_insert_condition_exprs(),
                                                                  insert_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(merge_stmt->get_condition_exprs(),
                                                                  where_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (update_has_subquery &&
             OB_FAIL(generate_merge_conditions_subquery(matched_expr,
                                                        merge_stmt->get_update_condition_exprs()))) {
    LOG_WARN("failed to generate condition subquery", K(ret));
  } else if (insert_has_subquery &&
             OB_FAIL(generate_merge_conditions_subquery(not_matched_expr,
                                                        merge_stmt->get_insert_condition_exprs()))) {
    LOG_WARN("failed to generate condition subquery", K(ret));
  } else if (where_has_subquery &&
             OB_FAIL(generate_merge_conditions_subquery(matched_expr,
                                                        merge_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to generate condition subquery", K(ret));
  } else if (update_has_subquery &&
             OB_FAIL(append(new_subquery_exprs, merge_stmt->get_update_condition_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else if (insert_has_subquery &&
             OB_FAIL(append(new_subquery_exprs, merge_stmt->get_insert_condition_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else if (where_has_subquery &&
             OB_FAIL(append(new_subquery_exprs, merge_stmt->get_condition_exprs()))) {
    LOG_WARN("failed to append subquery exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
      ObRawExpr *raw_expr = NULL;
      if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(raw_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize case expr", K(ret));
      } else if (OB_FAIL(raw_expr->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformPreProcess::generate_merge_conditions_subquery(ObRawExpr *matched_expr,
                                                              ObIArray<ObRawExpr*> &condition_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *and_expr = NULL;
  ObSEArray<ObRawExpr *, 4> param_conditions;
  ObSEArray<ObRawExpr*, 8> subquery_exprs;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(matched_expr) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(condition_exprs, subquery_exprs))) {
    LOG_WARN("failed to classify subquery exprs", K(ret));
  } else if (subquery_exprs.empty()) {
    /*do nothing*/
  } else if (OB_FAIL(param_conditions.push_back(matched_expr)) ||
             OB_FAIL(append(param_conditions, condition_exprs))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory, param_conditions, and_expr))) {
    LOG_WARN("failed to build matched expr", K(ret));
  } else if (OB_ISNULL(and_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    condition_exprs.reuse();
    if (OB_FAIL(condition_exprs.push_back(and_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObTransformPreProcess::get_update_insert_target_subquery(ObMergeStmt *merge_stmt,
                                                             ObRawExpr *matched_expr,
                                                             ObRawExpr *not_matched_expr,
                                                             bool update_has_subquery,
                                                             bool insert_has_subquery,
                                                             ObIArray<ObRawExpr*> &new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExpr *null_expr = NULL;
  ObSEArray<ObRawExpr*, 8> temp_exprs;
  ObSEArray<ObRawExpr*, 8> assign_exprs;
  ObSEArray<ObRawExpr*, 8> update_subquery_exprs;
  ObSEArray<ObRawExpr*, 8> insert_values_subquery_exprs;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(merge_stmt->get_assignments_exprs(assign_exprs))) {
    LOG_WARN("failed to get table assignment exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(assign_exprs,
                                                         update_subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(merge_stmt->get_values_vector(),
                                                         insert_values_subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*expr_factory, null_expr))) {
    LOG_WARN("failed to build null expr", K(ret));
  } else if (OB_ISNULL(null_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(null_expr), K(ret));
  } else {
    if (!update_subquery_exprs.empty()) {
      ObRawExpr *update_matched_expr = NULL;
      if (merge_stmt->get_update_condition_exprs().empty()) {
        update_matched_expr = matched_expr;
      } else if (update_has_subquery) {
        if (OB_UNLIKELY(1 != merge_stmt->get_update_condition_exprs().count()) ||
            OB_ISNULL(merge_stmt->get_update_condition_exprs().at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret));
        } else {
          update_matched_expr = merge_stmt->get_update_condition_exprs().at(0);
        }
      } else if (OB_FAIL(temp_exprs.push_back(matched_expr)) ||
                 OB_FAIL(append(temp_exprs, merge_stmt->get_update_condition_exprs()))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory,
                                                        temp_exprs,
                                                        update_matched_expr))) {
        LOG_WARN("failed to build and expr", K(ret));
      } else if (OB_ISNULL(update_matched_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else { /*do nothing*/ }

      for (int64_t i = 0; OB_SUCC(ret) && i < update_subquery_exprs.count(); i++) {
        ObRawExpr *raw_expr = NULL;
        ObRawExpr *cast_expr = NULL;
        ObRawExpr *case_when_expr = NULL;
        if (OB_ISNULL(raw_expr = update_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                              ctx_->expr_factory_,
                              ctx_->session_info_,
                              *null_expr,
                              raw_expr->get_result_type(),
                              cast_expr))) {
          LOG_WARN("try add cast expr above failed", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(*expr_factory,
                                                                update_matched_expr,
                                                                raw_expr,
                                                                cast_expr,
                                                                case_when_expr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (OB_ISNULL(case_when_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(new_subquery_exprs.push_back(case_when_expr))) {
          LOG_WARN("failed to push back case when expr", K(ret));
        } else { /*do nothing*/ }
      }
    }
    if (OB_SUCC(ret) && !insert_values_subquery_exprs.empty()) {
      ObRawExpr *insert_matched_expr = NULL;
      if (merge_stmt->get_insert_condition_exprs().empty()) {
        insert_matched_expr = not_matched_expr;
      } else if (insert_has_subquery) {
        if (OB_UNLIKELY(1 != merge_stmt->get_insert_condition_exprs().count()) ||
            OB_ISNULL(merge_stmt->get_insert_condition_exprs().at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          insert_matched_expr = merge_stmt->get_insert_condition_exprs().at(0);
        }
      } else if (OB_FAIL(temp_exprs.push_back(not_matched_expr)) ||
                 OB_FAIL(temp_exprs.assign(merge_stmt->get_insert_condition_exprs()))) {
        LOG_WARN("failed to fill and child exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory, temp_exprs,
                                                        insert_matched_expr))) {
        LOG_WARN("failed to build and expr", K(ret));
      } else if (OB_ISNULL(insert_matched_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else { /*do nothing*/ }

      for (int64_t i = 0; OB_SUCC(ret) && i < insert_values_subquery_exprs.count(); i++) {
        ObRawExpr *raw_expr = NULL;
        ObRawExpr *case_when_expr = NULL;
        if (OB_ISNULL(raw_expr = insert_values_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(*expr_factory,
                                                                insert_matched_expr,
                                                                raw_expr,
                                                                null_expr,
                                                                case_when_expr))) {
          LOG_WARN("failed to build case when expr", K(ret));
        } else if (OB_ISNULL(case_when_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(new_subquery_exprs.push_back(case_when_expr))) {
          LOG_WARN("failed to push back subquery exprs", K(ret));
        } else { /*do nothing*/ }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
      ObRawExpr *raw_expr = NULL;
      if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(raw_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize case expr", K(ret));
      } else if (OB_FAIL(raw_expr->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret) && !new_subquery_exprs.empty()) {
      ObSEArray<ObRawExpr*, 8> old_subquery_exprs;
      if (OB_FAIL(append(old_subquery_exprs, update_subquery_exprs)) ||
          OB_FAIL(append(old_subquery_exprs, insert_values_subquery_exprs))) {
        LOG_WARN("failed to append exprs", K(ret));
      } else if (OB_UNLIKELY(old_subquery_exprs.count() != new_subquery_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected array count", K(old_subquery_exprs.count()),
            K(new_subquery_exprs.count()), K(ret));
      } else if (OB_FAIL(merge_stmt->replace_relation_exprs(old_subquery_exprs, new_subquery_exprs))) {
        LOG_WARN("failed to replace merge stmt", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObTransformPreProcess::get_delete_condition_subquery(ObMergeStmt *merge_stmt,
                                                         ObRawExpr *matched_expr,
                                                         bool update_has_subquery,
                                                         ObIArray<ObRawExpr*> &new_subquery_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  bool delete_has_subquery = false;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::check_expr_contain_subquery(merge_stmt->get_delete_condition_exprs(),
                                                                  delete_has_subquery))) {
    LOG_WARN("failed to check whether expr contain subquery", K(ret));
  } else if (!delete_has_subquery) {
    /*do nothing*/
  } else {
    ObSqlBitSet<> check_table_set;
    ObSEArray<ObRawExpr*, 8> temp_exprs;
    ObRawExpr *delete_matched_expr = NULL;
    ObSEArray<ObRawExpr*, 8> all_column_exprs;
    ObSEArray<ObRawExpr*, 8> delete_column_exprs;
    if (OB_FAIL(check_table_set.add_member(merge_stmt->get_table_bit_index(
                                           merge_stmt->get_target_table_id())))) {
      LOG_WARN("failed to add table set", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(merge_stmt->get_delete_condition_exprs(),
                                                            all_column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*merge_stmt,
                                                             all_column_exprs,
                                                             check_table_set,
                                                             delete_column_exprs))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (delete_column_exprs.empty()) {
      /*do nothing*/
    } else {
      ObSEArray<ObRawExpr*, 8> old_exprs;
      ObSEArray<ObRawExpr*, 8> new_exprs;
      ObAssignments &table_assigns  = merge_stmt->get_merge_table_info().assignments_;
      for (int64_t j = 0; OB_SUCC(ret) && j < table_assigns.count(); j++) {
        if (OB_ISNULL(table_assigns.at(j).column_expr_) ||
            OB_ISNULL(table_assigns.at(j).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!ObOptimizerUtil::find_item(delete_column_exprs,
                                               table_assigns.at(j).column_expr_)) {
          /*do nothing*/
        } else if (table_assigns.at(j).expr_->has_flag(CNT_SUB_QUERY) ||
                   table_assigns.at(j).expr_->has_flag(CNT_ONETIME)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not support replace column expr with subquery expr", K(ret));
        } else if (OB_FAIL(old_exprs.push_back(table_assigns.at(j).column_expr_)) ||
                   OB_FAIL(new_exprs.push_back(table_assigns.at(j).expr_))) {
          LOG_WARN("failed to push back exprs", K(ret));
        }  else { /*do nothing*/ }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObTransformUtils::replace_exprs(old_exprs,
                                                    new_exprs,
                                                    merge_stmt->get_delete_condition_exprs()))) {
          LOG_WARN("failed to replace exprs", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (merge_stmt->get_update_condition_exprs().empty()) {
      delete_matched_expr = matched_expr;
    } else if (update_has_subquery) {
      if (OB_UNLIKELY(1 != merge_stmt->get_update_condition_exprs().count()) ||
          OB_ISNULL(merge_stmt->get_update_condition_exprs().at(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret));
      } else {
        delete_matched_expr = merge_stmt->get_update_condition_exprs().at(0);
      }
    } else if (OB_FAIL(temp_exprs.push_back(matched_expr)) ||
               OB_FAIL(append(temp_exprs, merge_stmt->get_update_condition_exprs()))) {
      LOG_WARN("failed to push back exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory,
                                                      temp_exprs,
                                                      delete_matched_expr))) {
      LOG_WARN("failed to build matched expr", K(ret));
    } else if (OB_ISNULL(delete_matched_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else { /*do nothing*/}

    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if (OB_FAIL(generate_merge_conditions_subquery(delete_matched_expr,
                                                          merge_stmt->get_delete_condition_exprs()))) {
      LOG_WARN("failed to generate merge conditions", K(ret));
    } else if (OB_FAIL(append(new_subquery_exprs, merge_stmt->get_delete_condition_exprs()))) {
      LOG_WARN("failed to append new exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < new_subquery_exprs.count(); i++) {
        ObRawExpr *raw_expr = NULL;
        if (OB_ISNULL(raw_expr = new_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(raw_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize case expr", K(ret));
        } else if (OB_FAIL(raw_expr->pull_relation_id())) {
          LOG_WARN("failed to pull relation id and levels", K(ret));
        } else { /*do nothing*/ }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_update_only_merge_into(ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  ObMergeStmt *merge_stmt = NULL;
  const int64_t TARGET_TABLE_IDX = 0;
  const int64_t SOURCE_TABLE_IDX = 1;
  TableItem *target_table = NULL;
  TableItem *source_table = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (stmt->get_stmt_type() != stmt::T_MERGE) {
    /*do nothing*/
  } else if (OB_FALSE_IT(merge_stmt = static_cast<ObMergeStmt*>(stmt))) {
  } else if (OB_ISNULL(source_table = merge_stmt->get_table_item(SOURCE_TABLE_IDX)) ||
             OB_ISNULL(target_table = merge_stmt->get_table_item(TARGET_TABLE_IDX))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(stmt));
  } else if (OB_FAIL(merge_stmt->add_from_item(source_table->table_id_, source_table->is_joined_table()))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(merge_stmt->add_from_item(target_table->table_id_, target_table->is_joined_table()))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(merge_stmt->get_condition_exprs().assign(merge_stmt->get_match_condition_exprs()))) {
    LOG_WARN("failed to assign match condition exprs", K(ret));
  } else if (OB_FAIL(append(merge_stmt->get_condition_exprs(), merge_stmt->get_update_condition_exprs()))) {
    LOG_WARN("failed to append condition exprs", K(ret));
  } else if (OB_FALSE_IT(merge_stmt->get_match_condition_exprs().reset())) {
  } else if (OB_FALSE_IT(merge_stmt->get_update_condition_exprs().reset())) {
  } else if (OB_FAIL(transform_merge_into_subquery(merge_stmt))) {
    LOG_WARN("failed to transform merge into subquery", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::create_source_view_for_merge_into(ObMergeStmt *merge_stmt, TableItem *&view_table)
{
  int ret = OB_SUCCESS;
  const int64_t SOURCE_TABLE_IDX = 1;
  TableItem *source_table = NULL;
  ObSEArray<ObRawExpr*, 8> insert_values_subquery_exprs;
  if (OB_ISNULL(merge_stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(merge_stmt), K(ctx_));
  } else if (OB_ISNULL(source_table = merge_stmt->get_table_item(SOURCE_TABLE_IDX))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(merge_stmt), K(ctx_));
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               merge_stmt,
                                                               view_table,
                                                               source_table))) {
    LOG_WARN("failed to create empty view table", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          merge_stmt,
                                                          view_table,
                                                          source_table))) {
    LOG_WARN("failed to create view with table", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_subquery_exprs(merge_stmt->get_values_vector(),
                                                         insert_values_subquery_exprs))) {
    LOG_WARN("failed to get subquery exprs", K(ret));
  } else if (!insert_values_subquery_exprs.empty()) {
    ObSelectStmt* view_stmt = view_table->ref_query_;
    ObSEArray<ObRawExpr *, 8> tmp_column_exprs;
    ObSEArray<ObRawExpr *, 8> tmp_select_exprs;
    if (OB_ISNULL(view_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select stmt is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                            insert_values_subquery_exprs,
                                                            view_stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *view_table, merge_stmt, tmp_column_exprs))) {
      LOG_WARN("failed to create column items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(tmp_column_exprs, *view_stmt,
                                                                            tmp_select_exprs))) {
      LOG_WARN("failed to convert column expr to select expr", K(ret));
    } else {
      ObSEArray<ObRawExpr *, 8> old_column_exprs;
      ObSEArray<ObRawExpr *, 8> view_column_exprs;
      ObSEArray<ObRawExpr *, 8> old_subquery_exprs;
      ObSEArray<ObRawExpr *, 8> new_subquery_column_exprs;
      for (int64_t i = 0; OB_SUCC(ret) && i < insert_values_subquery_exprs.count(); ++i) {
        ObRawExpr* expr = NULL;
        int64_t idx = -1;
        if (OB_ISNULL(expr = insert_values_subquery_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!ObOptimizerUtil::find_item(tmp_select_exprs, expr, &idx)) {
        } else if (OB_FAIL(old_subquery_exprs.push_back(expr))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(new_subquery_column_exprs.push_back(tmp_column_exprs.at(idx)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_select_exprs.count(); ++i) {
        ObRawExpr *expr = tmp_select_exprs.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (!expr->is_column_ref_expr()) {
          // do nothing
        } else if (OB_FAIL(old_column_exprs.push_back(expr))) {
          LOG_WARN("failed to push back", K(ret));
        } else if (OB_FAIL(view_column_exprs.push_back(tmp_column_exprs.at(i)))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(merge_stmt->replace_relation_exprs(old_subquery_exprs,
                                                            new_subquery_column_exprs))) {
        LOG_WARN("failed to replace relation exprs", K(ret));
      } else if (OB_FAIL(view_stmt->replace_relation_exprs(view_column_exprs, old_column_exprs))) {
        LOG_WARN("failed tp replace relation exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_insert_only_merge_into(ObDMLStmt* stmt, ObDMLStmt*& out)
{
  int ret = OB_SUCCESS;
  ObStmtFactory *stmt_factory = NULL;
  ObMergeStmt *merge_stmt = NULL;
  ObInsertStmt *insert_stmt = NULL;
  ObSelectStmt *select_stmt = NULL;
  TableItem *target_table = NULL;
  TableItem *inner_source_table = NULL;
  SemiInfo  *semi_info = NULL;
  TableItem *view_table = NULL;
  TableItem *inner_target_table = NULL;
  ObSEArray<ObDMLStmt::PartExprItem, 8> part_items;
  ObSEArray<ObDMLStmt::PartExprItem, 8> inner_part_items;
  ObSEArray<ColumnItem, 8> column_items;
  ObSEArray<ColumnItem, 8> inner_column_items;
  ObSEArray<ObRawExpr*, 8> old_column_exprs;
  ObSEArray<ObRawExpr*, 8> new_column_exprs;
  int32_t old_bit_id = OB_INVALID_INDEX;
  int32_t new_bit_id = OB_INVALID_INDEX;
  const int64_t TARGET_TABLE_IDX = 0;
  bool dummy = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->get_stmt_type() != stmt::T_MERGE) {
    /*do nothing*/
  } else if (OB_FALSE_IT(merge_stmt = static_cast<ObMergeStmt*>(stmt))) {
  } else if (OB_ISNULL(target_table = merge_stmt->get_table_item(TARGET_TABLE_IDX))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(stmt), K(ctx_));
  } else if (OB_FAIL(create_source_view_for_merge_into(merge_stmt,
                                                       view_table))) {
    LOG_WARN("failed to create source view for merge into", K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObInsertStmt>(insert_stmt))) {
    LOG_WARN("failed to create insert stmt", K(ret));
  } else if (OB_FAIL(insert_stmt->ObDelUpdStmt::assign(*merge_stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(insert_stmt->get_insert_table_info().assign(merge_stmt->get_merge_table_info()))) {
    LOG_WARN("failed to assign table info", K(ret));
  } else if (OB_FALSE_IT(insert_stmt->set_stmt_type(stmt::T_INSERT))) {
  } else if (OB_FAIL(insert_stmt->add_from_item(view_table->table_id_, view_table->is_joined_table()))) {
    LOG_WARN("failed to add from item to insert stmt", K(ret));
  } else if (OB_ISNULL(select_stmt = view_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret), K(select_stmt));
  } else if (OB_ISNULL(inner_source_table = select_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_ISNULL(inner_target_table = select_stmt->create_table_item(*ctx_->allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("create table item failed");
  } else {
    ObRawExprCopier expr_copier(*ctx_->expr_factory_);
    if (OB_FAIL(inner_target_table->deep_copy(expr_copier, *target_table))) {
      LOG_WARN("failed to copy table item", K(ret));
    } else if (OB_FALSE_IT(inner_target_table->table_id_ = select_stmt->get_query_ctx()->available_tb_id_--)) {
    } else if (OB_FAIL(ObTransformUtils::add_table_item(select_stmt, inner_target_table))) {
      LOG_WARN("failed to add table item", K(ret));
    } else if (target_table->is_generated_table()) {
      ObSelectStmt *ref_stmt = NULL;
      if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(ref_stmt))) {
        LOG_WARN("failed to create insert stmt", K(ret));
      } else if (OB_FAIL(ref_stmt->deep_copy(*stmt_factory, expr_copier, *target_table->ref_query_))) {
        LOG_WARN("failed to copy ref query", K(ret));
      } else if (OB_FAIL(ref_stmt->recursive_adjust_statement_id(ctx_->allocator_,
                                                                 ctx_->src_hash_val_,
                                                                 1))) {
        LOG_WARN("failed to recursive adjust statement id", K(ret));
      } else if (OB_FAIL(ref_stmt->update_stmt_table_id(*target_table->ref_query_))) {
        LOG_WARN("failed to update table id", K(ret));
      } else if (OB_FALSE_IT(inner_target_table->ref_query_ = ref_stmt)) {
      } else if (OB_FAIL(ref_stmt->adjust_subquery_list())) {
        LOG_WARN("failed to adjust subquery list", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(OB_INVALID_INDEX ==
                           (new_bit_id = select_stmt->get_table_bit_index(inner_target_table->table_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table index id", K(ret));
    } else if (OB_UNLIKELY(OB_INVALID_INDEX ==
                           (old_bit_id = insert_stmt->get_table_bit_index(target_table->table_id_)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table index id", K(ret));
    } else if (OB_FAIL(insert_stmt->get_column_items(target_table->table_id_, column_items))) {
      LOG_WARN("failed to get column items", K(ret));
    } else if (OB_FAIL(deep_copy_stmt_objects<ColumnItem>(expr_copier,
                                                          column_items,
                                                          inner_column_items))) {
      LOG_WARN("failed to deep copy column items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::update_table_id_for_column_item(column_items,
                                                                         target_table->table_id_,
                                                                         inner_target_table->table_id_,
                                                                         old_bit_id,
                                                                         new_bit_id,
                                                                         inner_column_items))) {
      LOG_WARN("failed to update table id for column item", K(ret));
    } else if (OB_FAIL(select_stmt->add_column_item(inner_column_items))) {
      LOG_WARN("failed to add column itemes", K(ret));
    } else if (OB_FAIL(insert_stmt->get_part_expr_items(target_table->table_id_, part_items))) {
      LOG_WARN("failed to get part expr items", K(ret));
    } else if (OB_FAIL(deep_copy_stmt_objects<ObDMLStmt::PartExprItem>(expr_copier,
                                                                       part_items,
                                                                       inner_part_items))) {
      LOG_WARN("failed to deep copy part expr items", K(ret));
    } else if (OB_FAIL(ObTransformUtils::update_table_id_for_part_item(part_items,
                                                                       target_table->table_id_,
                                                                       inner_target_table->table_id_,
                                                                       inner_part_items))) {
      LOG_WARN("failed to update table id for part item", K(ret));
    } else if (OB_FAIL(select_stmt->set_part_expr_items(inner_part_items))) {
      LOG_WARN("failed to set part expr items", K(ret));
    } else if (OB_FAIL(insert_stmt->get_column_exprs(view_table->table_id_, old_column_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                                         old_column_exprs,
                                         *select_stmt,
                                         new_column_exprs))) {
      LOG_WARN("failed to convert column expr to select expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_copier_exprs(expr_copier,
                                                              old_column_exprs,
                                                              new_column_exprs))) {
      LOG_WARN("failed to extract copier exprs", K(ret));
    } else if (OB_FAIL(select_stmt->add_condition_exprs(merge_stmt->get_insert_condition_exprs()))) {
      LOG_WARN("failed to add condition exprs", K(ret));
    } else if (OB_ISNULL(semi_info = static_cast<SemiInfo *>(ctx_->allocator_->alloc(sizeof(SemiInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate semi info", K(ret));
    } else if (OB_FALSE_IT(semi_info = new(semi_info)SemiInfo())) {
    } else if (OB_FAIL(semi_info->semi_conditions_.assign(merge_stmt->get_match_condition_exprs()))) {
      LOG_WARN("failed to assign join conditions", K(ret));
    } else if (OB_FALSE_IT(semi_info->join_type_ = LEFT_ANTI_JOIN)) {
    } else if (OB_FALSE_IT(semi_info->right_table_id_ = inner_target_table->table_id_)) {
    } else if (OB_FALSE_IT(semi_info->semi_id_ = select_stmt->get_query_ctx()->available_tb_id_--)) {
    } else if (OB_FAIL(semi_info->left_table_ids_.push_back(inner_source_table->table_id_))) {
      LOG_WARN("failed to assign semi info left table ids", K(ret));
    } else if (OB_FAIL(select_stmt->get_semi_infos().push_back(semi_info))) {
      LOG_WARN("failed to assign semi infos", K(ret));
    } else if (OB_FAIL(select_stmt->replace_relation_exprs(old_column_exprs, new_column_exprs))) {
      LOG_WARN("failed to replace relation exprs", K(ret));
    } else if (OB_FAIL(select_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(transform_exprs(select_stmt, dummy))) {
      LOG_WARN("failed to transform exprs", K(ret));
    } else if (OB_FAIL(insert_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else {
      out = insert_stmt;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_exprs(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or context is null", K(ret));
  } else {
    ObArray<ObRawExprPointer> relation_exprs;
    ObStmtExprGetter getter;
    getter.set_relation_scope();
    getter.add_scope(SCOPE_BASIC_TABLE);
    if (OB_FAIL(stmt->get_relation_exprs(relation_exprs, getter))) {
      LOG_WARN("failed to get all relation exprs", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < relation_exprs.count(); i++) {
        bool is_happened = false;
        ObRawExpr *expr = NULL;
        if (OB_FAIL(relation_exprs.at(i).get(expr))) {
          LOG_WARN("failed to get expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret));
        } else if (OB_FAIL(transform_expr(*ctx_->expr_factory_,
                                          *ctx_->session_info_,
                                          expr,
                                          is_happened))) {
          LOG_WARN("transform expr failed", K(ret));
        } else if (OB_FAIL(relation_exprs.at(i).set(expr))) {
          LOG_WARN("failed to set expr", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_expr(ObRawExprFactory &expr_factory,
                                          const ObSQLSessionInfo &session,
                                          ObRawExpr *&expr,
                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL", K(ret));
  }
  if (OB_SUCC(ret)) {
    // rewrite `c1 in (c2, c3)` to `c1 = c2 or c1 = c3`
    if (OB_FAIL(replace_in_or_notin_recursively(
                expr_factory, session, expr, trans_happened))) {
      LOG_WARN("replace in or not in failed", K(ret), K(expr));
    }
  }
  if (OB_SUCC(ret)) {
    // rewrite
    //   `cast c1 when c2 then xxx when c3 then xxx else xxx end`
    // to:
    //   `cast when c1 = c2 then xxx when c1 = c3 then xxx else xxx end`
    if (OB_FAIL(transform_arg_case_recursively(
                expr_factory, session, expr, trans_happened))) {
      LOG_WARN("transform arg case failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const uint64_t ob_version = GET_MIN_CLUSTER_VERSION();
    // The rewriting is done for the purpose of MySQL compatibility.
    if ((ob_version >= CLUSTER_VERSION_4_2_1_0) && lib::is_mysql_mode()) {
      if (OB_FAIL(replace_align_date4cmp_recursively(
                      expr_factory, expr))) {
            LOG_WARN("replace align_date4cmp failed", K(ret), K(expr));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_in_or_notin_expr_without_row(ObRawExprFactory &expr_factory,
                                                                  const ObSQLSessionInfo &session,
                                                                  const bool is_in_expr,
                                                                  ObRawExpr *&in_expr,
                                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExpr *left_expr = in_expr->get_param_expr(0);
  ObRawExpr *right_expr = in_expr->get_param_expr(1);
  ObSEArray<DistinctObjMeta, 4> distinct_types;
  for (int i = 0; OB_SUCC(ret) && i < right_expr->get_param_count(); i++) {
    if (OB_ISNULL(right_expr->get_param_expr(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null param expr", K(ret), K(right_expr->get_param_expr(i)));
    } else {
      ObObjType obj_type = right_expr->get_param_expr(i)->get_result_type().get_type();
      ObCollationType coll_type = right_expr->get_param_expr(i)
                                              ->get_result_type().get_collation_type();
      ObCollationLevel coll_level = right_expr->get_param_expr(i)
                                              ->get_result_type().get_collation_level();
      if (OB_UNLIKELY(obj_type == ObMaxType)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected obj type", K(ret), K(obj_type), K(*in_expr));
      } else if (OB_FAIL(add_var_to_array_no_dup(distinct_types,
                                               DistinctObjMeta(obj_type, coll_type, coll_level)))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        LOG_DEBUG("add param expr type", K(i), K(obj_type));
      }
    }
  } // for end

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (1 == distinct_types.count()) {
    // only one type contained in right row expr, do not need rewrite
    // set should_deduce_type = true
    ObOpRawExpr *op_raw_expr = dynamic_cast<ObOpRawExpr *>(in_expr);
    if (OB_ISNULL(op_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null op_raw_expr", K(ret));
    } else {
      op_raw_expr->set_deduce_type_adding_implicit_cast(true);
    }
  } else {
    LOG_DEBUG("distinct types", K(distinct_types));
    ObSEArray<ObRawExpr *, 4> transed_in_exprs;
    ObSEArray<ObRawExpr *, 4> same_type_exprs;
    for (int i = 0; OB_SUCC(ret) && i < distinct_types.count(); i++) {
      same_type_exprs.reuse();
      DistinctObjMeta obj_meta = distinct_types.at(i);
      for (int j = 0; OB_SUCC(ret) && j < right_expr->get_param_count(); j++) {
        ObObjType obj_type = right_expr->get_param_expr(j)->get_result_type().get_type();
        ObCollationType coll_type = right_expr->get_param_expr(j)
                                                ->get_result_type().get_collation_type();
        ObCollationLevel coll_level = right_expr->get_param_expr(j)
                                                ->get_result_type().get_collation_level();
        DistinctObjMeta tmp_meta(obj_type, coll_type, coll_level);
        if (obj_meta == tmp_meta
            && OB_FAIL(same_type_exprs.push_back(right_expr->get_param_expr(j)))) {
          LOG_WARN("failed to add param expr", K(ret));
        } else { /* do nothing */ }
      }  // for end
      if (OB_SUCC(ret) && OB_FAIL(create_partial_expr(expr_factory, left_expr, same_type_exprs,
                                                      is_in_expr, transed_in_exprs))) {
        LOG_WARN("failed to create partial expr", K(ret));
      }
    } // for end
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_final_transed_or_and_expr(expr_factory,
                                                     session,
                                                     is_in_expr,
                                                     transed_in_exprs,
                                                     in_expr))) {
      LOG_WARN("failed to get final transed or expr", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_in_or_notin_expr_with_row(ObRawExprFactory &expr_factory,
                                                               const ObSQLSessionInfo &session,
                                                               const bool is_in_expr,
                                                               ObRawExpr *&in_expr,
                                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExpr *left_expr = in_expr->get_param_expr(0);
  ObRawExpr *right_expr = in_expr->get_param_expr(1);
  int row_dim = T_REF_QUERY != left_expr->get_expr_type() ? left_expr->get_param_count() :
                                    static_cast<ObQueryRefRawExpr*>(left_expr)->get_output_column();
  ObSEArray<ObSEArray<DistinctObjMeta, 4>, 4> distinct_row_types;
  ObSEArray<ObSEArray<DistinctObjMeta, 4>, 4> all_row_types;

  for (int i = 0; OB_SUCC(ret) && i < right_expr->get_param_count(); i++) {
    if (OB_ISNULL(right_expr->get_param_expr(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null param expr", K(ret));
    } else if (OB_UNLIKELY(right_expr->get_param_expr(i)->get_param_count() != row_dim)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param cnt", K(row_dim),
               K(right_expr->get_param_expr(i)->get_param_count()));
    } else {
      ObSEArray<DistinctObjMeta, 4> tmp_row_type;
      for (int j = 0; OB_SUCC(ret) && j < right_expr->get_param_expr(i)->get_param_count();
           j++) {
        if (OB_ISNULL(right_expr->get_param_expr(i)->get_param_expr(j))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null param expr", K(ret));
        } else {
          const ObRawExpr *param_expr = right_expr->get_param_expr(i)->get_param_expr(j);
          const ObObjType obj_type = param_expr->get_result_type().get_type();
          const ObCollationType coll_type = param_expr->get_result_type().get_collation_type();
          const ObCollationLevel coll_level = param_expr->get_result_type().get_collation_level();
          if (OB_UNLIKELY(obj_type == ObMaxType)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected obj type", K(ret), K(obj_type), K(*in_expr));
          } else if (OB_FAIL(tmp_row_type.push_back(
                                               DistinctObjMeta(obj_type, coll_type, coll_level)))) {
            LOG_WARN("failed to push back element", K(ret));
          } else { /* do nothing */ }
        }
      } // for end
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(add_row_type_to_array_no_dup(distinct_row_types, tmp_row_type))) {
        LOG_WARN("failed to add_row_type_to_array_no_dup", K(ret));
      } else if (OB_FAIL(all_row_types.push_back(tmp_row_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else { /* do nothing */ }
    }
  } // for end
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (1 == distinct_row_types.count()) {
    // all rows are same, do nothing
    ObOpRawExpr *op_raw_expr = dynamic_cast<ObOpRawExpr *>(in_expr);
    if (OB_ISNULL(op_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null op_raw_expr", K(ret));
    } else {
      op_raw_expr->set_deduce_type_adding_implicit_cast(true);
    }
  } else {
    ObSEArray<ObRawExpr *, 4> transed_in_exprs;
    ObSEArray<ObRawExpr *, 4> same_type_exprs;
    for (int i = 0; OB_SUCC(ret) && i < distinct_row_types.count(); i++) {
      same_type_exprs.reuse();
      for (int j = 0; OB_SUCC(ret) && j < all_row_types.count(); j++) {
        if (is_same_row_type(distinct_row_types.at(i), all_row_types.at(j)) &&
            OB_FAIL(same_type_exprs.push_back(right_expr->get_param_expr(j)))) {
          LOG_WARN("failed to add param expr", K(ret));
        }
      } // for end
      if (OB_SUCC(ret) && OB_FAIL(create_partial_expr(expr_factory, left_expr, same_type_exprs,
                                                      is_in_expr, transed_in_exprs))) {
        LOG_WARN("failed to create partial expr", K(ret));
      }
    } // for end
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_final_transed_or_and_expr(
                expr_factory, session, is_in_expr, transed_in_exprs, in_expr))) {
      LOG_WARN("failed to get final transed in expr", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::create_partial_expr(ObRawExprFactory &expr_factory,
                                               ObRawExpr *left_expr,
                                               ObIArray<ObRawExpr*> &same_type_exprs,
                                               const bool is_in_expr,
                                               ObIArray<ObRawExpr*> &transed_in_exprs)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *tmp_expr = NULL;
  ObOpRawExpr *tmp_row_expr = NULL;
  ObRawExpr *tmp_left_expr = left_expr;
  ObRawExpr *tmp_right_expr = NULL;
  if (OB_UNLIKELY(same_type_exprs.empty()) || OB_ISNULL(left_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty same_type_exprs", K(ret), K(same_type_exprs), K(left_expr));
  } else if (left_expr->get_expr_type() == T_OP_ROW &&
             OB_FAIL(ObRawExprCopier::copy_expr_node(expr_factory,
                                                     left_expr,
                                                     tmp_left_expr))) {
    // Comment for T_OP_ROW
    // for T_OP_ROW expr, the cast is not added above the expr
    // it is added on the expr's param.
    // when the expr is used by different predicates
    // we may need add different cast for its param
    // hence, we need to copy the expr node.
    // we do not need to copy its param, the param can be shared used.
    LOG_WARN("failed to copy expr node", K(ret));
  } else if (1 == same_type_exprs.count()) { // = / <>
    if (OB_FAIL(expr_factory.create_raw_expr(is_in_expr ? T_OP_EQ : T_OP_NE, tmp_expr))) {
      LOG_WARN("failed to create or create raw expr", K(ret));
    } else {
      tmp_right_expr = same_type_exprs.at(0);
    }
  } else if (OB_FAIL(expr_factory.create_raw_expr(is_in_expr ? T_OP_IN : T_OP_NOT_IN, tmp_expr)) // in / not in
             || OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, tmp_row_expr))) {
    LOG_WARN("failed to create or create raw expr", K(ret));
  } else if (OB_ISNULL(tmp_row_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), K(tmp_row_expr));
  } else if (OB_FAIL(append(tmp_row_expr->get_param_exprs(), same_type_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {
    tmp_right_expr = tmp_row_expr;
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tmp_expr) || OB_ISNULL(tmp_left_expr) || OB_ISNULL(tmp_right_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret), K(tmp_expr),
                                    K(tmp_left_expr), K(tmp_right_expr));
  } else if (OB_FAIL(tmp_expr->add_param_expr(tmp_left_expr))
            || OB_FAIL(tmp_expr->add_param_expr(tmp_right_expr))) {
    LOG_WARN("failed to add param exprs", K(ret));
  } else if (OB_FAIL(transed_in_exprs.push_back(tmp_expr))) {
    LOG_WARN("failed to push back element", K(ret));
  } else {
    tmp_expr->set_deduce_type_adding_implicit_cast(true);
    LOG_DEBUG("partial in expr", K(*tmp_expr), K(*tmp_right_expr), K(*left_expr));
  }
  return ret;
}

int ObTransformPreProcess::transform_arg_case_recursively(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session,
    ObRawExpr *&expr,
    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (T_OP_ARG_CASE == expr->get_expr_type()) {
    if (OB_FAIL(transform_arg_case_expr(expr_factory, session, expr, trans_happened))) {
      LOG_WARN("failed to transform_arg_case_expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(SMART_CALL(transform_arg_case_recursively(expr_factory,
                           session,
                           expr->get_param_expr(i),
                           trans_happened)))) {
      LOG_WARN("failed to transform arg case expr", K(ret), K(i));
    }
  }
  return ret;
}

// in engine 3.0 transform arg_case_when_expr to simple_case_when_expr
// eg:
// case arg when when1 then then1       case when arg = when1 then then1
//          when when1 then then2  ->        when arg = when2 then then2
//          else else1                       else else1
int ObTransformPreProcess::transform_arg_case_expr(ObRawExprFactory &expr_factory,
                                                   const ObSQLSessionInfo &session,
                                                   ObRawExpr *&expr,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr *case_expr = static_cast<ObCaseOpRawExpr*>(expr);
  ObRawExpr *arg_expr = case_expr->get_arg_param_expr();
  ObCaseOpRawExpr *new_case_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_CASE, new_case_expr))) {
    LOG_WARN("failed to create case expr", K(ret));
  } else if (OB_ISNULL(new_case_expr) || OB_ISNULL(arg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret), KP(new_case_expr), KP(arg_expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < case_expr->get_when_expr_size(); ++i) {
    ObRawExpr *when_expr = case_expr->get_when_param_expr(i);
    ObRawExpr *then_expr = case_expr->get_then_param_expr(i);
    ObRawExpr *tmp_arg_expr = arg_expr;
    ObOpRawExpr *equal_expr = NULL;
    if (OB_ISNULL(when_expr) || OB_ISNULL(then_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("when_expr is NULL", K(ret), KP(when_expr), KP(then_expr));
    } else if (T_OP_ROW == arg_expr->get_expr_type() &&
               OB_FAIL(ObRawExprCopier::copy_expr_node(expr_factory,
                                                       arg_expr,
                                                       tmp_arg_expr))) {
      // See the Comment for T_OP_ROW above
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_FAIL(create_equal_expr_for_case_expr(expr_factory,
                                                       session,
                                                       tmp_arg_expr,
                                                       when_expr,
                                                       case_expr->get_result_type(),
                                                       equal_expr))) {
      LOG_WARN("failed to create equal expr", K(ret));
    } else if (OB_FAIL(new_case_expr->add_when_param_expr(equal_expr)) ||
              OB_FAIL(new_case_expr->add_then_param_expr(then_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  } // for end
  ObRawExpr* tmp_case_expr = static_cast<ObRawExpr*>(new_case_expr);
  if (OB_SUCC(ret) &&
      !case_expr->is_called_in_sql() &&
      OB_FAIL(ObRawExprUtils::set_call_in_pl(tmp_case_expr))) {
    LOG_WARN("failed to set call_in_pl flag", K(ret));
  }
  if (OB_SUCC(ret)) {
    new_case_expr->set_default_param_expr(case_expr->get_default_param_expr());
    if (OB_FAIL(new_case_expr->formalize(&session))) {
      LOG_WARN("failed to formalize", K(ret));
    } else {
      expr = static_cast<ObRawExpr*>(new_case_expr);
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::create_equal_expr_for_case_expr(ObRawExprFactory &expr_factory,
                                                           const ObSQLSessionInfo &session,
                                                           ObRawExpr *arg_expr,
                                                           ObRawExpr *when_expr,
                                                           const ObExprResType &case_res_type,
                                                           ObOpRawExpr *&equal_expr)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = ObMaxType;
  const ObExprResType &arg_type = arg_expr->get_result_type();
  const ObExprResType &when_type = when_expr->get_result_type();
  ObRawExpr *new_when_expr = NULL; // cast expr may added
  ObRawExpr *new_arg_expr = NULL;
  if (OB_ISNULL(arg_expr) || OB_ISNULL(when_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(arg_expr), KP(when_expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_EQ, equal_expr))) {
    LOG_WARN("failed to create equal expr", K(ret));
  } else {
    if (OB_FAIL(ObExprArgCase::get_cmp_type(obj_type, arg_type.get_type(),
                                            when_type.get_type(),
                                            ObMaxType))) { // last argument is unused
      LOG_WARN("failed to get_cmp_type", K(ret));
    } else if (lib::is_mysql_mode() && ob_is_string_type(obj_type)) {
      // when cmp_type is string, need to use case_res_type.calc_type_.cs_type_ as
      // collation type. it is aggregated by all when_exprs.
      // eg: select case col_utf8_general_ci when col_utf8_general_ci then 'a'
      //                                     when col_utf8_bin then 'b' end from tbl;
      //     use col_utf8_bin to compare(see in ObExprArgCase::calc_result_typeN())
      ObExprResType cmp_type;
      cmp_type.set_type(obj_type);
      cmp_type.set_collation_type(case_res_type.get_calc_collation_type());
      cmp_type.set_collation_level(case_res_type.get_calc_collation_level());
      if (ObRawExprUtils::try_add_cast_expr_above(&expr_factory, &session,
                                                  *arg_expr, cmp_type, new_arg_expr) ||
          ObRawExprUtils::try_add_cast_expr_above(&expr_factory, &session,
                                                  *when_expr, cmp_type, new_when_expr)) {
        LOG_WARN("failed to add_cast", K(ret), KP(new_arg_expr), KP(new_when_expr));
      }
    } else {
      new_arg_expr = arg_expr;
      new_when_expr = when_expr;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(new_when_expr) || OB_ISNULL(new_arg_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret), KP(new_when_expr), KP(new_arg_expr));
    } else if (OB_FAIL(equal_expr->add_param_expr(new_arg_expr)) ||
               OB_FAIL(equal_expr->add_param_expr(new_when_expr))) {
      LOG_WARN("failed to add_param_expr", K(ret));
    }
  }
  return ret;
}

bool ObTransformPreProcess::is_same_row_type(const ObIArray<DistinctObjMeta> &left,
                                             const ObIArray<DistinctObjMeta> &right)
{
  bool ret_bool = true;
  if (left.count() != right.count()) {
    ret_bool = false;
  }
  for (int i = 0; ret_bool && i < left.count(); i++) {
    ret_bool = (left.at(i) == right.at(i));
  } // for end
  return ret_bool;
}

int ObTransformPreProcess::get_final_transed_or_and_expr(
    ObRawExprFactory &expr_factory,
    const ObSQLSessionInfo &session,
    const bool is_in_expr,
    ObIArray<ObRawExpr *> &transed_in_exprs,
    ObRawExpr *&final_or_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *transed_or_expr = NULL;
  ObOpRawExpr *tmp_or_expr = NULL;
  ObOpRawExpr *last_or_expr = NULL;
  ObItemType op_type = is_in_expr ? T_OP_OR : T_OP_AND;
  if (OB_FAIL(expr_factory.create_raw_expr(op_type, transed_or_expr))) {
    LOG_WARN("failed to create or expr", K(ret));
  } else if (OB_ISNULL(transed_or_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null expr", K(ret), K(transed_or_expr));
  } else if (OB_FAIL(transed_or_expr->add_param_expr(transed_in_exprs.at(0)))) {
    LOG_WARN("failed to add param expr", K(ret));
  } else {
    last_or_expr = transed_or_expr;
    int cur_idx = 1;
    for (; OB_SUCC(ret) && cur_idx < transed_in_exprs.count() - 1; cur_idx++) {
      if (OB_FAIL(expr_factory.create_raw_expr(op_type, tmp_or_expr))) {
        LOG_WARN("failed to create raw expr", K(ret));
      } else if (OB_ISNULL(tmp_or_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null expr", K(ret));
      } else if (OB_FAIL(last_or_expr->add_param_expr(tmp_or_expr))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else if (OB_FAIL(tmp_or_expr->add_param_expr(transed_in_exprs.at(cur_idx)))) {
        LOG_WARN("failed to add param expr", K(ret));
      } else {
        last_or_expr = tmp_or_expr;
      }
    }  // for end
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(last_or_expr->add_param_expr(transed_in_exprs.at(cur_idx)))) {
      LOG_WARN("failed to add param expr", K(ret));
    } else if (OB_FAIL(transed_or_expr->formalize(&session))) {
      LOG_WARN("expr formalize failed", K(ret));
    } else {
      final_or_expr = transed_or_expr;
    }
  }
  return ret;
}

int ObTransformPreProcess::add_row_type_to_array_no_dup(
                             ObIArray<ObSEArray<DistinctObjMeta, 4>> &row_type_array,
                             const ObSEArray<DistinctObjMeta, 4> &row_type)
{
  int ret = OB_SUCCESS;
  bool founded = false;
  for (int i = 0; OB_SUCC(ret) && !founded && i < row_type_array.count(); i++) {
    if (is_same_row_type(row_type_array.at(i), row_type)) {
      founded = true;
    }
  }
  if (!founded && OB_FAIL(row_type_array.push_back(row_type))) {
    LOG_WARN("failed to push back element", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::check_and_transform_in_or_notin(ObRawExprFactory &expr_factory,
                                                           const ObSQLSessionInfo &session,
                                                           ObRawExpr *&in_expr,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != in_expr->get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param cnt", K(ret), K(in_expr->get_param_count()));
  } else if (OB_ISNULL(in_expr->get_param_expr(0)) ||
             OB_ISNULL(in_expr->get_param_expr(1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null params", K(ret), K(in_expr->get_param_expr(0)),
             K(in_expr->get_param_expr(1)));
  } else if (T_OP_ROW == in_expr->get_param_expr(0)->get_expr_type() ||
             (T_REF_QUERY == in_expr->get_param_expr(0)->get_expr_type() &&
              static_cast<ObQueryRefRawExpr*>(in_expr->get_param_expr(0))->get_output_column() > 1)) {
    // (x, y) in ((x0, y0), (x1, y1), ...)
    // (select x, y from ...) in ((x0, y0), (x1, y1), ...))
    LOG_DEBUG("Before Transform", K(*in_expr));
    ret = transform_in_or_notin_expr_with_row(
        expr_factory, session, T_OP_IN == in_expr->get_expr_type(), in_expr, trans_happened);
  } else {
    // x in (x0, x1, ...)
    LOG_DEBUG("Before Transform", K(*in_expr));
    ret = transform_in_or_notin_expr_without_row(
        expr_factory, session, T_OP_IN == in_expr->get_expr_type(), in_expr, trans_happened);
  }
  if (OB_SUCC(ret)) {
    ObOpRawExpr *op_raw_expr = dynamic_cast<ObOpRawExpr *>(in_expr);
    if (OB_ISNULL(op_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null op_raw_expr", K(ret));
    } else if (OB_FAIL(op_raw_expr->formalize(&session))) {
      LOG_WARN("formalize expr failed", K(ret));
    } else {
      LOG_DEBUG("After Transform", K(*op_raw_expr));
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_in_or_notin_recursively(ObRawExprFactory &expr_factory,
                                                           const ObSQLSessionInfo &session,
                                                           ObRawExpr *&root_expr,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < root_expr->get_param_count(); i++) {
    if (OB_ISNULL(root_expr->get_param_expr(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null param expr", K(ret));
    } else if (OB_FAIL(SMART_CALL(replace_in_or_notin_recursively(expr_factory,
                                                        session,
                                                        root_expr->get_param_expr(i),
                                                        trans_happened)))) {
      LOG_WARN("failed to replace in or notin recursively", K(ret));
    }
  }
  if (OB_SUCC(ret) &&
    (T_OP_IN == root_expr->get_expr_type() || T_OP_NOT_IN == root_expr->get_expr_type())) {
    if (OB_FAIL(check_and_transform_in_or_notin(
                expr_factory, session, root_expr, trans_happened))) {
      LOG_WARN("failed to check and transform", K(ret));
    }
  }
  return ret;
}

ObItemType ObTransformPreProcess::reverse_cmp_type_of_align_date4cmp(const ObItemType &cmp_type)
{
  ObItemType new_cmp_type = cmp_type;
  switch (cmp_type) {
    case T_OP_LE: {
      new_cmp_type = T_OP_GE;
      break;
    }
    case T_OP_LT: {
      new_cmp_type = T_OP_GT;
      break;
    }
    case T_OP_GE: {
      new_cmp_type = T_OP_LE;
      break;
    }
    case T_OP_GT: {
      new_cmp_type = T_OP_LT;
      break;
    }
    default: {
      break;
    }
  }
  return new_cmp_type;
}

int ObTransformPreProcess::replace_cast_expr_align_date4cmp(ObRawExprFactory &expr_factory,
                                                            const ObItemType &cmp_type,
                                                            ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null.", K(ret));
  } else if (expr->get_expr_type() == T_FUN_SYS_CAST) {
    const bool is_implicit_cast = !CM_IS_EXPLICIT_CAST(expr->get_extra());
    ObExprResType cast_res_type = expr->get_result_type();
    if (is_implicit_cast && (cast_res_type.is_date() || cast_res_type.is_datetime())) {
      ObRawExpr *cast_child = expr->get_param_expr(0);
      if (OB_ISNULL(cast_child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cast child expr is null.", K(ret));
      } else if (ObExprAlignDate4Cmp::is_align_date4cmp_support_obj_type(cast_child->get_data_type())) {
        // create a new align_date4cmp_expr to replace const_expr
        ObSysFunRawExpr *align_date4cmp_expr = NULL;
        if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_ALIGN_DATE4CMP, align_date4cmp_expr))) {
          LOG_WARN("create align_date4cmp_expr fail.", K(ret), K(align_date4cmp_expr));
        } else if (OB_ISNULL(align_date4cmp_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("align_date4cmp_expr is null.", K(ret));
        } else {
          align_date4cmp_expr->set_func_name("INTERNAL_FUNCTION");
          // Copy cast_mode to facilitate determining the method of handling invalid_time.
          align_date4cmp_expr->set_extra(expr->get_extra());
          // add first param_expr(just the cast_child) to align_date4cmp_expr
          if (OB_FAIL(align_date4cmp_expr->add_param_expr(cast_child))) {
            LOG_WARN("align_date4cmp_expr add param cast_expr fail.", K(ret), KPC(cast_child));
          } else {
            // create second param_expr (a ObConstRawExpr used to represent cmp_type)
            // and add it to align_date4cmp_expr.
            ObConstRawExpr *cmp_type_expr = NULL;
            if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory,
                                              ObIntType, cmp_type, cmp_type_expr))) {
            LOG_WARN("failed to build const int cmp_type_expr", K(ret));
            } else if (OB_ISNULL(cmp_type_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("cmp_type_expr is null.", K(ret));
            } else if (OB_FAIL(align_date4cmp_expr->add_param_expr(cmp_type_expr))){
              LOG_WARN("align_date4cmp_expr add param cmp_type_expr fail.", K(ret), KPC(cmp_type_expr));
            } else {
              // create third param_expr (a ObConstRawExpr used to represent res_type)
              // and add it to align_date4cmp_expr.
              ObConstRawExpr *res_type_expr = NULL;
              ObObjType cast_res_type = expr->get_result_type().get_type();
              int64_t cast_res_type_int = cast_res_type;
              if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory,
                                                  ObIntType, cast_res_type_int, res_type_expr))) {
                LOG_WARN("failed to build const int res_type_expr", K(ret));
              } else if (OB_ISNULL(res_type_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("res_type_expr is null.", K(ret));
              } else if (OB_FAIL(align_date4cmp_expr->add_param_expr(res_type_expr))){
                LOG_WARN("align_date4cmp_expr add param res_type_expr fail.", K(ret), KPC(res_type_expr));
              } else {
                // Change the expr pointer pointing to cast_expr
                // to point to the newly created align_date4cmp_expr.
                expr = align_date4cmp_expr;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_and_transform_align_date4cmp(ObRawExprFactory &expr_factory,
                                                           ObRawExpr *&cmp_expr,
                                                           const ObItemType &cmp_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != cmp_expr->get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param cnt", K(ret), K(cmp_expr->get_param_count()));
  } else {
    ObRawExpr *expr0 = cmp_expr->get_param_expr(0);
    ObRawExpr *expr1 = cmp_expr->get_param_expr(1);
    if (OB_ISNULL(expr0) || OB_ISNULL(expr1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null params", K(ret), K(cmp_expr->get_param_expr(0)),
              K(cmp_expr->get_param_expr(1)));
    } else {
      // By default, align_date4cmp_expr expects the first parameter to be
      // the value on the right side of the comparison operator.
      // Therefore, if you pass the value on the left side,
      // you need to reverse the comparison operator that is passed in.
      const ObItemType reverse_cmp_type = reverse_cmp_type_of_align_date4cmp(cmp_type);
      if (OB_FAIL(replace_cast_expr_align_date4cmp(expr_factory, reverse_cmp_type, expr0))) {
        LOG_WARN("replace left cast_expr fail.", K(ret), KPC(expr0));
      } else {
        cmp_expr->get_param_expr(0) = expr0;
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(replace_cast_expr_align_date4cmp(expr_factory, cmp_type, expr1))) {
          LOG_WARN("replace right cast_expr fail.", K(ret), KPC(expr1));
        } else {
          cmp_expr->get_param_expr(1) = expr1;
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_align_date4cmp_recursively(ObRawExprFactory &expr_factory,
                                                           ObRawExpr *&root_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_expr is null.", K(ret));
  }
  // Traverse all exprs from the root.
  for (int i = 0; OB_SUCC(ret) && i < root_expr->get_param_count(); i++) {
    if (OB_ISNULL(root_expr->get_param_expr(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null param expr", K(ret));
    } else if (OB_FAIL(SMART_CALL(replace_align_date4cmp_recursively(expr_factory,
                                                        root_expr->get_param_expr(i))))) {
      LOG_WARN("failed to replace in or notin recursively", K(ret));
    }
  }
  // If the current expression is cmp_expr, check if align_date4cmp transformation is necessary.
  if (OB_SUCC(ret) && IS_COMMON_COMPARISON_OP(root_expr->get_expr_type())) {
    if (OB_FAIL(check_and_transform_align_date4cmp(
                expr_factory, root_expr, root_expr->get_expr_type()))) {
      LOG_WARN("failed to check and transform", K(ret));
    }
  }
  return ret;
}

/*@brief ObTransformPreProcess::transformer_aggr_expr 用于将一些复杂的聚合函数展开为普通的聚合运算;
* eg:var_pop(expr) ==> SUM(expr*expr) - SUM(expr)* SUM(expr)/ COUNT(expr)) / COUNT(expr)
* 其中ObExpandAggregateUtils这个类主要涉及到相关的函数用于展开复杂的聚合函数:
*   1.ObExpandAggregateUtils::expand_aggr_expr ==> 用于处理普通的aggr函数接口
*   2.ObExpandAggregateUtils::expand_window_aggr_expr  ==> 用于处理窗口函数中的aggr的函数接口
*
 */
int ObTransformPreProcess::transformer_aggr_expr(ObDMLStmt *stmt,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_expand_aggr = false;
  bool is_expand_window_aggr = false;
  bool is_happened = false;
  //之前的逻辑保证了两者嵌套聚合及普通函数的改写顺序，传进来的trans_happened包含了是否发生嵌套聚合函数改写的信息
  bool is_trans_nested_aggr_happened = trans_happened;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null allocator", K(ret));
  } else if (OB_FAIL(ObExpandAggregateUtils::expand_aggr_expr(stmt, ctx_, is_expand_aggr))) {
    LOG_WARN("failed to expand aggr expr", K(ret));
  } else if (OB_FAIL(ObExpandAggregateUtils::expand_window_aggr_expr(stmt,
                                                                     ctx_,
                                                                     is_expand_window_aggr))) {
    LOG_WARN("failed to expand window aggr expr", K(ret));
  //如果发生了嵌套聚合函数改写：
  // select max(avg(c1)) from t1 group by c2;
  // ==>
  // select max(a) from (select avg(c1) as a from t1 group by c2);
  // 需要改写view里面的聚合函数，同时需要注释的是嵌套聚合函数只有内外两层，不会生成超过2层的结构
  } else if (is_trans_nested_aggr_happened) {
    TableItem *table_item = NULL;
    if (OB_UNLIKELY(stmt->get_table_items().count() != 1) ||
        OB_ISNULL(table_item = stmt->get_table_item(0)) ||
        OB_UNLIKELY(!table_item->is_generated_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(table_item), K(stmt->get_table_items().count()), K(ret));
    } else if (OB_FAIL(transformer_aggr_expr(table_item->ref_query_, is_happened))) {
      LOG_WARN("failed to transformer aggr expr", K(ret));
    } else {/*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    trans_happened = is_expand_aggr | is_expand_window_aggr | is_happened;
  }
  return ret;
}

/**
 * @brief ObTransformSimplify::transform_rownum_as_limit_offset
 * 将 rownum 改写为 limit
 * select * from t where ... and rownum < ?;
 * => select * from t where ... limit ?;
 *
 * select * from (select * from t order by c) where rownum < ?;
 * => select * from (select * from t order by c limit ?);
 *
 * 将 rownum 改写为 limit / offset
 * select * from (select rownum rn, ... from t) where rn > ? and rn < ?;
 * => select * from (select rownum rn, ... from t limit ? offset ?);
 */
int ObTransformPreProcess::transform_rownum_as_limit_offset(
                                            const ObIArray<ObParentDMLStmt> &parent_stmts,
                                            ObDMLStmt *&stmt,
                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  //bool is_rownum_gen_col_happened = false;
  bool is_rownum_happened = false;
  bool is_generated_rownum_happened = false;
  trans_happened = false;
  if (OB_FAIL(transform_common_rownum_as_limit(stmt, is_rownum_happened))) {
    LOG_WARN("failed to transform common rownum as limit", K(ret));
  } else if (OB_FAIL(transform_generated_rownum_as_limit(parent_stmts, stmt,
                                                         is_generated_rownum_happened))) {
    LOG_WARN("failed to transform rownum gen col as limit", K(ret));
  } else {
    trans_happened = is_rownum_happened || is_generated_rownum_happened;
  }
  return ret;
}

/**
 * @brief ObTransformPreProcess::transform_common_rownum_as_limit
 * 将 rownum 改写为 limit
 * select * from t where ... and rownum < ? order by c1;
 * => select * from (select * from t where ... limit ?) order by c1;
 *
**/
int ObTransformPreProcess::transform_common_rownum_as_limit(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObRawExpr *limit_expr = NULL;
  bool is_valid = false;
  ObSelectStmt *child_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_)|| OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(ctx_));
  } else if (stmt->is_hierarchical_query()) {//层次查询暂时无法支持
    /*do nothing*/
  } else if (is_oracle_mode() && stmt->is_select_stmt() &&
             static_cast<ObSelectStmt*>(stmt)->has_for_update()) {
    // in oracle mode, do not create view when has for update
  } else if (OB_FAIL(try_transform_common_rownum_as_limit_or_false(stmt, limit_expr, is_valid))) {
    LOG_WARN("failed to try transform rownum expr as limit", K(ret));
  } else if (!is_valid) {
    LOG_TRACE("limit expr is null", K(limit_expr), K(is_valid));
  //不含有order by的update/delete stmt不用再进行分离spj
  } else if (FALSE_IT(trans_happened = true)) {
  } else if (limit_expr == NULL) {
    //do nothing
  } else if (!stmt->is_select_stmt() && !stmt->has_order_by()) {
    stmt->set_limit_offset(limit_expr, NULL);
  } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, stmt, child_stmt, false))) {
    LOG_WARN("failed to create simple view", K(ret));
  } else if (OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child_stmt));
  } else {
    child_stmt->set_limit_offset(limit_expr, NULL);
  }
  return ret;
}

/**
 * @brief ObTransformPreProcess::try_transform_common_rownum_as_limit
 * 满足以下条件时，stmt 中的 rownum 是可以改写 limit
 * 1. rownum 表达式位于where中，形式为： rownum < ?, rownum <= ?, rownum = ? 且参数为 int/number 型
 *    对于等值条件: rownum = 1 转化为 limit 1, 其它转化为 limit 0
 * 2. stmt 没有 distinct, groupby, order by, aggr, window function 等
 *  e.g. select rownum, * from t where rownum < 2;
 */
int ObTransformPreProcess::try_transform_common_rownum_as_limit_or_false(ObDMLStmt *stmt,
                                                                         ObRawExpr *&limit_expr,
                                                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  limit_expr = NULL;
  ObRawExpr *limit_value = NULL;
  ObItemType op_type = T_INVALID;
  ObPhysicalPlanCtx *plan_ctx = NULL;

  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->exec_ctx_)
      || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    ObIArray<ObRawExpr *> &conditions = stmt->get_condition_exprs();
    int64_t expr_idx = -1;
    bool is_eq_cond = false;
    bool is_const_filter = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_eq_cond && i < conditions.count(); ++i) {
      ObRawExpr *cond_expr = NULL;
      ObRawExpr *const_expr = NULL;
      ObItemType expr_type = T_INVALID;
      if (OB_ISNULL(cond_expr = conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret), K(i));
      } else if (cond_expr->has_flag(CNT_DYNAMIC_PARAM)) {
        // do nothing
      } else if (OB_FAIL(ObOptimizerUtil::get_rownum_filter_info(cond_expr,
                                                                 expr_type,
                                                                 const_expr,
                                                                 is_const_filter))) {
        LOG_WARN("failed to check is filter rownum", K(ret));
      } else if (!is_const_filter) {
        // do nothing
      } else if (T_OP_LE == expr_type || T_OP_LT == expr_type) {
        limit_value = const_expr;
        op_type = expr_type;
        expr_idx = i;
        is_valid = true;
      } else if (T_OP_EQ == expr_type) {
        limit_value = const_expr;
        expr_idx = i;
        is_valid = true;
        is_eq_cond = true;
      } else {/*do nothing*/}
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < conditions.count(); ++i) {
      ObRawExpr *cond_expr = NULL;
      if (i == expr_idx) {
        //do nothing
      } else if (OB_ISNULL(cond_expr = conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret), K(i));
      } else if (cond_expr->has_flag(CNT_ROWNUM)) {
        is_valid = false;
      }
    }

    ObConstRawExpr *one_expr = NULL;
    bool is_true = false;
    if (OB_FAIL(ret) || !is_valid) {
    } else if (OB_FAIL(conditions.remove(expr_idx))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else if (!is_eq_cond) {
      ObRawExpr *int_expr = NULL;
      ObRawExpr *cmp_expr = NULL;
      if (OB_FAIL(ObOptimizerUtil::convert_rownum_filter_as_limit(*ctx_->expr_factory_,
                                                                      ctx_->session_info_, op_type,
                                                                      limit_value, int_expr))) {
        LOG_WARN("failed to convert rownum filter as limit", K(ret));
      } else if (OB_FAIL(ObTransformUtils::compare_const_expr_result(ctx_, int_expr, T_OP_GE, 1, is_true))) {
        LOG_WARN("compare expr failed", K(ret));
      } else if (one_expr == NULL && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                                                  ObIntType,
                                                                                  1,
                                                                                  one_expr))) {
        LOG_WARN("build const expr failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_, ctx_->session_info_, T_OP_GE,
                                                                cmp_expr, int_expr, one_expr))) {
        LOG_WARN("create doubel op expr", K(ret));
      } else if (is_true) {
        limit_expr = int_expr;
        if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, cmp_expr, true))) {
          LOG_WARN("add cons failed", K(ret));
        }
      } else {
        ObRawExpr *b_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, b_expr, false))) {
            LOG_WARN("build expr failed", K(ret));
        } else if (OB_ISNULL(b_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("b_Expr is null", K(ret));
        } else if (OB_FAIL(conditions.push_back(b_expr))) {
          LOG_WARN("push back failed", K(ret));
        } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, cmp_expr, false))) {
          LOG_WARN("add cons failed", K(ret));
        } else {
          limit_expr = NULL;
        }
      }
    } else if (is_eq_cond) {
      ObRawExpr *cmp_expr = NULL;
      if (OB_FAIL(ObTransformUtils::compare_const_expr_result(ctx_, limit_value, T_OP_EQ, 1, is_true))) {
        LOG_WARN("compare expr failed", K(ret));
      } else if (one_expr == NULL && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                      ObIntType,
                                                      1,
                                                      one_expr))) {
          LOG_WARN("build const expr failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_, ctx_->session_info_, T_OP_EQ,
                                                                cmp_expr, limit_value, one_expr))) {
        LOG_WARN("create doubel op expr", K(ret));
      } else if (is_true) {
        if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, cmp_expr, true))) {
          LOG_WARN("add cons failed", K(ret));
        } else {
          limit_expr = one_expr;
        }
      } else {
        ObRawExpr *b_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_const_bool_expr(ctx_->expr_factory_, b_expr, false))) {
            LOG_WARN("build expr failed", K(ret));
        } else if (OB_ISNULL(b_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("b_Expr is null", K(ret));
        } else if (OB_FAIL(conditions.push_back(b_expr))) {
          LOG_WARN("push back failed", K(ret));
        } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, cmp_expr, false))) {
          LOG_WARN("add cons failed", K(ret));
        } else {
          limit_expr = NULL; //the limit expr must be int type or null.
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_generated_rownum_as_limit(
                                            const ObIArray<ObParentDMLStmt> &parent_stmts,
                                            ObDMLStmt *stmt,
                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObDMLStmt *upper_stmt = NULL;
  ObSelectStmt *select_stmt = NULL;
  ObRawExpr *view_offset = NULL;
  ObRawExpr *view_limit = NULL;
  ObRawExpr *offset_expr = NULL;
  ObRawExpr *limit_expr = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (parent_stmts.empty()) {
    /*do nothing*/
  } else if (!stmt->is_select_stmt() || OB_ISNULL(select_stmt = static_cast<ObSelectStmt*>(stmt))
             || OB_ISNULL(upper_stmt = parent_stmts.at(parent_stmts.count()-1).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (select_stmt->is_hierarchical_query() || //层次查询不进行改写
             upper_stmt->is_hierarchical_query()) {
    /*do nothing*/
  } else if (OB_FAIL(try_transform_generated_rownum_as_limit_offset(upper_stmt, select_stmt,
                                                                    view_limit, view_offset))) {
    LOG_WARN("failed to try transform generated rownum as offset offset", K(ret));
  } else if (NULL == view_limit && NULL == view_offset) {
    LOG_TRACE("no limit expr");
    //if the view already has a limit or offset, then the new limit should be merged.
  } else if (OB_FAIL(ObTransformUtils::merge_limit_offset(ctx_, stmt->get_limit_expr(), view_limit,
                                                          stmt->get_offset_expr(), view_offset,
                                                          limit_expr, offset_expr))) {
    LOG_WARN("failed to merge limit offset", K(ret));
  } else {
    stmt->set_limit_offset(limit_expr, offset_expr);
    trans_happened = true;
  }
  return ret;
}

// 将 generate rownum 改写为 limit / offset
// 1. select * from (select rownum rn, ... from t) where rn > 2 and rn <= 5;
//      => select * from (select rownum rn, ... from t limit 3 offset 2);
// 2. select * from (select rownum rn, ... from t) where rn = 4;
//      => select * from (select rownum rn, ... from t limit 1 offset 3);
// 3. select * from (select rownum rn, ... from t) where rn = 4.2;
//      => select * from (select rownum rn, ... from t limit 0 offset 0);
int ObTransformPreProcess::try_transform_generated_rownum_as_limit_offset(ObDMLStmt *upper_stmt,
                                                                          ObSelectStmt *select_stmt,
                                                                          ObRawExpr *&limit_expr,
                                                                          ObRawExpr *&offset_expr)
{
  int ret = OB_SUCCESS;
  TableItem *table = NULL;
  limit_expr = NULL;
  offset_expr = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(upper_stmt) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->exec_ctx_)
      || OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!upper_stmt->is_sel_del_upd() || !upper_stmt->is_single_table_stmt()) {
    // 仅对单表视图查询尝试提取 limit/offset
  } else if (OB_ISNULL(table = upper_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!is_oracle_mode() || select_stmt->is_fetch_with_ties()
             || NULL != select_stmt->get_offset_expr()
             || NULL != select_stmt->get_limit_percent_expr()
             || select_stmt->has_order_by()
             || select_stmt->has_rollup()
             || select_stmt->has_group_by()
             || select_stmt->has_window_function()) {
    // oracle 模式下, 以下情况禁止转化:
    // 1. is fetch with ties / has limit percent expr / has offset expr
    // 2. order by, 即使输出 rownum 也为乱序:
    //      select * from (select rounum rn, c1 from t order by c1) where rn > 10;
    // 3. rollup/group by 无法输出非聚合列 rownum
    // 4. window func 内部可能进行排序, 不进行转化
  } else {
    bool is_eq_cond = false;
    ObRawExpr *limit_cond = NULL;
    ObRawExpr *offset_cond = NULL;
    ObRawExpr *limit_value = NULL;
    ObRawExpr *offset_value = NULL;
    ObItemType limit_cmp_type = T_INVALID;
    ObItemType offset_cmp_type = T_INVALID;
    ObRawExpr *select_expr = NULL;
    ColumnItem *column_item = NULL;
    ObRawExpr *upper_cond_expr = NULL;
    ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    ObIArray<ObRawExpr*> &upper_conds = upper_stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && !is_eq_cond && i < select_items.count(); ++i) {
      if (OB_ISNULL(select_expr = select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!select_expr->has_flag(IS_ROWNUM)) {
        /*do nothing*/
      } else if (OB_ISNULL(column_item = upper_stmt->get_column_item_by_id(table->table_id_,
                                                                      i + OB_APP_MIN_COLUMN_ID))) {
        /*do nothing*/
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !is_eq_cond && j < upper_conds.count(); ++j) {
          if (OB_ISNULL(upper_cond_expr = upper_conds.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (upper_cond_expr->is_op_expr() && 2 == upper_cond_expr->get_param_count() &&
                     (IS_RANGE_CMP_OP(upper_cond_expr->get_expr_type())
                      || T_OP_EQ == upper_cond_expr->get_expr_type())) {
            ObRawExpr *param1 = upper_cond_expr->get_param_expr(0);
            ObRawExpr *param2 = upper_cond_expr->get_param_expr(1);
            ObRawExpr *const_expr = NULL;
            ObItemType expr_type = T_INVALID;
            if (OB_ISNULL(param1) || OB_ISNULL(param2)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null expr", K(ret), K(param1), K(param2));
            } else if (param1 == column_item->expr_ &&
                       param2->is_static_scalar_const_expr()) {
              expr_type = upper_cond_expr->get_expr_type();
              const_expr = param2;
            } else if (param1->is_static_scalar_const_expr() &&
                       param2 == column_item->expr_) {
              const_expr = param1;
              ret = ObOptimizerUtil::flip_op_type(upper_cond_expr->get_expr_type(), expr_type);
            } else {/*do nothing*/}

            // 多个可转换为 limit/offset 的conds, 仅转换第一个
            if (OB_FAIL(ret) || NULL == const_expr) {
            } else if (!const_expr->get_result_type().is_integer_type()
                       && !const_expr->get_result_type().is_number()) {
              /*do nothing*/
            } else if (T_OP_EQ == expr_type) { // 有等值条件优先转化
              limit_value = const_expr;
              limit_cond = upper_cond_expr;
              is_eq_cond = true;
              offset_cond = NULL;
            } else if (NULL == limit_cond && (T_OP_LE == expr_type || T_OP_LT == expr_type)) {
              limit_value = const_expr;
              limit_cmp_type = expr_type;
              limit_cond = upper_cond_expr;
            } else if (NULL == offset_cond && (T_OP_GE == expr_type || T_OP_GT == expr_type)) {
              offset_value = const_expr;
              offset_cmp_type = expr_type;
              offset_cond = upper_cond_expr;
            }
          }
        }
      }
    }

    ObRawExpr *init_limit_expr = NULL;
    ObRawExpr *init_offset_expr = NULL;
    ObRawExpr *minus_expr = NULL;
    ObConstRawExpr *zero_expr = NULL;
    ObConstRawExpr *one_expr = NULL;
    bool limit_is_not_neg = false;
    bool offset_is_not_neg = false;
    bool minus_is_not_neg = false;
    ObRawExpr* limit_cmp_expr = NULL;
    ObRawExpr* offset_cmp_expr = NULL;
    ObRawExpr* minus_cmp_expr = NULL;
    ObSEArray<ObRawExpr *, 4> params;
    bool is_not_neg = false;
    bool is_null = false;
    bool is_not_null = false;
    ObNotNullContext not_null_ctx(*ctx_, upper_stmt);
    if (OB_FAIL(ret) || (limit_cond == NULL && offset_cond == NULL)) {
    } else if (!lib::is_oracle_mode() && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                      ObIntType,
                                                      0,
                                                      zero_expr))) {
        LOG_WARN("create zero expr failed", K(ret));
    } else if (!lib::is_oracle_mode() && OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                      ObIntType,
                                                      1,
                                                      one_expr))) {
        LOG_WARN("create one expr failed", K(ret));
    } else if (lib::is_oracle_mode() && OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_,
                                                                                        ObNumberType,
                                                                                        ObNumber::get_zero(),
                                                                                        zero_expr))) {
      LOG_WARN("create zero expr failed", K(ret));
    } else if (lib::is_oracle_mode() && OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_,
                                                                                        ObNumberType,
                                                                                        ObNumber::get_positive_one(),
                                                                                        one_expr))) {
      LOG_WARN("create one expr failed", K(ret));
    } else if (limit_value != NULL && OB_FAIL(ObTransformUtils::is_const_expr_not_null(not_null_ctx, limit_value, is_not_null, is_null))) {
      LOG_WARN("check value is null failed", K(ret));
    } else if (limit_value != NULL && !is_not_null && !is_null) {
      //not calculable
    } else if (offset_value != NULL && !is_null &&
                                      OB_FAIL(ObTransformUtils::is_const_expr_not_null(not_null_ctx, offset_value, is_not_null, is_null))) {
      LOG_WARN("check value is null failed", K(ret));
    } else if (offset_value != NULL && !is_not_null && !is_null) {
      //not calculable
    } else if (NULL != limit_cond && OB_FAIL(ObOptimizerUtil::remove_item(upper_conds,
                                                                          limit_cond))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else if (NULL != offset_cond && OB_FAIL(ObOptimizerUtil::remove_item(upper_conds,
                                                                           offset_cond))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else if (is_null) {
      limit_expr = zero_expr;
      offset_expr = NULL;
      ObRawExpr *and_expr = NULL;
      ObRawExpr *tmp_is_not_expr = NULL;
      ObSEArray<ObRawExpr *, 2> not_null_exprs;
      if (limit_value != NULL && (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_,
                                                            limit_value,
                                                            true /*is_not_null*/,
                                                            tmp_is_not_expr)) ||
                                  OB_FAIL(not_null_exprs.push_back(tmp_is_not_expr)))) {
        LOG_WARN("push back failed", K(ret));
      } else if (offset_value != NULL && (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_,
                                                            offset_value,
                                                            true /*is_not_null*/,
                                                            tmp_is_not_expr)) ||
                                        OB_FAIL(not_null_exprs.push_back(tmp_is_not_expr)))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_, not_null_exprs, and_expr))) {
        LOG_WARN("build and expr failed", K(ret));
      } else if (OB_FAIL(and_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("formalize failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, and_expr, false))) {
        LOG_WARN("add cons failed", K(ret));
      }
    } else if (is_eq_cond) {
      if (OB_FAIL(transform_generated_rownum_eq_cond(limit_value, limit_expr, offset_expr))) {
        LOG_WARN("show limit expr", K(ret), K(limit_expr), K(offset_expr));
      }
    } else if (NULL != limit_value &&
               (OB_FAIL(ObOptimizerUtil::convert_rownum_filter_as_limit(*ctx_->expr_factory_,
                                                                        ctx_->session_info_,
                                                                        limit_cmp_type,
                                                                        limit_value,
                                                                        init_limit_expr)))) { //int > 0
      LOG_WARN("failed to create limit expr from rownum", K(ret));
    } else if (NULL != offset_value &&
              OB_FAIL(ObOptimizerUtil::convert_rownum_filter_as_offset(*ctx_->expr_factory_,
                                                                        ctx_->session_info_,
                                                                        offset_cmp_type,
                                                                        offset_value,
                                                                        init_offset_expr,
                                                                        zero_expr,
                                                                        offset_is_not_neg,
                                                                        ctx_))) {
      LOG_WARN("failed tp conver rownum as filter", K(ret));
    } else if (NULL != limit_value && OB_FAIL(ObTransformUtils::compare_const_expr_result(ctx_, init_limit_expr,
                                                                                T_OP_GE, 1,
                                                                                limit_is_not_neg))) {
      LOG_WARN("check is not neg false", K(ret));
    } else if (NULL != limit_value && OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                                                      ctx_->session_info_,
                                                                                      T_OP_GE,
                                                                                      limit_cmp_expr,
                                                                                      init_limit_expr,
                                                                                      one_expr))) {
      LOG_WARN("check is not neg false", K(ret));
    } else if (NULL != offset_value && OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                                                      ctx_->session_info_,
                                                                                      T_OP_GE,
                                                                                      offset_cmp_expr,
                                                                                      offset_value,
                                                                                      zero_expr))) {
      LOG_WARN("create expr failed", K(ret));
    } else if (offset_value == NULL && limit_value != NULL) {
      if (limit_is_not_neg) {
        limit_expr = init_limit_expr;
      } else {
        limit_expr = zero_expr;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, limit_cmp_expr, limit_is_not_neg))) {
        LOG_WARN("add cons failed", K(ret));
      }
    } else if (limit_value == NULL && offset_value != NULL) {
      offset_expr = init_offset_expr;
      if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, offset_cmp_expr, offset_is_not_neg))) {
        LOG_WARN("add cons failed", K(ret));
      }
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                             ctx_->session_info_,
                                                             T_OP_MINUS, minus_expr,
                                                             init_limit_expr,
                                                             init_offset_expr))) { // int - int = int
      LOG_WARN("failed to create double op expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::compare_const_expr_result(ctx_, minus_expr, T_OP_GE, 1,
                                                                        minus_is_not_neg))) {
      LOG_WARN("get const expr compare result", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                              ctx_->session_info_,
                                                              T_OP_GE,
                                                              minus_cmp_expr,
                                                              minus_expr,
                                                              one_expr))) {
      LOG_WARN("create expr failed", K(ret));
    } else if (OB_FAIL(params.push_back(minus_cmp_expr)) || OB_FAIL(params.push_back(limit_cmp_expr))) {
      LOG_WARN("push back failed", K(ret));
    } else if (!minus_is_not_neg || !limit_is_not_neg) {
      limit_expr = zero_expr;
      offset_expr = NULL;
      if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, offset_cmp_expr, offset_is_not_neg))) {
        LOG_WARN("add cons failed", K(ret));
      } else if (params.count() > 0) {
        ObRawExpr *and_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_, params, and_expr))) {
          LOG_WARN("build and expr failed", K(ret));
        } else if (OB_FAIL(and_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("formalize failed", K(ret));
        } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, and_expr, false))) {
          LOG_WARN("add cons failed", K(ret));
        }
      }
    } else {
      limit_expr = minus_expr;
      offset_expr = init_offset_expr;
      if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, offset_cmp_expr, offset_is_not_neg))) {
        LOG_WARN("add cons failed", K(ret));
      } else if (params.count() > 0) {
        ObRawExpr *and_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_, params, and_expr))) {
          LOG_WARN("build and expr failed", K(ret));
        } else if (OB_FAIL(and_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("formalize failed", K(ret));
        } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, and_expr, true))) {
          LOG_WARN("add cons failed", K(ret));
        }
      }
    }

  //添加 cast 作用:
  //  1.对于新引擎, 必须保证输出结果为 int
    ObExprResType dst_type;
    dst_type.set_int();
    ObSysFunRawExpr *cast_expr = NULL;
    if (NULL != limit_expr) {
      if (limit_expr->get_result_type().is_int()) {
        //do nothing
      } else {
        OZ(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_, limit_expr, dst_type,
                                            cast_expr, ctx_->session_info_));
        CK(NULL != cast_expr);
        if (OB_SUCC(ret)) {
          limit_expr = cast_expr;
        }
      }
    }
    if (OB_SUCC(ret) && NULL != offset_expr) {
      if (offset_expr->get_result_type().is_int()) {
        //do nothing
      } else {
        OZ(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_, offset_expr, dst_type,
                                            cast_expr, ctx_->session_info_));
        CK(NULL != cast_expr);
        if (OB_SUCC(ret)) {
          offset_expr = cast_expr;
        }
      }
    }
  }
  return ret;
}


//select * from (select rownum rn, ... from t) where rn = val;
//      => select * from (select rownum rn, ... from t limit ? offset ?);
//  val 正整数: limit 1 offset val - 1;
//        其它: limit 0 offset 0;
int ObTransformPreProcess::transform_generated_rownum_eq_cond(ObRawExpr *eq_value,
                                                              ObRawExpr *&limit_expr,
                                                              ObRawExpr *&offset_expr)
{
  int ret = OB_SUCCESS;
  limit_expr = NULL;
  offset_expr = NULL;
  ObRawExpr *minus_expr = NULL;
  ObConstRawExpr *zero_expr = NULL;
  ObConstRawExpr *one_expr = NULL;
  ObSysFunRawExpr *floor_expr = NULL;
  bool trans_limit_zero = false;
  bool eqval_is_ge_one = false;
  bool is_int = false;
  ObSEArray<ObRawExpr *, 4> params;
  ObRawExpr *ge_expr = NULL;
  ObRawExpr *eq_expr = NULL;
  if (OB_ISNULL(eq_value) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->exec_ctx_)
      || OB_ISNULL(ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_FUN_SYS_FLOOR, floor_expr))) {
    LOG_WARN("failed to create fun sys floor", K(ret));
  } else if (OB_ISNULL(floor_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("floor expr is null", K(ret));
  } else if (OB_FAIL(floor_expr->set_param_expr(eq_value))) {
    LOG_WARN("failed to set param expr", K(ret));
  } else if (OB_FAIL(floor_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("formalize failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                              ObIntType, 0, zero_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                              ObIntType, 1, one_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                          ctx_->session_info_, T_OP_MINUS,
                                                          minus_expr, eq_value, one_expr))) {
              LOG_WARN("failed to create double op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_, ctx_->session_info_, T_OP_GE,
                                                            ge_expr, eq_value, one_expr))) {
    LOG_WARN("create double op expr failed", K(ret));
  } else if (OB_FAIL(params.push_back(ge_expr))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_, ctx_->session_info_, T_OP_EQ,
                                                            eq_expr, eq_value, floor_expr))) {
    LOG_WARN("create double op expr failed", K(ret));
  } else if (OB_FAIL(params.push_back(eq_expr))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::compare_const_expr_result(ctx_, eq_value, T_OP_GE,
                                                                1, eqval_is_ge_one))) {
    LOG_WARN("ensure expr value not neg failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::compare_const_expr_result(ctx_, eq_expr, T_OP_EQ, 1, is_int))) {
    LOG_WARN("ensure expr value not neg failed", K(ret));
  } else if (is_int && eqval_is_ge_one) {
    limit_expr = one_expr;
    offset_expr = minus_expr;
    ObRawExpr *and_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_, params, and_expr))) {
      LOG_WARN("build and expr failed", K(ret));
    } else if (OB_FAIL(and_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("formalize failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, and_expr, true))) {
      LOG_WARN("add cons failed", K(ret));
    }
  } else {
    limit_expr = zero_expr;
    offset_expr = NULL;
    ObRawExpr *and_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::build_and_expr(*ctx_->expr_factory_, params, and_expr))) {
      LOG_WARN("build and expr failed", K(ret));
    } else  if (OB_FAIL(and_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("formalize failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_param_bool_constraint(ctx_, and_expr, false))) {
      LOG_WARN("add cons failed", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_udt_column_value_expr_inner(ObDMLStmt *stmt, ObDmlTableInfo &table_info, ObRawExpr *&old_expr, ObRawExpr *hidd_expr)
{
  // replace udt column in value expr
  int ret = OB_SUCCESS;
  if (old_expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *col_ref = static_cast<ObColumnRefRawExpr *>(old_expr);
    if (col_ref->is_xml_column()) {
      bool need_transform = false;
      ObColumnRefRawExpr *hidd_col = static_cast<ObColumnRefRawExpr *>(hidd_expr);
      for (int j = 0; j < table_info.column_exprs_.count() && OB_ISNULL(hidd_col); j++) {
        if (OB_FAIL(ObTransformUtils::create_udt_hidden_columns(ctx_, stmt, *col_ref, hidd_col, need_transform))) {
          LOG_WARN("failed to create udt hidden exprs", K(ret));
        }
      }
      if (!need_transform) {
        // do nothing
      } else if (OB_ISNULL(hidd_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hidden column is null", K(ret));
      } else if (!hidd_col->get_result_type().is_blob()) {
        // if column is not clob type, don't need add xml_binary
        old_expr = hidd_col;
      } else if (OB_FAIL(transform_xml_binary(hidd_col, old_expr))) {
        LOG_WARN("transform xml binary failed", K(ret));
      }
    }
  } else {
    for (int i = 0; i < old_expr->get_param_count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(transform_udt_column_value_expr_inner(stmt, table_info, old_expr->get_param_expr(i), hidd_expr))) {
        LOG_WARN("transform udt column value expr failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_udt_column_value_expr(ObDMLStmt *stmt, ObDmlTableInfo &table_info, ObRawExpr *old_expr, ObRawExpr *&new_expr, ObRawExpr *hidd_expr)
{
  int ret = OB_SUCCESS;
  // to do: add SYS_MAKEXMLBinary
  ObSysFunRawExpr *make_xml_expr = NULL;
  ObConstRawExpr *c_expr = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(transform_udt_column_value_expr_inner(stmt, table_info, old_expr, hidd_expr))) {
    LOG_WARN("replace udt column failed", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_FUN_SYS_PRIV_MAKE_XML_BINARY, make_xml_expr))) {
    LOG_WARN("failed to create fun make xml binary expr", K(ret));
  } else if (OB_ISNULL(make_xml_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml expr is null", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_INT, c_expr))) {
    LOG_WARN("create dest type expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null");
  } else {
    ObObj val;
    val.set_int(0);
    c_expr->set_value(val);
    c_expr->set_param(val);
    if (OB_FAIL(make_xml_expr->set_param_exprs(c_expr, old_expr))) {
      LOG_WARN("set param expr fail", K(ret));
    } else {
      make_xml_expr->set_func_name(ObString::make_string("_make_xml_binary"));
      if (OB_FAIL(make_xml_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("make xml epxr formlize fail", K(ret));
      }
      new_expr = make_xml_expr;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_udt_column_conv_param_expr(ObDmlTableInfo &table_info, ObRawExpr *old_expr, ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  // to do: add SYS_MAKEXMLBinary
  ObSysFunRawExpr *make_xml_expr = NULL;
  ObConstRawExpr *c_expr = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_FUN_SYS_PRIV_MAKE_XML_BINARY, make_xml_expr))) {
    LOG_WARN("failed to create fun make xml binary expr", K(ret));
  } else if (OB_ISNULL(make_xml_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml expr is null", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_INT, c_expr))) {
    LOG_WARN("create dest type expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null");
  } else {
    ObObj val;
    val.set_int(0);
    c_expr->set_value(val);
    c_expr->set_param(val);
    if (OB_FAIL(make_xml_expr->set_param_exprs(c_expr, old_expr))) {
      LOG_WARN("set param expr fail", K(ret));
    } else {
      make_xml_expr->set_func_name(ObString::make_string("_make_xml_binary"));
      if (OB_FAIL(make_xml_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("make xml epxr formlize fail", K(ret));
      }
      new_expr = make_xml_expr;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_udt_column_conv_function(ObDmlTableInfo &table_info,
                                                              ObIArray<ObRawExpr*> &column_conv_exprs,
                                                              ObColumnRefRawExpr &udt_col,
                                                              ObColumnRefRawExpr &hidd_col)
{
  int ret = OB_SUCCESS;
  bool hidd_found = false;

  for (int64_t j = 0; OB_SUCC(ret) && !hidd_found && j < table_info.column_exprs_.count(); ++j) {
    ObColumnRefRawExpr *col = table_info.column_exprs_.at(j);
    if (col->get_column_id() == hidd_col.get_column_id()) {
      ObRawExpr *old_conv_expr = column_conv_exprs.at(j);
      const ObColumnRefRawExpr *tmp = ObRawExprUtils::get_column_ref_expr_recursively(old_conv_expr->get_param_expr(4));
      ObRawExpr *old_col_ref = const_cast<ObRawExpr *>(static_cast<const ObRawExpr *>(tmp));
      hidd_found = true;
      ObRawExpr *new_value_expr = NULL;
      if (OB_ISNULL(old_col_ref)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old expr is null", K(ret));
      } else if (OB_FAIL(transform_udt_column_conv_param_expr(table_info, old_col_ref, new_value_expr))) {
        LOG_WARN("failed to transform udt value exprs", K(ret));
      } else if (OB_FAIL(static_cast<ObSysFunRawExpr*>(old_conv_expr)->replace_param_expr(4, new_value_expr))) {
        LOG_WARN("failed to push back udt hidden exprs", K(ret));
      }
    }
  }
  return ret;
}


int ObTransformPreProcess::replace_udt_assignment_exprs(ObDMLStmt *stmt,
                                                        ObDmlTableInfo &table_info,
                                                        ObIArray<ObAssignment> &assignments,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;

  for (int64_t j = 0; OB_SUCC(ret) && j < assignments.count(); ++j) {
    ObColumnRefRawExpr *hidd_col = NULL;
    ObRawExpr *value_expr = NULL;
    ObAssignment &assign = assignments.at(j);
    bool trigger_exist = false;
    if (OB_ISNULL(assign.column_expr_) || OB_ISNULL(assign.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assgin expr is null", K(ret));
    } else if (!assign.column_expr_->is_xml_column()) {
      // do nothins
    } else {
      for (int j = 0; j < table_info.column_exprs_.count() && OB_ISNULL(hidd_col); j++) {
        if (table_info.column_exprs_.at(j)->get_column_id() != assign.column_expr_->get_column_id() &&
            table_info.column_exprs_.at(j)->get_udt_set_id() == assign.column_expr_->get_udt_set_id()) {
            hidd_col = table_info.column_exprs_.at(j);
        }
      }
      if (OB_ISNULL(hidd_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("hidden column is null", K(ret));
       } else if (assign.expr_->get_expr_type() == T_FUN_SYS_WRAPPER_INNER) {
        trigger_exist = true;
        value_expr = assign.expr_->get_param_expr(0);
      } else {
        value_expr = assign.expr_;
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (value_expr->get_expr_type() != T_FUN_COLUMN_CONV) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpectd expr type", K(ret), K(assign.expr_->get_expr_type()));
      } else if (OB_ISNULL(value_expr = value_expr->get_param_expr(4))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw expr param is null");
      } else if (value_expr->get_expr_type() == T_FUN_SYS_CAST) {
        // don't need cast expr
        value_expr = value_expr->get_param_expr(0);
      } else if (value_expr->is_const_raw_expr()) {
        if (value_expr->get_expr_type() == T_QUESTIONMARK) {
          const ParamStore &param_store = ctx_->exec_ctx_->get_physical_plan_ctx()->get_param_store();
          ObConstRawExpr *param_expr = static_cast<ObConstRawExpr *>(value_expr);
          int64_t param_idx = param_expr->get_value().get_unknown();
          if (param_idx < 0 || param_idx >= param_store.count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("param_idx is invalid", K(ret), K(param_idx));
          } else if (param_store.at(param_idx).is_xml_sql_type() ||
                     param_store.at(param_idx).is_extend_xml_type()){
            // do nothing
          } else if (ob_is_number_tc(param_store.at(param_idx).get_type())) {
            ret = OB_ERR_INVALID_TYPE_FOR_OP;
            LOG_WARN("old_expr_type invalid ObLongTextType type", K(ret), K(param_store.at(param_idx).get_type()));
          } else {
            ObExprResType res_type;
            res_type.set_varchar();
            res_type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
            value_expr->set_result_type(res_type);
          }
        }
      }
    }
    if (OB_SUCC(ret) && assign.column_expr_->is_xml_column()) {
      ObRawExpr *new_value_expr = NULL;
      ObRawExpr *old_column_expr = assign.column_expr_;
      if (OB_FAIL(transform_udt_column_value_expr(stmt, table_info, value_expr, new_value_expr))){
        LOG_WARN("failed to transform udt value exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(*ctx_->expr_factory_,
                                                                *ctx_->allocator_,
                                                                *hidd_col,
                                                                new_value_expr,
                                                                ctx_->session_info_))) {
        LOG_WARN("fail to build column conv expr", K(ret), K(hidd_col));
      } else {
        trans_happened = true;
        assign.column_expr_ = hidd_col;
        if (trigger_exist) {
          if (OB_FAIL(static_cast<ObSysFunRawExpr*>(assign.expr_)->replace_param_expr(0, new_value_expr))) {
            LOG_WARN("failed to replace wrapper expr param", K(ret));
          }
        } else {
          assign.expr_ = new_value_expr;
        }
      }
      // process returning clause for update
      if (OB_SUCC(ret) && stmt->get_stmt_type() == stmt::T_UPDATE) {
        ObDelUpdStmt *update_stmt = static_cast<ObDelUpdStmt *>(stmt);
        if (update_stmt->is_returning()) {
          for (int64_t i = 0; OB_SUCC(ret) && i < update_stmt->get_returning_exprs().count(); i++) {
            ObRawExpr *sys_makexml_expr = NULL;
            if (OB_ISNULL(hidd_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("hidden col is NULL", K(ret), K(i), K(j));
            } else if (OB_FAIL(transform_xml_binary(new_value_expr, sys_makexml_expr))) {
              LOG_WARN("fail to create sys_makexml expr", K(ret));
            } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(update_stmt->get_returning_exprs().at(i),
                                                                  old_column_expr,
                                                                  sys_makexml_expr))) {
              LOG_WARN("fail to replace xml column in returning exprs", K(ret), K(i));
            }
          } // end for
        }
      } //end if: process returning clause
    }
  }

  return ret;
}

int ObTransformPreProcess::transform_xml_binary(ObRawExpr *hidden_blob_expr, ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *sys_makexml = NULL;
  ObConstRawExpr *c_expr = NULL;
  ObColumnRefRawExpr *hidd_col = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(hidden_blob_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(ctx_), KP(hidden_blob_expr));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_FUN_SYS_MAKEXML, sys_makexml))) {
    LOG_WARN("failed to create fun sys_makexml expr", K(ret));
  } else if (OB_ISNULL(sys_makexml)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys_makexml expr is null", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_INT, c_expr))) {
    LOG_WARN("create dest type expr failed", K(ret));
  } else if (OB_ISNULL(c_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const expr is null");
  } else {
    ObObj val;
    val.set_int(0);
    c_expr->set_value(val);
    c_expr->set_param(val);
    if (OB_FAIL(sys_makexml->set_param_exprs(c_expr, hidden_blob_expr))) {
      LOG_WARN("set param expr fail", K(ret));
    } else if (FALSE_IT(sys_makexml->set_func_name(ObString::make_string("SYS_MAKEXML")))) {
    } else if (OB_FAIL(sys_makexml->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize", K(ret));
    } else {
      new_expr = sys_makexml;
    }
  }
  return ret;
}

int ObTransformPreProcess::set_hidd_col_not_null_attr(const ObColumnRefRawExpr &udt_col, ObIArray<ObColumnRefRawExpr *> &column_exprs)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t j = 0; OB_SUCC(ret) && !found && j < column_exprs.count(); ++j) {
    ObColumnRefRawExpr *col_hidden = column_exprs.at(j);
    if (col_hidden->get_udt_set_id() == udt_col.get_udt_set_id() &&
        udt_col.get_column_id() != col_hidden->get_column_id()) {
      found = true;
      if (udt_col.get_result_type().has_result_flag(HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG)) {
        col_hidden->set_result_flag(HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG);
      }
      if (udt_col.get_result_type().has_result_flag(NOT_NULL_WRITE_FLAG)) {
        col_hidden->set_result_flag(NOT_NULL_WRITE_FLAG);
      }
    }
  }
  if (!found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find hidden column", K(ret), K(udt_col.get_udt_set_id()));
  }
  return ret;
}

int ObTransformPreProcess::get_dml_view_col_exprs(const ObDMLStmt *stmt, ObIArray<ObColumnRefRawExpr*> &assign_col_exprs)
{
  int ret = OB_SUCCESS;
  if (stmt->get_stmt_type() == stmt::T_UPDATE) {
    const ObUpdateStmt *upd_stmt = static_cast<const ObUpdateStmt*>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_stmt->get_update_table_info().count(); ++i) {
      ObUpdateTableInfo* table_info = upd_stmt->get_update_table_info().at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < table_info->assignments_.count(); ++j) {
        ObAssignment &assign = table_info->assignments_.at(j);
        if (OB_ISNULL(assign.column_expr_) || OB_ISNULL(assign.expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("assgin expr is null", K(ret));
        } else if (assign.column_expr_->is_xml_column() &&
                  OB_FAIL(add_var_to_array_no_dup(assign_col_exprs, assign.column_expr_))) {
          LOG_WARN("failed to push back column expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::get_update_generated_udt_in_parent_stmt(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                                                   const ObDMLStmt *stmt,
                                                                   ObIArray<ObColumnRefRawExpr*> &col_exprs)
{
  int ret = OB_SUCCESS;

  const ObDMLStmt *root_stmt = NULL;
  ObSEArray<ObColumnRefRawExpr*, 8> parent_col_exprs;
  const ObSelectStmt *sel_stmt = NULL;
  const ObDMLStmt *parent_stmt = NULL;
  int64_t table_id = OB_INVALID;
  bool is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_select_stmt() || parent_stmts.empty()) {
    //do nothing
  } else if (OB_FALSE_IT(sel_stmt = static_cast<const ObSelectStmt*>(stmt))) {
  } else if (OB_ISNULL(root_stmt = parent_stmts.at(parent_stmts.count()-1).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_parent_stmt(root_stmt, stmt, parent_stmt, table_id, is_valid))) {
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else if (OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(get_dml_view_col_exprs(parent_stmt, parent_col_exprs))) {
    LOG_WARN("failed to get assignment columns", K(ret));
  } else if (!parent_col_exprs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_col_exprs.count(); ++i) {
      ObColumnRefRawExpr* col = parent_col_exprs.at(i);
      int64_t sel_idx = -1;
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null column expr", K(ret));
      } else if (OB_FALSE_IT(sel_idx = col->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
      } else if (sel_idx < 0 || sel_idx >= sel_stmt->get_select_item_size()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item index is incorrect", K(sel_idx), K(ret));
      } else {
        ObRawExpr *sel_expr = sel_stmt->get_select_item(sel_idx).expr_;
        ObColumnRefRawExpr *col_expr = NULL;
        if (OB_ISNULL(sel_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (!sel_expr->is_column_ref_expr()) {
          //do nothing
        } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(sel_expr))) {
        } else if (OB_FAIL(add_var_to_array_no_dup(col_exprs, col_expr))) {
          LOG_WARN("failed to push back column expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_skip_child_select_view(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                                          ObDMLStmt *stmt, bool &skip_for_view_table)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt *sel_stmt = NULL;
  const ObDMLStmt *parent_stmt = NULL;
  bool is_valid = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_select_stmt() || parent_stmts.count() != 1 ||
             stmt->get_table_size() != 1) {
    //do nothing
  } else if (OB_FALSE_IT(sel_stmt = static_cast<const ObSelectStmt*>(stmt))) {
  } else if (OB_ISNULL(parent_stmt = parent_stmts.at(0).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get parent stmt", K(ret));
  } else if (parent_stmt->get_table_size() != 1 ||
             !(parent_stmt->is_delete_stmt() || parent_stmt->is_update_stmt())) {
    // do nothing
  } else {
    const sql::TableItem *basic_table_item = stmt->get_table_item(0);
    const sql::TableItem *view_table_item = parent_stmt->get_table_item(0);
    if (OB_ISNULL(basic_table_item) || OB_ISNULL(view_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table item", K(ret));
    } else if (basic_table_item->is_basic_table() && view_table_item->is_generated_table()) {
      if (OB_NOT_NULL(view_table_item->ref_query_) &&
          view_table_item->ref_query_->get_table_size() > 0 &&
          OB_NOT_NULL(view_table_item->ref_query_->get_table_item(0)) &&
          basic_table_item->table_id_ == view_table_item->ref_query_->get_table_item(0)->table_id_) {
        skip_for_view_table = true;
      }
    } else if (!basic_table_item->is_basic_table() || !view_table_item->is_view_table_) {
      // do nothing
    } else if (OB_ISNULL(view_table_item->view_base_item_)) {
      // do nothing
    } else if (basic_table_item->table_id_ == view_table_item->view_base_item_->table_id_) {
      skip_for_view_table = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_query_udt_columns_exprs(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                                             ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<DmlStmtScope, 4> scopes;
  ObSEArray<ObRawExpr*, 4> replace_exprs;
  ObSEArray<ObRawExpr*, 4> from_col_exprs;
  ObSEArray<ObRawExpr*, 4> to_col_exprs;
  ObSEArray<ObColumnRefRawExpr*, 8> parent_assign_xml_col_exprs;
  bool skip_for_view_table = false;

  if (OB_FAIL(get_update_generated_udt_in_parent_stmt(parent_stmts, stmt, parent_assign_xml_col_exprs))) {
    LOG_WARN("Fail to get update generated column array.", K(ret));
  } else if (OB_FAIL(check_skip_child_select_view(parent_stmts, stmt, skip_for_view_table))) {
    LOG_WARN("Fail to check if select in delete view.", K(ret));
  } else if (skip_for_view_table) {
    // do nothing
  } else {
    FastUdtExprChecker expr_checker(replace_exprs);
    if (OB_FAIL(scopes.push_back(SCOPE_DML_COLUMN)) ||
        (stmt->get_stmt_type() != stmt::T_MERGE && OB_FAIL(scopes.push_back(SCOPE_DML_VALUE))) ||
        OB_FAIL(scopes.push_back(SCOPE_DML_CONSTRAINT)) ||
        OB_FAIL(scopes.push_back(SCOPE_INSERT_DESC)) ||
        OB_FAIL(scopes.push_back(SCOPE_BASIC_TABLE)) ||
        OB_FAIL(scopes.push_back(SCOPE_DICT_FIELDS)) ||
        OB_FAIL(scopes.push_back(SCOPE_SHADOW_COLUMN)) ||
        ((stmt->get_stmt_type() == stmt::T_INSERT || stmt->get_stmt_type() == stmt::T_UPDATE) &&
          OB_FAIL(scopes.push_back(SCOPE_RETURNING))) ||
        (stmt->get_stmt_type() != stmt::T_MERGE && OB_FAIL(scopes.push_back(SCOPE_INSERT_VECTOR)))) {
      LOG_WARN("Fail to create scope array.", K(ret));
    }
    ObStmtExprGetter visitor;
    visitor.checker_ = &expr_checker;
    visitor.remove_scope(scopes);
    if (OB_SUCC(ret) && OB_FAIL(stmt->iterate_stmt_expr(visitor))) {
      LOG_WARN("get relation exprs failed", K(ret));
    }

    //collect query udt exprs which need to be replaced
    for (int64_t i = 0; OB_SUCC(ret) && i < replace_exprs.count(); i++) {
      if (OB_ISNULL(replace_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replace expr is null", K(ret));
      } else {
        ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(replace_exprs.at(i));
        ObColumnRefRawExpr *hidd_col = NULL;
        ObRawExpr *sys_makexml_expr = NULL;
        bool need_transform = false;
        if (!col_expr->is_xml_column() || has_exist_in_array(parent_assign_xml_col_exprs, col_expr)) {
          // do nothing
        } else if (OB_FAIL(ObTransformUtils::create_udt_hidden_columns(ctx_,
                                                                      stmt,
                                                                      *col_expr,
                                                                      hidd_col,
                                                                      need_transform))) {
          LOG_WARN("failed to create udt hidden exprs", K(ret));
        } else if (need_transform == false) {
          // do nothing
        } else if (OB_FAIL(transform_xml_binary(hidd_col, sys_makexml_expr))) {
          LOG_WARN("failed to push back udt hidden exprs", K(ret));
        } else if (OB_FAIL(from_col_exprs.push_back(col_expr))) {
          LOG_WARN("failed to push back udt exprs", K(ret));
        } else if (OB_FAIL(to_col_exprs.push_back(sys_makexml_expr))) {
          LOG_WARN("failed to push back udt hidden exprs", K(ret));
        }
      }

      //do replace
      if (OB_SUCC(ret) && !from_col_exprs.empty()) {
        ObStmtExprReplacer replacer;
        replacer.remove_scope(scopes);
        if (OB_FAIL(replacer.add_replace_exprs(from_col_exprs, to_col_exprs))) {
          LOG_WARN("failed to add replace exprs", K(ret));
        } else if (OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
          LOG_WARN("failed to iterate stmt expr", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }
  }

  return ret;
}

int ObTransformPreProcess::transform_udt_columns_constraint_exprs(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<DmlStmtScope, 4> scopes;
  ObSEArray<ObRawExpr*, 4> replace_exprs;
  ObSEArray<ObRawExpr*, 4> from_col_exprs;
  ObSEArray<ObRawExpr*, 4> to_col_exprs;
  FastUdtExprChecker expr_checker(replace_exprs);
  ObStmtExprGetter visitor;
  visitor.remove_all();
  visitor.add_scope(SCOPE_DML_CONSTRAINT);
  visitor.checker_ = &expr_checker;

  if (OB_SUCC(ret) && OB_FAIL(stmt->iterate_stmt_expr(visitor))) {
    LOG_WARN("get relation exprs failed", K(ret));
  }

  //collect query udt exprs which need to be replaced
  for (int64_t i = 0; OB_SUCC(ret) && i < replace_exprs.count(); i++) {
    if (OB_ISNULL(replace_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("replace expr is null", K(ret));
    } else {
      ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr*>(replace_exprs.at(i));
      ObColumnRefRawExpr *hidd_col = NULL;
      ObRawExpr *sys_makexml_expr = NULL;
      bool need_transform = false;
      if (!col_expr->is_xml_column()) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::create_udt_hidden_columns(ctx_,
                                                                     stmt,
                                                                     *col_expr,
                                                                     hidd_col,
                                                                     need_transform))) {
        LOG_WARN("failed to create udt hidden exprs", K(ret));
      } else if (need_transform == false) {
        // do nothing;
      } else if (OB_FAIL(transform_xml_binary(hidd_col, sys_makexml_expr))) {
        LOG_WARN("failed to push back udt hidden exprs", K(ret));
      } else if (OB_FAIL(from_col_exprs.push_back(col_expr))) {
        LOG_WARN("failed to push back udt exprs", K(ret));
      } else if (OB_FAIL(to_col_exprs.push_back(sys_makexml_expr))) {
        LOG_WARN("failed to push back udt hidden exprs", K(ret));
      }
    }
  }

  //do replace
  if (OB_SUCC(ret) && !from_col_exprs.empty()) {
    ObStmtExprReplacer replacer;
    replacer.remove_all();
    replacer.add_scope(SCOPE_DML_CONSTRAINT);
    if (OB_FAIL(replacer.add_replace_exprs(from_col_exprs, to_col_exprs))) {
      LOG_WARN("failed to add replace exprs", K(ret));
    } else if (OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
      LOG_WARN("failed to iterate stmt expr", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}


int ObTransformPreProcess::transform_udt_columns(const ObIArray<ObParentDMLStmt> &parent_stmts,
                                                 ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<ObRawExpr*, 1> udt_exprs;
  ObSEArray<ObRawExpr *, 1> from_col_exprs;
  ObSEArray<ObRawExpr *, 1> to_col_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret), K(ctx_), K(ctx_->session_info_));
  } else if (is_mysql_mode() || ctx_->session_info_->get_ddl_info().is_ddl()) {
    // do nothing
  } else if (OB_FAIL(transform_query_udt_columns_exprs(parent_stmts, stmt, trans_happened))) {
      LOG_WARN("failed to do query udt exprs transform", K(ret));
  } else if (OB_FAIL(transform_udt_columns_constraint_exprs(stmt, trans_happened))) {
    LOG_WARN("failed to do udt constraint exprs transform", K(ret));
  } else {
    // xmltype transform
    if (stmt->get_stmt_type() == stmt::T_UPDATE) {
      ObUpdateStmt *upd_stmt = static_cast<ObUpdateStmt *>(stmt);
      for (int64_t i = 0; OB_SUCC(ret) && i < upd_stmt->get_update_table_info().count(); ++i) {
        bool is_happened = false;
        ObUpdateTableInfo* table_info = upd_stmt->get_update_table_info().at(i);
        if (OB_ISNULL(table_info)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null table info", K(ret));
        } else if (OB_FAIL(replace_udt_assignment_exprs(upd_stmt, *table_info, table_info->assignments_,
                                                        is_happened))) {
          LOG_WARN("failed to replace assignment exprs", K(ret));
        } else {
          trans_happened |= is_happened;
          LOG_TRACE("succeed to do const propagation for assignment expr", K(is_happened));
        }
      }
    } else if (stmt->get_stmt_type() == stmt::T_MERGE) {
      bool is_happened = false;
      ObMergeStmt *merge_stmt = static_cast<ObMergeStmt *>(stmt);
      ObMergeTableInfo &table_info = merge_stmt->get_merge_table_info();
      if (OB_FAIL(replace_udt_assignment_exprs(merge_stmt, table_info,
                                               table_info.assignments_,
                                               is_happened))) {
        LOG_WARN("failed to replace assignment exprs", K(ret));
      } else if (table_info.values_desc_.count() == 0) {
        // do nothing
      } else {
        trans_happened |= is_happened;
        common::ObIArray<ObRawExpr*> &value_vector = table_info.values_vector_;
        int64_t column_count =  table_info.values_desc_.count();
        int64_t row_count = value_vector.count() / column_count;

        for (int64_t i = 0; OB_SUCC(ret) && i < table_info.column_exprs_.count(); ++i) {
          ObColumnRefRawExpr *col = table_info.column_exprs_.at(i);
          if (col->is_xml_column()) {
            if (OB_FAIL(set_hidd_col_not_null_attr(*col, table_info.column_exprs_))) {
              LOG_WARN("failed to set hidden column not null attr", K(ret));
            }
          }
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < table_info.values_desc_.count(); ++i) {
          ObColumnRefRawExpr *value_desc = table_info.values_desc_.at(i);
          if (value_desc->is_xml_column()) {
            ObColumnRefRawExpr *hidd_col = NULL;
            ObRawExpr *new_value_expr = NULL;
            ObRawExpr *value_expr = value_vector.at(i);
            bool need_transform = false; // not_used
            if (OB_FAIL(ObTransformUtils::create_udt_hidden_columns(ctx_, merge_stmt, *value_desc, hidd_col, need_transform))) {
              LOG_WARN("failed to create udt hidden exprs", K(ret));
            } else if (need_transform == false) {
              // do nothing
            } else {
              bool hidd_found = false;
              for (int64_t j = 0; OB_SUCC(ret) && !hidd_found && j < table_info.column_exprs_.count(); ++j) {
                ObColumnRefRawExpr *col = table_info.column_exprs_.at(j);
                if (col->get_column_id() == hidd_col->get_column_id()) {
                  ObRawExpr *old_conv_expr = merge_stmt->get_column_conv_exprs().at(j);
                  hidd_found = true;
                  if (OB_FAIL(transform_udt_column_conv_param_expr(table_info, value_expr, new_value_expr))) {
                    LOG_WARN("failed to transform udt value exprs", K(ret));
                  } else if (OB_FAIL(static_cast<ObSysFunRawExpr*>(old_conv_expr)->replace_param_expr(4, new_value_expr))) {
                    LOG_WARN("failed to push back udt hidden exprs", K(ret));
                  }
                }
              }
            }
          }
        }

        LOG_TRACE("succeed to do const propagation for assignment expr", K(is_happened));
      }
    } else if (stmt->get_stmt_type() == stmt::T_INSERT || stmt->get_stmt_type() == stmt::T_INSERT_ALL) {
      ObDelUpdStmt *insert_stmt = static_cast<ObDelUpdStmt*>(stmt);
      // common::ObIArray<ObRawExpr*> &value_vector = insert_stmt->get_values_vector();
      // int64_t column_count = insert_stmt->get_values_desc().count();
      // int64_t row_count = insert_stmt->get_values_vector().count() / column_count;
      ObInsertAllStmt *insert_all_stmt = static_cast<ObInsertAllStmt*>(stmt);
      ObInsertStmt *single_insert_stmt = static_cast<ObInsertStmt*>(stmt);
      common::ObArray<ObInsertTableInfo*> table_info_arr;
      if (stmt->get_stmt_type() == stmt::T_INSERT) {
        table_info_arr.push_back(&(single_insert_stmt->get_insert_table_info()));
      } else {
        ObIArray<ObInsertAllTableInfo*> &table_infos = insert_all_stmt->get_insert_all_table_info();
        for (int64_t i = 0; i < table_infos.count(); i++) {
          table_info_arr.push_back(table_infos.at(i));
        }
      }

      for (int64_t count = 0; OB_SUCC(ret) && count < table_info_arr.count(); ++count) {
        ObInsertTableInfo *table_info = table_info_arr.at(count);
        for (int64_t i = 0; OB_SUCC(ret) && i < table_info->column_exprs_.count(); ++i) {
          ObColumnRefRawExpr *col = table_info->column_exprs_.at(i);
          if (col->is_xml_column()) {
            if (OB_FAIL(set_hidd_col_not_null_attr(*col, table_info->column_exprs_))) {
              LOG_WARN("failed to set hidden column not null attr", K(ret));
            }
          }
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < table_info->values_desc_.count(); ++i) {
          ObColumnRefRawExpr *value_desc = table_info->values_desc_.at(i);
          if (value_desc->is_xml_column()) {
            ObColumnRefRawExpr *hidd_col = NULL;
            bool need_transform = false;
            if (OB_FAIL(ObTransformUtils::create_udt_hidden_columns(ctx_,
                                                                    insert_stmt,
                                                                    *value_desc,
                                                                    hidd_col,
                                                                    need_transform))) {
              LOG_WARN("failed to create udt hidden exprs", K(ret));
            } else if (need_transform == false) {
              // do nothing
            } else if (OB_FAIL(transform_udt_column_conv_function(*table_info,
                                                                  table_info->column_conv_exprs_,
                                                                  *value_desc, *hidd_col))) {
              LOG_WARN("failed to push back column conv exprs", K(ret));
            }
          }
        }
        // process returning exprs
        if (OB_SUCC(ret) && insert_stmt->is_returning()) {
          ObIArray<ObRawExpr*> &column_convert = table_info->column_conv_exprs_;
          const ObIArray<ObColumnRefRawExpr *> &table_columns = table_info->column_exprs_;
          ObSEArray<std::pair<int64_t, int64_t>, 8> xml_col_idxs; // use pair to store xml col idx and its hidden col idx
          for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.count(); i++) {
            ObColumnRefRawExpr *ref_col = table_columns.at(i);
            if (ref_col->is_xml_column()) {
              bool is_found = false;
              for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < table_columns.count(); j++) {
                if (ref_col->get_column_id() != table_columns.at(j)->get_column_id() &&
                    ref_col->get_udt_set_id() == table_columns.at(j)->get_udt_set_id()) {
                  is_found = true;
                  xml_col_idxs.push_back(std::make_pair(i, j));
                }
              } // end for
              if (!is_found) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("failed to find hidden column", K(ret), K(i));
              }
            }
          } // end for

          CK(column_convert.count() == table_columns.count());
          for (int64_t i = 0; OB_SUCC(ret) && i < insert_stmt->get_returning_exprs().count(); i++) {
            for (int64_t j = 0; OB_SUCC(ret) && j < xml_col_idxs.count(); j++) {
              ObRawExpr *hidd_col = NULL;
              ObRawExpr *sys_makexml_expr = NULL;
              if (OB_ISNULL(hidd_col = column_convert.at(xml_col_idxs.at(j).second))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("hidden col is NULL", K(ret), K(i), K(j));
              } else if (OB_FAIL(transform_xml_binary(hidd_col, sys_makexml_expr))) {
                LOG_WARN("fail to create expr sys_makexml", K(ret));
              } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(insert_stmt->get_returning_exprs().at(i),
                                                                table_columns.at(xml_col_idxs.at(j).first),
                                                                sys_makexml_expr))) {
                LOG_WARN("fail to replace xml column in returning exprs", K(ret), K(i), K(j));
              }
            }
          }
        } // end if
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transformer_rowid_expr(ObDMLStmt *stmt,
                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<ObRawExpr*, 1> rowid_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (is_mysql_mode()) {
    /*do nothing*/
  } else if (OB_FAIL(stmt->get_stmt_rowid_exprs(rowid_exprs))) {
    LOG_WARN("failed to get stmt rowid exprs", K(ret));
  } else if (!rowid_exprs.empty()) {
    ObSEArray<ObRawExpr*, 1> old_rowid_exprs;
    ObSEArray<ObRawExpr*, 1> new_rowid_exprs;
    //before transform pre, rowid expr just only have two expr types:
    // 1. sys calc urowid expr is used to calc rowid or sys calc urowid expr have been calculated.
    // 2. empty rowid column expr for generate table, we need transform empty rowid column expr
    //    to real sys calc urowid expr here.
    for (int i = 0; OB_SUCC(ret) && i < rowid_exprs.count(); ++i) {
      ObRawExpr *expr = NULL;
      if (OB_ISNULL(expr = rowid_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(expr));
      } else if (expr->get_expr_type() == T_FUN_SYS_CALC_UROWID ||
                 expr->get_expr_type() == T_QUESTIONMARK) {
        /*do nothing*/
      } else if (expr->is_column_ref_expr()) {
        ObRawExpr *new_rowid_expr = NULL;
        if (OB_FAIL(old_rowid_exprs.push_back(expr))) {
          LOG_WARN("failed to push back rowid expr", K(ret));
        } else if (OB_FAIL(do_transform_rowid_expr(*stmt,
                                                   static_cast<ObColumnRefRawExpr*>(expr),
                                                   new_rowid_expr))) {
          LOG_WARN("failed to do transform rowid expr", K(ret));
        } else if (OB_ISNULL(new_rowid_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(new_rowid_expr));
        } else if (OB_FAIL(new_rowid_exprs.push_back(new_rowid_expr))) {
          LOG_WARN("failed to push back rowid expr", K(ret));
        } else if (OB_FAIL(stmt->remove_column_item(
                                        static_cast<ObColumnRefRawExpr*>(expr)->get_table_id(),
                                        static_cast<ObColumnRefRawExpr*>(expr)->get_column_id()))) {
          LOG_WARN("failed to remove column item", K(ret));
        } else {/*do nothing*/}
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected rowid expr", K(ret), KPC(expr));
      }
    }
    if (OB_SUCC(ret) && !old_rowid_exprs.empty()) {
      if (OB_UNLIKELY(old_rowid_exprs.count() != new_rowid_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(old_rowid_exprs), K(new_rowid_exprs), K(ret));
      } else if (OB_FAIL(stmt->replace_relation_exprs(old_rowid_exprs, new_rowid_exprs))) {
        LOG_WARN("replace inner stmt expr failed", K(ret));
      } else {
        trans_happened = true;
        LOG_DEBUG("Succeed to transformer rowid expr", K(*stmt), K(old_rowid_exprs),
                                                       K(new_rowid_exprs));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::do_transform_rowid_expr(ObDMLStmt &stmt,
                                                   ObColumnRefRawExpr *empty_rowid_col_expr,
                                                   ObRawExpr *&new_rowid_expr)
{
  int ret = OB_SUCCESS;
  new_rowid_expr = NULL;
  if (OB_ISNULL(empty_rowid_col_expr) ||
      OB_UNLIKELY(!ObCharset::case_insensitive_equal(OB_HIDDEN_LOGICAL_ROWID_COLUMN_NAME,
                                                     empty_rowid_col_expr->get_column_name()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), KPC(empty_rowid_col_expr));
  } else {
    TableItem *table_item = NULL;
    if (OB_ISNULL(table_item = stmt.get_table_item_by_id(empty_rowid_col_expr->get_table_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_item is NULL", K(ret), K(empty_rowid_col_expr->get_table_id()));
    } else if (OB_UNLIKELY(!table_item->is_generated_table() && !table_item->is_temp_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(*table_item));
    } else if (OB_FAIL(recursive_generate_rowid_select_item(table_item->ref_query_,
                                                            new_rowid_expr))) {
      LOG_WARN("failed to recursive generate rowid select item", K(ret));
    } else if (OB_ISNULL(new_rowid_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new_rowid_expr cannot be NULL", K(ret));
    } else if (OB_FAIL(create_rowid_item_for_stmt(&stmt, table_item, new_rowid_expr))) {
      LOG_WARN("create rowid item for stmt failed", K(ret));
    } else {
      LOG_TRACE("succeed to do transform rowid expr", K(*new_rowid_expr), K(stmt));
    }
  }
  return ret;
}

int ObTransformPreProcess::add_rowid_constraint(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL (exec_ctx = ctx_->exec_ctx_)|| OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx_ or query_ctx is NULL", K(ret), KP(ctx_), KP(stmt.get_query_ctx()));
  }
  for (int i = 0; OB_SUCC(ret) && i < stmt.get_condition_size(); ++i) {
    ObRawExpr *expr = stmt.get_condition_expr(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is NULL", K(ret));
    } else if (expr->get_param_count() == 2) {
      ObRawExpr *l_expr = expr->get_param_expr(0);
      ObRawExpr *r_expr = expr->get_param_expr(1);
      ObRawExpr *calc_urowid_expr = NULL;
      ObRawExpr *const_expr = NULL;
      if (OB_ISNULL(l_expr) || OB_ISNULL(r_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret), KP(l_expr), KP(r_expr));
      } else if ((l_expr->get_expr_type() == T_FUN_SYS_CALC_UROWID &&
                  r_expr->is_static_scalar_const_expr())) {
        calc_urowid_expr = l_expr;
        const_expr = r_expr;
      } else if ((r_expr->get_expr_type() == T_FUN_SYS_CALC_UROWID &&
                  l_expr->is_static_scalar_const_expr())) {
        calc_urowid_expr = r_expr;
        const_expr = l_expr;
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(calc_urowid_expr)) {
        void *cons_buf = NULL;
        ObRowidConstraint *rowid_cons = NULL;
        if (OB_ISNULL(cons_buf = ctx_->allocator_->alloc(sizeof(ObRowidConstraint)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        }
        rowid_cons = new(cons_buf)ObRowidConstraint(exec_ctx->get_allocator());
        if (OB_SUCC(ret)) {
          if (OB_FAIL(rowid_cons->rowid_type_array_.init(calc_urowid_expr->get_param_count()))) {
            LOG_WARN("failed to init array", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < calc_urowid_expr->get_param_count(); ++i) {
          if (calc_urowid_expr->get_param_expr(i)->has_flag(IS_COLUMN)) {
            const ObColumnRefRawExpr *col_expr =
                       static_cast<const ObColumnRefRawExpr *>(calc_urowid_expr->get_param_expr(i));
            // pk_vals may store generated col which is partition key but not primary key
            if (!col_expr->is_rowkey_column()) {
              /*do nothing*/
            } else if OB_FAIL(rowid_cons->rowid_type_array_.push_back(col_expr->get_data_type())) {
              LOG_WARN("push back pk_column item failed", K(ret));
            }
          }
        }
        if (OB_SUCC(ret)) {
          ObPhysicalPlanCtx *plan_ctx = NULL;
          ObObj rowid_val;
          bool got_result = false;
          if (OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx()) ||
              OB_ISNULL(exec_ctx->get_sql_ctx())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("plan ctx is NULL", K(ret));
          } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                                       const_expr,
                                                                       rowid_val,
                                                                       got_result,
                                                                       *ctx_->allocator_,
                                                                       false))) {
            LOG_WARN("failed to calc const or calculable expr", K(ret));
          } else if (rowid_val.is_null()) {
            //do nothing
          } else if (!rowid_val.is_urowid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("val should be urowid", K(ret), K(rowid_val));
          } else {
            ObSEArray<ObObj, 4> pk_vals;
            const ObURowIDData &urowid_data = rowid_val.get_urowid();
            if (OB_FAIL(urowid_data.get_pk_vals(pk_vals))) {
              LOG_WARN("get pk values failed", K(ret));
            } else {
              rowid_cons->rowid_version_ = urowid_data.get_version();
              int64_t pk_cnt = urowid_data.get_real_pk_count(pk_vals);
              if (pk_cnt != rowid_cons->rowid_type_array_.count()) {
                ret = OB_INVALID_ROWID;
                LOG_WARN("invalid rowid, table rowkey cnt and encoded row cnt mismatch", K(ret),
                                              K(pk_cnt), K(rowid_cons->rowid_type_array_.count()));
              } else {
                for (int i = 0; OB_SUCC(ret) && i < pk_cnt; i++) {
                  if (!pk_vals.at(i).is_null() &&
                      pk_vals.at(i).get_type() != rowid_cons->rowid_type_array_.at(i)) {
                    ret = OB_INVALID_ROWID;
                    LOG_WARN("invalid rowid, table rowkey type and encoded type mismatch", K(ret),
                              K(pk_vals.at(i).get_type()), K(rowid_cons->rowid_type_array_.at(i)));
                  }
                }
              }
            }
          }
          if (OB_SUCC(ret)) {
            ObStaticEngineExprCG expr_cg(exec_ctx->get_allocator(),
                                         exec_ctx->get_my_session(),
                                         exec_ctx->get_sql_ctx()->schema_guard_,
                                         plan_ctx->get_original_param_cnt(),
                                         plan_ctx->get_datum_param_store().count(),
                                         exec_ctx->get_min_cluster_version());
            rowid_cons->expect_result_ = PreCalcExprExpectResult::PRE_CALC_ROWID;
            if (OB_FAIL(expr_cg.generate_calculable_expr(const_expr,
                                                         rowid_cons->pre_calc_expr_info_))) {
              LOG_WARN("failed to generate calculable expr", K(ret));
            } else if (OB_UNLIKELY(!stmt.get_query_ctx()->all_pre_calc_constraints_.add_last(rowid_cons))) {
              LOG_WARN("failed to add rowid constraint", K(ret));
            } else {
              LOG_TRACE("Succeed to add rowid constraint", KPC(expr));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::recursive_generate_rowid_select_item(ObSelectStmt *select_stmt,
                                                                ObRawExpr *&rowid_expr)
{
  int ret = OB_SUCCESS;
  bool can_gen_rowid = false;
  rowid_expr = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or ctx_ is NULL", K(ret), KP(select_stmt), KP(ctx_));
  } else {
    ObIArray<TableItem*> &table_items = select_stmt->get_table_items();
    TableItem *table_item = NULL;
    ObSysFunRawExpr *rowid_func_expr = NULL;
    LOG_DEBUG("transformer rowid_expr recursively begin", KPC(select_stmt), K(table_items));
    for (int64_t i = 0; OB_SUCC(ret) && NULL == rowid_expr && i < table_items.count(); ++i) {
      if (OB_ISNULL(table_item = table_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_item is NULL", K(ret), K(i), K(table_items));
      } else if (OB_FAIL(check_can_gen_rowid_on_this_table(select_stmt,
                                                           table_item,
                                                           can_gen_rowid))) {
        LOG_WARN("check_can_gen_rowid failed", K(ret));
      } else if (!can_gen_rowid) {
        LOG_DEBUG("cannot gen rowid in this table item", K(ret), KPC(table_item));
      } else if (table_item->is_generated_table() || table_item->is_temp_table()) {
        if (OB_FAIL(SMART_CALL(recursive_generate_rowid_select_item(table_item->ref_query_,
                                                                    rowid_expr)))) {
          LOG_WARN("failed to recursive generate rowid select item", K(ret));
        } else if (NULL == rowid_expr) {
          // ignore create rowid item.
        } else if (OB_FAIL(create_rowid_item_for_stmt(select_stmt, table_item, rowid_expr))) {
          LOG_WARN("create rowid item for stmt failed", K(ret));
        } else if (ObTransformUtils::create_select_item(*(ctx_->allocator_),
                                                        rowid_expr,
                                                        select_stmt)) {
          LOG_WARN("failed to create select item", K(ret));
        } else {/*do nothing*/}
      } else if (OB_FAIL(build_rowid_expr(select_stmt, table_item, rowid_func_expr))) {
        LOG_WARN("build rowid_expr failed", K(ret), KPC(table_item));
      } else if (OB_FAIL(ObTransformUtils::create_select_item(*(ctx_->allocator_),
                                                                rowid_func_expr,
                                                                select_stmt))) {
          LOG_WARN("create_select_item failed", K(ret));
      } else {
        rowid_expr = rowid_func_expr;
      }
    } // end for
    if (OB_SUCC(ret) && OB_ISNULL(rowid_expr)) {
      ret = OB_ROWID_VIEW_NO_KEY_PRESERVED;
      LOG_WARN("rowid view no key preserved", K(ret));
      LOG_USER_ERROR(OB_ROWID_VIEW_NO_KEY_PRESERVED);
    } else {
      LOG_TRACE("succeed to recursive generate rowid select item", KPC(select_stmt), KPC(rowid_expr));
    }
  }
  return ret;
}

// select rowid from (select * from t1, t2 where t1.c1 = t2.pk;
// t1: this_table
// t2: other_table
// we check if other_table's join exprs is unique or not,
// if is unique, we can generate rowid on this_table
int ObTransformPreProcess::check_can_gen_rowid_on_this_table(ObSelectStmt *select_stmt,
                                                             TableItem *this_table,
                                                             bool &can_gen_rowid)
{
  int ret = OB_SUCCESS;
  can_gen_rowid = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(this_table)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt or table_item is null", K(ret), KP(select_stmt), KP(this_table));
  } else if (select_stmt->has_group_by() || select_stmt->has_distinct()) {
    ret = OB_ROWID_VIEW_HAS_DISTINCT_ETC;
    LOG_WARN("stmt got group by or distinct", K(ret),
        K(select_stmt->has_group_by()), K(select_stmt->has_distinct()));
    LOG_USER_ERROR(OB_ROWID_VIEW_HAS_DISTINCT_ETC);
  } else if (is_ora_sys_view_table(this_table->ref_id_)) {
    // Because the implementation of these views is differnet from oracle,
    // therefore, the check is added to prevent these views from trying to build rowid expr
    // v$sesstat,v$session_wait,v$sysstat
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "view trying to build rowid");
    LOG_WARN("stmt got system table", K(ret));
  } else {
    ObSEArray<TableItem*, 4> unique_tables;
    ObSEArray<TableItem*, 4> non_unique_tables;
    ObIArray<TableItem*> &table_items = select_stmt->get_table_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      TableItem *item = table_items.at(i);
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is NULL", K(ret), K(i));
      } else if (item->table_id_ == this_table->table_id_) {
        if (OB_FAIL(unique_tables.push_back(item))) {
          LOG_WARN("push table item failed", K(ret));
        }
      } else if (OB_FAIL(non_unique_tables.push_back(item))) {
        LOG_WARN("push table item failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && non_unique_tables.empty()) {
      can_gen_rowid = true;
    }
    if (OB_SUCC(ret) && !can_gen_rowid) {
      bool found_unique = true;
      ObSEArray<ObRawExpr*, 4> join_exprs;
      const ObIArray<ObRawExpr*> &cond_exprs = select_stmt->get_condition_exprs();
      while (OB_SUCC(ret) && found_unique && !non_unique_tables.empty()) {
        found_unique = false;
        for (int64_t i = non_unique_tables.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
          bool is_unique = false;
          join_exprs.reuse();
          TableItem *table = non_unique_tables.at(i);
          if (OB_ISNULL(table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table item is NULL", K(ret));
          } else if (OB_FAIL(ObTransformUtils::get_table_joined_exprs(*select_stmt, unique_tables,
                                                                      *table, cond_exprs,
                                                                      join_exprs))) {
            LOG_WARN("get_table_joined_exprs failed", K(ret));
          } else if (join_exprs.empty()) {
          } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(*select_stmt,
                                                                  table,
                                                                  join_exprs,
                                                                  ctx_->session_info_,
                                                                  ctx_->schema_checker_,
                                                                  is_unique))) {
            LOG_WARN("check exprs unique failed", K(ret));
          } else if (!is_unique) {
          } else if (OB_FAIL(unique_tables.push_back(table))) {
            LOG_WARN("push table table failed", K(ret));
          } else if (OB_FAIL(ObOptimizerUtil::remove_item(non_unique_tables, table))) {
            LOG_WARN("remove table table failed", K(ret));
          } else {
            found_unique = true;
          }
        } // for end
      } // while end
      if (OB_SUCC(ret) && non_unique_tables.empty()) {
        can_gen_rowid = true;
      }
    }
    LOG_DEBUG("check_can_gen_rowid_on_this_table done", K(ret), KPC(select_stmt),
              KPC(this_table), K(can_gen_rowid));
  }
  return ret;
}

int ObTransformPreProcess::build_rowid_expr(ObSelectStmt *select_stmt,
                                            TableItem *table_item,
                                            ObSysFunRawExpr *&rowid_expr)
{
  int ret = OB_SUCCESS;
  rowid_expr = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(table_item) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->allocator_)
      || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt or table_item is NULL", K(ret), KP(select_stmt), KP(table_item), KP(ctx_));
  } else {
    const ObTableSchema *table_schema = NULL;
    ObSEArray<ObRawExpr*, 4> index_keys;
    uint64_t tid = table_item->ref_id_;
    ObRawExpr *same_rowid_expr = NULL;
    if (OB_FAIL(ctx_->schema_checker_->get_table_schema(ctx_->session_info_->get_effective_tenant_id(), tid, table_schema, table_item->is_link_table()))) {
      LOG_WARN("fail to get table schema", K(ret), K(tid));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema is NULL", K(ret));
    } else {
      const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
      ObColumnRefRawExpr *expr = NULL;
      for (int i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        uint64_t  column_id = OB_INVALID_ID;
        if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
          LOG_WARN("Failed to get column_id from rowkey_info", K(ret));
        } else if (OB_ISNULL(expr = select_stmt->get_column_expr_by_id(table_item->table_id_,
                                                                       column_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null, rowkey column not exists in stmt", K(ret),
                                                        K(table_item->table_id_), K(column_id));
        } else if (OB_FAIL(index_keys.push_back(expr))) {
          LOG_WARN("failed to add row key expr", K(ret));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObRawExprUtils::build_rowid_expr(select_stmt,
                                                          *(ctx_->expr_factory_),
                                                          *(ctx_->allocator_),
                                                          *(ctx_->session_info_),
                                                          *table_schema,
                                                          table_item->table_id_,
                                                          index_keys,
                                                          rowid_expr))) {
        LOG_WARN("build rowid col_expr failed", K(ret));
      } else if (OB_FAIL(select_stmt->check_and_get_same_rowid_expr(rowid_expr, same_rowid_expr))) {
        LOG_WARN("failed to check and get same rowid expr", K(ret));
      } else if (same_rowid_expr != NULL) {
        rowid_expr = static_cast<ObSysFunRawExpr*>(same_rowid_expr);
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObTransformPreProcess::create_rowid_item_for_stmt(ObDMLStmt *dml_stmt,
                                                      TableItem *table_item,
                                                      ObRawExpr *&rowid_expr)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  rowid_expr = NULL;
  if (OB_ISNULL(dml_stmt) || OB_ISNULL(table_item)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt or item is NULL", K(ret), K(dml_stmt), K(table_item));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *table_item,
                                                               dml_stmt, column_exprs))) {
    LOG_WARN("create columns for view failed", K(ret), KPC(table_item), KPC(dml_stmt));
  } else if (column_exprs.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_expr is invalid", K(ret), K(column_exprs));
  } else {
    ObRawExpr *expr = column_exprs.at(column_exprs.count() - 1);
    if (OB_ISNULL(expr) || OB_UNLIKELY(!(expr->is_column_ref_expr()))
        || OB_UNLIKELY(!(expr->get_result_type().is_urowid()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last col must be rowid col", K(ret), KPC(expr));
    } else {
      rowid_expr = expr;
    }
  }
  return ret;
}

//transform batch update stmt to multiple update with nlj:
// update t1 set b=1 where a=1; update t1 set b=2 where a=2; update t1 set b=3 where a=3;...
//-> update t1, (select 1, 1 from dual ...) v set t1.b=v.b where t1.a=v.a;
// batch delete is as batch update
int ObTransformPreProcess::transform_for_upd_del_batch_stmt(ObDMLStmt *batch_stmt,
                                                            ObSelectStmt *inner_view_stmt,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  // create select list
  if (OB_FAIL(mock_select_list_for_upd_del(*batch_stmt, *inner_view_stmt))) {
    LOG_WARN("mock select list for batch stmt failed", K(ret));
  } else if (OB_FAIL(formalize_batch_stmt(batch_stmt,
                                          inner_view_stmt,
                                          batch_stmt->get_query_ctx()->ab_param_exprs_,
                                          trans_happened))) {
    LOG_WARN("fail to formalize batch stmt", K(ret), KPC(batch_stmt));
  }
  return ret;
}

int ObTransformPreProcess::transform_for_ins_batch_stmt(ObDMLStmt *batch_stmt,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObInsertStmt *insert_stmt = NULL;
  ObExecContext *exec_ctx = nullptr;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObRawExpr*, 4> params;
  ObPseudoColumnRawExpr *stmt_id_expr = NULL;

  if (OB_ISNULL(ctx_)
      || OB_ISNULL(exec_ctx = ctx_->exec_ctx_)
      || OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx())
      || OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(exec_ctx), K(plan_ctx), K(ret));
  } else if (OB_ISNULL(batch_stmt) ||
             OB_UNLIKELY(!batch_stmt->is_insert_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch_stmt or inner_view_stmt is null", K(ret), K(batch_stmt));
  } else if (FALSE_IT(insert_stmt = static_cast<ObInsertStmt*>(batch_stmt))) {
  } else if (OB_FAIL(create_stmt_id_expr(stmt_id_expr))) {
    LOG_WARN("fail to create stmt id expr", K(ret));
  } else if (FALSE_IT(batch_stmt->get_query_ctx()->ins_values_batch_opt_ = true)) {
  } else if (FALSE_IT(static_cast<ObDelUpdStmt*>(batch_stmt)->set_ab_stmt_id_expr(stmt_id_expr))) {
  } else if (OB_FAIL(ObRawExprUtils::extract_params(insert_stmt->get_values_vector(), params))) {
    LOG_WARN("extract param expr from related exprs failed", K(ret));
  } else {
    common::ObIArray<ObRawExpr*> &value_vector = insert_stmt->get_values_vector();
    const ParamStore &param_store = plan_ctx->get_param_store();
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      ObConstRawExpr *param_expr = static_cast<ObConstRawExpr *>(params.at(i));
      ObPseudoColumnRawExpr *group_id = NULL;
      int64_t param_idx = param_expr->get_value().get_unknown();
      if (param_idx < 0 || param_idx >= param_store.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param_idx is invalid", K(ret), K(param_idx), K(param_store));
      } else if (!param_store.at(param_idx).is_batch_parameters()) {
        // 不是batch 参数, 不需要打标记
      } else {
        param_expr->set_is_batch_stmt_parameter();
      }
    }
    // 给所有的batch参数表达上边打上了标记，需要重新做表达式推导
    // 这里因为insert values的特殊性，所以只需要推导value_vector中的表达式即可
    for (int64_t i = 0; OB_SUCC(ret) && i < value_vector.count(); ++i) {
      if (OB_FAIL(value_vector.at(i)->formalize(session_info))) {
        LOG_WARN("formalize expr failed", K(ret), K(i), KPC(value_vector.at(i)));
      }
    }
  }

  return ret;
}

int ObTransformPreProcess::check_contain_param_expr(ObDMLStmt *stmt,
                                                    TableItem *table_item,
                                                    bool &contain_param)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (contain_param) {
    // break;
  } else {
    if (table_item->is_joined_table()) {
      JoinedTable *joined_table_item = static_cast<JoinedTable *>(table_item);
      if (OB_FAIL(ObRawExprUtils::is_contain_params(joined_table_item->get_join_conditions(), contain_param))) {
        LOG_WARN("fail to get releation exprs", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_contain_param_expr(stmt, joined_table_item->left_table_, contain_param)))) {
        LOG_WARN("failed to collect temp table item", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_contain_param_expr(stmt, joined_table_item->right_table_, contain_param)))) {
        LOG_WARN("failed to collect temp table item", K(ret));
      }
    } else if (table_item->is_generated_table()) {
      ObDMLStmt *ref_query_stmt = NULL;
      if (OB_ISNULL(ref_query_stmt = table_item->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ref_query is null", K(ret), KPC(table_item));
      } else if (OB_FAIL(check_stmt_contain_param_expr(ref_query_stmt, contain_param))) {
        LOG_WARN("fail to check contain param expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_stmt_contain_param_expr(ObDMLStmt *stmt, bool &contain)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> related_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt->get_relation_exprs(related_exprs))) {
    LOG_WARN("fail to get releation exprs", K(ret), KPC(stmt));
  } else if (OB_FAIL(ObRawExprUtils::is_contain_params(related_exprs, contain))) {
    LOG_WARN("fail to get releation exprs", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::check_stmt_can_batch(ObDMLStmt *batch_stmt, bool &contain_param)
{
  int ret = OB_SUCCESS;
  contain_param = false;
  if (OB_ISNULL(batch_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch_stmt is null", K(ret));
  } else {
    const common::ObIArray<FromItem> &from_items = batch_stmt->get_from_items();
    for (int64_t i = 0; OB_SUCC(ret) && !contain_param && i < from_items.count(); ++i) {
      TableItem *table_item = NULL;
      const FromItem &from_item = from_items.at(i);
      if (OB_ISNULL(table_item = batch_stmt->get_table_item(from_item))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_item is null", K(ret), K(from_item));
      } else if (OB_FAIL(check_contain_param_expr(batch_stmt, table_item, contain_param))) {
        LOG_WARN("fail to check contain param expr", K(ret), KPC(table_item));
      }
    }
  }

  return ret;
}

int ObTransformPreProcess::create_inner_view_stmt(ObDMLStmt *batch_stmt, ObSelectStmt*& inner_view_stmt)
{
  int ret = OB_SUCCESS;
  ObStmtFactory *stmt_factory = NULL;
  if (OB_ISNULL(batch_stmt) || OB_ISNULL(stmt_factory = ctx_->stmt_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(batch_stmt), K(stmt_factory), K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(inner_view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else {
    inner_view_stmt->set_stmt_type(stmt::T_SELECT);
    inner_view_stmt->set_query_ctx(batch_stmt->get_query_ctx());
    stmt::StmtType stmt_type = batch_stmt->get_stmt_type();
    if (OB_FAIL(inner_view_stmt->adjust_statement_id(ctx_->allocator_,
                                                     ctx_->src_qb_name_,
                                                     ctx_->src_hash_val_))) {
      LOG_WARN("failed to recursive adjust statement id", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_batch_stmt(ObDMLStmt *batch_stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* inner_view_stmt = NULL;
  ObExecContext *exec_ctx = nullptr;
  stmt::StmtType stmt_type = batch_stmt->get_stmt_type();
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(exec_ctx = ctx_->exec_ctx_) ||
      OB_ISNULL(batch_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(exec_ctx), K(ret));
  } else if (!exec_ctx->get_sql_ctx()->is_batch_params_execute()) {
    //rewrite only when stmt is batch multi statement
  } else if (!batch_stmt->is_dml_write_stmt()) {
    // 非dml_stmt暂时不用处理
  } else if (stmt_type == stmt::T_UPDATE || stmt_type == stmt::T_DELETE) {
    int64_t child_size = 0;
    if (OB_FAIL(batch_stmt->get_child_stmt_size(child_size))) {
      LOG_WARN("fail to get child_stmt size", K(ret), KPC(batch_stmt));
    } else if (child_size != 0) {
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_TRACE("only DML Stmt without subquery supports batch execution, need to rollback",
                K(ret), K(child_size), KPC(batch_stmt));
    } else if (OB_FAIL(create_inner_view_stmt(batch_stmt, inner_view_stmt))) {
      LOG_WARN("fail to create inner_view_stmt", K(ret));
    } else if (OB_FAIL(transform_for_upd_del_batch_stmt(batch_stmt, inner_view_stmt, trans_happened))) {
      LOG_WARN("fail to transform upd or del batch stmt", K(ret));
    }
  } else if (stmt_type == stmt::T_INSERT || stmt_type == stmt::T_REPLACE) {
    // insert的改写
    int64_t child_size = 0;
    bool can_batch = false;
    ObInsertStmt *insert_stmt = static_cast<ObInsertStmt *>(batch_stmt);
    if (OB_FAIL(check_insert_can_batch(insert_stmt, can_batch))) {
      LOG_WARN("fail to check insert can batch", K(ret), KPC(insert_stmt));
    } else if (!can_batch) {
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_TRACE("can't support insert batch optimization", K(ret), KPC(batch_stmt));
    } else if (OB_FAIL(transform_for_ins_batch_stmt(batch_stmt, trans_happened))) {
      LOG_WARN("fail to transform ins batch stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::check_insert_can_batch(ObInsertStmt *insert_stmt, bool &can_batch)
{
  int ret = OB_SUCCESS;
  can_batch = true;
  if (insert_stmt->value_from_select()) {
    can_batch = false;
    LOG_TRACE("insert select stmt not supported batch exec opt", K(ret));
  } else if (!insert_stmt->is_insert_single_value()) {
    can_batch = false;
    LOG_TRACE("multi row insert not supported batch exec opt", K(ret));
  } else {
    common::ObIArray<ObRawExpr*> &value_vector = insert_stmt->get_values_vector();
    for (int64_t i = 0; OB_SUCC(ret) && i < value_vector.count(); i++) {
      if (value_vector.at(i)->has_flag(CNT_SUB_QUERY)) {
        can_batch = false;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::formalize_batch_stmt(ObDMLStmt *batch_stmt,
                                                ObSelectStmt* inner_view_stmt,
                                                const ObIArray<ObRawExpr *> &other_exprs,
                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  TableItem *view_table_item = NULL;
  ObSEArray<ObRawExpr *, 4> view_columns;
  ObSQLSessionInfo *session_info = NULL;
  ObRawExpr *stmt_id_expr = NULL;
  if (OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_,
                                                          batch_stmt,
                                                          inner_view_stmt,
                                                          view_table_item))) {
    //create generated table item for inner view
    LOG_WARN("failed to add new table item", K(ret));
  } else if (OB_ISNULL(view_table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (OB_FAIL(batch_stmt->add_from_item(view_table_item->table_id_, false))) {
    LOG_WARN("add from item to batch stmt failed", K(ret), KPC(view_table_item));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_,
                                                               *view_table_item,
                                                               batch_stmt,
                                                               view_columns))) {
    LOG_WARN("failed to create columns for view", K(ret));
  } else if (OB_ISNULL(stmt_id_expr = view_columns.at(view_columns.count() - 1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt id expr is null", K(ret), K(stmt_id_expr));
  } else if (FALSE_IT(view_columns.pop_back())) {
    // do nothing
  } else if (OB_FAIL(batch_stmt->replace_relation_exprs(other_exprs, view_columns))) {
    LOG_WARN("replace inner stmt expr failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*batch_stmt))) {
    LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
  } else if (OB_FAIL(batch_stmt->formalize_stmt(session_info))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  } else {
    trans_happened = true;
    static_cast<ObDelUpdStmt*>(batch_stmt)->set_ab_stmt_id_expr(stmt_id_expr);
    LOG_DEBUG("debug transform_for_batch_stmt",
             K(batch_stmt->get_query_ctx()->ab_param_exprs_), K(view_columns));
  }
  return ret;
}

int ObTransformPreProcess::create_stmt_id_expr(ObPseudoColumnRawExpr *&stmt_id_expr)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assign value_vector failed", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_PSEUDO_STMT_ID, stmt_id_expr))) {
    LOG_WARN("create pseudo column raw expr failed", K(ret));
  } else if (OB_ISNULL(stmt_id_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt id expr is null", K(ret));
  } else {
    stmt_id_expr->set_data_type(ObIntType);
    stmt_id_expr->set_accuracy(ObAccuracy::MAX_ACCURACY[ObIntType]);
    stmt_id_expr->set_table_id(OB_INVALID_ID);
    stmt_id_expr->set_explicited_reference();
    stmt_id_expr->set_expr_name(ObString::make_string("stmt_id"));
  }
  return ret;
}

int ObTransformPreProcess::create_params_expr(ObPseudoColumnRawExpr *&pseudo_param_expr,
                                              ObRawExpr *origin_param_expr,
                                              int64_t name_id)
{
  int ret = OB_SUCCESS;
  char *pseudo_name = nullptr;
  ObRawExprFactory *expr_factory = NULL;
  const int64_t buf_len = 64;
  int64_t pos = 0;
  if (OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assign value_vector failed", K(ret));
  } else if (OB_ISNULL(pseudo_name = static_cast<char*>(ctx_->allocator_->alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate name buffer failed", K(ret));
  } else if (OB_FAIL(databuff_printf(pseudo_name, buf_len, pos,
                                     "group_param_%ld", name_id))) {
    LOG_WARN("databuff print column name failed", K(ret));
  } else if (OB_FAIL(expr_factory->create_raw_expr(T_PSEUDO_GROUP_PARAM, pseudo_param_expr))) {
    LOG_WARN("create pseudo colmn raw expr failed", K(ret));
  } else {
    pseudo_param_expr->set_expr_name(ObString(pos, pseudo_name));
    pseudo_param_expr->set_result_type(origin_param_expr->get_result_type());
    pseudo_param_expr->set_table_id(OB_INVALID_ID);
    pseudo_param_expr->set_explicited_reference();
  }
  return ret;
}

int ObTransformPreProcess::create_params_exprs(ObDMLStmt &batch_stmt,
                                              ObIArray<ObRawExpr*> &params_exprs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObRawExpr*> &ab_param_exprs = batch_stmt.get_query_ctx()->ab_param_exprs_;
  for (int64_t i = 0; OB_SUCC(ret) && i < ab_param_exprs.count(); i++) {
    ObPseudoColumnRawExpr *group_param_expr = nullptr;
    if (OB_FAIL(create_params_expr(group_param_expr, ab_param_exprs.at(i), i))) {
      LOG_WARN("create pseudo colmn raw expr failed", K(ret), K(i), K(ab_param_exprs));
    } else if (OB_ISNULL(group_param_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group_param_expr is null", K(ret));
    } else if (OB_FAIL(params_exprs.push_back(group_param_expr))) {
      LOG_WARN("fail to push group param exprs", K(ret), KPC(group_param_expr), K(params_exprs));
    }
  }
  return ret;
}

// 1、make value_vector as select_list,then extract_calculable_expr
// 2、after pre_calc, generate the mock select_list for inner_view
int ObTransformPreProcess::mock_select_list_for_ins_values(ObDMLStmt &batch_stmt,
                                                           ObSelectStmt &inner_view,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = nullptr;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  const ParamStore *param_store = nullptr;
  ObSEArray<ObRawExpr *, 4> select_list;
  ObInsertStmt &insert_stmt = static_cast<ObInsertStmt &>(batch_stmt);
  ObIArray<ObRawExpr*> &value_vector = insert_stmt.get_values_vector();
  ObIArray<ObColumnRefRawExpr*> &value_descs = insert_stmt.get_values_desc();
  if (OB_ISNULL(ctx_)
      || OB_ISNULL(exec_ctx = ctx_->exec_ctx_)
      || OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(exec_ctx), K(plan_ctx), K(ret));
  } else {
    param_store = &plan_ctx->get_param_store();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < value_vector.count(); ++i) {
    ObConstRawExpr *param_expr = static_cast<ObConstRawExpr *>(value_vector.at(i));
    ObRawExpr *value_desc = value_descs.at(i);
    int64_t param_idx = param_expr->get_value().get_unknown();
    if (param_idx < 0 || param_idx >= param_store->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_idx is invalid", K(ret), K(param_idx), KPC(param_store));
    } else {
      ObPseudoColumnRawExpr *group_param_expr = nullptr;
      //create pseudo column for the sql array param
      if (OB_FAIL(create_params_expr(group_param_expr, value_desc, select_list.count()))) {
        LOG_WARN("fail to create group param expr", K(ret), K(value_desc));
      } else if (OB_FAIL(batch_stmt.get_query_ctx()->ab_param_exprs_.push_back(param_expr))) {
        LOG_WARN("add param expr to select list exprs failed", K(ret));
      } else if (OB_FAIL(select_list.push_back(group_param_expr))) {
        LOG_WARN("store group id expr failed", K(ret));
      }
    }
  }

  //create stmt id expr with inner view
  if (OB_SUCC(ret)) {
    ObPseudoColumnRawExpr *stmt_id_expr = NULL;
    if (OB_FAIL(create_stmt_id_expr(stmt_id_expr))) {
      LOG_WARN("fail to create stmt id expr", K(ret));
    } else if (OB_FAIL(select_list.push_back(stmt_id_expr))) {
      LOG_WARN("store select id expr failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, select_list, &inner_view))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (!batch_stmt.get_query_ctx()->ab_param_exprs_.empty()) {
      inner_view.set_ab_param_flag(true);
      batch_stmt.get_query_ctx()->ins_values_batch_opt_ = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::mock_select_list_for_ins_select(ObDMLStmt &batch_stmt,
                                                           ObSelectStmt &inner_view,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  const ParamStore *param_store = nullptr;
  ObExecContext *exec_ctx = nullptr;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObSEArray<ObRawExpr*, 4> params;
  ObSEArray<ObRawExpr*, 8> related_exprs;
  ObSEArray<ObRawExpr *, 4> select_list;
  int64_t child_size = 0;
  bool need_rollback = false;
  if (OB_ISNULL(ctx_)
      || OB_ISNULL(exec_ctx = ctx_->exec_ctx_)
      || OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(exec_ctx), K(plan_ctx), K(ret));
  } else if (OB_ISNULL(param_store = &plan_ctx->get_param_store())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param store is null", K(ret));
  } else if (OB_FAIL(batch_stmt.get_child_stmt_size(child_size))) {
    LOG_WARN("get child size failed", K(ret));
  } else if (child_size > 0) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
    LOG_WARN("child size greater than 0", K(ret), K(child_size));
  } else if (OB_FAIL(check_stmt_can_batch(&batch_stmt, need_rollback))) {
    LOG_WARN("fail to check multi_batch stmt", K(ret));
  } else if (need_rollback) {
    ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
  } else if (OB_FAIL(batch_stmt.get_relation_exprs(related_exprs))) {
    LOG_WARN("get relation exprs failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_params(related_exprs, params))) {
    LOG_WARN("extract param expr from related exprs failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
      ObConstRawExpr *param_expr = static_cast<ObConstRawExpr *>(params.at(i));
      ObPseudoColumnRawExpr *group_id = NULL;
      int64_t param_idx = param_expr->get_value().get_unknown();
      if (param_idx < 0 || param_idx >= param_store->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param_idx is invalid", K(ret), K(param_idx), KPC(param_store));
      } else if (param_store->at(param_idx).is_ext_sql_array()
          && !has_exist_in_array(batch_stmt.get_query_ctx()->ab_param_exprs_,
                                 static_cast<ObRawExpr*>(param_expr))) {
        ObPseudoColumnRawExpr *group_param_expr = nullptr;
        //create pseudo column for the sql array param
        if (OB_FAIL(create_params_expr(group_param_expr, param_expr, select_list.count()))) {
          LOG_WARN("fail to create group param expr", K(ret), K(param_expr));
        } else if (OB_FAIL(batch_stmt.get_query_ctx()->ab_param_exprs_.push_back(param_expr))) {
          LOG_WARN("add param expr to select list exprs failed", K(ret));
        } else if (OB_FAIL(select_list.push_back(group_param_expr))) {
          LOG_WARN("store group id expr failed", K(ret));
        }
      }
    }

    //create stmt id expr with inner view
    if (OB_SUCC(ret)) {
      ObPseudoColumnRawExpr *stmt_id_expr = NULL;
      if (OB_FAIL(create_stmt_id_expr(stmt_id_expr))) {
        LOG_WARN("fail to create stmt id expr", K(ret));
      } else if (OB_FAIL(select_list.push_back(stmt_id_expr))) {
        LOG_WARN("store select id expr failed", K(ret));
      }
    }

    // create select item
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, select_list, &inner_view))) {
        LOG_WARN("failed to create select items", K(ret));
      } else if (!batch_stmt.get_query_ctx()->ab_param_exprs_.empty()) {
        inner_view.set_ab_param_flag(true);
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::mock_select_list_for_upd_del(ObDMLStmt &batch_stmt,
                                                        ObSelectStmt &inner_view)
{
  int ret = OB_SUCCESS;
  const ParamStore *param_store = nullptr;
  ObExecContext *exec_ctx = nullptr;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObSEArray<ObRawExpr*, 4> params;
  ObSEArray<ObRawExpr*, 8> related_exprs;
  ObSEArray<ObRawExpr *, 4> select_list;

  if (OB_ISNULL(ctx_)
      || OB_ISNULL(exec_ctx = ctx_->exec_ctx_)
      || OB_ISNULL(plan_ctx = exec_ctx->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx_), K(exec_ctx), K(plan_ctx), K(ret));
  } else if (FALSE_IT(param_store = &plan_ctx->get_param_store())) {
    // do nothing
  } else if (OB_FAIL(batch_stmt.get_relation_exprs(related_exprs))) {
    LOG_WARN("get relation exprs failed", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::extract_params(related_exprs, params))) {
    LOG_WARN("extract param expr from related exprs failed", K(ret));
  } else { }

  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    ObConstRawExpr *param_expr = static_cast<ObConstRawExpr *>(params.at(i));
    int64_t param_idx = param_expr->get_value().get_unknown();
    if (param_idx < 0 || param_idx >= param_store->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param_idx is invalid", K(ret), K(param_idx), KPC(param_store));
    } else if (param_store->at(param_idx).is_batch_parameters()
        && !has_exist_in_array(batch_stmt.get_query_ctx()->ab_param_exprs_,
                               static_cast<ObRawExpr*>(param_expr))) {
      ObPseudoColumnRawExpr *group_param_expr = nullptr;
      //create pseudo column for the sql array param
      if (OB_FAIL(create_params_expr(group_param_expr, param_expr, select_list.count()))) {
        LOG_WARN("fail to create group param expr", K(ret), K(param_expr));
      } else if (OB_FAIL(batch_stmt.get_query_ctx()->ab_param_exprs_.push_back(param_expr))) {
        LOG_WARN("add param expr to select list exprs failed", K(ret));
      } else if (OB_FAIL(select_list.push_back(group_param_expr))) {
        LOG_WARN("store group id expr failed", K(ret));
      }
    }
  }
  //create stmt id expr with inner view
  if (OB_SUCC(ret)) {
    ObPseudoColumnRawExpr *stmt_id_expr = NULL;
    if (OB_FAIL(create_stmt_id_expr(stmt_id_expr))) {
      LOG_WARN("fail to create stmt id expr", K(ret));
    } else if (OB_FAIL(select_list.push_back(stmt_id_expr))) {
      LOG_WARN("store select id expr failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, select_list, &inner_view))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (!batch_stmt.get_query_ctx()->ab_param_exprs_.empty()) {
      inner_view.set_ab_param_flag(true);
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_full_outer_join(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSEArray<JoinedTable*, 4> joined_tables;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(joined_tables.assign(stmt->get_joined_tables()))) {
    LOG_WARN("failed to assign joined table", K(ret));
  } else {
    bool is_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables.count(); ++i) {
      if (OB_ISNULL(joined_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(joined_tables));
      } else if (OB_FAIL(recursively_eliminate_full_join(*stmt, joined_tables.at(i),
                                                         is_happened))) {
        LOG_WARN("failed to recursively eliminate full join", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::convert_join_preds_vector_to_scalar(JoinedTable &joined_table,
                                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> new_join_cond;
  bool is_happened = false;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get NULL ptr", K(ret) , KP(ctx_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table.get_join_conditions().count(); i++) {
      if (OB_FAIL(ObTransformUtils::convert_preds_vector_to_scalar(*ctx_,
                                                           joined_table.get_join_conditions().at(i),
                                                           new_join_cond,
                                                           is_happened))) {
        LOG_WARN("failed to convert predicate vector to scalar", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_happened) {
      trans_happened = true;
      joined_table.get_join_conditions().reset();
      if (OB_FAIL(append(joined_table.get_join_conditions(), new_join_cond))) {
        LOG_WARN("failed to append join conditions", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::recursively_eliminate_full_join(ObDMLStmt &stmt,
                                                           TableItem *table_item,
                                                           bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool has_euqal = false;
  bool has_subquery = false;
  JoinedTable *joined_table = static_cast<JoinedTable *>(table_item);
  TableItem *view_table = NULL;
  TableItem *from_table = table_item;
  if (OB_ISNULL(table_item) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(table_item), K(ctx_));
  } else if (!table_item->is_joined_table()) {
    /* do nothing */
  } else if (OB_FAIL(recursively_eliminate_full_join(stmt,
                                                     joined_table->left_table_,
                                                     trans_happened))) {
    LOG_WARN("failed to transform full nl join.", K(ret));
  } else if (OB_FAIL(recursively_eliminate_full_join(stmt,
                                                     joined_table->right_table_,
                                                     trans_happened))) {
    LOG_WARN("failed to transform full nl join.", K(ret));
  } else if (!joined_table->is_full_join()) {
    /* do nothing */
  } else if (OB_FAIL(convert_join_preds_vector_to_scalar(*joined_table, trans_happened))) {
    LOG_WARN("failed to convert join preds vector to scalar", K(ret));
  } else if (OB_FAIL(check_join_condition(&stmt, joined_table, has_euqal, has_subquery))) {
    LOG_WARN("failed to check join condition", K(ret));
  } else if (has_euqal || has_subquery) {
    /* do nothing */
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               &stmt,
                                                               view_table,
                                                               from_table))) {
    LOG_WARN("failed to create empty view table", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          &stmt,
                                                          view_table,
                                                          from_table))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("view table is null", K(ret), K(view_table));
  } else if (OB_FAIL(expand_full_outer_join(view_table->ref_query_))) {
    LOG_WARN("failed to create view for full nl join.", K(ret));
  } else {
    trans_happened = true;
    view_table->for_update_ = false;
  }
  return ret;
}

int ObTransformPreProcess::check_join_condition(ObDMLStmt *stmt,
                                                JoinedTable *table,
                                                bool &has_equal,
                                                bool &has_subquery)
{
  int ret = OB_SUCCESS;
  has_equal = false;
  has_subquery = false;
  ObSqlBitSet<> left_tables;
  ObSqlBitSet<> right_tables;
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_ISNULL(table->left_table_)
      || OB_ISNULL(table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*table->left_table_, left_tables))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*table->right_table_, right_tables))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  } else {
    ObRawExpr *cond = NULL;
    ObRawExpr *left_param = NULL;
    ObRawExpr *right_param = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < table->join_conditions_.count(); i++) {
      if (OB_ISNULL(cond = table->join_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null condition", K(ret));
      } else if (OB_FALSE_IT(has_subquery |= cond->has_flag(CNT_SUB_QUERY))) {
      } else if (cond->get_relation_ids().is_empty() || !cond->has_flag(IS_JOIN_COND)) {
        /* do nothing */
      } else if (OB_UNLIKELY(cond->get_param_count() != 2) ||
                OB_ISNULL(left_param = cond->get_param_expr(0)) ||
                OB_ISNULL(right_param = cond->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null param", K(ret));
      } else if ((left_param->get_relation_ids().is_subset(left_tables) &&
                  right_param->get_relation_ids().is_subset(right_tables)) ||
                (right_param->get_relation_ids().is_subset(left_tables) &&
                  left_param->get_relation_ids().is_subset(right_tables))) {
        has_equal = true;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::expand_full_outer_join(ObSelectStmt *&ref_query)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *left_stmt = NULL;
  ObSelectStmt *right_stmt = NULL;
  ObSelectStmt *union_stmt = NULL;
  JoinedTable *joined_table = NULL;
  const int64_t sub_num = 1;
  if (OB_ISNULL(left_stmt = ref_query)
      || OB_UNLIKELY(left_stmt->get_joined_tables().count() != 1)
      || OB_ISNULL(joined_table = left_stmt->get_joined_tables().at(0))
      || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined table count.", K(ret), K(ctx_), K(left_stmt), K(joined_table));
  } else if (OB_FALSE_IT(joined_table->joined_type_ = LEFT_OUTER_JOIN)) {
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt(right_stmt))) {
      LOG_WARN("failed to create select stmt", K(ret));
  } else if (OB_FAIL(SMART_CALL(right_stmt->deep_copy(*ctx_->stmt_factory_,
                                                      *ctx_->expr_factory_,
                                                      *left_stmt)))) {
      LOG_WARN("failed to deep copy select stmt", K(ret));
  } else if (OB_FAIL(right_stmt->recursive_adjust_statement_id(ctx_->allocator_,
                                                               ctx_->src_hash_val_,
                                                               sub_num))) {
    LOG_WARN("failed to recursive adjust statement id", K(ret));
  } else if (OB_FAIL(right_stmt->update_stmt_table_id(*left_stmt))) {
    LOG_WARN("failed to updatew table id in stmt.", K(ret));
  } else if (right_stmt->get_joined_tables().count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected joined table count.", K(ret), K(right_stmt->get_joined_tables()));
  } else if (OB_FAIL(switch_left_outer_to_semi_join(right_stmt,
                                                    right_stmt->get_joined_tables().at(0),
                                                    right_stmt->get_select_items()))) {
    LOG_WARN("failed to switch join table to semi.", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_set_stmt(ctx_, ObSelectStmt::UNION, false, left_stmt,
                                                       right_stmt, union_stmt))) {
    LOG_WARN("failed to create union stmt.", K(ret));
  } else if (OB_ISNULL(union_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(union_stmt));
  } else {
    ref_query = union_stmt;
  }
  return ret;
}

int ObTransformPreProcess::switch_left_outer_to_semi_join(ObSelectStmt *&sub_stmt,
                                                          JoinedTable *joined_table,
                                                          const ObIArray<SelectItem> &select_items)
{
  int ret = OB_SUCCESS;
  SemiInfo *semi_info = NULL;
  ObSEArray<SelectItem, 4> output_select_items;
  ObSEArray<FromItem, 4> from_items;
  ObSEArray<SemiInfo *, 4> semi_infos;
  ObSEArray<JoinedTable *, 4> joined_tables;
  if (OB_ISNULL(joined_table) || OB_ISNULL(sub_stmt)
      || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (OB_ISNULL(joined_table->left_table_) || OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer.", K(ret));
  } else if (joined_table->left_table_->is_joined_table()) {
    TableItem *view_table = NULL;
    TableItem *push_table = joined_table->left_table_;
    if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                          sub_stmt,
                                                          view_table,
                                                          push_table))) {
      LOG_WARN("failed to create empty view table", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                            sub_stmt,
                                                            view_table,
                                                            push_table))) {
      LOG_WARN("failed to create inline view with table", K(ret));
    } else {
      joined_table->left_table_ = view_table;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(semi_info = static_cast<SemiInfo*>(ctx_->allocator_->alloc(
                                                                    sizeof(SemiInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc semi info", K(ret));
  } else if (FALSE_IT(semi_info = new(semi_info)SemiInfo())) {
  } else if (OB_FAIL(semi_infos.push_back(semi_info))) {
    LOG_WARN("failed to push back semi info", K(ret));
  } else if (joined_table->right_table_->type_ == TableItem::JOINED_TABLE &&
      OB_FAIL(joined_tables.push_back(static_cast<JoinedTable*>(joined_table->right_table_)))) {
    LOG_WARN("failed push back joined table.", K(ret));
  } else if (OB_FAIL(create_select_items_for_semi_join(sub_stmt,
                                                       joined_table->left_table_,
                                                       select_items,
                                                       output_select_items))) {
    LOG_WARN("failed to assign to column itmes.", K(ret));
  } else if (OB_FAIL(sub_stmt->get_select_items().assign(output_select_items))) {
    LOG_WARN("failed to assign select items.", K(ret));
  } else if (OB_FAIL(semi_info->semi_conditions_.assign(joined_table->join_conditions_))) {
    LOG_WARN("failed to assign to condition exprs.", K(ret));
  } else {
    FromItem from_item;
    TableItem* right_table_item = NULL;
    from_item.is_joined_ = joined_table->right_table_->type_ == TableItem::JOINED_TABLE;
    from_item.table_id_ = joined_table->right_table_->table_id_;
    semi_info->join_type_ = LEFT_ANTI_JOIN;
    semi_info->right_table_id_ = joined_table->left_table_->table_id_;
    semi_info->semi_id_ = sub_stmt->get_query_ctx()->available_tb_id_--;
    if (OB_FAIL(from_items.push_back(from_item))) {
      LOG_WARN("failed to push back from item", K(ret));
    } else if (OB_ISNULL(right_table_item = sub_stmt->get_table_item_by_id(semi_info->right_table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null pointer.", K(ret));
    } else if (OB_FALSE_IT(right_table_item->for_update_ = false)) {
    } else if (from_item.is_joined_) {
      JoinedTable *table = static_cast<JoinedTable*>(joined_table->right_table_);
      ret = semi_info->left_table_ids_.assign(table->single_table_ids_);
    } else {
      ret = semi_info->left_table_ids_.push_back(from_item.table_id_);
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sub_stmt->get_joined_tables().assign(joined_tables))) {
    LOG_WARN("failed to assign join table.", K(ret));
  } else if (OB_FAIL(sub_stmt->get_semi_infos().assign(semi_infos))) {
    LOG_WARN("failed to assign semi infos.", K(ret));
  } else if (OB_FAIL(sub_stmt->get_from_items().assign(from_items))) {
    LOG_WARN("failed to assign from items.", K(ret));
  } else if (OB_FAIL(sub_stmt->formalize_stmt(ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::create_select_items_for_semi_join(ObDMLStmt *stmt,
                                                            TableItem *table_item,
                                                            const ObIArray<SelectItem> &select_items,
                                                            ObIArray<SelectItem> &output_select_items)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> index_left;
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item) ||
      OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (OB_FAIL(extract_idx_from_table_items(stmt,
                                                  table_item,
                                                  index_left))) {
    LOG_WARN("failed to extract idx from join table.", K(ret));
  } else if (OB_FAIL(output_select_items.assign(select_items))) {
    LOG_WARN("failed to assign select items", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < output_select_items.count(); i++) {
    if (output_select_items.at(i).expr_->get_relation_ids().overlap(index_left)) {
      ObRawExpr *null_expr = NULL;
      ObRawExpr *cast_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_,
                                                  null_expr))) {
        LOG_WARN("failed build null exprs.", K(ret));
      } else if (OB_ISNULL(null_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::try_add_cast_expr_above(
                            ctx_->expr_factory_,
                            ctx_->session_info_,
                            *null_expr,
                            output_select_items.at(i).expr_->get_result_type(),
                            cast_expr))) {
        LOG_WARN("try add cast expr above failed", K(ret));
      } else {
        output_select_items.at(i).expr_ = cast_expr;
      }
    } else { /*do nothing.*/ }
  }
  return ret;
}

int ObTransformPreProcess::extract_idx_from_table_items(ObDMLStmt *sub_stmt,
                                                        const TableItem *table_item,
                                                        ObSqlBitSet<> &rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item) || OB_ISNULL(sub_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null error.", K(ret));
  } else if (table_item->is_joined_table()) {
    const JoinedTable *joined_table = static_cast<const JoinedTable*>(table_item);
    if (OB_FAIL(extract_idx_from_table_items(sub_stmt,
                                             joined_table->left_table_,
                                             rel_ids))) {
      LOG_WARN("failed to extract idx from join table.", K(ret));
    } else if (OB_FAIL(extract_idx_from_table_items(sub_stmt,
                                                    joined_table->right_table_,
                                                    rel_ids))) {
      LOG_WARN("failed to extract idx from join table.", K(ret));
    } else {}
  } else {
    if (OB_FAIL(rel_ids.add_member(sub_stmt->get_table_bit_index(table_item->table_id_)))) {
      LOG_WARN("failed to add member to rel ids.", K(ret));
    } else {}
  }
  return ret;
}

int ObTransformPreProcess::transform_rollup_exprs(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObSelectStmt *sel_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> static_const_exprs;
  ObSEArray<ObRawExpr*, 4> static_remove_const_exprs;
  ObSEArray<ObRawExpr*, 4> exec_param_exprs;
  ObSEArray<ObRawExpr*, 4> exec_param_remove_const_exprs;
  ObSEArray<ObRawExpr*, 4> column_ref_exprs;
  ObSEArray<ObRawExpr*, 4> column_ref_remove_const_exprs;
  ObSEArray<ObRawExpr*, 4> query_ref_exprs;
  ObSEArray<ObRawExpr*, 4> query_ref_remove_const_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!stmt->is_select_stmt()) {
    //do nothing
  } else if (OB_FALSE_IT(sel_stmt = static_cast<ObSelectStmt*>(stmt))) {
  } else if (!sel_stmt->has_rollup()) {
    //do nothing
  } else if (OB_FAIL(get_rollup_const_exprs(sel_stmt,
                                            static_const_exprs,
                                            static_remove_const_exprs,
                                            exec_param_exprs,
                                            exec_param_remove_const_exprs,
                                            column_ref_exprs,
                                            column_ref_remove_const_exprs,
                                            query_ref_exprs,
                                            query_ref_remove_const_exprs,
                                            trans_happened))) {
    LOG_WARN("failed to get rollup const exprs", K(ret));
  } else if (static_const_exprs.empty() && exec_param_exprs.empty() && query_ref_exprs.empty()) {
    //do nothing
  } else if (OB_FAIL(replace_remove_const_exprs(
                                        sel_stmt,
                                        static_const_exprs,
                                        static_remove_const_exprs,
                                        exec_param_exprs,
                                        exec_param_remove_const_exprs,
                                        column_ref_exprs,
                                        column_ref_remove_const_exprs,
                                        query_ref_exprs,
                                        query_ref_remove_const_exprs))) {
    LOG_WARN("failed to replace remove const exprs", K(ret));
  }
  return ret;
}
 int ObTransformPreProcess::get_rollup_const_exprs(ObSelectStmt *stmt,
                                                  ObIArray<ObRawExpr*> &static_const_exprs,
                                                  ObIArray<ObRawExpr*> &static_remove_const_exprs,
                                                  ObIArray<ObRawExpr*> &exec_param_exprs,
                                                  ObIArray<ObRawExpr*> &exec_params_remove_const_exprs,
                                                  ObIArray<ObRawExpr*> &column_ref_exprs,
                                                  ObIArray<ObRawExpr*> &column_ref_remove_const_exprs,
                                                  ObIArray<ObRawExpr*> &query_ref_exprs,
                                                  ObIArray<ObRawExpr*> &query_ref_remove_const_exprs,
                                                  bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<int64_t, ObRawExpr *>, 2> dummy_onetime_exprs;
  ObRawExpr *remove_const_expr = NULL;
  bool is_const = false;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < stmt->get_rollup_expr_size(); ++i) {
    ObRawExpr *expr = stmt->get_rollup_exprs().at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (expr->has_flag(CNT_VOLATILE_CONST) || (!expr->is_const_expr() && !expr->is_query_ref_expr())) {
      //do nothing
    } else if (expr->is_query_ref_expr()) {
      // In mysql mode, rollup expr can reference subquery which may be transformed to a static const or onetime expr
      // e.g. select (select 1) as field1, sum(id) from t1 GROUP BY field1 with rollup;
      //      select (select count(1) from t1) as field1, sum(id) from t1 GROUP BY field1 with rollup;
      if (ObOptimizerUtil::find_item(query_ref_exprs, expr)) {
        //do nothing, skip dup exprs
      } else if (OB_FAIL(ObRawExprUtils::build_remove_const_expr(
                                        *ctx_->expr_factory_,
                                        *ctx_->session_info_,
                                        expr,
                                        remove_const_expr))) {
        LOG_WARN("failed to build remove const expr", K(ret));
      } else if (OB_ISNULL(remove_const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(query_ref_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(query_ref_remove_const_exprs.push_back(remove_const_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        stmt->get_rollup_exprs().at(i) = remove_const_expr;
        trans_happened = true;
      }
    } else if (expr->is_static_const_expr()) { //static const expr
      if (ObOptimizerUtil::find_item(static_const_exprs, expr)) {
        //do nothing, skip dup exprs
      } else if (OB_FAIL(ObRawExprUtils::build_remove_const_expr(
                                        *ctx_->expr_factory_,
                                        *ctx_->session_info_,
                                        expr,
                                        remove_const_expr))) {
        LOG_WARN("failed to build remove const expr", K(ret));
      } else if (OB_ISNULL(remove_const_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(static_const_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(static_remove_const_exprs.push_back(remove_const_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else {
        stmt->get_rollup_exprs().at(i) = remove_const_expr;
        trans_happened = true;
      }
    }  else if (ObOptimizerUtil::find_item(exec_param_exprs, expr)) {
      //do nothing, skip dup exprs
    } else if (OB_FAIL(ObRawExprUtils::build_remove_const_expr(
                                      *ctx_->expr_factory_,
                                      *ctx_->session_info_,
                                      expr,
                                      remove_const_expr))) {
      LOG_WARN("failed to build remove const expr", K(ret));
    } else if (OB_ISNULL(remove_const_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else {
      stmt->get_rollup_exprs().at(i) = remove_const_expr;
      trans_happened = true;
      if (lib::is_mysql_mode() && expr->is_exec_param_expr()) {
        ObExecParamRawExpr *exec_expr = static_cast<ObExecParamRawExpr *>(expr);
        const ObRawExpr *ref_expr = exec_expr->get_ref_expr();
        if (OB_ISNULL(ref_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (ref_expr->is_column_ref_expr()) {
          if (OB_FAIL(column_ref_exprs.push_back(expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (OB_FAIL(column_ref_remove_const_exprs.push_back(remove_const_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(exec_param_exprs.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(exec_params_remove_const_exprs.push_back(remove_const_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_remove_const_exprs(ObSelectStmt *stmt,
                                                      ObIArray<ObRawExpr*> &static_const_exprs,
                                                      ObIArray<ObRawExpr*> &static_remove_const_exprs,
                                                      ObIArray<ObRawExpr*> &exec_params,
                                                      ObIArray<ObRawExpr*> &exec_params_remove_const_exprs,
                                                      ObIArray<ObRawExpr*> &column_ref_exprs,
                                                      ObIArray<ObRawExpr*> &column_ref_remove_const_exprs,
                                                      ObIArray<ObRawExpr*> &query_ref_exprs,
                                                      ObIArray<ObRawExpr*> &query_ref_remove_const_exprs)
{
  int ret = OB_SUCCESS;
  ObQueryCtx *query_ctx = NULL;
  ObStmtCompareContext compare_ctx;
  if (OB_ISNULL(stmt) || OB_ISNULL(query_ctx = stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(stmt), K(query_ctx));
  } else if (static_const_exprs.count() != static_remove_const_exprs.count() ||
             exec_params.count() != exec_params_remove_const_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect expr size", K(ret));
  } else if (OB_FALSE_IT(compare_ctx.init(&query_ctx->calculable_items_))) {
    LOG_WARN("failed to init compare context", K(ret));
  } else {
    if (is_mysql_mode()) {
      ObStmtExprReplacer replacer;
      replacer.remove_all();
      replacer.add_scope(SCOPE_HAVING);
      replacer.add_scope(SCOPE_SELECT);
      replacer.add_scope(SCOPE_ORDERBY);
      replacer.set_recursive(false);
      replacer.set_skip_bool_param_mysql(true);
      if (OB_FAIL(replacer.add_replace_exprs(static_const_exprs, static_remove_const_exprs))) {
        LOG_WARN("failed to add replace exprs", K(ret));
      } else if (OB_FAIL(replacer.add_replace_exprs(column_ref_exprs, column_ref_remove_const_exprs))) {
        LOG_WARN("failed to add replace exprs", K(ret));
      } else if (OB_FAIL(replacer.add_replace_exprs(query_ref_exprs, query_ref_remove_const_exprs))) {
        LOG_WARN("failed to add replace exprs", K(ret));
      } else if (OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
        LOG_WARN("failed to iterate stmt expr", K(ret));
      }
    } else {
      ObStmtExprReplacer replacer;
      replacer.remove_all();
      replacer.add_scope(SCOPE_HAVING);
      replacer.add_scope(SCOPE_SELECT);
      replacer.add_scope(SCOPE_ORDERBY);
      replacer.set_recursive(false);
      replacer.set_skip_bool_param_mysql(false);
      if (OB_FAIL(replacer.add_replace_exprs(exec_params, exec_params_remove_const_exprs))) {
        LOG_WARN("failed to add replace exprs", K(ret));
      } else if (OB_FAIL(stmt->iterate_stmt_expr(replacer))) {
        LOG_WARN("failed to iterate stmt expr", K(ret));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_aggr_item_size(); ++i) {
    ObAggFunRawExpr *agg_expr = NULL;
    if (OB_ISNULL(agg_expr =stmt->get_aggr_item(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (T_FUN_GROUPING == agg_expr->get_expr_type() || T_FUN_GROUPING_ID == agg_expr->get_expr_type()) {
      for (int64_t j = 0; j < agg_expr->get_param_count(); ++j) {
        bool replaced = false;
        int64_t pos = OB_INVALID_ID;
        if (OB_ISNULL(agg_expr->get_param_expr(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (!agg_expr->get_param_expr(j)->is_const_expr()) {
        } else if (ObOptimizerUtil::find_item(static_const_exprs, agg_expr->get_param_expr(j), &pos)) {
          if (OB_UNLIKELY(pos < 0 || pos >= static_remove_const_exprs.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected array pos", K(ret), K(pos), K(static_remove_const_exprs.count()));
          } else {
            agg_expr->get_param_expr(j) = static_remove_const_exprs.at(pos);
          }
        } else if (ObOptimizerUtil::find_item(exec_params, agg_expr->get_param_expr(j), &pos)) {
          if (OB_UNLIKELY(pos < 0 || pos >= exec_params_remove_const_exprs.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected array pos", K(ret), K(pos), K(exec_params_remove_const_exprs.count()));
          } else {
            agg_expr->get_param_expr(j) = exec_params_remove_const_exprs.at(pos);
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_cast_multiset_for_stmt(ObDMLStmt *&stmt,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)
      || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or context is null", K(ret));
  } else {
    ObArray<ObRawExprPointer> relation_exprs;
    ObStmtExprGetter getter;
    if (OB_FAIL(stmt->get_relation_exprs(relation_exprs, getter))) {
      LOG_WARN("failed to get all relation exprs", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < relation_exprs.count(); i++) {
        bool is_happened = false;
        ObRawExpr *expr = NULL;
        if (OB_FAIL(relation_exprs.at(i).get(expr))) {
          LOG_WARN("failed to get expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret));
        } else if (OB_FAIL(transform_cast_multiset_for_expr(*stmt,
                                                            expr,
                                                            is_happened))) {
          LOG_WARN("transform expr failed", K(ret));
        } else if (OB_FAIL(relation_exprs.at(i).set(expr))) {
          LOG_WARN("failed to set expr", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      }
    }
    if (OB_SUCC(ret) && trans_happened &&
        OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_cast_multiset_for_expr(ObDMLStmt &stmt,
                                                            ObRawExpr *&expr,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) ||
      OB_UNLIKELY(T_FUN_SYS_CAST == expr->get_expr_type() &&
                  (expr->get_param_count() != 2 ||
                   expr->get_param_expr(0) == NULL))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret), KPC(expr));
  } else if (T_FUN_SYS_CAST == expr->get_expr_type() &&
             expr->get_param_expr(0)->is_multiset_expr()) {
    ObQueryRefRawExpr *subquery_expr = static_cast<ObQueryRefRawExpr *>(expr->get_param_expr(0));
    ObConstRawExpr *const_expr = static_cast<ObConstRawExpr *>(expr->get_param_expr(1));
    uint64_t udt_id = OB_INVALID_ID;
    if (OB_ISNULL(const_expr) || OB_ISNULL(subquery_expr->get_ref_stmt()) ||
       OB_UNLIKELY(!const_expr->is_const_raw_expr()) ||
       OB_UNLIKELY(OB_INVALID_ID == (udt_id = const_expr->get_udt_id()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected params", K(ret), KPC(expr));
    } else {
      const share::schema::ObUDTTypeInfo *dest_info = NULL;
      const share::schema::ObUDTCollectionType* coll_info = NULL;
      const pl::ObUserDefinedType *pl_type = NULL;
      const pl::ObCollectionType *coll_type = NULL;
      const uint64_t dest_tenant_id = pl::get_tenant_id_by_object_id(udt_id);
      if (OB_FAIL(ctx_->schema_checker_->get_udt_info(dest_tenant_id, udt_id, dest_info))) {
        LOG_WARN("failed to get udt info", K(ret));
      } else if (OB_ISNULL(dest_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected dest_info", K(ret), KPC(dest_info));
      } else if (OB_FAIL(dest_info->transform_to_pl_type(*ctx_->allocator_, pl_type))) {
        LOG_WARN("failed to get pl type", K(ret));
      } else if (OB_ISNULL(coll_type = static_cast<const pl::ObCollectionType *>(pl_type)) ||
                 OB_UNLIKELY(!pl_type->is_collection_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected pl type", K(ret), KPC(pl_type));
      } else if (!coll_type->get_element_type().is_obj_type()) {
        // If the element type of collection is udt,
        // add constructor to the multiset subquery.
        // e.g. create type obj1 as object(a int, b int);
        //      create type tbl1 as table of obj1;
        //      cast(multiset(select '1.1','2.2' from dual) as tbl1)
        //    =>cast(multiset(select obj1('1.1','2.2') from dual) as tbl1)
        if (OB_FAIL(add_constructor_to_multiset(stmt,
                                                subquery_expr,
                                                coll_type->get_element_type(),
                                                trans_happened))) {
          LOG_WARN("failed to add constuctor to multiset", K(ret));
        }
      } else {
        // If the element type of collection is not udt,
        // add column conv to the multiset subquery.
        if (OB_FAIL(add_column_conv_to_multiset(subquery_expr,
                                                coll_type->get_element_type(),
                                                trans_happened))) {
          LOG_WARN("failed to add constuctor to multiset", K(ret));
        }
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      OZ (SMART_CALL(transform_cast_multiset_for_expr(stmt,
                                                      expr->get_param_expr(i),
                                                      trans_happened)));
    }
  }
  return ret;
}

int ObTransformPreProcess::add_constructor_to_multiset(ObDMLStmt &stmt,
                                                       ObQueryRefRawExpr *multiset_expr,
                                                       const pl::ObPLDataType &elem_type,
                                                       bool& trans_happened)
{
  int ret = OB_SUCCESS;
  uint64_t elem_udt_id = elem_type.get_user_type_id();
  ObSelectStmt *multiset_stmt = multiset_expr->get_ref_stmt();
  const share::schema::ObUDTTypeInfo *elem_info = NULL;
  const share::schema::ObUDTCollectionType* coll_info = NULL;
  const pl::ObUserDefinedType *pl_type = NULL;
  const pl::ObRecordType *object_type = NULL;
  ObObjectConstructRawExpr *object_expr = NULL;
  ObSQLSessionInfo *session = ctx_->session_info_;
  int64_t rowsize = 0;
  const uint64_t tenant_id = pl::get_tenant_id_by_object_id(elem_udt_id);
  bool add_constructor = true;
  if (multiset_stmt->get_select_item_size() == 1) {
    // do not add constructor if the select item is null or
    // the type of select item is already the target element type
    ObRawExpr *in_expr = multiset_stmt->get_select_item(0).expr_;
    if (OB_ISNULL(in_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (ObNullType == in_expr->get_data_type()) {
      add_constructor = false;
    } else if (ObExtendType == in_expr->get_data_type()) {
      if (in_expr->get_udt_id() == elem_udt_id) {
        add_constructor = false;
      }
    }
  }

  ObSelectStmt *new_stmt = NULL;
  if (OB_FAIL(ret) || !add_constructor) {
  } else if (OB_ISNULL(ctx_->exec_ctx_->get_sql_proxy()) ||
             OB_ISNULL(ctx_->schema_checker_->get_schema_mgr()) ||
             OB_UNLIKELY(OB_INVALID_ID == elem_udt_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", KPC(multiset_expr), K(elem_type));
  } else if (!multiset_stmt->is_set_stmt() && !multiset_stmt->is_distinct()) {
    // if multiset stmt is set stmt, create a view for it.
    // e.g. create type tbl_obj as table of obj
    //      cast(multiset(select 1 as a from dual union select 2 as a from dual) as tbl_obj)
    //   => cast(multiset(select a from (select 1 as a from dual union select 2 as a from dual) v) as tbl_obj)
    //   => cast(multiset(select obj(a) from (select 1 as a from dual union select 2 as a from dual) v) as tbl_obj)
  } else if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_, multiset_stmt, new_stmt))) {
    LOG_WARN("failed to create dummy view", K(ret));
  } else if (OB_ISNULL(new_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    multiset_stmt = new_stmt;
    multiset_expr->set_ref_stmt(multiset_stmt);
  }
  ObIArray<SelectItem> &select_items = multiset_stmt->get_select_items();
  if (OB_FAIL(ret) || !add_constructor) {
    // do nothing
  } else if (OB_FAIL(ctx_->schema_checker_->get_udt_info(tenant_id, elem_udt_id, elem_info))) {
    LOG_WARN("failed to get udt info", K(ret));
  } else if (OB_ISNULL(elem_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected info", K(ret), KPC(elem_info));
  } else if (OB_FAIL(elem_info->transform_to_pl_type(*ctx_->allocator_, pl_type))) {
    LOG_WARN("failed to get pl type", K(ret));
  } else if (OB_ISNULL(object_type = static_cast<const pl::ObRecordType *>(pl_type)) ||
             OB_UNLIKELY(!pl_type->is_record_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected pl type", K(ret), KPC(pl_type));
  } else if (select_items.count() != object_type->get_member_count()) {
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("unexpected select item count", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_FUN_PL_OBJECT_CONSTRUCT, object_expr))) {
    LOG_WARN("failed to create expr", K(ret));
  } else if (OB_ISNULL(object_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null expr", K(ret));
  } else {
    pl::ObPLINS *ns = NULL;
    pl::ObPLPackageGuard package_guard(session->get_effective_tenant_id());
    pl::ObPLResolveCtx resolve_ctx(*ctx_->allocator_,
                                   *session,
                                   *ctx_->schema_checker_->get_schema_guard(),
                                   package_guard,
                                   *ctx_->exec_ctx_->get_sql_proxy(),
                                   false);
    if (OB_FAIL(package_guard.init())) {
      LOG_WARN("failed to init package guard", K(ret));
    } else if (OB_ISNULL(ns =
        ((NULL == session->get_pl_context()) ?
        static_cast<pl::ObPLINS *>(&resolve_ctx) :
        static_cast<pl::ObPLINS *>(session->get_pl_context()->get_current_ctx())))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(object_type->get_size(*ns, pl::PL_TYPE_ROW_SIZE, rowsize))) {
      LOG_WARN("failed to get size", K(ret));
    } else if (OB_FAIL(object_expr->add_access_name(object_type->get_name()))) {
      LOG_WARN("failed to add access name", K(ret));
    } else {
      object_expr->set_rowsize(rowsize);
      ObExprResType res_type;
      res_type.set_type(ObExtendType);
      res_type.set_extend_type(pl::PL_RECORD_TYPE);
      res_type.set_udt_id(object_type->get_user_type_id());
      object_expr->set_udt_id(object_type->get_user_type_id());
      object_expr->set_result_type(res_type);
      object_expr->set_func_name(object_type->get_name());
      object_expr->set_coll_schema_version(elem_info->get_schema_version());
      // Add udt info to dependency
      if (object_expr->need_add_dependency()) {
        ObSchemaObjVersion udt_schema_version;
        if (OB_FAIL(object_expr->get_schema_object_version(udt_schema_version))) {
          LOG_WARN("failed to get schema version", K(ret));
        } else if (OB_FAIL(stmt.add_global_dependency_table(udt_schema_version))) {
          LOG_WARN("failed to add dependency", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < object_type->get_member_count(); ++i) {
      const pl::ObPLDataType *pl_type = object_type->get_record_member_type(i);
      ObExprResType param_type;
      const ObDataType *data_type = NULL;
      if (OB_ISNULL(pl_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (!pl_type->is_obj_type()) {
        // if the param type is udt,
        // select item must has the same type
        if (pl_type->get_user_type_id() !=
            select_items.at(i).expr_->get_udt_id()) {
          ret = OB_ERR_INVALID_TYPE_FOR_OP;
          LOG_WARN("inconsistent datatypes", K(ret), KPC(pl_type), KPC(select_items.at(i).expr_));
        } else {
          param_type.set_ext();
          param_type.set_extend_type(pl_type->get_type());
          param_type.set_udt_id(pl_type->get_user_type_id());
        }
      } else if (ObExtendType == select_items.at(i).expr_->get_data_type()) {
        // if the param type is not udt,
        // select item should not be udt either
        ret = OB_ERR_INVALID_TYPE_FOR_OP;
        LOG_WARN("inconsistent datatypes", K(ret), KPC(pl_type), KPC(select_items.at(i).expr_));
      } else if (OB_ISNULL(data_type = pl_type->get_data_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else {
        param_type.set_meta(data_type->get_meta_type());
        param_type.set_accuracy(data_type->get_accuracy());
      }
      if (OB_SUCC(ret) &&
          OB_FAIL(object_expr->add_elem_type(param_type))) {
        LOG_WARN("failed to add elem type", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      SelectItem &item = select_items.at(i);
      if (OB_ISNULL(item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(object_expr->add_param_expr(item.expr_))) {
        LOG_WARN("failed to add param expr", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      select_items.reset();
      multiset_expr->get_column_types().reset();
      multiset_expr->set_output_column(1);
      trans_happened = true;
      if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                       object_expr,
                                                       multiset_stmt))) {
        LOG_WARN("failed to create select item", K(ret));
      } else if (OB_FAIL(object_expr->formalize(session))) {
        LOG_WARN("failed to formalize", K(ret));
      } else if (OB_FAIL(multiset_expr->get_column_types().push_back(object_expr->get_result_type()))) {
        LOG_WARN("failed to add result type", K(ret));
      }
    }
  }

  return ret;
}

int ObTransformPreProcess::add_column_conv_to_multiset(ObQueryRefRawExpr *multiset_expr,
                                                       const pl::ObPLDataType &elem_type,
                                                       bool& trans_happened)
{
  int ret = OB_SUCCESS;
  const ObDataType *data_type = NULL;
  ObSelectStmt *multiset_stmt = multiset_expr->get_ref_stmt();
  ObSelectStmt *new_stmt = NULL;
  if (OB_ISNULL(data_type = elem_type.get_data_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null element data type", K(ret), K(elem_type));
  } else if (OB_UNLIKELY(multiset_stmt->get_select_item_size() != 1) ||
            OB_UNLIKELY(multiset_expr->get_column_types().count() != 1)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("unexpected column count", K(ret), KPC(multiset_stmt), KPC(multiset_expr));
  } else {
    if (!multiset_stmt->is_set_stmt() && !multiset_stmt->is_distinct()) {
      // do nothing
      // if multiset stmt is set stmt, create a view for it.
    } else if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_, multiset_stmt, new_stmt))) {
      LOG_WARN("failed to create dummy view", K(ret));
    } else if (OB_ISNULL(new_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      multiset_stmt = new_stmt;
      multiset_expr->set_ref_stmt(multiset_stmt);
    }
    if (OB_SUCC(ret)) {
      SelectItem &select_item = multiset_stmt->get_select_item(0);
      ObRawExpr *child = select_item.expr_;
      if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(ctx_->session_info_,
                                                         *ctx_->expr_factory_,
                                                         data_type->get_obj_type(),
                                                         data_type->get_collation_type(),
                                                         data_type->get_accuracy_value(),
                                                         true,
                                                         NULL,
                                                         NULL,
                                                         child,
                                                         true))) {
        LOG_WARN("failed to build column conv expr", K(ret));
      } else {
        select_item.expr_ = child;
        multiset_expr->get_column_types().at(0) = child->get_result_type();
        trans_happened = true;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_outerjoin_exprs(ObDMLStmt *stmt, bool &is_happened)
{
  int ret = OB_SUCCESS;
  int64_t set_size = 32;
  ObStmtExprGetter visitor;
  visitor.set_relation_scope();
  visitor.remove_scope(DmlStmtScope::SCOPE_JOINED_TABLE);
  ObArray<ObRawExpr *> relation_exprs;
  hash::ObHashSet<uint64_t> expr_set;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs, visitor))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (set_size < relation_exprs.count()) {
    set_size = relation_exprs.count();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(expr_set.create(set_size, "RewriteExpr", "RewriteExpr"))) {
      LOG_WARN("failed to create expr set", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    if (OB_FAIL(ObTransformUtils::append_hashset(relation_exprs.at(i), expr_set))) {
      LOG_WARN("failed to append hashset", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
    JoinedTable *joined_table = NULL;
    if (!stmt->get_from_item(i).is_joined_) {
      // do nothing
    } else if (OB_ISNULL(joined_table = stmt->get_joined_table(stmt->get_from_item(i).table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret), K(joined_table));
    } else if (OB_FAIL(remove_shared_expr(stmt, joined_table, expr_set, false))) {
      LOG_WARN("failed to remove shared expr", K(ret));
    }
  }
  if (expr_set.created()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = expr_set.destroy())) {
      LOG_WARN("failed to destroy expr set", K(ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObTransformPreProcess::remove_shared_expr(ObDMLStmt *stmt,
                                              JoinedTable *joined_table,
                                              hash::ObHashSet<uint64_t> &expr_set,
                                              bool is_nullside)
{
  int ret = OB_SUCCESS;
  TableItem *left = NULL;
  TableItem *right = NULL;
  TableItem *nullside_table = NULL;
  ObArray<ObRawExpr *> padnull_exprs;
  if (OB_ISNULL(joined_table) ||
      OB_ISNULL(left = joined_table->left_table_) ||
      OB_ISNULL(right = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("joined table is null", K(ret), K(joined_table));
  } else if (joined_table->is_full_join()) {
    nullside_table = joined_table;
  } else if (joined_table->is_left_join()) {
    nullside_table = right;
  } else if (joined_table->is_right_join()) {
    nullside_table = left;
  }
  if (OB_SUCC(ret) && NULL != nullside_table) {
    ObArray<uint64_t> table_ids;
    if (nullside_table->is_joined_table()) {
      if (OB_FAIL(append(table_ids, static_cast<JoinedTable *>(nullside_table)->single_table_ids_))) {
        LOG_WARN("failed to append single table ids", K(ret));
      }
    } else if (OB_FAIL(table_ids.push_back(nullside_table->table_id_))) {
      LOG_WARN("failed to push back table id", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); ++i) {
      if (OB_FAIL(stmt->get_column_exprs(table_ids.at(i), padnull_exprs))) {
        LOG_WARN("failed to get column exprs", K(ret));
      }
    }
  }
  if (!padnull_exprs.empty() || is_nullside) {
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
      bool has = false;
      if (OB_FAIL(do_remove_shared_expr(expr_set,
                                        padnull_exprs,
                                        is_nullside,
                                        joined_table->join_conditions_.at(i),
                                        has))) {
        LOG_WARN("failed to remove shared expr", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
    if (OB_FAIL(ObTransformUtils::append_hashset(joined_table->join_conditions_.at(i),
                                                 expr_set))) {
      LOG_WARN("failed to append expr into hashset", K(ret));
    }
  }
  if (OB_SUCC(ret) && left->is_joined_table()) {
    bool left_is_nullside = is_nullside || (joined_table->is_right_join() ||
                                            joined_table->is_full_join());
    if (OB_FAIL(remove_shared_expr(stmt,
                                   static_cast<JoinedTable *>(left),
                                   expr_set,
                                   left_is_nullside))) {
      LOG_WARN("failed to remove shared expr", K(ret));
    }
  }
  if (OB_SUCC(ret) && right->is_joined_table()) {
    bool right_is_nullside = is_nullside || (joined_table->is_left_join() ||
                                             joined_table->is_full_join());
    if (OB_FAIL(remove_shared_expr(stmt,
                                   static_cast<JoinedTable *>(right),
                                   expr_set,
                                   right_is_nullside))) {
      LOG_WARN("failed to remove shared expr", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::do_remove_shared_expr(hash::ObHashSet<uint64_t> &expr_set,
                                                 ObIArray<ObRawExpr *> &padnull_exprs,
                                                 bool is_nullside,
                                                 ObRawExpr *&expr,
                                                 bool &has_padnull_column)
{
  int ret = OB_SUCCESS;
  bool need_copy = false;
  uint64_t key = reinterpret_cast<uint64_t>(expr);
  has_padnull_column = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (expr->is_column_ref_expr()) {
    has_padnull_column = ObOptimizerUtil::find_item(padnull_exprs, expr);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    bool has = false;
    if (OB_FAIL(do_remove_shared_expr(expr_set,
                                      padnull_exprs,
                                      is_nullside,
                                      expr->get_param_expr(i),
                                      has))) {
      LOG_WARN("failed to remove shared expr", K(ret));
    } else if (has) {
      has_padnull_column = true;
    }
  }
  if (OB_SUCC(ret) &&
      OB_HASH_EXIST == expr_set.exist_refactored(key) &&
      !expr->is_column_ref_expr() &&
      !expr->is_query_ref_expr() &&
      !expr->is_const_raw_expr() &&
      !expr->is_exec_param_expr() &&
      !expr->is_pseudo_column_expr()) {
    bool bret = false;
    ObRawExpr *new_expr = NULL;
    if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params have null", K(ret));
    } else if (padnull_exprs.empty() || !has_padnull_column) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(expr, padnull_exprs, bret))) {
      LOG_WARN("failed to check is null propogate expr", K(ret));
    } else if (!bret) {
      need_copy = true;
    }
    if (OB_SUCC(ret) && !need_copy && is_nullside) {
      if (OB_FAIL(check_nullside_expr(expr, bret))) {
        LOG_WARN("failed to check nullside expr", K(ret));
      } else if (!bret) {
        need_copy = true;
      }
    }
    if (OB_SUCC(ret) && need_copy) {
      if (OB_FAIL(ObRawExprCopier::copy_expr_node(*ctx_->expr_factory_,
                                                  expr,
                                                  new_expr))) {
        LOG_WARN("failed to copy expr node", K(ret));
      } else {
        expr = new_expr;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_nullside_expr(ObRawExpr *expr, bool &bret)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> column_exprs;
  bret = false;
  if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
    LOG_WARN("failed to extract column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(expr, column_exprs, bret))) {
    LOG_WARN("failed to check is null propagate expr", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::check_pre_aggregate(const ObSelectStmt &select_stmt,
                                               bool &can_pre_aggr)
{
  int ret = OB_SUCCESS;
  can_pre_aggr = false;
  if (!select_stmt.has_group_by() || select_stmt.get_aggr_item_size() == 0) {
  } else {
    can_pre_aggr = true;
    const ObIArray<ObAggFunRawExpr*> &aggr_items = select_stmt.get_aggr_items();
    int64_t cnt_group_func = 0;
    for (int64_t i = 0; OB_SUCC(ret) && can_pre_aggr && i < aggr_items.count(); ++i) {
      const ObAggFunRawExpr *aggr_expr = aggr_items.at(i);
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (aggr_expr->get_expr_type() == T_FUN_MAX ||
                 aggr_expr->get_expr_type() == T_FUN_MIN ||
                 (!aggr_expr->is_param_distinct() &&
                  (aggr_expr->get_expr_type() == T_FUN_SUM ||
                   aggr_expr->get_expr_type() == T_FUN_COUNT ||
                   aggr_expr->get_expr_type() == T_FUN_COUNT_SUM))) {
      } else if (aggr_expr->get_expr_type() == T_FUN_GROUPING ||
                 aggr_expr->get_expr_type() == T_FUN_GROUPING_ID ||
                 aggr_expr->get_expr_type() == T_FUN_GROUP_ID) {
        cnt_group_func++;
      } else {
        can_pre_aggr = false;
      }
    }
    if (OB_SUCC(ret) && cnt_group_func == aggr_items.count()) {
      can_pre_aggr = false;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_last_insert_id(ObDMLStmt *stmt, bool &trans_happened) {
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else if (stmt->is_select_stmt()) {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt*>(stmt);
    bool is_happened = false;
    if (OB_FAIL(expand_for_last_insert_id(*stmt, sel_stmt->get_having_exprs(), is_happened))) {
      LOG_WARN("fail to expand having exprs",K(ret));
    } else {
      trans_happened |= is_happened;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(expand_for_last_insert_id(*stmt, sel_stmt->get_condition_exprs(), is_happened))) {
      LOG_WARN("fail to expand condition exprs",K(ret));
    } else {
      trans_happened |= is_happened;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt->get_joined_tables().count(); ++i) {
      if (OB_FAIL(expand_last_insert_id_for_join(*stmt, sel_stmt->get_joined_tables().at(i), is_happened))) {
        LOG_WARN("failed to expand join conditions", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  } else if (stmt->is_delete_stmt() || stmt->is_update_stmt()) {
    bool is_happened = false;
    if (OB_FAIL(expand_for_last_insert_id(*stmt, stmt->get_condition_exprs(), is_happened))) {
      LOG_WARN("fail to expand having exprs",K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  return ret;
}

int ObTransformPreProcess::expand_last_insert_id_for_join(ObDMLStmt &stmt, JoinedTable *join_table, bool &has_happened) {
  int ret = OB_SUCCESS;
  bool is_happened = false;
  has_happened = false;
  if (OB_ISNULL(join_table) || OB_ISNULL(join_table->left_table_) || OB_ISNULL(join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(join_table));
  } else if (OB_FAIL(expand_for_last_insert_id(stmt, join_table->join_conditions_, has_happened))) {
    LOG_WARN("failed to expand join conditions", K(ret));
  } else if (join_table->left_table_->is_joined_table() &&
            OB_FAIL(expand_last_insert_id_for_join(stmt, static_cast<JoinedTable*>(join_table->left_table_), is_happened))) {
    LOG_WARN("fail to expand last_insert_id in left join table", K(ret));
  } else if (FALSE_IT(has_happened |= is_happened)) {
  } else if (join_table->right_table_->is_joined_table() &&
            OB_FAIL(expand_last_insert_id_for_join(stmt, static_cast<JoinedTable*>(join_table->right_table_), is_happened))) {
    LOG_WARN("fail to expand last_insert_id in right join table", K(ret));
  } else {
    has_happened |= is_happened;
  }
  return ret;
}

int ObTransformPreProcess::expand_for_last_insert_id(ObDMLStmt &stmt, ObIArray<ObRawExpr*> &exprs, bool &is_happended) {
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> new_exprs;
  is_happended = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObRawExpr *expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(expr), K(i));
    } else if (expr->has_flag(CNT_LAST_INSERT_ID) &&
          (IS_RANGE_CMP_OP(expr->get_expr_type()) || T_OP_EQ == expr->get_expr_type()) &&
          !expr->has_flag(CNT_RAND_FUNC) &&
          !expr->has_flag(CNT_SUB_QUERY) &&
          !expr->has_flag(CNT_ROWNUM) &&
          !expr->has_flag(CNT_SEQ_EXPR) &&
          !expr->has_flag(CNT_DYNAMIC_USER_VARIABLE)) {
      bool removable = false;
      ObRawExpr *left = expr->get_param_expr(0);
      ObRawExpr *right = expr->get_param_expr(1);
      ObRawExpr *check_expr = NULL;
      if (OB_ISNULL(left) || OB_ISNULL(right)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(left), K(right));
      } else if (left->has_flag(CNT_LAST_INSERT_ID) && !left->has_flag(CNT_COLUMN) && right->has_flag(CNT_COLUMN)) {
        check_expr = right;
      } else if (right->has_flag(CNT_LAST_INSERT_ID) && !right->has_flag(CNT_COLUMN) && left->has_flag(CNT_COLUMN)) {
        check_expr = left;
      }
      if (OB_FAIL(ret) || NULL == check_expr) {
        //do nothing
      } else if (OB_FAIL(ObTransformUtils::check_is_index_part_key(*ctx_, stmt, check_expr, removable))) {
        LOG_WARN("fail to check if it's a index/part condition", K(ret));
      } else if (!removable) {
        //do nothing if the param which does not contain last_insert_id is not a index key with lossless cast or a index key.
      } else if (OB_FAIL(check_last_insert_id_removable(expr, removable))) {
        LOG_WARN("fail to check whether last_insert_id can be removed", K(ret));
      } else if (removable) {
        ObRawExpr *param_expr = check_expr == left ? right : left;
        ObRawExpr *new_expr = NULL;
        if (OB_FAIL(ObRawExprCopier::copy_expr(
                            *ctx_->expr_factory_,
                            param_expr,
                            param_expr))) {
          LOG_WARN("failed to copy expr", K(ret));
        } else if (OB_ISNULL(param_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new param is invalid", K(ret));
        } else if (OB_FAIL(remove_last_insert_id(param_expr))) {
          LOG_WARN("failed to remove last insert id exprs", K(ret));
        } else if (OB_FAIL(param_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("failed to formalize expr", K(ret));
        } else if (!param_expr->is_const_expr()) {
          //do nothing
        } else if (OB_FAIL(ObRawExprCopier::copy_expr_node(*ctx_->expr_factory_,
                                                  expr,
                                                  new_expr))) {
          LOG_WARN("failed to copy expr", K(ret));
        } else if (OB_ISNULL(new_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to build new expr", K(ret));
        } else if (new_expr->get_param_expr(0) == check_expr) {
          new_expr->get_param_expr(1) = param_expr;
        } else {
          new_expr->get_param_expr(0) = param_expr;
        }
        if (OB_SUCC(ret) && NULL != new_expr) {
          if (OB_FAIL(new_expr->formalize(ctx_->session_info_))) {
            LOG_WARN("failed to formalize expr", K(ret));
          } else if (OB_FAIL(exprs.push_back(new_expr))) {
            LOG_WARN("failed to push back new pred", K(ret));
          } else {
            is_happended = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_last_insert_id_removable(const ObRawExpr *expr, bool &is_removable) {
  int ret = OB_SUCCESS;
  is_removable = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(expr));
  } else if (expr->has_flag(IS_LAST_INSERT_ID)) {
    if (1 != expr->get_param_count()) {
      is_removable = false;
    } else {
      const ObRawExpr *param = expr->get_param_expr(0);
      if (OB_ISNULL(param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(param));
      } else if (!param->has_flag(IS_CONST) && !param->has_flag(IS_CONST_EXPR)) {
        is_removable = false;
      }
    }
  } else if (expr->has_flag(CNT_LAST_INSERT_ID)) {
    bool flag = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_removable && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(check_last_insert_id_removable(expr->get_param_expr(i), flag))) {
        LOG_WARN("fail to check whether last insert id expr is removable", K(ret));
      } else {
        is_removable = is_removable & flag;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::remove_last_insert_id(ObRawExpr *&expr) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (expr->has_flag(IS_LAST_INSERT_ID)) {
    if (1 == expr->get_param_count()) {
      ObRawExpr *new_expr = expr->get_param_expr(0);
      if (OB_ISNULL(new_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(new_expr));
      } else if (new_expr->has_flag(IS_CONST) || new_expr->has_flag(IS_CONST_EXPR)) {
        expr = new_expr;
      }
    }
  } else if (expr->has_flag(CNT_LAST_INSERT_ID)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(remove_last_insert_id(expr->get_param_expr(i)))) {
        LOG_WARN("fail to check whether last insert id expr is removable", K(ret));
      }
    }
  } else {}
  return ret;
}

int ObTransformPreProcess::expand_correlated_cte(ObDMLStmt *stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt::TempTableInfo, 8> temp_table_infos;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(stmt->collect_temp_table_infos(temp_table_infos))) {
    LOG_WARN("failed to collect temp table infos", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); i ++) {
    bool is_correlated = false;
    ObSEArray<ObSelectStmt *, 4> dummy;
    if (OB_FAIL(check_is_correlated_cte(temp_table_infos.at(i).temp_table_query_, dummy, is_correlated))) {
      LOG_WARN("failed to check is correlated cte", K(ret));
    } else if (!is_correlated) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::expand_temp_table(ctx_, temp_table_infos.at(i)))) {
      LOG_WARN("failed to extend temp table", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::check_exec_param_correlated(const ObRawExpr *expr, bool &is_correlated)
{
  int ret = OB_SUCCESS;
  is_correlated = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_exec_param_expr()) {
    if (!expr->has_flag(BE_USED)) {
      is_correlated = true;
    }
  } else if (expr->has_flag(CNT_DYNAMIC_PARAM)) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_exec_param_correlated(expr->get_param_expr(i),
                                                        is_correlated)))) {
        LOG_WARN("failed to check exec param correlated", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_is_correlated_cte(ObSelectStmt *stmt, ObIArray<ObSelectStmt *> &visited_cte, bool &is_correlated)
{
  int ret = OB_SUCCESS;
  ObArray<ObSelectStmt *> child_stmts;
  ObArray<ObRawExpr *> relation_exprs;
  is_correlated = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  } else if (OB_FAIL(stmt->get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < relation_exprs.count(); ++i) {
    ObRawExpr *expr = relation_exprs.at(i);
    if (OB_FAIL(check_exec_param_correlated(expr, is_correlated))) {
      LOG_WARN("failed to check exec param level", K(ret));
    }
  }
  if (!is_correlated) {
    // add flag to mark the exec param refer the same table
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_subquery_expr_size(); ++i) {
      ObQueryRefRawExpr *query_ref = stmt->get_subquery_exprs().at(i);
      if (OB_ISNULL(query_ref)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query ref is null", K(ret), K(query_ref));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < query_ref->get_exec_params().count(); ++j) {
        ObRawExpr *exec_param = query_ref->get_exec_params().at(j);
        if (OB_ISNULL(exec_param)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("exec param is null", K(ret));
        } else if (OB_FAIL(exec_param->add_flag(BE_USED))) {
          LOG_WARN("failed to add flag", K(ret));
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && !is_correlated && i < child_stmts.count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_is_correlated_cte(child_stmts.at(i), visited_cte, is_correlated)))) {
        LOG_WARN("failed to get non correlated subquery", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
      TableItem *table = stmt->get_table_item(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!table->is_temp_table()) {
        //do nothing
      } else if (ObOptimizerUtil::find_item(visited_cte, table->ref_query_)) {
        //do nothing
      } else if (OB_FAIL(visited_cte.push_back(table->ref_query_))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_is_correlated_cte(table->ref_query_, visited_cte, is_correlated)))) {
        LOG_WARN("failed to get non correlated subquery", K(ret));
      }
    }

    // clear flag
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_subquery_expr_size(); ++i) {
      ObQueryRefRawExpr *query_ref = stmt->get_subquery_exprs().at(i);
      if (OB_ISNULL(query_ref)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("query ref is null", K(ret), K(query_ref));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < query_ref->get_exec_params().count(); ++j) {
        ObRawExpr *exec_param = query_ref->get_exec_params().at(j);
        if (OB_ISNULL(exec_param)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("exec param is null", K(ret));
        } else if (OB_FAIL(exec_param->clear_flag(BE_USED))) {
          LOG_WARN("failed to clear flag", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_can_transform_insert_only_merge_into(const ObMergeStmt *merge_stmt,
                                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(merge_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (merge_stmt->has_update_clause() || !merge_stmt->has_insert_clause()) {
    is_valid = false;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < merge_stmt->get_values_vector().count(); ++i) {
      const ObRawExpr *expr = merge_stmt->get_values_vector().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (expr->is_static_const_expr()) {
        is_valid = false;
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
