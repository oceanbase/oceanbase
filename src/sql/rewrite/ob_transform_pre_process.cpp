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
#include "sql/session/ob_sql_session_info.h"
#include "share/config/ob_server_config.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/dml/ob_merge_resolver.h"
#include "sql/rewrite/ob_expand_aggregate_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
using namespace common;
namespace sql {
int ObTransformPreProcess::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    if (OB_FAIL(eliminate_having(stmt, is_happened))) {
      LOG_WARN("faield to elinimate having", K(ret));
    } else {
      trans_happened |= is_happened;
      LOG_TRACE("succeed to eliminating having statement", K(is_happened));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_materialized_view(stmt, is_happened))) {
        LOG_WARN("failed to transform for materialized view", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform materialized_view", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(replace_func_is_serving_tenant(stmt, is_happened))) {
        LOG_WARN("failed to replace function is_serving_tenant", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to replace function", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_temporary_table(stmt, is_happened))) {
        LOG_WARN("failed to transform for temporary table", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform for temporary table", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_merge_into(stmt, is_happened))) {
        LOG_WARN("failed to transform for merge into", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform for merge into", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_exprs(stmt, is_happened))) {
        LOG_WARN("transform exprs failed", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("success to transform exprs", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_nested_aggregate(stmt, is_happened))) {
        LOG_WARN("failed to transform for nested aggregate.", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform for nested aggregate", K(is_happened), K(ret));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(transformer_aggr_expr(stmt, is_happened))) {
          LOG_WARN("failed to transform aggr expr", K(ret));
        } else {
          trans_happened |= is_happened;
          LOG_TRACE("succeed to transform aggr expr", K(is_happened), K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_hierarchical_query(stmt, is_happened))) {
        LOG_WARN("failed to transform for hierarchical query", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform hierarchical query", K(is_happened));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_rownum_as_limit_offset(parent_stmts, stmt, is_happened))) {
        LOG_WARN("failed to transform rownum as limit", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform rownum as limit", K(is_happened));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(transform_for_grouping_sets_and_multi_rollup(stmt, is_happened))) {
        LOG_WARN("failed to transform for transform for grouping sets and multi rollup.", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to transform for grouping sets and multi rollup", K(is_happened), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("transform pre process succ", K(*stmt));
      if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
        LOG_WARN("failed to formalize stmt", K(ret));
      }
    }
  }
  return ret;
}

/*@brief,ObTransformPreProcess::transform_for_grouping_sets_and_multi_rollup, tranform stmt with
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
int ObTransformPreProcess::transform_for_grouping_sets_and_multi_rollup(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or ctx is null.", K(ret));
  } else if (!stmt->is_select_stmt()) {
    /* do nothing.*/
  } else {
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
    ObIArray<ObGroupingSetsItem>& grouping_sets_items = select_stmt->get_grouping_sets_items();
    ObIArray<ObMultiRollupItem>& multi_rollup_items = select_stmt->get_multi_rollup_items();
    if (!select_stmt->has_grouping_sets() && multi_rollup_items.count() == 0) {
      /* do nothing.*/
    } else if (OB_UNLIKELY(grouping_sets_items.count() == 0 && multi_rollup_items.count() == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error",
          K(select_stmt->has_grouping_sets()),
          K(multi_rollup_items.count()),
          K(grouping_sets_items.count()));
    } else if (grouping_sets_items.count() == 1 && grouping_sets_items.at(0).grouping_sets_exprs_.count() == 1 &&
               grouping_sets_items.at(0).multi_rollup_items_.count() == 0 && multi_rollup_items.count() == 0) {
      // if grouping sets expr has only one,we can remove grouping sets directly.
      if (OB_FAIL(append(
              select_stmt->get_group_exprs(), grouping_sets_items.at(0).grouping_sets_exprs_.at(0).groupby_exprs_))) {
        LOG_WARN("failed to append group exprs", K(ret));
      } else {
        grouping_sets_items.reset();
        bool is_happened = false;
        if (!select_stmt->has_group_by() && OB_FAIL(ObTransformUtils::set_limit_expr(select_stmt, ctx_))) {
          LOG_WARN("add limit expr failed", K(ret));
        } else if (!select_stmt->has_group_by() && OB_FAIL(eliminate_having(select_stmt, is_happened))) {
          LOG_WARN("failed to eliminate having", K(ret));
        } else {
          trans_happened = true;
          LOG_TRACE("eliminate having", K(is_happened));
        }
      }
    } else if (select_stmt->get_stmt_hint().use_px_ == ObUsePxHint::DISABLE) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support no use px for grouping sets.", K(ret));
    } else {
      // as following steps to transform grouping sets stmt to set stmt:
      // 1. Creating spj stmt from origin stmt;
      // 2. According to the spj stmt separated in the previous step, creating temp table and add it
      //    to origin stmt
      // 3. According to grouping sets items or multi rollup items and stmt created in the previous
      //    step,creating set stmt
      // 4. Merging set stmt created in the previous step and origin stmt into transform stmt.
      ObSelectStmt* view_stmt = NULL;
      ObSelectStmt* transform_stmt = NULL;
      ObSelectStmt* set_view_stmt = NULL;
      // step 1, creating spj stmt
      if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, select_stmt, view_stmt))) {
        LOG_WARN("failed to create spj view.", K(ret));
        // step 2, creating temp table
      } else if (OB_FAIL(add_generated_table_as_temp_table(ctx_, select_stmt))) {
        LOG_WARN("failed to add generated table as temp table", K(ret));
        // setp 3, creating set stmt
      } else if (OB_FAIL(create_set_view_stmt(select_stmt, set_view_stmt))) {
        LOG_WARN("failed to create grouping sets view.", K(ret));
        // step 4, merge stmt
      } else if (OB_FAIL(replace_with_set_stmt_view(select_stmt, set_view_stmt, transform_stmt))) {
        LOG_WARN("failed to create union view for grouping sets.", K(ret));
      } else {
        stmt = transform_stmt;
        trans_happened = true;
        LOG_TRACE("succeed to transform transform for grouping sets and multi rollup", K(*stmt));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::add_generated_table_as_temp_table(ObTransformerCtx* ctx, ObDMLStmt* stmt)
{
  int ret = OB_SUCCESS;
  TableItem* table_item = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(ctx->allocator_) || OB_ISNULL(stmt) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ctx), K(stmt), K(ret));
  } else if (OB_UNLIKELY(1 != stmt->get_table_items().count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(stmt->get_table_items().count()), K(ret));
  } else if (OB_ISNULL(table_item = stmt->get_table_item(0)) || OB_ISNULL(table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(table_item), K(ret));
  } else {
    table_item->type_ = TableItem::TEMP_TABLE;
    table_item->ref_id_ = ObSqlTempTableInfo::generate_temp_table_id();
    ObSqlTempTableInfo* temp_table_info = NULL;
    void* ptr = NULL;
    if (OB_ISNULL(ptr = ctx->allocator_->alloc(sizeof(ObSqlTempTableInfo)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      temp_table_info = new (ptr) ObSqlTempTableInfo();
      temp_table_info->ref_table_id_ = table_item->ref_id_;
      temp_table_info->table_query_ = table_item->ref_query_;
      temp_table_info->table_query_->set_temp_table_info(temp_table_info);
      if (OB_FAIL(stmt->generate_view_name(*ctx->allocator_, temp_table_info->table_name_, true))) {
        LOG_WARN("failed to generate view name", K(ret));
      } else if (OB_FAIL(stmt->get_query_ctx()->add_temp_table(temp_table_info))) {
        LOG_WARN("failed to add temp table", K(ret));
      } else {
        // adjust temp-table name
        table_item->table_name_ = temp_table_info->table_name_;
        table_item->alias_name_ = table_item->table_name_;
        for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_items().count(); i++) {
          if (OB_ISNULL(stmt->get_column_item(i)) || OB_ISNULL(stmt->get_column_item(i)->expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else {
            stmt->get_column_item(i)->expr_->set_table_name(table_item->table_name_);
          }
        }
      }
    }
  }
  return ret;
}

/*@brief, ObTransformPreProcess::create_select_list_from_grouping_sets, according to oracle action:
 * if the expr not in group by expr except in aggr, the expr will replaced with NULL; eg:
 *  select c1, c2, max(c1), max(c2) from t1 group by grouping sets(c1,c2) having c1 > 1 or c2 > 1 or sum(c1) > 2 or
 * sum(c2) > 2;
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
 */
int ObTransformPreProcess::create_select_list_from_grouping_sets(ObSelectStmt* stmt,
    ObIArray<ObGroupbyExpr>& groupby_exprs_list, int64_t cur_index, ObIArray<ObRawExpr*>& old_exprs,
    ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null.", K(ret), K(stmt), K(ctx_));
  } else {
    common::ObSEArray<SelectItem, 4> select_items;
    ObRelIds rel_ids;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); i++) {
      if (OB_FAIL(extract_select_expr_and_replace_expr(stmt->get_select_item(i).expr_,
              stmt->get_group_exprs(),
              stmt->get_rollup_exprs(),
              stmt->get_aggr_items(),
              groupby_exprs_list,
              cur_index,
              select_items,
              old_exprs,
              new_exprs,
              rel_ids))) {
        LOG_WARN("failed to extract select expr and replace expr", K(ret));
      } else { /*do nothing*/
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      ObRawExpr* expr = stmt->get_column_items().at(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (ObOptimizerUtil::find_item(stmt->get_group_exprs(), expr) ||
                 ObOptimizerUtil::find_item(stmt->get_rollup_exprs(), expr) ||
                 expr->get_relation_ids().is_subset2(rel_ids)) {
        /*do nothing*/
      } else {
        ObRawExpr* null_expr = NULL;
        ObSysFunRawExpr* cast_expr = NULL;
        if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, null_expr))) {
          LOG_WARN("failed build null exprs.", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                       *ctx_->expr_factory_, null_expr, expr->get_result_type(), cast_expr, ctx_->session_info_))) {
          LOG_WARN("create cast expr failed", K(ret));
        } else if (OB_FAIL(cast_expr->add_flag(IS_INNER_ADDED_EXPR))) {
          LOG_WARN("failed to add flag", K(ret));
        } else if (OB_FAIL(old_exprs.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(new_exprs.push_back(cast_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (select_items.empty()) {
        stmt->get_select_items().reset();
        if (OB_FAIL(ObTransformUtils::create_dummy_select_item(*stmt, ctx_))) {
          LOG_WARN("failed to create dummy select item", K(ret));
        } else { /*do nothing*/
        }
      } else if (OB_FAIL(stmt->get_select_items().assign(select_items))) {
        LOG_WARN("failed to assign to select items.", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::extract_select_expr_and_replace_expr(ObRawExpr* expr, ObIArray<ObRawExpr*>& groupby_exprs,
    ObIArray<ObRawExpr*>& rollup_exprs, ObIArray<ObAggFunRawExpr*>& aggr_items,
    ObIArray<ObGroupbyExpr>& groupby_exprs_list, int64_t cur_index, ObIArray<SelectItem>& select_items,
    ObIArray<ObRawExpr*>& old_exprs, ObIArray<ObRawExpr*>& new_exprs, ObRelIds& rel_ids)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (expr->is_aggr_expr() || ObOptimizerUtil::find_item(groupby_exprs, expr) ||
             ObOptimizerUtil::find_item(rollup_exprs, expr)) {
    if (!is_expr_in_select_item(select_items, expr)) {
      SelectItem select_item;
      select_item.expr_ = expr;
      select_item.expr_name_ = expr->get_expr_name();
      select_item.alias_name_ = expr->get_expr_name();
      if (OB_FAIL(select_items.push_back(select_item))) {
        LOG_WARN("failed to push back into select items.", K(ret));
      } else if (expr->get_expr_type() == T_FUN_GROUPING) {
        if (OB_UNLIKELY(expr->get_param_count() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(*expr), K(ret));
        } else if (ObOptimizerUtil::find_item(groupby_exprs, expr->get_param_expr(0)) ||
                   ObOptimizerUtil::find_item(rollup_exprs, expr->get_param_expr(0))) {
          /*do nothing*/
        } else {
          ObConstRawExpr* one_expr = NULL;
          ObSysFunRawExpr* cast_expr = NULL;
          if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 1, one_expr))) {
            LOG_WARN("failed to build const int expr", K(ret));
          } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                         *ctx_->expr_factory_, one_expr, expr->get_result_type(), cast_expr, ctx_->session_info_))) {
            LOG_WARN("create cast expr failed", K(ret));
          } else if (OB_FAIL(cast_expr->add_flag(IS_INNER_ADDED_EXPR))) {
            LOG_WARN("failed to add flag", K(ret));
          } else if (OB_FAIL(old_exprs.push_back(expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (OB_FAIL(rel_ids.add_members2(expr->get_relation_ids()))) {
            LOG_WARN("failed to get relation ids", K(ret));
          } else if (OB_FAIL(new_exprs.push_back(cast_expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          } else if (OB_FAIL(ObOptimizerUtil::remove_item(aggr_items, static_cast<ObAggFunRawExpr*>(expr)))) {
            LOG_WARN("failed to remove item", K(ret));
          } else { /*do nothing*/
          }
        }
      }
    }
  } else if (is_select_expr_in_other_groupby_exprs(expr, groupby_exprs_list, cur_index)) {
    if (!ObOptimizerUtil::find_item(old_exprs, expr)) {
      ObRawExpr* null_expr = NULL;
      ObSysFunRawExpr* cast_expr = NULL;
      SelectItem select_item;
      select_item.expr_ = expr;
      select_item.expr_name_ = expr->get_expr_name();
      select_item.alias_name_ = expr->get_expr_name();
      if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_, null_expr))) {
        LOG_WARN("failed build null exprs.", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::create_cast_expr(
                     *ctx_->expr_factory_, null_expr, expr->get_result_type(), cast_expr, ctx_->session_info_))) {
        LOG_WARN("create cast expr failed", K(ret));
      } else if (OB_FAIL(cast_expr->add_flag(IS_INNER_ADDED_EXPR))) {
        LOG_WARN("failed to add flag", K(ret));
      } else if (OB_FAIL(old_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(rel_ids.add_members2(expr->get_relation_ids()))) {
        LOG_WARN("failed to get relation ids", K(ret));
      } else if (OB_FAIL(new_exprs.push_back(cast_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(select_items.push_back(select_item))) {
        LOG_WARN("failed to push back into select items.", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_select_expr_and_replace_expr(expr->get_param_expr(i),
              groupby_exprs,
              rollup_exprs,
              aggr_items,
              groupby_exprs_list,
              cur_index,
              select_items,
              old_exprs,
              new_exprs,
              rel_ids)))) {
        LOG_WARN("failed to extract select expr and replace expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_select_and_having_exprs(
    ObSelectStmt* select_stmt, ObIArray<ObRawExpr*>& old_exprs, ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else {
    // replace select expr
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); ++i) {
      SelectItem& select_item = select_stmt->get_select_item(i);
      if (OB_ISNULL(select_item.expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(select_item.expr_));
      } else if (OB_FAIL(replace_stmt_special_exprs(select_stmt, select_item.expr_, old_exprs, new_exprs))) {
        LOG_WARN("failed to replace exception aggr exprs", K(ret));
      }
    }
    // replace having expr into null
    if (OB_SUCC(ret)) {
      ObIArray<ObRawExpr*>& having_exprs = select_stmt->get_having_exprs();
      for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs.count(); ++i) {
        if (OB_ISNULL(having_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(having_exprs.at(i)));
        } else if (OB_FAIL(replace_stmt_special_exprs(select_stmt, having_exprs.at(i), old_exprs, new_exprs, true))) {
          LOG_WARN("failed to replace exception aggr exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObTransformPreProcess::is_select_expr_in_other_groupby_exprs(
    ObRawExpr* expr, ObIArray<ObGroupbyExpr>& groupby_exprs_list, int64_t cur_index)
{
  bool is_true = false;
  for (int64_t i = 0; !is_true && i < groupby_exprs_list.count(); ++i) {
    if (i == cur_index) {
      /*do nothing */
    } else {
      is_true = ObOptimizerUtil::find_equal_expr(groupby_exprs_list.at(i).groupby_exprs_, expr);
    }
  }
  return is_true;
}

bool ObTransformPreProcess::is_expr_in_select_item(ObIArray<SelectItem>& select_items, ObRawExpr* expr)
{
  bool is_true = false;
  for (int64_t i = 0; !is_true && i < select_items.count(); ++i) {
    is_true = (select_items.at(i).expr_ == expr);
  }
  return is_true;
}

int ObTransformPreProcess::replace_stmt_special_exprs(ObSelectStmt* select_stmt, ObRawExpr*& expr,
    ObIArray<ObRawExpr*>& old_exprs, ObIArray<ObRawExpr*>& new_exprs, bool ignore_const /*default false*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (ignore_const && expr->has_const_or_const_expr_flag()) {
    /*do nothing*/
  } else if (ObOptimizerUtil::find_item(select_stmt->get_group_exprs(), expr) ||
             ObOptimizerUtil::find_item(select_stmt->get_rollup_exprs(), expr)) {
    /*do nothing*/
  } else {
    int64_t idx = -1;
    if (!ObOptimizerUtil::find_item(old_exprs, expr, &idx)) {
      if (expr->is_aggr_expr()) {
        // do nothing
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
          if (OB_FAIL(SMART_CALL(replace_stmt_special_exprs(
                  select_stmt, expr->get_param_expr(i), old_exprs, new_exprs, ignore_const)))) {
            LOG_WARN("failed to replace exception aggr exprs", K(ret));
          } else { /*do nothing */
          }
        }
      }
    } else if (OB_UNLIKELY(idx < 0 || idx >= new_exprs.count()) || OB_ISNULL(new_exprs.at(idx))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx), K(new_exprs.count()), K(new_exprs));
    } else {
      expr = new_exprs.at(idx);
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_with_set_stmt_view(
    ObSelectStmt* origin_stmt, ObSelectStmt* set_view_stmt, ObSelectStmt*& union_stmt)
{
  int ret = OB_SUCCESS;
  ObRelIds rel_ids;
  TableItem* view_table_item = NULL;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  ObSEArray<ColumnItem, 4> temp_column_items;
  if (OB_ISNULL(origin_stmt) || OB_ISNULL(set_view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("origin stmt is null", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_from_tables(*origin_stmt, rel_ids))) {
    LOG_WARN("failed to get from tables.", K(ret));
  } else if (FALSE_IT(origin_stmt->get_table_items().reset())) {
  } else if (FALSE_IT(origin_stmt->get_from_items().reset())) {
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, origin_stmt, set_view_stmt, view_table_item))) {
    LOG_WARN("failed to add new table item.", K(ret));
  } else if (OB_FAIL(origin_stmt->add_from_item(view_table_item->table_id_))) {
    LOG_WARN("failed to add from item", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *view_table_item, origin_stmt, new_exprs))) {
    LOG_WARN("failed to get select exprs from grouping sets view.", K(ret));
  } else { /* do nothing. */
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < origin_stmt->get_column_size(); i++) {
    if (!origin_stmt->get_column_item(i)->expr_->get_relation_ids().overlap(rel_ids)) {
      if (OB_FAIL(temp_column_items.push_back(*origin_stmt->get_column_item(i)))) {
        LOG_WARN("faield to push back into column items.", K(ret));
      } else { /* do nothing. */
      }
    } else { /* do nothing. */
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(origin_stmt->get_column_items().assign(temp_column_items))) {
    LOG_WARN("failed to assign column items.", K(ret));
  } else if (OB_FAIL(extract_stmt_replace_expr(origin_stmt, old_exprs))) {
    LOG_WARN("failed to extract stmt replace expr", K(ret));
  } else if (OB_UNLIKELY(old_exprs.count() != 0 && old_exprs.count() != new_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(old_exprs), K(new_exprs));
  } else {
    origin_stmt->get_grouping_sets_items().reset();
    origin_stmt->get_multi_rollup_items().reset();
    origin_stmt->get_aggr_items().reset();
    origin_stmt->get_group_exprs().reset();
    origin_stmt->get_rollup_exprs().reset();
    origin_stmt->reassign_grouping();
    origin_stmt->reassign_rollup();
    origin_stmt->reassign_grouping_sets();
    origin_stmt->get_table_items().reset();
    if (old_exprs.count() != 0 && OB_FAIL(origin_stmt->replace_inner_stmt_expr(old_exprs, new_exprs))) {
      LOG_WARN("failed to replace inner stmt exprs.", K(ret));
    } else if (OB_FAIL(origin_stmt->get_table_items().push_back(view_table_item))) {
      LOG_WARN("add table item failed", K(ret));
    } else if (OB_FAIL(origin_stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash.", K(ret));
    } else if (OB_FAIL(origin_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column items rel id.", K(ret));
    } else if (OB_FAIL(origin_stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalized stmt.", K(ret));
    } else {
      origin_stmt->set_need_temp_table_trans(true);
      union_stmt = origin_stmt;
    }
  }
  return ret;
}

int ObTransformPreProcess::extract_stmt_replace_expr(ObSelectStmt* select_stmt, ObIArray<ObRawExpr*>& old_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(select_stmt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_select_item_size(); i++) {
      if (OB_FAIL(
              extract_replace_expr_from_select_expr(select_stmt->get_select_item(i).expr_, select_stmt, old_exprs))) {
        LOG_WARN("failed to extract replace expr from select expr", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::extract_replace_expr_from_select_expr(
    ObRawExpr* expr, ObSelectStmt* select_stmt, ObIArray<ObRawExpr*>& old_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr));
  } else if (expr->is_aggr_expr() || ObOptimizerUtil::find_item(select_stmt->get_group_exprs(), expr) ||
             ObOptimizerUtil::find_item(select_stmt->get_rollup_exprs(), expr) ||
             select_stmt->is_expr_in_groupings_sets_item(expr) || select_stmt->is_expr_in_multi_rollup_items(expr)) {
    // here use find_equal_expr function, because same exprs in groupings set item have different ptr
    if (!ObOptimizerUtil::find_equal_expr(old_exprs, expr)) {
      if (OB_FAIL(old_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_replace_expr_from_select_expr(expr->get_param_expr(i), select_stmt, old_exprs)))) {
        LOG_WARN("failed to extract replace expr from select expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::create_set_view_stmt(ObSelectStmt* origin_stmt, ObSelectStmt*& set_view_stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* groupby_stmt = NULL;
  ObSelectStmt* part_union_stmt = NULL;
  ObSelectStmt* temp_stmt = NULL;
  if (OB_ISNULL(origin_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("origin stmt is null", K(ret));
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->stmt_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx or ctx stmt factory is null", K(ret));
  } else {
    /* as following to create set stmt:
     * 1. get total group by stmt count(grouping sets + multi rollup)
     * 2. deep copy origin stmt;
     * 3. generate group by exprs and add stmt;
     * 4. deal with stmt other attribute: select list, aggr, column and so on;
     * 5. create set stmt.
     */
    int64_t count =
        get_total_count_of_groupby_stmt(origin_stmt->get_grouping_sets_items(), origin_stmt->get_multi_rollup_items());
    if (OB_UNLIKELY(count < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(count));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
        ObSEArray<ObGroupbyExpr, 4> groupby_exprs_list;
        bool is_happened = false;
        if (OB_FAIL(ctx_->stmt_factory_->create_stmt<ObSelectStmt>(groupby_stmt))) {
          LOG_WARN("failed to create stmt from ctx.", K(ret));
        } else if (OB_ISNULL(groupby_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("groupby stmt is null", K(ret));
        } else if (FALSE_IT(groupby_stmt->set_query_ctx(origin_stmt->get_query_ctx()))) {
        } else if (OB_FAIL(groupby_stmt->deep_copy(*ctx_->stmt_factory_, *ctx_->expr_factory_, *origin_stmt))) {
          LOG_WARN("failed to deep copy from stmt.", K(ret));
        } else if (OB_FAIL(get_groupby_exprs_list(groupby_stmt->get_grouping_sets_items(),
                       groupby_stmt->get_multi_rollup_items(),
                       groupby_exprs_list))) {
          LOG_WARN("failed to get groupby exprs list", K(ret));
        } else if (OB_UNLIKELY(i >= groupby_exprs_list.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(i), K(groupby_exprs_list.count()), K(ret));
        } else if (OB_FAIL(
                       append_array_no_dup(groupby_stmt->get_group_exprs(), groupby_exprs_list.at(i).groupby_exprs_))) {
          LOG_WARN("failed to assign the group exprs.", K(ret));
        } else {
          // stmt only reserve group by, having and aggr info
          groupby_stmt->get_order_items().reset();
          groupby_stmt->set_limit_offset(NULL, NULL);
          groupby_stmt->set_limit_percent_expr(NULL);
          groupby_stmt->set_fetch_with_ties(false);
          groupby_stmt->set_has_fetch(false);
          groupby_stmt->clear_sequence();
          groupby_stmt->set_select_into(NULL);
          groupby_stmt->get_grouping_sets_items().reset();
          groupby_stmt->get_multi_rollup_items().reset();
          groupby_stmt->get_window_func_exprs().reset();
          if (groupby_stmt->get_rollup_expr_size() == 0) {
            groupby_stmt->reassign_rollup();
          }
          ObSEArray<ObRawExpr*, 4> old_exprs;
          ObSEArray<ObRawExpr*, 4> new_exprs;
          if (OB_FAIL(ObTransformUtils::replace_stmt_expr_with_groupby_exprs(groupby_stmt))) {
            LOG_WARN("failed to replace stmt expr with groupby columns", K(ret));
          } else if (OB_FAIL(create_select_list_from_grouping_sets(
                         groupby_stmt, groupby_exprs_list, i, old_exprs, new_exprs))) {
            LOG_WARN("failed to create select list from grouping sets.", K(ret));
            // why not use replace_inner_stmt_expr?, see this example:
            // select nvl(c1,1), c1, max(c1) from t1 grouping sets(nvl(c1,1), c1);
          } else if (OB_FAIL(replace_select_and_having_exprs(groupby_stmt, old_exprs, new_exprs))) {
            LOG_WARN("failed to replace select and having expr", K(ret));
            // select c1 from t1 group by grouping sets(c1, ());
            //<==>
            // select c1 from t1 group by c1 union all select NULL from t1 limit 1;
          } else if (!groupby_stmt->has_group_by() && OB_FAIL(ObTransformUtils::set_limit_expr(groupby_stmt, ctx_))) {
            LOG_WARN("add limit expr failed", K(ret));
          } else if (!groupby_stmt->has_group_by() && OB_FAIL(eliminate_having(groupby_stmt, is_happened))) {
            LOG_WARN("failed to eliminate having", K(ret));
          } else if (OB_FAIL(groupby_stmt->update_stmt_table_id(*origin_stmt))) {
            LOG_WARN("failed to update stmt table id.", K(ret));
          } else if (OB_FAIL(groupby_stmt->formalize_stmt(ctx_->session_info_))) {
            LOG_WARN("failed to formalized stmt.", K(ret));
          } else if (i >= 1) {
            if (i == groupby_exprs_list.count() - 1) {
              groupby_stmt->set_is_last_access(true);
            }
            if (OB_FAIL(ObTransformUtils::create_union_stmt(ctx_, false, part_union_stmt, groupby_stmt, temp_stmt))) {
              LOG_WARN("failed to create union stmt.", K(ret));
            } else if (OB_ISNULL(temp_stmt)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null", K(ret), K(temp_stmt));
            } else if (FALSE_IT(temp_stmt->set_query_ctx(origin_stmt->get_query_ctx()))) {
            } else if (OB_FAIL(temp_stmt->formalize_stmt(ctx_->session_info_))) {
              LOG_WARN("failed to formalize stmt.", K(ret));
            } else {
              part_union_stmt = temp_stmt;
            }
          } else if (i == 0) {
            part_union_stmt = groupby_stmt;
          } else { /*do nothing.*/
          }
        }
      }
      set_view_stmt = part_union_stmt;
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 4> select_exprs;
    if (OB_FAIL(set_view_stmt->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs.", K(ret));
    } else if (FALSE_IT(set_view_stmt->get_select_items().reset())) {
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, select_exprs, set_view_stmt))) {
      LOG_WARN("failed to create select items.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int64_t ObTransformPreProcess::get_total_count_of_groupby_stmt(
    ObIArray<ObGroupingSetsItem>& grouping_sets_items, ObIArray<ObMultiRollupItem>& multi_rollup_items)
{
  int64_t cnt_grouping_sets = 1;
  int64_t cnt_multi_rollup = 1;
  for (int64_t i = 0; i < grouping_sets_items.count(); ++i) {
    int64_t tmp_count = 1;
    ObIArray<ObMultiRollupItem>& rollup_items = grouping_sets_items.at(i).multi_rollup_items_;
    for (int64_t j = 0; j < rollup_items.count(); ++j) {
      tmp_count = tmp_count * (rollup_items.at(j).rollup_list_exprs_.count() + 1);
    }
    cnt_grouping_sets =
        cnt_grouping_sets * (grouping_sets_items.at(i).grouping_sets_exprs_.count() + (tmp_count > 1 ? tmp_count : 0));
  }
  for (int64_t i = 0; i < multi_rollup_items.count(); ++i) {
    cnt_multi_rollup = cnt_multi_rollup * (multi_rollup_items.at(i).rollup_list_exprs_.count() + 1);
  }
  return cnt_grouping_sets * cnt_multi_rollup;
}

int ObTransformPreProcess::get_groupby_exprs_list(ObIArray<ObGroupingSetsItem>& grouping_sets_items,
    ObIArray<ObMultiRollupItem>& multi_rollup_items, ObIArray<ObGroupbyExpr>& groupby_exprs_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGroupbyExpr, 4> grouping_sets_exprs_list;
  ObSEArray<ObGroupbyExpr, 4> multi_rollup_exprs_list;
  if (OB_FAIL(expand_grouping_sets_items(grouping_sets_items, grouping_sets_exprs_list))) {
    LOG_WARN("failed to expand grouping sets items", K(ret));
  } else if (OB_FAIL(expand_multi_rollup_items(multi_rollup_items, multi_rollup_exprs_list))) {
    LOG_WARN("failed to expand grouping sets items", K(ret));
  } else if (OB_FAIL(
                 combination_two_rollup_list(grouping_sets_exprs_list, multi_rollup_exprs_list, groupby_exprs_list))) {
    LOG_WARN("failed to expand grouping sets items", K(ret));
  } else {
    LOG_TRACE("succeed to get groupby exprs list",
        K(grouping_sets_exprs_list),
        K(multi_rollup_exprs_list),
        K(groupby_exprs_list));
  }
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
    ObIArray<ObGroupingSetsItem>& grouping_sets_items, ObIArray<ObGroupbyExpr>& grouping_sets_exprs)
{
  int ret = OB_SUCCESS;
  int64_t total_cnt = 1;
  ObSEArray<ObGroupingSetsItem, 4> tmp_grouping_sets_items;
  for (int64_t i = 0; i < grouping_sets_items.count(); ++i) {
    total_cnt = total_cnt * (grouping_sets_items.at(i).grouping_sets_exprs_.count() +
                                grouping_sets_items.at(i).multi_rollup_items_.count());
  }
  if (OB_UNLIKELY(total_cnt < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(total_cnt), K(grouping_sets_items));
  } else if (OB_FAIL(tmp_grouping_sets_items.prepare_allocate(total_cnt))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
      int64_t bit_count = i;
      int64_t index = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < grouping_sets_items.count(); ++j) {
        ObGroupingSetsItem& item = grouping_sets_items.at(j);
        int64_t item_count = item.grouping_sets_exprs_.count() + item.multi_rollup_items_.count();
        index = bit_count % (item_count);
        bit_count = bit_count / (item_count);
        if (index < item.grouping_sets_exprs_.count()) {
          if (OB_FAIL(
                  tmp_grouping_sets_items.at(i).grouping_sets_exprs_.push_back(item.grouping_sets_exprs_.at(index)))) {
            LOG_WARN("failed to push back item", K(ret));
          }
        } else if (OB_UNLIKELY(index - item.grouping_sets_exprs_.count() >= item.multi_rollup_items_.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(ret));
        } else if (OB_FAIL(tmp_grouping_sets_items.at(i).multi_rollup_items_.push_back(
                       item.multi_rollup_items_.at(index - item.grouping_sets_exprs_.count())))) {
          LOG_WARN("failed to push back item", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < total_cnt; ++i) {
        ObGroupbyExpr groupby_item;
        for (int64_t j = 0; OB_SUCC(ret) && j < tmp_grouping_sets_items.at(i).grouping_sets_exprs_.count(); ++j) {
          if (OB_FAIL(append(groupby_item.groupby_exprs_,
                  tmp_grouping_sets_items.at(i).grouping_sets_exprs_.at(j).groupby_exprs_))) {
            LOG_WARN("failed to append exprs", K(ret));
          } else { /*do nothing*/
          }
        }
        if (tmp_grouping_sets_items.at(i).multi_rollup_items_.count() > 0) {
          ObSEArray<ObGroupbyExpr, 4> groupby_exprs_list;
          if (OB_FAIL(
                  expand_multi_rollup_items(tmp_grouping_sets_items.at(i).multi_rollup_items_, groupby_exprs_list))) {
            LOG_WARN("failed to expand multi rollup items", K(ret));
          } else {
            for (int64_t k = 0; OB_SUCC(ret) && k < groupby_exprs_list.count(); ++k) {
              if (OB_FAIL(append(groupby_exprs_list.at(k).groupby_exprs_, groupby_item.groupby_exprs_))) {
                LOG_WARN("failed to append exprs", K(ret));
              } else if (OB_FAIL(grouping_sets_exprs.push_back(groupby_exprs_list.at(k)))) {
                LOG_WARN("failed to push back groupby exprs", K(ret));
              } else { /*do nothing*/
              }
            }
          }
        } else if (OB_FAIL(grouping_sets_exprs.push_back(groupby_item))) {
          LOG_WARN("failed to push back item", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::expand_multi_rollup_items(
    ObIArray<ObMultiRollupItem>& multi_rollup_items, ObIArray<ObGroupbyExpr>& rollup_list_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObGroupbyExpr, 4> result_rollup_list_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_rollup_items.count(); ++i) {
    ObMultiRollupItem& multi_rollup_item = multi_rollup_items.at(i);
    ObSEArray<ObGroupbyExpr, 4> tmp_rollup_list_exprs;
    ObGroupbyExpr empty_item;
    if (OB_FAIL(tmp_rollup_list_exprs.push_back(empty_item))) {
      LOG_WARN("failed to push back item", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < multi_rollup_item.rollup_list_exprs_.count(); ++j) {
        ObGroupbyExpr item;
        if (OB_FAIL(append(item.groupby_exprs_, tmp_rollup_list_exprs.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(append(item.groupby_exprs_, multi_rollup_item.rollup_list_exprs_.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(tmp_rollup_list_exprs.push_back(item))) {
          LOG_WARN("failed to push back item", K(ret));
        } else { /*do nothing*/
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(combination_two_rollup_list(result_rollup_list_exprs, tmp_rollup_list_exprs, rollup_list_exprs))) {
          LOG_WARN("failed to combination two rollup list", K(ret));
        } else if (OB_FAIL(result_rollup_list_exprs.assign(rollup_list_exprs))) {
          LOG_WARN("failed to assign exprs", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::combination_two_rollup_list(ObIArray<ObGroupbyExpr>& rollup_list_exprs1,
    ObIArray<ObGroupbyExpr>& rollup_list_exprs2, ObIArray<ObGroupbyExpr>& rollup_list_exprs)
{
  int ret = OB_SUCCESS;
  rollup_list_exprs.reset();
  if (rollup_list_exprs1.count() == 0 && rollup_list_exprs2.count() > 0) {
    if (OB_FAIL(append(rollup_list_exprs, rollup_list_exprs2))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else { /*do nothing*/
    }
  } else if (rollup_list_exprs1.count() > 0 && rollup_list_exprs2.count() == 0) {
    if (OB_FAIL(append(rollup_list_exprs, rollup_list_exprs1))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else { /*do nothing*/
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rollup_list_exprs1.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs2.count(); ++j) {
        ObGroupbyExpr item;
        if (OB_FAIL(append(item.groupby_exprs_, rollup_list_exprs1.at(i).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(append(item.groupby_exprs_, rollup_list_exprs2.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        } else if (OB_FAIL(rollup_list_exprs.push_back(item))) {
          LOG_WARN("failed to push back item", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_hierarchical_query(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param is null", K(ret));
  } else if (stmt->is_select_stmt() && stmt->is_hierarchical_query()) {
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
    TableItem* table = NULL;
    if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else if (OB_FAIL(create_connect_by_view(*select_stmt))) {
      LOG_WARN("failed to create connect by view", K(ret));
    } else if (OB_UNLIKELY(select_stmt->get_table_size() != 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect table size after create connect by view", K(select_stmt->get_table_size()), K(ret));
    } else if (OB_ISNULL(table = select_stmt->get_table_item(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_UNLIKELY(!table->is_generated_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect generated table after create connect by view", K(ret));
    } else if (OB_ISNULL(table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", K(ret));
    } else if (OB_FAIL(create_and_mock_join_view(*table->ref_query_))) {
      LOG_WARN("failed to transform from item", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::create_connect_by_view(ObSelectStmt& stmt)
{
  int ret = OB_SUCCESS;
  ObSelectStmt* view_stmt = NULL;
  ObStmtFactory* stmt_factory = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  ObSEArray<ObRawExpr*, 4> select_list;
  // LEVEL, CONNECT_BY_ISLEAF, CONNECT_BY_ISCYCLE
  // CONNECT_BY_ROOT, PRIOR, CONNECT_BY_PATH
  ObSEArray<ObRawExpr*, 4> connect_by_related_exprs;
  if (OB_ISNULL(ctx_) || OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt_factory), K(expr_factory), K(stmt), K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(view_stmt->ObDMLStmt::assign(stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(view_stmt->adjust_statement_id())) {
    LOG_WARN("failed to adjust stmt id", K(ret));
  } else {
    view_stmt->set_stmt_type(stmt::T_SELECT);
    // 1. handle table, columns, from
    // dml_stmt: from table, semi table, joined table
    stmt.reset_CTE_table_items();
    stmt.get_semi_infos().reuse();
    stmt.get_column_items().reuse();
    stmt.get_part_exprs().reset();
    stmt.set_hierarchical_query(false);
    stmt.set_has_prior(false);
    if (stmt.is_order_siblings()) {
      stmt.get_order_items().reset();
      stmt.set_order_siblings(false);
    }
  }
  // 2. handle where conditions
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 8> join_conds;
    ObSEArray<ObRawExpr*, 8> other_conds;
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
      view_stmt->get_order_items().reset();
    }
    view_stmt->set_limit_offset(NULL, NULL);
    view_stmt->set_limit_percent_expr(NULL);
    view_stmt->set_fetch_with_ties(false);
    view_stmt->set_has_fetch(false);
    view_stmt->clear_sequence();
    view_stmt->set_select_into(NULL);
    view_stmt->get_pseudo_column_like_exprs().reset();
    view_stmt->set_hierarchical_query(true);
    view_stmt->set_nocycle(stmt.is_nocycle());
    if (OB_FAIL(pushdown_start_with_connect_by(stmt, *view_stmt))) {
      LOG_WARN("failed to pushdown start with exprs", K(ret));
    } else {
      stmt.get_start_with_exprs().reset();
      stmt.get_connect_by_exprs().reset();
    }
  }
  // 4. process connect by related expr
  if (OB_SUCC(ret)) {
    if (OB_FAIL(extract_connect_by_related_exprs(stmt, connect_by_related_exprs))) {
      LOG_WARN("failed to extract connect by related exprs", K(ret));
    }
  }
  // 5. finish creating the child stmts
  if (OB_SUCC(ret)) {
    // create select list
    ObSEArray<ObRawExpr*, 4> columns;
    ObRelIds rel_ids;
    ObSqlBitSet<> from_tables;
    ObSEArray<ObRawExpr*, 16> shared_exprs;
    if (OB_FAIL(ObTransformUtils::get_from_tables(*view_stmt, rel_ids))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(from_tables.add_members2(rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(view_stmt->get_column_exprs(columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*view_stmt, columns, from_tables, select_list))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_shared_expr(&stmt, view_stmt, shared_exprs))) {
      LOG_WARN("failed to extract shared expr", K(ret));
    } else if (OB_FAIL(append(select_list, shared_exprs))) {
      LOG_WARN("failed to append shared exprs", K(ret));
    } else if (OB_FAIL(append(select_list, connect_by_related_exprs))) {
      LOG_WARN("failed to append connect by related exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, select_list, view_stmt))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(view_stmt->adjust_subquery_stmt_parent(&stmt, view_stmt))) {
      LOG_WARN("failed to adjust sbuquery stmt parent", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*view_stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    }
  }
  // 6. link upper stmt and view stmt
  TableItem* table_item = NULL;
  ObSEArray<ObRawExpr*, 4> columns;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_subquery_exprs(), view_stmt->get_subquery_exprs()))) {
      LOG_WARN("failed to remove subqueries", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, &stmt, view_stmt, table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *table_item, &stmt, columns))) {
      LOG_WARN("failed to create columns for view", K(ret));
    }
  }
  // 7. formalize
  if (OB_SUCC(ret)) {
    stmt.get_table_items().pop_back();
    if (OB_FAIL(stmt.replace_inner_stmt_expr(select_list, columns))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(stmt.get_table_items().push_back(table_item))) {
      LOG_WARN("failed to push back view table item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else if (OB_FAIL(stmt.formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::create_and_mock_join_view(ObSelectStmt& stmt)
{
  int ret = OB_SUCCESS;
  bool has_for_update = false;
  ObSelectStmt* left_view_stmt = NULL;
  ObSelectStmt* right_view_stmt = NULL;
  ObStmtFactory* stmt_factory = NULL;
  ObRawExprFactory* expr_factory = NULL;
  ObSQLSessionInfo* session_info = NULL;
  ObSEArray<ObRawExpr*, 4> select_list;
  if (OB_ISNULL(ctx_) || OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_) || OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt_factory), K(expr_factory), K(stmt), K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(left_view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt<ObSelectStmt>(right_view_stmt))) {
    LOG_WARN("failed to create stmt", K(ret));
  } else if (OB_FAIL(left_view_stmt->ObDMLStmt::assign(stmt))) {
    LOG_WARN("failed to assign stmt", K(ret));
  } else if (OB_FAIL(left_view_stmt->adjust_statement_id())) {
    LOG_WARN("failed to adjust stmt id", K(ret));
  } else {
    has_for_update = stmt.has_for_update();
    left_view_stmt->set_stmt_type(stmt::T_SELECT);
    // 1. handle table, columns, from
    // dml_stmt: from table, semi table, joined table
    stmt.reset_table_items();
    stmt.reset_CTE_table_items();
    stmt.get_joined_tables().reuse();
    stmt.get_semi_infos().reuse();
    stmt.get_column_items().reuse();
    stmt.clear_from_items();
    stmt.get_part_exprs().reset();
    if (OB_FAIL(stmt.rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash", K(ret));
    }
  }
  // 2. handle where conditions
  if (OB_SUCC(ret)) {
    if (OB_FAIL(left_view_stmt->get_condition_exprs().assign(stmt.get_condition_exprs()))) {
      LOG_WARN("failed to assign conditions", K(ret));
    } else {
      stmt.get_condition_exprs().reset();
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
    left_view_stmt->get_start_with_exprs().reset();
    left_view_stmt->get_connect_by_exprs().reset();
    left_view_stmt->set_hierarchical_query(false);
    left_view_stmt->set_has_prior(false);
    left_view_stmt->set_order_siblings(false);
  }
  // 4. finish creating the left child stmts
  if (OB_SUCC(ret)) {
    // create select list
    ObSEArray<ObRawExpr*, 4> columns;
    ObSEArray<ObQueryRefRawExpr*, 4> query_refs;
    ObRelIds rel_ids;
    ObSqlBitSet<> from_tables;
    ObSEArray<ObRawExpr*, 16> shared_exprs;
    if (OB_FAIL(ObTransformUtils::get_from_tables(*left_view_stmt, rel_ids))) {
      LOG_WARN("failed to get from tables", K(ret));
    } else if (OB_FAIL(from_tables.add_members2(rel_ids))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(left_view_stmt->get_column_exprs(columns))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*left_view_stmt, columns, from_tables, select_list))) {
      LOG_WARN("failed to extract table exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_shared_expr(&stmt, left_view_stmt, shared_exprs))) {
      LOG_WARN("failed to extract shared expr", K(ret));
    } else if (OB_FAIL(append(select_list, shared_exprs))) {
      LOG_WARN("failed to append shared exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, select_list, left_view_stmt))) {
      LOG_WARN("failed to create select items", K(ret));
    } else if (OB_FAIL(left_view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    }
  }
  // 5. copy left stmt to right stmt
  if (OB_SUCC(ret)) {
    if (OB_FAIL(right_view_stmt->deep_copy(*stmt_factory, *expr_factory, *left_view_stmt))) {
      LOG_WARN("failed to copy stmt", K(ret));
    } else if (OB_FAIL(right_view_stmt->update_stmt_table_id(*left_view_stmt))) {
      LOG_WARN("failed to update stmt table id", K(ret));
    }
  }
  // 6. link upper stmt and view stmt
  TableItem* left_table_item = NULL;
  ObSEArray<ObRawExpr*, 4> left_columns;
  TableItem* right_table_item = NULL;
  ObSEArray<ObRawExpr*, 4> right_columns;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, &stmt, left_view_stmt, left_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(left_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(left_table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *left_table_item, &stmt, left_columns))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, &stmt, right_view_stmt, right_table_item))) {
      LOG_WARN("failed to add new table item", K(ret));
    } else if (OB_ISNULL(right_table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (OB_FAIL(stmt.add_from_item(right_table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *right_table_item, &stmt, right_columns))) {
      LOG_WARN("failed to create columns for copy stmt", K(ret));
    }
  }
  // 7. preprocess start with exprs
  if (OB_SUCC(ret)) {
    if (OB_FAIL(left_view_stmt->add_condition_exprs(stmt.get_start_with_exprs()))) {
      LOG_WARN("failed to push start with exprs to view", K(ret));
    } else if (OB_FAIL(left_view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(left_view_stmt->adjust_subquery_stmt_parent(&stmt, left_view_stmt))) {
      LOG_WARN("failed to adjust sbuquery stmt parent", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*left_view_stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else if (OB_FAIL(right_view_stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(right_view_stmt->adjust_subquery_stmt_parent(&stmt, right_view_stmt))) {
      LOG_WARN("failed to adjust sbuquery stmt parent", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(*right_view_stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else {
      stmt.get_start_with_exprs().reset();
    }
  }
  // 8. process for update tags
  if (OB_SUCC(ret) && has_for_update) {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_view_stmt->get_table_size(); ++i) {
      TableItem* table = left_view_stmt->get_table_item(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else {
        table->for_update_ = false;
      }
    }
  }
  // 9. do replace and formalize
  if (OB_SUCC(ret)) {
    stmt.get_table_items().pop_back();
    stmt.get_table_items().pop_back();
    if (OB_FAIL(ObOptimizerUtil::remove_item(stmt.get_subquery_exprs(), left_view_stmt->get_subquery_exprs()))) {
      LOG_WARN("failed to remove subqueries", K(ret));
    } else if (OB_FAIL(stmt.replace_inner_stmt_expr(select_list, right_columns))) {
      LOG_WARN("failed to replace inner stmt expr", K(ret));
    } else if (OB_FAIL(stmt.get_table_items().push_back(left_table_item))) {
      LOG_WARN("failed to push back view table item", K(ret));
    } else if (OB_FAIL(stmt.get_table_items().push_back(right_table_item))) {
      LOG_WARN("failed to push back view table item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::adjust_pseudo_column_like_exprs(stmt))) {
      LOG_WARN("failed to adjust pseudo column like exprs", K(ret));
    } else if (OB_FAIL(stmt.formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::classify_join_conds(
    ObSelectStmt& stmt, ObIArray<ObRawExpr*>& normal_join_conds, ObIArray<ObRawExpr*>& other_conds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_condition_size(); ++i) {
    ObRawExpr* cond_expr = NULL;
    bool in_from_item = false;
    if (OB_ISNULL(cond_expr = stmt.get_condition_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("condition expr is null", K(ret));
    } else if (cond_expr->has_flag(CNT_LEVEL) || cond_expr->has_flag(CNT_CONNECT_BY_ISLEAF) ||
               cond_expr->has_flag(CNT_CONNECT_BY_ISCYCLE) || cond_expr->has_flag(CNT_CONNECT_BY_ROOT) ||
               cond_expr->has_flag(CNT_PRIOR) || cond_expr->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(other_conds.push_back(cond_expr))) {
        LOG_WARN("failed to add condition expr into upper stmt", K(ret));
      }
    } else if (OB_FAIL(is_cond_in_one_from_item(stmt, cond_expr, in_from_item))) {
      LOG_WARN("failed to check cond is in from item", K(ret));
    } else if (!in_from_item) {
      if (OB_FAIL(normal_join_conds.push_back(cond_expr))) {
        LOG_WARN("failed to add condition expr into join view stmt", K(ret));
      }
    } else {
      if (OB_FAIL(other_conds.push_back(cond_expr))) {
        LOG_WARN("failed to add condition expr into upper stmt", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::is_cond_in_one_from_item(ObSelectStmt& stmt, ObRawExpr* expr, bool& in_from_item)
{
  int ret = OB_SUCCESS;
  TableItem* table = NULL;
  ObSqlBitSet<> table_ids;
  in_from_item = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->get_relation_ids().is_empty()) {
    in_from_item = true;
  }
  int64_t N = stmt.get_from_item_size();
  for (int64_t i = 0; OB_SUCC(ret) && !in_from_item && i < N; ++i) {
    table_ids.reuse();
    FromItem& item = stmt.get_from_items().at(i);
    if (item.is_joined_) {
      table = stmt.get_joined_table(item.table_id_);
    } else {
      table = stmt.get_table_item_by_id(item.table_id_);
    }
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::get_table_rel_ids(
                   stmt, table->is_joined_table() ? *(static_cast<JoinedTable*>(table)) : *table, table_ids))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    } else if (expr->get_relation_ids().is_subset(table_ids)) {
      in_from_item = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::extract_connect_by_related_exprs(ObSelectStmt& stmt, ObIArray<ObRawExpr*>& special_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> relation_exprs;
  if (OB_FAIL(stmt.get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to relation exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); i++) {
      ObRawExpr* expr = relation_exprs.at(i);
      if (OB_FAIL(extract_connect_by_related_exprs(expr, special_exprs))) {
        LOG_WARN("failed to extract connect by related exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::extract_connect_by_related_exprs(ObRawExpr* expr, ObIArray<ObRawExpr*>& special_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (T_LEVEL == expr->get_expr_type() || T_CONNECT_BY_ISLEAF == expr->get_expr_type() ||
             T_CONNECT_BY_ISCYCLE == expr->get_expr_type() || T_OP_CONNECT_BY_ROOT == expr->get_expr_type() ||
             T_OP_PRIOR == expr->get_expr_type() || T_FUN_SYS_CONNECT_BY_PATH == expr->get_expr_type()) {
    if (ObOptimizerUtil::find_item(special_exprs, expr)) {
      // do nothing
    } else if (OB_FAIL(special_exprs.push_back(expr))) {
      LOG_WARN("failed to push back connect by expr", K(ret));
    }
  } else if (expr->has_flag(CNT_LEVEL) || expr->has_flag(CNT_CONNECT_BY_ISLEAF) ||
             expr->has_flag(CNT_CONNECT_BY_ISCYCLE) || expr->has_flag(CNT_CONNECT_BY_ROOT) ||
             expr->has_flag(CNT_PRIOR) || expr->has_flag(CNT_SYS_CONNECT_BY_PATH)) {
    int64_t N = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_FAIL(extract_connect_by_related_exprs(expr->get_param_expr(i), special_exprs))) {
        LOG_WARN("failed to extract connect by eprs", K(ret));
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObTransformPreProcess::pushdown_start_with_connect_by(ObSelectStmt& stmt, ObSelectStmt& view)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> start_with_exprs;
  ObSEArray<ObRawExpr*, 8> connect_by_exprs;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (OB_FAIL(start_with_exprs.assign(stmt.get_start_with_exprs()))) {
    LOG_WARN("failed to assign start with exprs", K(ret));
  } else if (OB_FALSE_IT(stmt.get_start_with_exprs().reset())) {
  } else if (OB_FAIL(connect_by_exprs.assign(stmt.get_connect_by_exprs()))) {
    LOG_WARN("failed to assign connect by exprs", K(ret));
  } else if (OB_FALSE_IT(stmt.get_connect_by_exprs().reset())) {
  } else if (OB_FAIL(replace_rownum_expr(stmt, start_with_exprs))) {
    LOG_WARN("failed to replace rownum expr", K(ret));
  } else if (OB_FAIL(replace_rownum_expr(stmt, connect_by_exprs))) {
    LOG_WARN("failed to replace rownum expr", K(ret));
  } else if (OB_FAIL(view.get_start_with_exprs().assign(start_with_exprs))) {
    LOG_WARN("failed to assign start with exprs", K(ret));
  } else if (OB_FAIL(view.get_connect_by_exprs().assign(connect_by_exprs))) {
    LOG_WARN("failed to assign connect by exprs", K(ret));
  }
  return ret;
}

int ObTransformPreProcess::replace_rownum_expr(ObSelectStmt& stmt, ObIArray<ObRawExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is null", K(ret));
  } else if (OB_FAIL(check_has_rownum(exprs, has_rownum))) {
    LOG_WARN("failed to check has rownum", K(ret));
  } else if (has_rownum) {
    ObRawExpr* new_rownum_expr = NULL;
    ObRawExpr* old_rownum_expr = NULL;
    ObSEArray<ObRawExpr*, 2> new_exprs;
    ObSEArray<ObRawExpr*, 2> old_exprs;
    if (OB_FAIL(ObRawExprUtils::build_rownum_expr(*ctx_->expr_factory_, new_rownum_expr))) {
      LOG_WARN("failed to build rownum expr", K(ret));
    } else if (OB_ISNULL(new_rownum_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (stmt.get_rownum_expr(old_rownum_expr)) {
      LOG_WARN("failed to get rownum expr", K(ret));
    } else if (OB_ISNULL(old_rownum_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(new_exprs.push_back(new_rownum_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(old_exprs.push_back(old_rownum_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_exprs(old_exprs, new_exprs, exprs))) {
      LOG_WARN("failed to replace exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::check_has_rownum(ObIArray<ObRawExpr*>& exprs, bool& has_rownum)
{
  int ret = OB_SUCCESS;
  has_rownum = false;
  for (int64_t i = 0; OB_SUCC(ret) && !has_rownum && i < exprs.count(); ++i) {
    ObRawExpr* expr = exprs.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (expr->has_flag(CNT_ROWNUM)) {
      has_rownum = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_materialized_view(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (stmt->is_select_stmt()) {
    ObArray<MVDesc> mv_array;
    if (OB_FAIL(get_all_related_mv_array(stmt, mv_array))) {
      SQL_REWRITE_LOG(WARN, "fail to get_all_related_mv_array", K(ret));
    } else if (OB_FAIL(inner_transform(stmt, mv_array, trans_happened))) {
      SQL_REWRITE_LOG(WARN, "fail to do transformer", K(ret));
    }
  }
  return ret;
}

uint64_t ObTransformPreProcess::get_real_tid(uint64_t tid, ObSelectStmt& stmt)
{
  uint64_t real_tid = OB_INVALID_ID;
  for (int64_t i = 0; i < stmt.get_table_size(); i++) {
    TableItem* table_item = stmt.get_table_item(i);
    if (tid == table_item->table_id_) {
      real_tid = table_item->ref_id_;
      break;
    }
  }
  return real_tid;
}

int ObTransformPreProcess::is_col_in_mv(uint64_t tid, uint64_t cid, MVDesc mv_desc, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;

  const ObTableSchema* table_schema = NULL;
  if (OB_FAIL(ctx_->schema_checker_->get_table_schema(mv_desc.mv_tid_, table_schema))) {
    LOG_WARN("fail to get table schema", K(mv_desc.mv_tid_));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table schema", K(mv_desc.mv_tid_));
  } else {
    for (int64_t i = 0; i < table_schema->get_column_count(); i++) {
      const ObColumnSchemaV2* column_schema = table_schema->get_column_schema_by_idx(i);
      uint64_t tmp_cid = cid;
      if (tid != mv_desc.base_tid_) {
        tmp_cid = ObTableSchema::gen_materialized_view_column_id(tmp_cid);
      }
      if (tmp_cid == column_schema->get_column_id()) {
        result = true;
        break;
      }
    }
  }

  return ret;
}

int ObTransformPreProcess::is_all_column_covered(
    ObSelectStmt& stmt, MVDesc mv_desc, const JoinTableIdPair& idp, bool& result)
{
  int ret = OB_SUCCESS;
  result = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_column_size(); i++) {
    const ColumnItem* ci = stmt.get_column_item(i);
    if (ci->table_id_ == idp.first || ci->table_id_ == idp.second) {
      bool flag = false;
      uint64_t real_tid = get_real_tid(ci->table_id_, stmt);
      if (OB_FAIL(is_col_in_mv(real_tid, ci->column_id_, mv_desc, flag))) {
        SQL_REWRITE_LOG(WARN, "fail to do is_col_in_mv", K(ret));
      } else if (!flag) {
        result = false;
        break;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::expr_equal(
    ObSelectStmt* select_stmt, const ObRawExpr* r1, ObSelectStmt* mv_stmt, const ObRawExpr* r2, bool& result)
{
  int ret = OB_SUCCESS;
  result = false;
  if (r1->get_expr_class() != r2->get_expr_class()) {
    result = false;
  } else {
    switch (r1->get_expr_class()) {
      case ObRawExpr::EXPR_OPERATOR:
        if (r1->get_expr_type() != T_OP_EQ || r2->get_expr_type() != T_OP_EQ) {
          result = false;
        } else {
          if (r1->get_param_count() != 2 || r2->get_param_count() != 2) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            if (OB_FAIL(expr_equal(select_stmt, r1->get_param_expr(0), mv_stmt, r2->get_param_expr(0), result))) {
              SQL_REWRITE_LOG(WARN, "fail to do expr equal", K(ret));
            } else if (result) {
              if (OB_FAIL(expr_equal(select_stmt, r1->get_param_expr(1), mv_stmt, r2->get_param_expr(1), result))) {
                SQL_REWRITE_LOG(WARN, "fail to do expr equal", K(ret));
              }
            }

            if (OB_SUCC(ret) && !result) {
              if (OB_FAIL(expr_equal(select_stmt, r1->get_param_expr(0), mv_stmt, r2->get_param_expr(1), result))) {
                SQL_REWRITE_LOG(WARN, "fail to do expr equal", K(ret));
              } else if (result) {
                if (OB_FAIL(expr_equal(select_stmt, r1->get_param_expr(1), mv_stmt, r2->get_param_expr(0), result))) {
                  SQL_REWRITE_LOG(WARN, "fail to do expr equal", K(ret));
                }
              }
            }
          }
        }
        break;
      case ObRawExpr::EXPR_COLUMN_REF: {
        const ObColumnRefRawExpr* col_expr1 = static_cast<const ObColumnRefRawExpr*>(r1);
        const ObColumnRefRawExpr* col_expr2 = static_cast<const ObColumnRefRawExpr*>(r2);
        result = true;
        uint64_t tid1 = get_real_tid(col_expr1->get_table_id(), *select_stmt);
        uint64_t tid2 = get_real_tid(col_expr2->get_table_id(), *mv_stmt);
        if (tid1 != tid2) {
          result = false;
        } else {
          uint64_t cid1 = col_expr1->get_column_id();
          uint64_t cid2 = col_expr2->get_column_id();
          if (cid1 != cid2) {
            result = false;
          }
        }
        break;
      }
      default:
        result = false;
        break;
    }
  }
  return ret;
}

int ObTransformPreProcess::check_where_condition_covered(
    ObSelectStmt* select_stmt, ObSelectStmt* mv_stmt, bool& result, ObBitSet<>& expr_idx)
{
  int ret = OB_SUCCESS;
  result = true;
  if (OB_FAIL(expr_idx.reserve(select_stmt->get_condition_size()))) {
    SQL_REWRITE_LOG(WARN, "fail to reserve space for bit set", K(ret), K(select_stmt->get_condition_size()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mv_stmt->get_condition_size(); i++) {
    const ObRawExpr* expr = mv_stmt->get_condition_expr(i);
    bool flag = false;
    for (int64_t j = 0; OB_SUCC(ret) && j < select_stmt->get_condition_size(); j++) {
      if (!expr_idx.has_member(j)) {
        const ObRawExpr* select_expr = select_stmt->get_condition_expr(j);
        bool equal = false;
        if (OB_FAIL(expr_equal(select_stmt, select_expr, mv_stmt, expr, equal))) {
          SQL_REWRITE_LOG(WARN, "", K(ret));
        } else if (equal) {
          if (OB_FAIL(expr_idx.add_member(j))) {
            SQL_REWRITE_LOG(WARN, "fail to add member", K(ret));
          } else {
            flag = true;
            break;
          }
        }
      }
    }
    if (!flag) {
      result = false;
      break;
    }
  }
  return ret;
}

int ObTransformPreProcess::get_column_id(uint64_t mv_tid, uint64_t tid, uint64_t cid, uint64_t& mv_cid)
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  if (OB_FAIL(ctx_->schema_checker_->get_table_schema(mv_tid, table_schema))) {
    LOG_WARN("fail to get table schema", K(mv_tid), K(tid), K(cid), K(ret));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get table schema", K(mv_tid), K(tid), K(cid), K(ret));
  } else {
    if (tid == table_schema->get_data_table_id()) {
      mv_cid = cid;
    } else {
      mv_cid = table_schema->gen_materialized_view_column_id(cid);
    }
  }
  return ret;
}

int ObTransformPreProcess::rewrite_column_id(ObSelectStmt* select_stmt, MVDesc mv_desc, const JoinTableIdPair& idp)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_column_size(); i++) {
    ColumnItem* ci = select_stmt->get_column_item(i);
    if (ci->table_id_ == idp.first || ci->table_id_ == idp.second) {
      uint64_t mv_cid = 0;
      if (OB_FAIL(get_column_id(mv_desc.mv_tid_, get_real_tid(ci->table_id_, *select_stmt), ci->column_id_, mv_cid))) {
        LOG_WARN("fail to get column id", K(ret));
      } else {
        ci->expr_->set_ref_id(mv_desc.mv_tid_, mv_cid);
        ci->expr_->set_table_name(mv_desc.table_name_);
        ci->table_id_ = mv_desc.mv_tid_;
        ci->column_id_ = mv_cid;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::rewrite_column_id2(ObSelectStmt* select_stmt)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_column_size(); i++) {
    ColumnItem* ci = select_stmt->get_column_item(i);
    ci->expr_->get_relation_ids().reuse();
    if (OB_FAIL(ci->expr_->add_relation_id(select_stmt->get_table_bit_index(ci->expr_->get_table_id())))) {
      LOG_WARN("add relation id to expr failed", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::get_join_tid_cid(
    uint64_t tid, uint64_t cid, uint64_t& out_tid, uint64_t& out_cid, const ObTableSchema& mv_schema)
{
  int ret = OB_SUCCESS;
  out_tid = OB_INVALID_ID;
  out_cid = OB_INVALID_ID;
  const common::ObIArray<std::pair<uint64_t, uint64_t>>& join_conds = mv_schema.get_join_conds();
  std::pair<uint64_t, uint64_t> j1;
  std::pair<uint64_t, uint64_t> j2;
  for (int64_t i = 0; OB_SUCC(ret) && i < join_conds.count(); i += 2) {
    j1 = join_conds.at(i);
    j2 = join_conds.at(i + 1);
    if (j1.first == tid && j1.second == cid) {
      out_tid = j2.first;
      out_cid = j2.second;
    } else if (j2.first == tid && j2.second == cid) {
      out_tid = j1.first;
      out_cid = j1.second;
    }
  }
  return ret;
}

int ObTransformPreProcess::is_rowkey_equal_covered(
    ObSelectStmt* select_stmt, uint64_t mv_tid, uint64_t rowkey_tid, uint64_t base_or_dep_tid, bool& is_covered)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> rowkey_covered_set;
  is_covered = false;

  const ObTableSchema* mv_schema = NULL;
  const ObTableSchema* table_schema = NULL;
  const ObRowkeyInfo* rowkey_info = NULL;
  if (OB_FAIL(ctx_->schema_checker_->get_table_schema(rowkey_tid, table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(rowkey_tid));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema is null", K(rowkey_tid));
  } else {
    rowkey_info = &(table_schema->get_rowkey_info());
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ctx_->schema_checker_->get_table_schema(mv_tid, mv_schema))) {
      LOG_WARN("fail to get table schema", K(ret), K(mv_tid));
    } else if (NULL == mv_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema is null", K(mv_tid));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_condition_size(); i++) {
    const ObRawExpr* expr = select_stmt->get_condition_expr(i);
    ObArray<ObRawExpr*> column_exprs;
    if (expr->has_flag(IS_SIMPLE_COND)) {
      const ObRawExpr* tmp_expr = NULL;
      const ObColumnRefRawExpr* col_expr = NULL;
      uint64_t tid = OB_INVALID_ID;
      uint64_t cid = OB_INVALID_ID;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
        LOG_WARN("fail to extract_column_exprs", K(ret));
      } else if (column_exprs.count() != 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("When expr is simple cond, column_exprs count must be one");
      } else {
        tmp_expr = column_exprs.at(0);
      }

      if (OB_SUCC(ret)) {
        if (!tmp_expr->is_column_ref_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_expr must be column ref");
        } else {
          col_expr = static_cast<const ObColumnRefRawExpr*>(tmp_expr);
        }
      }

      if (OB_SUCC(ret)) {
        cid = col_expr->get_column_id();
        tid = get_real_tid(col_expr->get_table_id(), *select_stmt);
        if (rowkey_tid == tid) {
          int64_t idx = 0;
          if (OB_SUCCESS == (rowkey_info->get_index(cid, idx))) {
            if (OB_FAIL(rowkey_covered_set.add_member(idx))) {
              LOG_WARN("fail to add bit set", K(ret), K(idx));
            } else if (rowkey_covered_set.num_members() == rowkey_info->get_size()) {
              is_covered = true;
              break;
            }
          }
        } else if (tid == base_or_dep_tid) {
          uint64_t out_tid = OB_INVALID_ID;
          uint64_t out_cid = OB_INVALID_ID;
          if (OB_FAIL(get_join_tid_cid(tid, cid, out_tid, out_cid, *mv_schema))) {
            LOG_WARN("fail to get join tid cid", K(ret));
          } else if (out_tid != OB_INVALID_ID) {
            int64_t idx = 0;
            if (OB_SUCCESS == (rowkey_info->get_index(out_cid, idx))) {
              if (OB_FAIL(rowkey_covered_set.add_member(idx))) {
                LOG_WARN("fail to add bit set", K(ret), K(idx));
              } else if (rowkey_covered_set.num_members() == rowkey_info->get_size()) {
                is_covered = true;
                break;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::check_can_transform(
    ObSelectStmt*& select_stmt, MVDesc mv_desc, bool& result, JoinTableIdPair& idp, ObBitSet<>& expr_idx)
{
  int ret = OB_SUCCESS;
  result = false;
  bool match = true;
  ObSelectStmt* mv = mv_desc.mv_stmt_;
  idp.first = OB_INVALID_ID;
  idp.second = OB_INVALID_ID;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_from_item_size(); i++) {
    const FromItem& from_item = select_stmt->get_from_item(i);
    if (from_item.is_joined_) {
      continue;
    }
    TableItem* ti = select_stmt->get_table_item_by_id(from_item.table_id_);
    if (!ti->is_basic_table()) {
      continue;
    }

    if (ti->ref_id_ == mv_desc.base_tid_ && idp.first == OB_INVALID_ID) {
      idp.first = ti->table_id_;
    } else if (ti->ref_id_ == mv_desc.dep_tid_ && idp.second == OB_INVALID_ID) {
      idp.second = ti->table_id_;
    }
    if (idp.first != OB_INVALID_ID && idp.second != OB_INVALID_ID) {
      match = true;
      break;
    }
  }
  if (OB_SUCC(ret) && match) {
    const ObIArray<ObOrgIndexHint>& org_indexes = select_stmt->get_stmt_hint().org_indexes_;
    if (org_indexes.count() != 0) {
      match = false;
    }
  }
  if (OB_SUCC(ret) && match) {
    if (OB_FAIL(is_all_column_covered(*select_stmt, mv_desc, idp, result))) {
      SQL_REWRITE_LOG(WARN, "fail to check is_all_column_covered", K(ret));
    }
  }

  if (OB_SUCC(ret) && result) {
    if (OB_FAIL(check_where_condition_covered(select_stmt, mv, result, expr_idx))) {
      SQL_REWRITE_LOG(WARN, "fail to check where condition cover", K(ret));
    }
  }

  if (OB_SUCC(ret) && result) {
    bool is_covered = false;
    if (OB_FAIL(
            is_rowkey_equal_covered(select_stmt, mv_desc.mv_tid_, mv_desc.base_tid_, mv_desc.dep_tid_, is_covered))) {
      LOG_WARN("fail to check is_rowkey_equal_covered", K(ret), K(mv_desc.base_tid_));
    } else if (!is_covered) {
      if (OB_FAIL(
              is_rowkey_equal_covered(select_stmt, mv_desc.mv_tid_, mv_desc.dep_tid_, mv_desc.base_tid_, is_covered))) {
        LOG_WARN("fail to check is_rowkey_equal_covered", K(ret), K(mv_desc.dep_tid_));
      } else if (is_covered) {
        result = false;
      }
    }
  }

  return ret;
}

int ObTransformPreProcess::reset_part_expr(ObSelectStmt* select_stmt, uint64_t mv_tid, const JoinTableIdPair& idp)
{
  int ret = OB_SUCCESS;
  common::ObIArray<ObDMLStmt::PartExprItem>& part_exprs = select_stmt->get_part_exprs();
  for (int64_t i = 0; i < part_exprs.count(); i++) {
    if (part_exprs.at(i).table_id_ == idp.first || part_exprs.at(i).table_id_ == idp.second) {
      part_exprs.at(i).table_id_ = mv_tid;
    }
  }
  return ret;
}

int ObTransformPreProcess::reset_table_item(
    ObSelectStmt* select_stmt, uint64_t mv_tid, uint64_t base_tid, const JoinTableIdPair& idp)
{
  int ret = OB_SUCCESS;
  ObArray<TableItem*> table_item_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_table_size(); i++) {
    TableItem* table_item = select_stmt->get_table_item(i);
    if (table_item->table_id_ != idp.first && table_item->table_id_ != idp.second) {
      if (OB_FAIL(table_item_array.push_back(table_item))) {
        SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
      }
    }
  }
  TableItem* new_table_item = NULL;
  if (OB_SUCC(ret)) {
    new_table_item = select_stmt->create_table_item(*(ctx_->allocator_));
    if (NULL == new_table_item) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_REWRITE_LOG(WARN, "fail to create table item");
    } else {
      new_table_item->is_index_table_ = true;
      new_table_item->is_materialized_view_ = true;
      new_table_item->table_id_ = mv_tid;
      new_table_item->ref_id_ = base_tid;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_item_array.push_back(new_table_item))) {
      SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_stmt->reset_table_item(table_item_array))) {
      SQL_REWRITE_LOG(WARN, "fail to reset_table_item", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    select_stmt->get_table_hash().reset();
    if (OB_FAIL(select_stmt->set_table_bit_index(OB_INVALID_ID))) {
      LOG_WARN("fail to set_table_bit_index", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_item_array.count(); i++) {
      if (OB_FAIL(select_stmt->set_table_bit_index(table_item_array.at(i)->table_id_))) {
        LOG_WARN("fail to set_table_bit_index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::reset_where_condition(ObSelectStmt* select_stmt, ObBitSet<>& expr_idx)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr*> new_conditions;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_condition_size(); i++) {
    ObRawExpr* expr = select_stmt->get_condition_expr(i);
    if (!expr_idx.has_member(i)) {
      if (OB_FAIL(new_conditions.push_back(expr))) {
        SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    select_stmt->get_condition_exprs().reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < new_conditions.count(); i++) {
      if (OB_FAIL(select_stmt->get_condition_exprs().push_back(new_conditions.at(i)))) {
        SQL_REWRITE_LOG(WARN, "fail to push back item", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::reset_from_item(ObSelectStmt* select_stmt, uint64_t mv_tid, const JoinTableIdPair& idp)
{
  int ret = OB_SUCCESS;
  ObArray<FromItem> new_from_item_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_from_item_size(); i++) {
    bool flag = true;
    const FromItem& from_item = select_stmt->get_from_item(i);
    if (!from_item.is_joined_) {
      TableItem* ti = select_stmt->get_table_item_by_id(from_item.table_id_);
      if (ti->is_basic_table()) {
        if (ti->table_id_ == idp.first || ti->table_id_ == idp.second) {
          flag = false;
        }
      }
    }

    if (flag) {
      if (OB_FAIL(new_from_item_array.push_back(from_item))) {
        SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    FromItem mv_from_item;
    mv_from_item.table_id_ = mv_tid;
    mv_from_item.is_joined_ = false;
    if (OB_FAIL(new_from_item_array.push_back(mv_from_item))) {
      SQL_REWRITE_LOG(WARN, "fail to push item to array", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(select_stmt->reset_from_item(new_from_item_array))) {
      SQL_REWRITE_LOG(WARN, "fail to reset_from_item", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::inner_transform(ObDMLStmt*& stmt, ObIArray<MVDesc>& mv_array, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool can_transform = false;
  ObRowDesc table_idx;
  ObBitSet<> expr_idx;
  trans_happened = false;
  if (stmt::T_SELECT == stmt->get_stmt_type()) {
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < mv_array.count(); i++) {
      MVDesc& mv_desc = mv_array.at(i);
      JoinTableIdPair idp;
      if (OB_FAIL(check_can_transform(select_stmt, mv_desc, can_transform, idp, expr_idx))) {
        SQL_REWRITE_LOG(WARN, "fail to check_can_transform", K(ret));
      } else if (can_transform) {
        if (OB_ISNULL(stmt->get_query_ctx())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("query ctx is NULL", K(ret));
        } else if (FALSE_IT(stmt->get_query_ctx()->is_contain_mv_ = true)) {
        } else if (OB_FAIL(reset_where_condition(select_stmt, expr_idx))) {
          SQL_REWRITE_LOG(WARN, "fail to reset_where_condition", K(ret));
        } else if (OB_FAIL(rewrite_column_id(select_stmt, mv_desc, idp))) {
          SQL_REWRITE_LOG(WARN, "fail to adjust_expr_column", K(ret));
        } else if (OB_FAIL(reset_from_item(select_stmt, mv_desc.mv_tid_, idp))) {
          SQL_REWRITE_LOG(WARN, "fail to reset_from_item", K(ret));
        } else if (OB_FAIL(reset_table_item(select_stmt, mv_desc.mv_tid_, mv_desc.base_tid_, idp))) {
          SQL_REWRITE_LOG(WARN, "fail to reset_table_item", K(ret));
        } else if (OB_FAIL(reset_part_expr(select_stmt, mv_desc.mv_tid_, idp))) {
          SQL_REWRITE_LOG(WARN, "fail to reset_part_expr", K(ret));
        } else if (OB_FAIL(rewrite_column_id2(select_stmt))) {
          SQL_REWRITE_LOG(WARN, "fail to adjust_expr_column", K(ret));
        } else if (OB_FAIL(select_stmt->pull_all_expr_relation_id_and_levels())) {
          LOG_WARN("pull select stmt relation ids failed", K(ret));
        } else {
          trans_happened = true;
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::get_view_stmt(const share::schema::ObTableSchema& index_schema, ObSelectStmt*& view_stmt)
{
  int ret = OB_SUCCESS;
  ObResolverParams resolver_ctx;
  resolver_ctx.allocator_ = ctx_->allocator_;
  resolver_ctx.schema_checker_ = ctx_->schema_checker_;
  resolver_ctx.session_info_ = ctx_->session_info_;
  resolver_ctx.stmt_factory_ = ctx_->stmt_factory_;
  resolver_ctx.expr_factory_ = ctx_->expr_factory_;
  resolver_ctx.query_ctx_ = ctx_->stmt_factory_->get_query_ctx();

  ObParser parser(*(ctx_->allocator_), DEFAULT_MYSQL_MODE, ctx_->session_info_->get_local_collation_connection());
  ObSelectResolver view_resolver(resolver_ctx);
  ParseResult view_result;
  ObString view_def = index_schema.get_view_schema().get_view_definition();
  if (OB_FAIL(ObSQLUtils::generate_view_definition_for_resolve(*(ctx_->allocator_),
          ctx_->session_info_->get_local_collation_connection(),
          index_schema.get_view_schema(),
          view_def))) {
    LOG_WARN("fail to generate view definition for resolve", K(ret));
  } else if (OB_FAIL(parser.parse(view_def, view_result))) {
    LOG_WARN("parse view defination failed", K(view_def), K(ret));
  } else {
    ParseNode* view_stmt_node = view_result.result_tree_->children_[0];
    if (!view_stmt_node) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected children for view result after parse", K(ret));
    } else if (OB_FAIL(view_resolver.resolve(*view_stmt_node))) {
      LOG_WARN("expand view table failed", K(ret));
    } else {
      view_stmt = view_resolver.get_select_stmt();
    }
  }
  return ret;
}

int ObTransformPreProcess::get_all_related_mv_array(ObDMLStmt*& stmt, ObIArray<MVDesc>& mv_array)
{
  int ret = OB_SUCCESS;
  uint64_t idx_tids[OB_MAX_INDEX_PER_TABLE];
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); i++) {
    TableItem* table_item = stmt->get_table_item(i);
    if (!table_item->is_index_table_ && table_item->is_basic_table()) {
      uint64_t table_id = table_item->ref_id_;
      int64_t idx_count = OB_MAX_INDEX_PER_TABLE;
      if (OB_FAIL(ctx_->schema_checker_->get_can_read_index_array(table_id, idx_tids, idx_count, true))) {
        SQL_REWRITE_LOG(WARN, "fail to get_can_read_index_array", K(table_id), K(ret));
      }
      MVDesc mv_desc;
      for (int j = 0; OB_SUCC(ret) && j < idx_count; j++) {
        uint64_t index_tid = idx_tids[j];
        const ObTableSchema* index_schema = NULL;
        if (OB_FAIL(ctx_->schema_checker_->get_table_schema(index_tid, index_schema))) {
          SQL_REWRITE_LOG(WARN, "fail to get table schema", K(table_id), K(index_tid), K(j), K(idx_count), K(ret));
        } else if (share::schema::MATERIALIZED_VIEW == index_schema->get_table_type()) {
          ObSelectStmt* view_stmt = NULL;
          mv_desc.mv_tid_ = index_tid;
          mv_desc.base_tid_ = table_id;
          mv_desc.dep_tid_ = index_schema->get_depend_table_ids().at(0);
          mv_desc.table_name_ = index_schema->get_table_name_str();
          if (OB_FAIL(get_view_stmt(*index_schema, view_stmt))) {
            SQL_REWRITE_LOG(WARN, "fail to get_view_stmt", K(ret));
          } else {
            mv_desc.mv_stmt_ = view_stmt;
            if (OB_FAIL(mv_array.push_back(mv_desc))) {
              SQL_REWRITE_LOG(WARN, "fail to push back view_stmt", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::eliminate_having(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL pointer", K(stmt), K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else {
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);
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

int ObTransformPreProcess::replace_func_is_serving_tenant(ObDMLStmt*& stmt, bool& trans_happened)
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
    common::ObIArray<ObRawExpr*>& cond_exprs = stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < cond_exprs.count(); ++i) {
      if (OB_ISNULL(cond_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cond expr is NULL", K(ret), K(i), K(cond_exprs));
      } else if (OB_FAIL(recursive_replace_func_is_serving_tenant(*stmt, cond_exprs.at(i), trans_happened))) {
        LOG_WARN("fail to recursive replace functino is_serving_tenant", K(ret), K(i), K(*cond_exprs.at(i)));
      } else if (OB_ISNULL(cond_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null pointer", K(ret));
      } else if (OB_FAIL(cond_exprs.at(i)->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret), K(i), K(*cond_exprs.at(i)));
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->pull_all_expr_relation_id_and_levels())) {
        LOG_WARN("pull stmt all expr relation ids failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::recursive_replace_func_is_serving_tenant(
    ObDMLStmt& stmt, ObRawExpr*& cond_expr, bool& trans_happened)
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
      if (OB_FAIL(SMART_CALL(
              recursive_replace_func_is_serving_tenant(stmt, cond_expr->get_param_expr(i), trans_happened)))) {
        LOG_WARN("fail to recursive replace_func_is_serving_tenant", K(ret));
      }
    }
    if (OB_SUCC(ret) && T_FUN_IS_SERVING_TENANT == cond_expr->get_expr_type()) {
      int64_t tenant_id_int64 = -1;
      if (OB_UNLIKELY(3 != cond_expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("T_FUN_IS_SERVING_TENANT must has 3 params", K(ret), K(cond_expr->get_param_count()), K(*cond_expr));
      } else if (OB_ISNULL(cond_expr->get_param_expr(0)) || OB_ISNULL(cond_expr->get_param_expr(1)) ||
                 OB_ISNULL(cond_expr->get_param_expr(2))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("T_FUN_IS_SERVING_TENANT has a null param",
            K(ret),
            K(cond_expr->get_param_expr(0)),
            K(cond_expr->get_param_expr(1)),
            K(cond_expr->get_param_expr(2)),
            K(*cond_expr));
      } else if (!cond_expr->get_param_expr(2)->has_flag(IS_CONST) &&
                 !cond_expr->get_param_expr(2)->has_flag(IS_CONST_EXPR) &&
                 !cond_expr->get_param_expr(2)->has_flag(IS_CALCULABLE_EXPR)) {
      } else if (OB_ISNULL(ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx_ is NULL", K(ret));
      } else if (OB_ISNULL(ctx_->exec_ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->allocator_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), K(ctx_->exec_ctx_), K(ctx_->session_info_), K(ctx_->allocator_));
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
          ObConstRawExpr* true_expr = NULL;
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
            ObConstRawExpr* false_expr = NULL;
            if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_VARCHAR, false_expr))) {
              LOG_WARN("create varchar expr failed", K(ret));
            } else if (OB_ISNULL(false_expr)) {
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
            ObOpRawExpr* in_op = NULL;
            ObOpRawExpr* left_row_op = NULL;
            ObOpRawExpr* right_row_op = NULL;
            if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_IN, in_op))) {
              LOG_WARN("create in operator expr", K(ret));
            } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ROW, left_row_op))) {
              LOG_WARN("create left row operator failed", K(ret));
            } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_ROW, right_row_op))) {
              LOG_WARN("create right row op failed", K(ret));
            } else if (OB_ISNULL(in_op) || OB_ISNULL(left_row_op) || OB_ISNULL(right_row_op)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("operator is null", K(in_op), K(left_row_op), K(right_row_op));
            } else { /*do nothing*/
            }

            for (int64_t i = 0; OB_SUCC(ret) && i < servers.count(); ++i) {
              ObAddr server = servers.at(i);
              ObOpRawExpr* row_op = NULL;
              ObConstRawExpr* ip_expr = NULL;
              ObConstRawExpr* port_expr = NULL;
              char* ip_buf = NULL;
              ObObj ip_obj;
              ObObj port_obj;
              if (OB_UNLIKELY(
                      NULL == (ip_buf = static_cast<char*>(ctx_->allocator_->alloc(OB_MAX_SERVER_ADDR_SIZE))))) {
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
                } else { /*do nothing*/
                }
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

int ObTransformPreProcess::calc_const_raw_expr_and_get_int(const ObStmt& stmt, ObRawExpr* const_expr,
    ObExecContext& exec_ctx, ObSQLSessionInfo* session, ObIAllocator& allocator, int64_t& result)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* sql_proxy = NULL;
  ObPhysicalPlanCtx* plan_ctx = NULL;
  if (OB_ISNULL(const_expr) || OB_ISNULL(session)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr or session is NULL", KP(session), KP(const_expr), K(ret));
  } else if (OB_UNLIKELY(!const_expr->has_flag(IS_CONST) && !const_expr->has_flag(IS_CONST_EXPR) &&
                         !const_expr->has_flag(IS_CALCULABLE_EXPR))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is not const expr", K(ret), K(*const_expr));
  } else if (OB_ISNULL(sql_proxy = exec_ctx.get_sql_proxy())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is NULL", K(ret));
  } else if (OB_ISNULL(plan_ctx = exec_ctx.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical plan ctx is NULL", K(ret));
  } else {
    ObPhysicalPlanCtx phy_plan_ctx(exec_ctx.get_allocator());
    ParamStore& local_param_store = phy_plan_ctx.get_param_store_for_update();
    const ParamStore& param_store = plan_ctx->get_param_store();
    for (int64_t i = 0; OB_SUCC(ret) && i < param_store.count(); ++i) {
      if (OB_FAIL(local_param_store.push_back(param_store.at(i)))) {
        LOG_WARN("fail to push back param to local param store",
            K(ret),
            K(i),
            K(param_store.at(i)),
            K(local_param_store),
            K(param_store));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      ObObj result_int_obj;
      ObPhysicalPlan phy_plan;
      SMART_VAR(ObExecContext, local_exec_ctx)
      {
        ObExprCtx expr_ctx;
        phy_plan_ctx.set_phy_plan(&phy_plan);
        const int64_t cur_time =
            plan_ctx->has_cur_time() ? plan_ctx->get_cur_time().get_timestamp() : ObTimeUtility::current_time();
        phy_plan_ctx.set_cur_time(cur_time, *session);

        expr_ctx.phy_plan_ctx_ = &phy_plan_ctx;
        expr_ctx.my_session_ = session;
        expr_ctx.exec_ctx_ = &local_exec_ctx;
        expr_ctx.exec_ctx_->set_sql_proxy(sql_proxy);
        expr_ctx.calc_buf_ = &allocator;
        if (OB_FAIL(calc_const_raw_expr(stmt, const_expr, expr_ctx, phy_plan, allocator, result_int_obj))) {
          LOG_WARN("fail to calc const raw expr", K(ret));
        } else if (OB_UNLIKELY(false == ob_is_integer_type(result_int_obj.get_type()))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("tenant id result must integer", K(ret), K(result_int_obj));
        } else {
          EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
          EXPR_GET_INT64_V2(result_int_obj, result);
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::calc_const_raw_expr(const ObStmt& stmt, ObRawExpr* const_expr, ObExprCtx& expr_ctx,
    ObPhysicalPlan& phy_plan, ObIAllocator& allocator, ObObj& result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(const_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL", K(ret));
  } else if (OB_UNLIKELY(!const_expr->has_flag(IS_CONST) && !const_expr->has_flag(IS_CONST_EXPR) &&
                         !const_expr->has_flag(IS_CALCULABLE_EXPR))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is not const or calculable expr", K(ret), K(*const_expr));
  } else {
    ObRawExpr* real_expr = const_expr;
    if (T_QUESTIONMARK == const_expr->get_expr_type()) {
      ObPhysicalPlanCtx* plan_ctx = NULL;
      ObObj unknown_obj;
      int64_t hidden_idx = -1;
      if (OB_ISNULL(ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx_ is NULL", K(ret));
      } else if (OB_ISNULL(ctx_->exec_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ctx_->exec_ctx_ is NULL", K(ret));
      } else if (OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plan ctx is NULL", K(ret));
      } else if (OB_UNLIKELY(ObRawExpr::EXPR_CONST != const_expr->get_expr_class())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr type is T_QUESTIONMARK but expr class is not EXPR_CONST", K(ret));
      } else if (FALSE_IT(unknown_obj = (static_cast<ObConstRawExpr*>(const_expr))->get_value())) {
      } else if (OB_FAIL(unknown_obj.get_unknown(hidden_idx))) {
        LOG_WARN("fail to get value from const raw expr", K(ret), K(unknown_obj));
      } else {
        const ParamStore& params = plan_ctx->get_param_store();
        const ObIArray<ObHiddenColumnItem>& calculable_exprs = stmt.get_calculable_exprs();
        if (hidden_idx >= params.count()) {
          int64_t c_expr_arr_idx = hidden_idx - params.count();
          if (OB_UNLIKELY(c_expr_arr_idx >= calculable_exprs.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN(
                "idx is >= count", K(c_expr_arr_idx), K(calculable_exprs.count()), K(hidden_idx), K(params.count()));
          } else {
            ObHiddenColumnItem hidden_col = calculable_exprs.at(c_expr_arr_idx);
            real_expr = hidden_col.expr_;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObNewRow tmp_row;
      RowDesc row_desc;
      ObExprGeneratorImpl expr_gen(0, 0, NULL, row_desc);
      ObSqlExpression sql_expr(allocator, 0);
      if (OB_ISNULL(real_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("real expr is NULL", K(ret));
      } else if (OB_FAIL(expr_gen.generate(*real_expr, sql_expr))) {
        LOG_WARN("fail to fill sql expression", K(ret), K(*real_expr));
      } else if (FALSE_IT(phy_plan.set_regexp_op_count(expr_gen.get_cur_regexp_op_count()))) {
        // will not reach here
      } else if (FALSE_IT(phy_plan.set_like_op_count(expr_gen.get_cur_like_op_count()))) {
        // will not reach here
      } else if (OB_FAIL(sql_expr.calc(expr_ctx, tmp_row, result))) {
        LOG_WARN("fail to calc value", K(ret), K(sql_expr));
      } else { /*do nothing*/
      }
    }
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
int ObTransformPreProcess::transform_for_temporary_table(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt or ctx", K(ret), K(stmt), K(ctx_));
  } else if (OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is NULL", K(ret));
  } else if (ObSQLSessionInfo::USER_SESSION == ctx_->session_info_->get_session_type()) {
    common::ObArray<TableItem*> table_item_list;
    int64_t num_from_items = stmt->get_from_item_size();
    // 1, collect all table item
    for (int64_t i = 0; OB_SUCC(ret) && i < num_from_items; ++i) {
      const FromItem& from_item = stmt->get_from_item(i);
      if (from_item.is_joined_) {
        JoinedTable* joined_table_item = stmt->get_joined_table(from_item.table_id_);
        if (OB_FAIL(collect_all_tableitem(stmt, joined_table_item, table_item_list))) {
          LOG_WARN("failed to collect table item", K(ret));
        }
      } else {
        TableItem* table_item = NULL;
        table_item = stmt->get_table_item_by_id(from_item.table_id_);
        if (OB_FAIL(collect_all_tableitem(stmt, table_item, table_item_list))) {
          LOG_WARN("failed to collect table item", K(ret));
        }
      }
    }
    // 2, for each temporary table item, add session_id = xxx filter
    for (int64 i = 0; OB_SUCC(ret) && i < table_item_list.count(); ++i) {
      TableItem* table_item = table_item_list.at(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null", K(ret));
      } else if (table_item->is_basic_table()) {
        uint64_t table_ref_id = table_item->ref_id_;
        const ObTableSchema* table_schema = NULL;
        if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ctx_ or schema_cheker_ is NULL", K(ret), K(ctx_), K(ctx_->schema_checker_));
        } else if (OB_FAIL(ctx_->schema_checker_->get_table_schema(table_ref_id, table_schema))) {
          LOG_WARN("failed to get table schema", K(table_ref_id), K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table should not be null", K(table_ref_id));
        } else if (table_schema->is_oracle_tmp_table()) {
          TableItem* view_table = NULL;
          ObSelectStmt* ref_query = NULL;
          TableItem* child_table = NULL;
          if (stmt->is_single_table_stmt()) {
            if (OB_FAIL(add_filter_for_temporary_table(*stmt, *table_item))) {
              LOG_WARN("add filter for temporary table failed", K(ret));
            } else {
              trans_happened = true;
            }
          } else if (OB_FAIL(ObTransformUtils::create_view_with_table(stmt, ctx_, table_item, view_table))) {
            LOG_WARN("failed to create view with table", K(ret));
          } else if (!view_table->is_generated_table() || OB_ISNULL(ref_query = view_table->ref_query_) ||
                     !ref_query->is_single_table_stmt() || OB_ISNULL(child_table = ref_query->get_table_item(0))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected view table", K(ret), K(*view_table));
          } else if (OB_FAIL(add_filter_for_temporary_table(*ref_query, *child_table))) {
            LOG_WARN("add filter for temporary table failed", K(ret));
          } else {
            trans_happened = true;
          }
        }
      }
    }
    ObInsertStmt* ins_stmt = dynamic_cast<ObInsertStmt*>(stmt);
    if (OB_SUCC(ret) && NULL != ins_stmt) {
      // value exprs for insert stmt is not included in relation expr array.
      ObIArray<ObRawExpr*>& value_vectors = ins_stmt->get_value_vectors();
      for (int64_t i = 0; OB_SUCC(ret) && i < value_vectors.count(); ++i) {
        ObRawExpr* expr = value_vectors.at(i);
        bool is_happened = false;
        if (OB_FAIL(transform_expr(*ctx_->expr_factory_, *ctx_->session_info_, expr, is_happened))) {
          LOG_WARN("transform expr failed", K(ret));
        } else if (OB_ISNULL(expr)) {
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

int ObTransformPreProcess::add_filter_for_temporary_table(ObDMLStmt& stmt, const TableItem& table_item)
{
  int ret = OB_SUCCESS;
  ObRawExpr* equal_expr = NULL;
  ObConstRawExpr* expr_const = NULL;
  ObColumnRefRawExpr* expr_col = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("some parameter is NULL", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_UINT64, expr_const))) {
    LOG_WARN("create const raw expr failed", K(ret));
  } else if (OB_ISNULL(expr_const)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(expr_col), K(expr_const), K(ret));
  } else {
    ObObj sid_obj;
    ColumnItem* exist_col_item = NULL;
    sid_obj.set_int(ctx_->session_info_->get_sessid_for_table());
    expr_const->set_value(sid_obj);
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
        expr_col->add_relation_id(stmt.get_table_bit_index(table_item.table_id_));
        expr_col->set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        expr_col->set_collation_level(CS_LEVEL_SYSCONST);
        expr_col->set_result_type(result_type);
        expr_col->set_expr_level(stmt.get_current_level());
        column_item.expr_ = expr_col;
        column_item.table_id_ = expr_col->get_table_id();
        column_item.column_id_ = expr_col->get_column_id();
        column_item.column_name_ = expr_col->get_column_name();
        if (OB_FAIL(stmt.add_column_item(column_item))) {
          LOG_WARN("add column item to stmt failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(
                   *(ctx_->expr_factory_), ctx_->session_info_, expr_const, expr_col, equal_expr))) {
      LOG_WARN("Creation of equal expr for outer stmt fails", K(ret));
    }
    if (OB_FAIL(stmt.get_condition_exprs().push_back(equal_expr))) {
      LOG_WARN("failed to push back new filter", K(ret));
    } else {
      LOG_TRACE("add new filter succeed", K(stmt.get_condition_exprs()), K(*equal_expr));
    }
  }
  return ret;
}

int ObTransformPreProcess::collect_all_tableitem(
    ObDMLStmt* stmt, TableItem* table_item, common::ObArray<TableItem*>& table_item_list)
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
      JoinedTable* joined_table_item = static_cast<JoinedTable*>(table_item);
      if (OB_FAIL(SMART_CALL(collect_all_tableitem(stmt, joined_table_item->left_table_, table_item_list)))) {
        LOG_WARN("failed to collect temp table item", K(ret));
      } else if (OB_FAIL(SMART_CALL(collect_all_tableitem(stmt, joined_table_item->right_table_, table_item_list)))) {
        LOG_WARN("failed to collect temp table item", K(ret));
      }
    } else if (table_item->is_basic_table() && OB_FAIL(add_var_to_array_no_dup(table_item_list, table_item))) {
      LOG_WARN("failed to push table item", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::get_first_level_output_exprs(
    ObSelectStmt* sub_stmt, common::ObIArray<ObRawExpr*>& inner_aggr_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else {
    common::ObIArray<ObAggFunRawExpr*>& aggr_exprs = sub_stmt->get_aggr_items();
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
          } else if (aggr_exprs.at(i)->get_param_expr(j)->has_const_or_const_expr_flag()) {
            // do nothing
          } else if (OB_FAIL(add_var_to_array_no_dup(inner_aggr_exprs, aggr_exprs.at(i)->get_param_expr(j)))) {
            LOG_WARN("failed to to add var to array no dup.", K(ret));
          } else { /*do nothing.*/
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::generate_child_level_aggr_stmt(ObSelectStmt* select_stmt, ObSelectStmt*& sub_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> complex_aggr_exprs;
  ObSEArray<SelectItem, 4> select_item_array;
  ObSEArray<ObAggFunRawExpr*, 4> aggr_expr_array;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else if (OB_FAIL(ctx_->stmt_factory_->create_stmt<ObSelectStmt>(sub_stmt))) {
    LOG_WARN("failed to create stmt.", K(ret));
  } else if (FALSE_IT(sub_stmt->set_query_ctx(select_stmt->get_query_ctx()))) {
  } else if (OB_FAIL(sub_stmt->deep_copy(*ctx_->stmt_factory_, *ctx_->expr_factory_, *select_stmt))) {
    LOG_WARN("failed to deep copy from nested stmt.", K(ret));
  } else if (OB_FAIL(get_first_level_output_exprs(sub_stmt, complex_aggr_exprs))) {
    LOG_WARN("failed to extract levels aggr.", K(ret));
  } else {
    sub_stmt->get_aggr_items().reset();
    sub_stmt->get_select_items().reset();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sub_stmt->get_having_exprs().count(); ++i) {
    if (OB_FAIL(ObTransformUtils::extract_aggr_expr(
            sub_stmt->get_current_level(), sub_stmt->get_having_exprs().at(i), sub_stmt->get_aggr_items()))) {
      LOG_WARN("failed to extract aggr exprs.", K(ret));
    } else { /*do nothing.*/
    }
  }
  for (int64_t j = 0; OB_SUCC(ret) && j < complex_aggr_exprs.count(); ++j) {
    if (OB_FAIL(ObTransformUtils::extract_aggr_expr(
            sub_stmt->get_current_level(), complex_aggr_exprs.at(j), sub_stmt->get_aggr_items()))) {
      LOG_WARN("failed to extract aggr exprs.", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, complex_aggr_exprs.at(j), sub_stmt))) {
      LOG_WARN("failed to push back into select item array.", K(ret));
    } else { /*do nothing.*/
    }
  }

  if (OB_SUCC(ret)) {
    // try transform_for_grouping_sets_and_multi_rollup:
    //  SELECT count(sum(c1)) FROM t1 GROUP BY GROUPING sets(c1, c2);
    bool is_happened = false;
    ObDMLStmt* dml_stmt = static_cast<ObDMLStmt*>(sub_stmt);
    if (OB_FAIL(transform_for_grouping_sets_and_multi_rollup(dml_stmt, is_happened))) {
      LOG_WARN("failed to transform for transform for grouping sets and multi rollup.", K(ret));
    } else if (is_happened) {
      sub_stmt = static_cast<ObSelectStmt*>(dml_stmt);
      LOG_TRACE("succeed to transform for grouping sets and multi rollup", K(is_happened), K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTransformPreProcess::remove_nested_aggr_exprs(ObSelectStmt* stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else {
    ObSEArray<ObAggFunRawExpr*, 4> aggr_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_aggr_items().count(); i++) {
      if (OB_ISNULL(stmt->get_aggr_items().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr of aggr expr is null", K(ret));
      } else if (stmt->get_aggr_items().at(i)->is_nested_aggr()) {
        /*do nothing.*/
      } else if (OB_FAIL(aggr_exprs.push_back(stmt->get_aggr_items().at(i)))) {
        LOG_WARN("failed to assign to inner aggr exprs.", K(ret));
      } else { /*do nothing.*/
      }
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
    const ObIArray<ObRawExpr*>& column_exprs, ObIArray<ColumnItem>& column_items)
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
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTransformPreProcess::generate_parent_level_aggr_stmt(ObSelectStmt*& select_stmt, ObSelectStmt* sub_stmt)
{
  int ret = OB_SUCCESS;
  TableItem* view_table_item = NULL;
  ObSEArray<ObRawExpr*, 4> old_exprs;
  ObSEArray<ObRawExpr*, 4> new_exprs;
  ObSEArray<ColumnItem, 4> column_items;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("select stmt is null", K(ret));
  } else {
    select_stmt->get_table_items().reset();
    select_stmt->get_from_items().reset();
    select_stmt->get_having_exprs().reset();
    select_stmt->get_order_items().reset();
    select_stmt->get_group_exprs().reset();
    select_stmt->get_rollup_exprs().reset();
    select_stmt->get_column_items().reset();
    select_stmt->get_part_exprs().reset();
    select_stmt->get_condition_exprs().reset();
    select_stmt->get_grouping_sets_items().reset();
    select_stmt->get_multi_rollup_items().reset();
    select_stmt->reassign_rollup();
    select_stmt->reassign_grouping_sets();
    if (OB_FAIL(get_first_level_output_exprs(select_stmt, old_exprs))) {
      LOG_WARN("failed to get column exprs from stmt from.", K(ret));
    } else if (OB_FAIL(remove_nested_aggr_exprs(select_stmt))) {
      LOG_WARN("failed to extract second aggr items.", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_, select_stmt, sub_stmt, view_table_item))) {
      LOG_WARN("failed to add new table item.", K(ret));
    } else if (OB_FAIL(select_stmt->add_from_item(view_table_item->table_id_))) {
      LOG_WARN("failed to add from item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *view_table_item, select_stmt, new_exprs))) {
      LOG_WARN("failed to get select exprs from grouping sets view.", K(ret));
    } else if (OB_FAIL(construct_column_items_from_exprs(new_exprs, column_items))) {
      LOG_WARN("failed to construct column items from exprs.", K(ret));
    } else if (OB_FAIL(select_stmt->replace_inner_stmt_expr(old_exprs, new_exprs))) {
      LOG_WARN("failed to replace inner stmt exprs.", K(ret));
    } else if (OB_FAIL(select_stmt->get_column_items().assign(column_items))) {
      LOG_WARN("failed to assign column items.", K(ret));
    } else if (OB_FAIL(select_stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild tables hash.", K(ret));
    } else if (OB_FAIL(select_stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column items rel id.", K(ret));
    } else if (OB_FAIL(select_stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalized stmt.", K(ret));
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_for_nested_aggregate(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt), K(ctx_));
  } else if (!stmt->is_select_stmt()) {
    /*do nothing.*/
  } else {
    ObSelectStmt* sub_stmt = NULL;
    ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt);

    if (!select_stmt->contain_nested_aggr()) {
      /*do nothing.*/
    } else if (OB_FAIL(generate_child_level_aggr_stmt(select_stmt, sub_stmt))) {
      LOG_WARN("failed to generate first level aggr stmt.", K(ret));
    } else if (OB_FAIL(generate_parent_level_aggr_stmt(select_stmt, sub_stmt))) {
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

int ObTransformPreProcess::transform_for_merge_into(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret), K(stmt), K(ctx_));
  } else if (stmt->get_stmt_type() != stmt::T_MERGE) {
    /*do nothing*/
  } else if (OB_ISNULL(stmt->get_query_ctx()) || OB_UNLIKELY(stmt->get_table_size() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(stmt), K(ret));
  } else {
    const int64_t TARGET_TABLE_IDX = 0;
    const int64_t SOURCE_TABLE_IDX = 1;
    TableItem* new_table = NULL;
    ObMergeStmt* merge_stmt = static_cast<ObMergeStmt*>(stmt);
    TableItem* target_table = stmt->get_table_item(TARGET_TABLE_IDX);
    TableItem* source_table = stmt->get_table_item(SOURCE_TABLE_IDX);
    if (OB_ISNULL(target_table) || OB_ISNULL(source_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table item", K(target_table), K(source_table), K(ret));
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
    } else if (OB_FAIL(merge_stmt->add_from_item(new_table->table_id_, new_table->is_joined_table()))) {
      LOG_WARN("fail to add from item", K(new_table), K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_exprs(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or context is null", K(ret));
  } else {
    ObArray<ObRawExprPointer> relation_exprs;
    if (OB_FAIL(stmt->get_relation_exprs(relation_exprs))) {
      LOG_WARN("failed to get all relation exprs", K(ret));
    } else {
      for (int i = 0; OB_SUCC(ret) && i < relation_exprs.count(); i++) {
        bool is_happened = false;
        ObRawExpr* expr = NULL;
        if (OB_FAIL(relation_exprs.at(i).get(expr))) {
          LOG_WARN("failed to get expr", K(ret));
        } else if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is NULL", K(ret));
        } else if (OB_FAIL(transform_expr(*ctx_->expr_factory_, *ctx_->session_info_, expr, is_happened))) {
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

int ObTransformPreProcess::transform_expr(
    ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("expr is NULL", K(ret));
  }
  if (OB_SUCC(ret) && session.use_static_typing_engine()) {
    // rewrite `c1 in (c2, c3)` to `c1 = c2 or c1 = c3`
    if (OB_FAIL(replace_in_or_notin_recursively(expr_factory, session, expr, trans_happened))) {
      LOG_WARN("replace in or not in failed", K(ret), K(expr));
    }
  }
  if (OB_SUCC(ret) && session.use_static_typing_engine()) {
    // rewrite
    //   `cast c1 when c2 then xxx when c3 then xxx else xxx end`
    // to:
    //   `cast when c1 = c2 then xxx when c1 = c3 then xxx else xxx end`
    if (OB_FAIL(transform_arg_case_recursively(expr_factory, session, expr, trans_happened))) {
      LOG_WARN("transform arg case failed", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_in_or_notin_expr_without_row(ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo& session, const bool is_in_expr, ObRawExpr*& in_expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExpr* left_expr = in_expr->get_param_expr(0);
  ObRawExpr* right_expr = in_expr->get_param_expr(1);
  ObSEArray<DistinctObjMeta, 4> distinct_types;
  for (int i = 0; OB_SUCC(ret) && i < right_expr->get_param_count(); i++) {
    if (OB_ISNULL(right_expr->get_param_expr(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null param expr", K(ret), K(right_expr->get_param_expr(i)));
    } else {
      ObObjType obj_type = right_expr->get_param_expr(i)->get_result_type().get_type();
      ObCollationType coll_type = right_expr->get_param_expr(i)->get_result_type().get_collation_type();
      ObCollationLevel coll_level = right_expr->get_param_expr(i)->get_result_type().get_collation_level();
      if (OB_FAIL(add_var_to_array_no_dup(distinct_types, DistinctObjMeta(obj_type, coll_type, coll_level)))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        LOG_DEBUG("add param expr type", K(i), K(obj_type));
      }
    }
  }  // for end

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (1 == distinct_types.count()) {
    // only one type contained in right row expr, do not need rewrite
    // set should_deduce_type = true
    ObOpRawExpr* op_raw_expr = dynamic_cast<ObOpRawExpr*>(in_expr);
    if (OB_ISNULL(op_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null op_raw_expr", K(ret));
    } else {
      op_raw_expr->set_deduce_type_adding_implicit_cast(true);
    }
  } else {
    LOG_DEBUG("distinct types", K(distinct_types));
    ObSEArray<ObRawExpr*, 4> transed_in_exprs;
    ObOpRawExpr* tmp_in_expr = NULL;
    ObOpRawExpr* tmp_row_expr = NULL;
    ObRawExpr* tmp_left_expr = NULL;
    ObItemType op_type = is_in_expr ? T_OP_IN : T_OP_NOT_IN;
    for (int i = 0; OB_SUCC(ret) && i < distinct_types.count(); i++) {
      DistinctObjMeta obj_meta = distinct_types.at(i);
      if (OB_FAIL(expr_factory.create_raw_expr(op_type, tmp_in_expr)) ||
          OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, tmp_row_expr)) ||
          OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, left_expr, tmp_left_expr, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to create or create raw expr", K(ret));
      } else if (OB_ISNULL(tmp_in_expr) || OB_ISNULL(tmp_row_expr) || OB_ISNULL(tmp_left_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null expr", K(ret), K(tmp_in_expr), K(tmp_row_expr), K(tmp_left_expr));
      } else {
        for (int j = 0; OB_SUCC(ret) && j < right_expr->get_param_count(); j++) {
          ObObjType obj_type = right_expr->get_param_expr(j)->get_result_type().get_type();
          ObCollationType coll_type = right_expr->get_param_expr(j)->get_result_type().get_collation_type();
          ObCollationLevel coll_level = right_expr->get_param_expr(j)->get_result_type().get_collation_level();
          DistinctObjMeta tmp_meta(obj_type, coll_type, coll_level);
          if (obj_meta == tmp_meta && OB_FAIL(tmp_row_expr->add_param_expr(right_expr->get_param_expr(j)))) {
            LOG_WARN("failed to add param expr", K(ret));
          } else { /* do nothing */
          }
        }  // for end
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(tmp_in_expr->add_param_expr(tmp_left_expr)) ||
                   OB_FAIL(tmp_in_expr->add_param_expr(tmp_row_expr))) {
          LOG_WARN("failed to add param exprs", K(ret));
        } else if (OB_FAIL(transed_in_exprs.push_back(tmp_in_expr))) {
          LOG_WARN("failed to push back element", K(ret));
        } else {
          tmp_in_expr->set_deduce_type_adding_implicit_cast(true);
          LOG_DEBUG("partial in expr", K(*tmp_in_expr), K(*tmp_row_expr), K(*left_expr));
          tmp_left_expr = NULL;
          tmp_row_expr = NULL;
          tmp_in_expr = NULL;
        }
      }
    }  // for end
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_final_transed_or_and_expr(expr_factory, session, is_in_expr, transed_in_exprs, in_expr))) {
      LOG_WARN("failed to get final transed or expr", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_in_or_notin_expr_with_row(ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo& session, const bool is_in_expr, ObRawExpr*& in_expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObRawExpr* left_expr = in_expr->get_param_expr(0);
  ObRawExpr* right_expr = in_expr->get_param_expr(1);
  int row_dim = left_expr->get_param_count();
  ObSEArray<ObSEArray<DistinctObjMeta, 4>, 4> distinct_row_types;
  ObSEArray<ObSEArray<DistinctObjMeta, 4>, 4> all_row_types;

  for (int i = 0; OB_SUCC(ret) && i < right_expr->get_param_count(); i++) {
    if (OB_ISNULL(right_expr->get_param_expr(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null param expr", K(ret));
    } else if (OB_UNLIKELY(right_expr->get_param_expr(i)->get_param_count() != row_dim)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param cnt", K(row_dim), K(right_expr->get_param_expr(i)->get_param_count()));
    } else {
      ObSEArray<DistinctObjMeta, 4> tmp_row_type;
      for (int j = 0; OB_SUCC(ret) && j < right_expr->get_param_expr(i)->get_param_count(); j++) {
        if (OB_ISNULL(right_expr->get_param_expr(i)->get_param_expr(j))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid null param expr", K(ret));
        } else {
          const ObRawExpr* param_expr = right_expr->get_param_expr(i)->get_param_expr(j);
          const ObObjType obj_type = param_expr->get_result_type().get_type();
          const ObCollationType coll_type = param_expr->get_result_type().get_collation_type();
          const ObCollationLevel coll_level = param_expr->get_result_type().get_collation_level();
          if (OB_FAIL(tmp_row_type.push_back(DistinctObjMeta(obj_type, coll_type, coll_level)))) {
            LOG_WARN("failed to push back element", K(ret));
          } else { /* do nothing */
          }
        }
      }  // for end
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(add_row_type_to_array_no_dup(distinct_row_types, tmp_row_type))) {
        LOG_WARN("failed to add_row_type_to_array_no_dup", K(ret));
      } else if (OB_FAIL(all_row_types.push_back(tmp_row_type))) {
        LOG_WARN("failed to push back element", K(ret));
      } else { /* do nothing */
      }
    }
  }  // for end
  ObSEArray<ObRawExpr*, 4> transed_in_exprs;
  ObOpRawExpr* tmp_in_expr = NULL;
  ObOpRawExpr* tmp_row_expr = NULL;
  ObRawExpr* tmp_left_expr = NULL;
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (1 == distinct_row_types.count()) {
    // all rows are same, do nothing
    ObOpRawExpr* op_raw_expr = dynamic_cast<ObOpRawExpr*>(in_expr);
    if (OB_ISNULL(op_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null op_raw_expr", K(ret));
    } else {
      op_raw_expr->set_deduce_type_adding_implicit_cast(true);
    }
  } else {
    ObItemType op_type = is_in_expr ? T_OP_IN : T_OP_NOT_IN;
    for (int i = 0; OB_SUCC(ret) && i < distinct_row_types.count(); i++) {
      if (OB_FAIL(expr_factory.create_raw_expr(op_type, tmp_in_expr)) ||
          OB_FAIL(expr_factory.create_raw_expr(T_OP_ROW, tmp_row_expr)) ||
          OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, left_expr, tmp_left_expr, COPY_REF_DEFAULT))) {
        LOG_WARN("failed to create or copy raw expr", K(ret));
      } else if (OB_ISNULL(tmp_in_expr) || OB_ISNULL(tmp_row_expr) || OB_ISNULL(tmp_left_expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid null raw expr", K(ret), K(tmp_in_expr), K(tmp_left_expr), K(tmp_row_expr));
      } else {
        for (int j = 0; OB_SUCC(ret) && j < all_row_types.count(); j++) {
          if (is_same_row_type(distinct_row_types.at(i), all_row_types.at(j)) &&
              OB_FAIL(tmp_row_expr->add_param_expr(right_expr->get_param_expr(j)))) {
            LOG_WARN("failed to add param expr", K(ret));
          }
        }  // for end
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(tmp_in_expr->add_param_expr(tmp_left_expr)) ||
                   OB_FAIL(tmp_in_expr->add_param_expr(tmp_row_expr))) {
          LOG_WARN("failed to add param exprs", K(ret));
        } else if (OB_FAIL(transed_in_exprs.push_back(tmp_in_expr))) {
          LOG_WARN("failed to push back element", K(ret));
        } else {
          tmp_in_expr->set_deduce_type_adding_implicit_cast(true);
          tmp_row_expr = NULL;
          tmp_in_expr = NULL;
        }
      }
    }  // for end
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(get_final_transed_or_and_expr(expr_factory, session, is_in_expr, transed_in_exprs, in_expr))) {
      LOG_WARN("failed to get final transed in expr", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_arg_case_recursively(
    ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (T_OP_ARG_CASE == expr->get_expr_type()) {
    if (OB_FAIL(transform_arg_case_expr(expr_factory, session, expr, trans_happened))) {
      LOG_WARN("failed to transform_arg_case_expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
    if (OB_FAIL(transform_arg_case_recursively(expr_factory, session, expr->get_param_expr(i), trans_happened))) {
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
int ObTransformPreProcess::transform_arg_case_expr(
    ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  ObCaseOpRawExpr* case_expr = static_cast<ObCaseOpRawExpr*>(expr);
  ObRawExpr* arg_expr = case_expr->get_arg_param_expr();
  ObCaseOpRawExpr* new_case_expr = NULL;
  if (OB_FAIL(expr_factory.create_raw_expr(T_OP_CASE, new_case_expr))) {
    LOG_WARN("failed to create case expr", K(ret));
  } else if (OB_ISNULL(new_case_expr) || OB_ISNULL(arg_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret), KP(new_case_expr), KP(arg_expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < case_expr->get_when_expr_size(); ++i) {
    ObRawExpr* when_expr = case_expr->get_when_param_expr(i);
    ObRawExpr* then_expr = case_expr->get_then_param_expr(i);
    ObRawExpr* tmp_arg_expr = NULL;
    ObOpRawExpr* equal_expr = NULL;
    if (OB_ISNULL(when_expr) || OB_ISNULL(then_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("when_expr is NULL", K(ret), KP(when_expr), KP(then_expr));
    } else if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, arg_expr, tmp_arg_expr, COPY_REF_DEFAULT))) {
      LOG_WARN("failed to copy arg_expr", K(ret), K(i));
    } else if (OB_FAIL(create_equal_expr_for_case_expr(
                   expr_factory, session, tmp_arg_expr, when_expr, case_expr->get_result_type(), equal_expr))) {
      LOG_WARN("failed to create equal expr", K(ret));
    } else if (OB_FAIL(new_case_expr->add_when_param_expr(equal_expr)) ||
               OB_FAIL(new_case_expr->add_then_param_expr(then_expr))) {
      LOG_WARN("failed to add param expr", K(ret));
    }
  }  // for end
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

int ObTransformPreProcess::create_equal_expr_for_case_expr(ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo& session, ObRawExpr* arg_expr, ObRawExpr* when_expr, const ObExprResType& case_res_type,
    ObOpRawExpr*& equal_expr)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = ObMaxType;
  const ObExprResType& arg_type = arg_expr->get_result_type();
  const ObExprResType& when_type = arg_expr->get_result_type();
  ObRawExpr* new_when_expr = NULL;  // cast expr may added
  ObRawExpr* new_arg_expr = NULL;
  if (OB_ISNULL(arg_expr) || OB_ISNULL(when_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(arg_expr), KP(when_expr));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_EQ, equal_expr))) {
    LOG_WARN("failed to create equal expr", K(ret));
  } else {
    if (OB_FAIL(ObExprArgCase::get_cmp_type(obj_type,
            arg_type.get_type(),
            when_type.get_type(),
            ObMaxType))) {  // last argument is unused
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
      if (ObRawExprUtils::try_add_cast_expr_above(&expr_factory, &session, *arg_expr, cmp_type, new_arg_expr) ||
          ObRawExprUtils::try_add_cast_expr_above(&expr_factory, &session, *when_expr, cmp_type, new_when_expr)) {
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

bool ObTransformPreProcess::is_same_row_type(
    const ObIArray<DistinctObjMeta>& left, const ObIArray<DistinctObjMeta>& right)
{
  bool ret_bool = true;
  if (left.count() != right.count()) {
    ret_bool = false;
  }
  for (int i = 0; ret_bool && i < left.count(); i++) {
    ret_bool = (left.at(i) == right.at(i));
  }  // for end
  return ret_bool;
}

int ObTransformPreProcess::get_final_transed_or_and_expr(ObRawExprFactory& expr_factory,
    const ObSQLSessionInfo& session, const bool is_in_expr, ObIArray<ObRawExpr*>& transed_in_exprs,
    ObRawExpr*& final_or_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr* transed_or_expr = NULL;
  ObOpRawExpr* tmp_or_expr = NULL;
  ObOpRawExpr* last_or_expr = NULL;
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
    ObIArray<ObSEArray<DistinctObjMeta, 4>>& row_type_array, const ObSEArray<DistinctObjMeta, 4>& row_type)
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

int ObTransformPreProcess::check_and_transform_in_or_notin(
    ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& in_expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(2 != in_expr->get_param_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param cnt", K(ret), K(in_expr->get_param_count()));
  } else if (OB_ISNULL(in_expr->get_param_expr(0)) || OB_ISNULL(in_expr->get_param_expr(1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null params", K(ret), K(in_expr->get_param_expr(0)), K(in_expr->get_param_expr(1)));
  } else if (T_OP_ROW == in_expr->get_param_expr(0)->get_expr_type()) {
    // (x, y) in ((x0, y0), (x1, y1), ...)
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
    ObOpRawExpr* op_raw_expr = dynamic_cast<ObOpRawExpr*>(in_expr);
    if (OB_ISNULL(op_raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid null op_raw_expr", K(ret));
    } else if (OB_FAIL(in_expr->formalize(&session))) {
      LOG_WARN("formalize expr failed", K(ret));
    } else {
      LOG_DEBUG("After Transform", K(*op_raw_expr));
    }
  }
  return ret;
}

int ObTransformPreProcess::replace_in_or_notin_recursively(
    ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session, ObRawExpr*& root_expr, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < root_expr->get_param_count(); i++) {
    if (OB_ISNULL(root_expr->get_param_expr(i))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid null param expr", K(ret));
    } else if (OB_FAIL(SMART_CALL(replace_in_or_notin_recursively(
                   expr_factory, session, root_expr->get_param_expr(i), trans_happened)))) {
      LOG_WARN("failed to replace in or notin recursively", K(ret));
    }
  }
  if (OB_SUCC(ret) && (T_OP_IN == root_expr->get_expr_type() || T_OP_NOT_IN == root_expr->get_expr_type())) {
    if (OB_FAIL(check_and_transform_in_or_notin(expr_factory, session, root_expr, trans_happened))) {
      LOG_WARN("failed to check and transform", K(ret));
    }
  }
  return ret;
}

int ObTransformPreProcess::transformer_aggr_expr(ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_expand_aggr = false;
  bool is_add_linear_inter_expr = false;
  bool is_expand_window_aggr = false;
  bool is_happened = false;
  bool is_trans_nested_aggr_happened = trans_happened;
  trans_happened = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid null allocator", K(ret));
  } else if (OB_FAIL(ObExpandAggregateUtils::add_linear_inter_expr(stmt, ctx_, is_add_linear_inter_expr))) {
    LOG_WARN("failed to add linear inter expr", K(ret));
  } else if (OB_FAIL(ObExpandAggregateUtils::expand_aggr_expr(stmt, ctx_, is_expand_aggr))) {
    LOG_WARN("failed to expand aggr expr", K(ret));
  } else if (OB_FAIL(ObExpandAggregateUtils::expand_window_aggr_expr(stmt, ctx_, is_expand_window_aggr))) {
    LOG_WARN("failed to expand window aggr expr", K(ret));
  } else if (is_trans_nested_aggr_happened) {
    TableItem* table_item = NULL;
    if (OB_UNLIKELY(stmt->get_table_items().count() != 1) || OB_ISNULL(table_item = stmt->get_table_item(0)) ||
        OB_UNLIKELY(!table_item->is_generated_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(table_item), K(stmt->get_table_items().count()), K(ret));
    } else if (OB_FAIL(transformer_aggr_expr(table_item->ref_query_, is_happened))) {
      LOG_WARN("failed to transformer aggr expr", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    trans_happened = is_expand_aggr | is_expand_window_aggr | is_happened;
  }
  return ret;
}

int ObTransformPreProcess::transform_rownum_as_limit_offset(
    const ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  // bool is_rownum_gen_col_happened = false;
  bool is_rownum_happened = false;
  bool is_generated_rownum_happened = false;
  trans_happened = false;
  if (OB_FAIL(transform_common_rownum_as_limit(stmt, is_rownum_happened))) {
    LOG_WARN("failed to transform common rownum as limit", K(ret));
  } else if (OB_FAIL(transform_generated_rownum_as_limit(parent_stmts, stmt, is_generated_rownum_happened))) {
    LOG_WARN("failed to transform rownum gen col as limit", K(ret));
  } else {
    trans_happened = is_rownum_happened || is_generated_rownum_happened;
  }
  return ret;
}

int ObTransformPreProcess::transform_common_rownum_as_limit(ObDMLStmt*& stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObRawExpr* limit_expr = NULL;
  ObSelectStmt* child_stmt = NULL;
  bool is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(ctx_));
  } else if (stmt->is_hierarchical_query()) {
    /*do nothing*/
  } else if (is_oracle_mode() && stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->has_for_update()) {
    // in oracle mode, do not create view when has for update
  } else if (OB_FAIL(try_transform_common_rownum_as_limit(stmt, limit_expr))) {
    LOG_WARN("failed to try transform rownum expr as limit", K(ret));
  } else if (NULL == limit_expr) {
    /*do nothing */
  } else if (!stmt->is_select_stmt() && !stmt->has_order_by()) {
    stmt->set_limit_offset(limit_expr, NULL);
    trans_happened = true;
  } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, stmt, child_stmt, false))) {
    LOG_WARN("failed to create simple view", K(ret));
  } else if (OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child_stmt));
  } else {
    child_stmt->set_limit_offset(limit_expr, NULL);
    trans_happened = true;
  }
  return ret;
}

int ObTransformPreProcess::try_transform_common_rownum_as_limit(ObDMLStmt* stmt, ObRawExpr*& limit_expr)
{
  int ret = OB_SUCCESS;
  limit_expr = NULL;
  ObRawExpr* limit_value = NULL;
  ObItemType op_type = T_INVALID;
  bool is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    ObIArray<ObRawExpr*>& conditions = stmt->get_condition_exprs();
    int64_t expr_idx = -1;
    bool is_eq_cond = false;
    bool is_const_filter = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_eq_cond && i < conditions.count(); ++i) {
      ObRawExpr* cond_expr = NULL;
      ObRawExpr* const_expr = NULL;
      ObItemType expr_type = T_INVALID;
      if (OB_ISNULL(cond_expr = conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret), K(i));
      } else if (OB_FAIL(ObOptimizerUtil::get_rownum_filter_info(cond_expr, expr_type, const_expr, is_const_filter))) {
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
      } else { /*do nothing*/
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < conditions.count(); ++i) {
      ObRawExpr* cond_expr = NULL;
      if (i == expr_idx) {
        // do nothing
      } else if (OB_ISNULL(cond_expr = conditions.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret), K(i));
      } else if (cond_expr->has_flag(CNT_ROWNUM)) {
        is_valid = false;
      }
    }

    if (OB_SUCC(ret) && is_valid) {
      ObRawExpr* eq_expr = NULL;
      ObConstRawExpr* zero_expr = NULL;
      ObConstRawExpr* one_expr = NULL;
      if (OB_UNLIKELY(expr_idx < 0 || expr_idx >= conditions.count())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("expr index is invalid", K(ret), K(expr_idx), K(conditions.count()));
      } else if (OB_FAIL(conditions.remove(expr_idx))) {
        LOG_WARN("failed to remove expr", K(ret));
      } else if (!is_eq_cond) {
        ObRawExpr* int_expr = NULL;
        if (OB_FAIL(ObOptimizerUtil::convert_rownum_filter_as_limit(
                *ctx_->expr_factory_, ctx_->session_info_, op_type, limit_value, int_expr))) {
          LOG_WARN("failed to convert rownum filter as limit", K(ret));
        } else if (OB_FAIL(
                       ObRawExprUtils::build_case_when_expr_for_limit(*ctx_->expr_factory_, int_expr, limit_expr))) {
          LOG_WARN("failed to build case when expr for limit", K(ret));
        }
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 0, zero_expr))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 1, one_expr))) {
        LOG_WARN("failed to build const int expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                     *ctx_->expr_factory_, T_OP_EQ, limit_value, one_expr, eq_expr))) {
        LOG_WARN("failed to build common binary op expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(
                     *ctx_->expr_factory_, eq_expr, one_expr, zero_expr, limit_expr))) {
        LOG_WARN("failed to build case when expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && NULL != limit_expr) {
      ObExprResType dst_type;
      dst_type.set_int();
      ObSysFunRawExpr* cast_expr = NULL;
      OZ(ObRawExprUtils::create_cast_expr(*ctx_->expr_factory_, limit_expr, dst_type, cast_expr, ctx_->session_info_));
      CK(NULL != cast_expr);
      if (OB_SUCC(ret)) {
        limit_expr = cast_expr;
      }
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_generated_rownum_as_limit(
    const ObIArray<ObParentDMLStmt>& parent_stmts, ObDMLStmt* stmt, bool& trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObDMLStmt* upper_stmt = NULL;
  ObSelectStmt* select_stmt = NULL;
  ObRawExpr* upper_offset = NULL;
  ObRawExpr* upper_limit = NULL;
  ObRawExpr* offset_expr = NULL;
  ObRawExpr* limit_expr = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (parent_stmts.empty()) {
    /*do nothing*/
  } else if (!stmt->is_select_stmt() || OB_ISNULL(select_stmt = static_cast<ObSelectStmt*>(stmt)) ||
             OB_ISNULL(upper_stmt = parent_stmts.at(parent_stmts.count() - 1).stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (select_stmt->is_hierarchical_query() ||
             (upper_stmt->is_select_stmt() && static_cast<ObSelectStmt*>(upper_stmt)->is_hierarchical_query())) {
    /*do nothing*/
  } else if (OB_FAIL(
                 try_transform_generated_rownum_as_limit_offset(upper_stmt, select_stmt, upper_limit, upper_offset))) {
    LOG_WARN("failed to try transform generated rownum as offset offset", K(ret));
  } else if (NULL == upper_limit && NULL == upper_offset) {
    /*do nothing*/
  } else if (OB_FAIL(ObTransformUtils::merge_limit_offset(ctx_,
                 stmt->get_limit_expr(),
                 upper_limit,
                 stmt->get_offset_expr(),
                 upper_offset,
                 limit_expr,
                 offset_expr))) {
    LOG_WARN("failed to merge limit offset", K(ret));
  } else {
    stmt->set_limit_offset(limit_expr, offset_expr);
    trans_happened = true;
  }
  return ret;
}

// 1. select * from (select rownum rn, ... from t) where rn > 2 and rn <= 5;
//      => select * from (select rownum rn, ... from t limit 3 offset 2);
// 2. select * from (select rownum rn, ... from t) where rn = 4;
//      => select * from (select rownum rn, ... from t limit 1 offset 3);
// 3. select * from (select rownum rn, ... from t) where rn = 4.2;
//      => select * from (select rownum rn, ... from t limit 0 offset 0);
int ObTransformPreProcess::try_transform_generated_rownum_as_limit_offset(
    ObDMLStmt* upper_stmt, ObSelectStmt* select_stmt, ObRawExpr*& limit_expr, ObRawExpr*& offset_expr)
{
  int ret = OB_SUCCESS;
  TableItem* table = NULL;
  limit_expr = NULL;
  offset_expr = NULL;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(upper_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!upper_stmt->is_sel_del_upd() || !upper_stmt->is_single_table_stmt()) {
  } else if (OB_ISNULL(table = upper_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!is_oracle_mode() || select_stmt->is_fetch_with_ties() || NULL != select_stmt->get_offset_expr() ||
             NULL != select_stmt->get_limit_percent_expr() || select_stmt->has_order_by() ||
             select_stmt->has_rollup() || select_stmt->has_group_by() || select_stmt->has_window_function()) {
  } else {
    bool is_eq_cond = false;
    ObRawExpr* limit_cond = NULL;
    ObRawExpr* offset_cond = NULL;
    ObRawExpr* limit_value = NULL;
    ObRawExpr* offset_value = NULL;
    ObItemType limit_cmp_type = T_INVALID;
    ObItemType offset_cmp_type = T_INVALID;
    ObRawExpr* select_expr = NULL;
    ColumnItem* column_item = NULL;
    ObRawExpr* cond_expr = NULL;
    ObIArray<SelectItem>& select_items = select_stmt->get_select_items();
    ObIArray<ObRawExpr*>& upper_conds = upper_stmt->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && !is_eq_cond && i < select_items.count(); ++i) {
      if (OB_ISNULL(select_expr = select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!select_expr->has_flag(IS_ROWNUM)) {
        /*do nothing*/
      } else if (OB_ISNULL(
                     column_item = upper_stmt->get_column_item_by_id(table->table_id_, i + OB_APP_MIN_COLUMN_ID))) {
        /*do nothing*/
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !is_eq_cond && j < upper_conds.count(); ++j) {
          if (OB_ISNULL(cond_expr = upper_conds.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (cond_expr->is_op_expr() && 2 == cond_expr->get_param_count() &&
                     (IS_RANGE_CMP_OP(cond_expr->get_expr_type()) || T_OP_EQ == cond_expr->get_expr_type())) {
            ObRawExpr* param1 = cond_expr->get_param_expr(0);
            ObRawExpr* param2 = cond_expr->get_param_expr(1);
            ObRawExpr* const_expr = NULL;
            ObItemType expr_type = T_INVALID;
            if (OB_ISNULL(param1) || OB_ISNULL(param2)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null expr", K(ret), K(param1), K(param2));
            } else if (param1 == column_item->expr_ && param2->has_flag(IS_CONST)) {
              expr_type = cond_expr->get_expr_type();
              const_expr = param2;
            } else if (param1->has_flag(IS_CONST) && param2 == column_item->expr_) {
              const_expr = param1;
              ret = ObOptimizerUtil::flip_op_type(cond_expr->get_expr_type(), expr_type);
            } else { /*do nothing*/
            }

            if (OB_FAIL(ret) || NULL == const_expr) {
            } else if (!const_expr->get_result_type().is_integer_type() && !const_expr->get_result_type().is_number()) {
              /*do nothing*/
            } else if (T_OP_EQ == expr_type) {
              limit_value = const_expr;
              limit_cond = cond_expr;
              is_eq_cond = true;
              offset_cond = NULL;
            } else if (NULL == limit_cond && (T_OP_LE == expr_type || T_OP_LT == expr_type)) {
              limit_value = const_expr;
              limit_cmp_type = expr_type;
              limit_cond = cond_expr;
            } else if (NULL == offset_cond && (T_OP_GE == expr_type || T_OP_GT == expr_type)) {
              offset_value = const_expr;
              offset_cmp_type = expr_type;
              offset_cond = cond_expr;
            }
          }
        }
      }
    }

    ObRawExpr* init_limit_expr = NULL;
    ObRawExpr* init_offset_expr = NULL;
    ObRawExpr* minus_expr = NULL;
    if (OB_FAIL(ret)) {
    } else if (NULL != limit_cond && OB_FAIL(ObOptimizerUtil::remove_item(upper_conds, limit_cond))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else if (NULL != offset_cond && OB_FAIL(ObOptimizerUtil::remove_item(upper_conds, offset_cond))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else if (is_eq_cond) {
      ret = transform_generated_rownum_eq_cond(limit_value, limit_expr, offset_expr);
    } else if (NULL != limit_value &&
               OB_FAIL(ObOptimizerUtil::convert_rownum_filter_as_limit(
                   *ctx_->expr_factory_, ctx_->session_info_, limit_cmp_type, limit_value, init_limit_expr))) {
      LOG_WARN("failed to create limit expr from rownum", K(ret));
    } else if (NULL != limit_value && OB_FAIL(ObRawExprUtils::build_case_when_expr_for_limit(
                                          *ctx_->expr_factory_, init_limit_expr, init_limit_expr))) {
      LOG_WARN("failed to build case when expr for limit", K(ret));
    } else if (NULL != offset_value &&
               OB_FAIL(ObOptimizerUtil::convert_rownum_filter_as_offset(
                   *ctx_->expr_factory_, ctx_->session_info_, offset_cmp_type, offset_value, init_offset_expr))) {
      LOG_WARN("failed to create limit expr from rownum", K(ret));
    } else if (NULL != offset_value && OB_FAIL(ObRawExprUtils::build_case_when_expr_for_limit(
                                           *ctx_->expr_factory_, init_offset_expr, init_offset_expr))) {
      LOG_WARN("failed to build case when expr for limit", K(ret));
    } else if (NULL == init_offset_expr || NULL == init_limit_expr) {
      limit_expr = init_limit_expr;
      offset_expr = init_offset_expr;
    } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                   ctx_->session_info_,
                   T_OP_MINUS,
                   minus_expr,
                   init_limit_expr,
                   init_offset_expr))) {
      LOG_WARN("failed to create double op expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr_for_limit(*ctx_->expr_factory_, minus_expr, limit_expr))) {
      LOG_WARN("failed to build case when expr for limit", K(ret));
    } else {
      offset_expr = init_offset_expr;
    }
  }
  return ret;
}

int ObTransformPreProcess::transform_generated_rownum_eq_cond(
    ObRawExpr* eq_value, ObRawExpr*& limit_expr, ObRawExpr*& offset_expr)
{
  int ret = OB_SUCCESS;
  limit_expr = NULL;
  offset_expr = NULL;
  ObRawExpr* minus_expr = NULL;
  ObRawExpr* cmp_gt_expr = NULL;
  ObRawExpr* cmp_eq_expr = NULL;
  ObRawExpr* and_expr = NULL;
  ObConstRawExpr* zero_expr = NULL;
  ObConstRawExpr* one_expr = NULL;
  ObSysFunRawExpr* floor_expr = NULL;
  if (OB_ISNULL(eq_value) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!eq_value->get_result_type().is_integer_type() && !eq_value->get_result_type().is_number()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected eq_value", K(ret), K(*eq_value));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 0, zero_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_, ObIntType, 1, one_expr))) {
    LOG_WARN("failed to build const int expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                 *ctx_->expr_factory_, ctx_->session_info_, T_OP_MINUS, minus_expr, eq_value, one_expr))) {
    LOG_WARN("failed to create double op expr", K(ret));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_FUN_SYS_FLOOR, floor_expr))) {
    LOG_WARN("failed to create fun sys floor", K(ret));
  } else if (OB_ISNULL(floor_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("floor expr is null", K(ret));
  } else if (OB_FAIL(floor_expr->set_param_expr(eq_value))) {
    LOG_WARN("failed to set param expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 *ctx_->expr_factory_, T_OP_GT, eq_value, zero_expr, cmp_gt_expr))) {
    LOG_WARN("failed to build common binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 *ctx_->expr_factory_, T_OP_EQ, eq_value, floor_expr, cmp_eq_expr))) {
    LOG_WARN("failed to build common binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(
                 *ctx_->expr_factory_, T_OP_AND, cmp_gt_expr, cmp_eq_expr, and_expr))) {
    LOG_WARN("failed to build common binary op expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(
                 *ctx_->expr_factory_, and_expr, one_expr, zero_expr, limit_expr))) {
    LOG_WARN("failed to build case when expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_case_when_expr(
                 *ctx_->expr_factory_, and_expr, minus_expr, zero_expr, offset_expr))) {
    LOG_WARN("failed to build case when expr", K(ret));
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
