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

#include "ob_transform_left_join_to_anti.h"
#include "common/ob_common_utility.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{

int ObTransformLeftJoinToAnti::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                   ObDMLStmt *&stmt,
                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null stmt", K(ret));
  } else if (stmt->is_set_stmt()) {
    // do nothing
  } else {
    ObSEArray<ObSEArray<TableItem *, 4>, 4> trans_tables;
    ObSEArray<JoinedTable*, 4> joined_tables;
    if (OB_FAIL(joined_tables.assign(stmt->get_joined_tables()))) {
      LOG_WARN("failed to assign joined table", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < joined_tables.count(); ++i) {
      TableItem *table = NULL;
      if (OB_ISNULL(table = joined_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret));
      } else if (OB_FAIL(SMART_CALL(transform_left_join_to_anti_join_rec(stmt,
                                                                         table,
                                                                         trans_tables,
                                                                         true,
                                                                         trans_happened)))) {
        LOG_WARN("failed to transform left join to anti join", K(ret));
      }
    }
    if (OB_SUCC(ret) && trans_happened && OB_FAIL(add_transform_hint(*stmt, &trans_tables))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObTransformLeftJoinToAnti::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  typedef ObSEArray<TableItem *, 4> single_or_joined_table;
  ObIArray<single_or_joined_table> *trans_tables = NULL;
  ObLeftToAntiHint *hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params) ||
      OB_FALSE_IT(trans_tables = static_cast<ObIArray<single_or_joined_table>*>(trans_params)) ||
      OB_UNLIKELY(trans_tables->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_tables));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_LEFT_TO_ANTI, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(stmt.get_stmt_hint())))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_tables->count(); ++i) {
      ObSEArray<ObTableInHint, 4> single_or_joined_hint_table;
      if (OB_FAIL(ObTransformUtils::get_sorted_table_hint(trans_tables->at(i),
                                                          single_or_joined_hint_table))) {
        LOG_WARN("failed to get table hint", K(ret));
      } else if (OB_FAIL(hint->get_tb_name_list().push_back(single_or_joined_hint_table))) {
        LOG_WARN("failed to push back table name list", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformLeftJoinToAnti::transform_left_join_to_anti_join_rec(ObDMLStmt *stmt,
                                                                    TableItem *table,
                                                                    ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                                                                    bool is_root_table,
                                                                    bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool left_trans = false;
  bool cur_trans = false;
  JoinedTable *joined_table = static_cast<JoinedTable *>(table);
  if (OB_ISNULL(stmt) || OB_ISNULL(table) || OB_UNLIKELY(!table->is_joined_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null stmt or table item", K(ret), K(stmt), K(table));
  } else if (joined_table->is_left_join()) {
    if (joined_table->left_table_->is_joined_table()) {
      if (OB_FAIL(SMART_CALL(transform_left_join_to_anti_join_rec(stmt,
                                                                  joined_table->left_table_,
                                                                  trans_tables,
                                                                  false,
                                                                  left_trans)))) {
        LOG_WARN("failed to transform joined table to anti join", K(ret));
      }
    }
    OPT_TRACE("try transform left join to anti join:", joined_table);
    if (OB_SUCC(ret) && OB_FAIL(transform_left_join_to_anti_join(stmt,
                                                                 table,
                                                                 trans_tables,
                                                                 is_root_table,
                                                                 cur_trans))) {
      LOG_WARN("failed to do transform left join to anti-join", K(ret));
    } else {
      trans_happened |= (left_trans || cur_trans);
    }
  }
  return ret;
}


int ObTransformLeftJoinToAnti::transform_left_join_to_anti_join(ObDMLStmt *&stmt,
                                                                TableItem *table,
                                                                ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                                                                bool is_root_table,
                                                                bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  JoinedTable *joined_table = static_cast<JoinedTable *>(table);
  ObSEArray<ObRawExpr *, 8> target_exprs;
  ObArray<ObRawExpr *> constraints;
  if (OB_ISNULL(stmt) || OB_UNLIKELY(!table->is_joined_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null stmt or table item", K(ret), K(stmt), K(table));
  } else if (OB_FAIL(check_hint_valid(*stmt, *joined_table->right_table_, is_valid))) {
    LOG_WARN("failed to check hint valid", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("hint disable transform");
  } else if (OB_FAIL(check_can_be_trans(stmt,
                                        stmt->get_condition_exprs(),
                                        joined_table,
                                        target_exprs,
                                        constraints,
                                        is_valid))) {
    LOG_WARN("failed to extract column conditions", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("can not transform");
  } else if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
    LOG_WARN("failed to add param not null constraints", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(stmt->get_condition_exprs(),
                                                  target_exprs))) {
    LOG_WARN("failed to remove condition exprs", K(ret));
  } else if (OB_FAIL(construct_trans_table_list(stmt, joined_table->right_table_, trans_tables))) {
    LOG_WARN("failed to construct transformed table list", K(ret));
  } else if (is_root_table) {
    if (OB_FAIL(trans_stmt_to_anti(stmt, joined_table))) {
      LOG_WARN("failed to create semi stmt", K(ret));
    } else {
      trans_happened = true;
    }
  } else {
    TableItem *view_table = NULL;
    ObDMLStmt *ref_query = NULL;
    TableItem *push_table = table;
    if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                          stmt,
                                                          view_table,
                                                          push_table))) {
      LOG_WARN("failed to create empty view", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                            stmt,
                                                            view_table,
                                                            push_table))) {
      LOG_WARN("failed to create semi view", K(ret));
    } else if (OB_ISNULL(view_table) ||
              !view_table->is_generated_table() ||
              OB_ISNULL(ref_query = view_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(view_table), K(ref_query));
    } else if (OB_UNLIKELY(ref_query->get_joined_tables().empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined tables should not be empty", K(ret));
    } else if (OB_FAIL(trans_stmt_to_anti(ref_query,
                                          ref_query->get_joined_tables().at(0)))) {
      LOG_WARN("failed to create semi stmt", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      trans_happened = true;
    }
  }
  return ret;
}

int ObTransformLeftJoinToAnti::trans_stmt_to_anti(ObDMLStmt *stmt, JoinedTable *joined_table)
{
  int ret = OB_SUCCESS;
  // 1. create semi info, adjust from item
  SemiInfo *semi_info = NULL;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  TableItem *view_table = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx_));
  } else if (OB_ISNULL(joined_table) ||
             OB_ISNULL(left_table = joined_table->left_table_) ||
             OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null table", K(ret), K(joined_table));
  } else if (OB_ISNULL(semi_info = static_cast<SemiInfo *>(ctx_->allocator_->alloc(sizeof(SemiInfo))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate semi info", K(ret));
  } else if (OB_FALSE_IT(semi_info = new(semi_info)SemiInfo())) {
  } else if (lib::is_oracle_mode() && OB_FAIL(clear_for_update(right_table))) {
    // avoid for update op in the right side of the anti/semi.
    LOG_WARN("failed to clear for update", K(ret));
  } else if (!right_table->has_for_update() && !right_table->is_joined_table()) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               stmt,
                                                               view_table,
                                                               right_table))) {
    LOG_WARN("failed to create empty view", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          stmt,
                                                          view_table,
                                                          right_table))) {
    LOG_WARN("failed to create right view table", K(ret));
  } else {
    right_table = view_table;
  }

  if (OB_SUCC(ret)) {
    semi_info->join_type_ = LEFT_ANTI_JOIN;
    semi_info->right_table_id_ = right_table->table_id_;
    semi_info->semi_id_ = stmt->get_query_ctx()->available_tb_id_--;
    int64_t idx = stmt->get_from_item_idx(joined_table->table_id_);
    if (OB_FAIL(semi_info->semi_conditions_.assign(joined_table->get_join_conditions()))) {
      LOG_WARN("failed to assign join conditions", K(ret));
    } else if (OB_UNLIKELY(idx < 0 || idx >= stmt->get_from_item_size())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index", K(ret), K(idx));
    } else {
      stmt->get_from_item(idx).table_id_ = left_table->table_id_;
      stmt->get_from_item(idx).is_joined_ = left_table->is_joined_table();
      // 1.1. adjust joined tables in stmt
      if (left_table->is_joined_table()) {
        JoinedTable *left_joined_table = static_cast<JoinedTable *>(left_table);
        if (OB_FAIL(semi_info->left_table_ids_.assign(left_joined_table->single_table_ids_))) {
          LOG_WARN("failed to assign semi info left table ids", K(ret));
        } else if (OB_FAIL(stmt->remove_joined_table_item(joined_table))) {
          LOG_WARN("failed to remove joined table item", K(ret));
        } else if (OB_FAIL(stmt->get_joined_tables().push_back(left_joined_table))) {
          LOG_WARN("failed to push back joined table item", K(ret));
        }
      } else {
        if (OB_FAIL(semi_info->left_table_ids_.push_back(left_table->table_id_))) {
          LOG_WARN("failed to assign semi info left table ids", K(ret));
        } else if (OB_FAIL(stmt->remove_joined_table_item(joined_table))) {
          LOG_WARN("failed to remove joined table", K(ret));
        }
      }
    }
  }
  // 2. generate null exprs for exprs in right side of the joined table
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr *, 4> from_exprs;
    ObSEArray<ObRawExpr *, 4> to_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_size(); ++i) {
      const ColumnItem *col_item = NULL;
      ObRawExpr *from_expr = NULL;
      ObRawExpr *to_expr = NULL;
      if (OB_ISNULL(col_item = stmt->get_column_item(i)) ||
          OB_ISNULL(from_expr = col_item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null column", K(ret));
      } else if (right_table->table_id_ != col_item->table_id_) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::build_null_expr(*ctx_->expr_factory_,
                                                         to_expr))) {
        LOG_WARN("failed to build null expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::add_cast_for_replace(*ctx_->expr_factory_,
                                                                from_expr, to_expr,
                                                                ctx_->session_info_))) {
        LOG_WARN("failed to add cast for replace", K(ret));
      } else if (OB_ISNULL(to_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null cast expr", K(ret));
      } else if (OB_FAIL(from_exprs.push_back(from_expr))) {
        LOG_WARN("failed to push back from expr", K(ret));
      } else if (OB_FAIL(to_exprs.push_back(to_expr))) {
        LOG_WARN("failed to push back to expr", K(ret));
      }
    }
    // do in-place modification
    // a shared expr in semi_condition may be modified by the above replacement,
    // we revert the replacement in the following.
    if (OB_SUCC(ret)) {
      ObRawExprCopier copier(*ctx_->expr_factory_);
      if (OB_FAIL(stmt->replace_relation_exprs(from_exprs, to_exprs))) {
        LOG_WARN("failed to replace relation exprs", K(ret));
      } else if (OB_FAIL(copier.add_replaced_expr(to_exprs, from_exprs))) {
        LOG_WARN("failed to add replaced expr", K(ret));
      } else if (OB_FAIL(copier.copy_on_replace(semi_info->semi_conditions_,
                                                semi_info->semi_conditions_))) {
        LOG_WARN("failed to revert modified shared exprs", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt->get_semi_infos().push_back(semi_info))) {
      LOG_WARN("failed to assign semi infos", K(ret));
    } else if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column item rel id", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    }
  }
  return ret;
}

int ObTransformLeftJoinToAnti::clear_for_update(TableItem *table) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null pointer", K(ret));
  } else {
    if (table->is_basic_table()) {
      table->for_update_ = false;
    } else if (table->is_joined_table()) {
      if (OB_FAIL(clear_for_update(static_cast<JoinedTable*>(table)->left_table_))) {
        LOG_WARN("fail to clear for update", K(ret));
      } else if (OB_FAIL(clear_for_update(static_cast<JoinedTable*>(table)->right_table_))) {
        LOG_WARN("fail to clear for update", K(ret));
      }
    }
  }

  return ret;
}

// extract column ref exprs in (is null) condition exprs.
int ObTransformLeftJoinToAnti::get_column_ref_in_is_null_condition(const ObRawExpr *expr, 
                                                                   ObIArray<const ObRawExpr*> &target)
{
  int ret = OB_SUCCESS;
  const ObConstRawExpr *second_param = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(expr->get_param_expr(0)) || OB_ISNULL(expr->get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(ret));
  } else if (!(second_param = static_cast<const ObConstRawExpr *>(expr->get_param_expr(1)))
                                    ->get_value().is_null()) {
    // skip exprs other than (IS NULL)
  } else if (expr->get_param_expr(0)->has_flag(IS_COLUMN)) {
    if (OB_FAIL(target.push_back(expr->get_param_expr(0)))) {
      LOG_WARN("fail to push back column ref exprs", K(ret));
    } 
  } else {
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr->get_param_expr(0), target))) {
      LOG_WARN("fail to extract column ref exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformLeftJoinToAnti::check_condition_expr_validity(const ObRawExpr *expr,
                                                             ObDMLStmt *stmt,
                                                             const JoinedTable *joined_table,
                                                             ObIArray<ObRawExpr *> &constraints,
                                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  TableItem *right_table = NULL;
  is_valid = false;
  if (OB_ISNULL(expr) || OB_ISNULL(stmt) || OB_ISNULL(joined_table) ||
      OB_ISNULL(right_table = joined_table->right_table_) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null expr", K(ret));
  } else if (expr->get_expr_type() == T_OP_IS) {
    bool first_expr_not_null = false;
    ObSEArray<const ObRawExpr *, 1> targets;
    ObSEArray<const ObRawExpr *, 1> targets_in_right;
    const ObRawExpr *first_param = expr->get_param_expr(0);
    ObNotNullContext not_null_context(*ctx_, stmt);
    ObArray<ObRawExpr *> tmp_constraints;
    /* do the following two things:
     * 1. check the first param is not null
     * 2. the first param contain col in right table
     *    && the first param is non_propagate to the col in right table.
     */
    if (right_table->is_joined_table() &&
        OB_FAIL(not_null_context.add_joined_table(static_cast<JoinedTable *>(right_table)))) {
      LOG_WARN("failed to add context", K(ret));
    } else if (OB_FAIL(not_null_context.add_filter(joined_table->get_join_conditions()))) {
      LOG_WARN("failed to add null reject conditions", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_context,
                                                          first_param,
                                                          first_expr_not_null,
                                                          &tmp_constraints))) {
      LOG_WARN("failed to check expr not null", K(ret));
    } else if (!first_expr_not_null) {
      // do not transform.
    } else if (OB_FAIL(get_column_ref_in_is_null_condition(expr, targets))) {
      LOG_WARN("fail to extract column ref in is null condition", K(ret));
    } else if (targets.count() > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < targets.count(); i++) {
        const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr*>(targets.at(i));
        bool is_right_table_col = false;
        // 1. check if the column is in right side of the joined table
        if (right_table->is_joined_table()) {
          is_right_table_col = is_contain(static_cast<JoinedTable *>(right_table)->single_table_ids_,
                                          col_expr->get_table_id());
        } else {
          is_right_table_col = (col_expr->get_table_id() == right_table->table_id_);
        }
        if (is_right_table_col) {
          if (OB_FAIL(targets_in_right.push_back(col_expr))) {
            LOG_WARN("failed to push back col expr", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && !targets_in_right.empty()) {
        if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(first_param,
                                                             targets_in_right,
                                                             is_valid))) {
          LOG_WARN("fail to check null propagate expr", K(ret));
        } else if (is_valid && OB_FAIL(append(constraints, tmp_constraints))) {
          LOG_WARN("failed to append constraints", K(ret));
        }
      }
    }
  } else if (expr->get_expr_type() == T_OP_AND) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      ObArray<ObRawExpr *> tmp_constraints;
      if (OB_FAIL(SMART_CALL(check_condition_expr_validity(expr->get_param_expr(i),
                                                           stmt,
                                                           joined_table,
                                                           tmp_constraints,
                                                           is_valid)))) {
        LOG_WARN("fail to check condition expr validity", K(ret));
      } else if (!is_valid) {
        // do nothing
      } else if (OB_FAIL(append(constraints, tmp_constraints))) {
        LOG_WARN("failed to append constraints", K(ret));
      } else {
        break;
      }
    }
  } else if (expr->get_expr_type() == T_OP_OR) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      if (OB_FAIL(SMART_CALL(check_condition_expr_validity(expr->get_param_expr(i),
                                                           stmt,
                                                           joined_table,
                                                           constraints,
                                                           is_valid)))) {
        LOG_WARN("fail to check condition expr validity", K(ret));
      } else if (!is_valid) {
        break;
      }
    }
  }
  return ret;
}

int ObTransformLeftJoinToAnti::check_can_be_trans(ObDMLStmt *stmt,
                                                  const ObIArray<ObRawExpr *> &cond_exprs,
                                                  const JoinedTable *joined_table,
                                                  ObIArray<ObRawExpr *> &target_exprs,
                                                  ObIArray<ObRawExpr *> &constraints,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  TableItem *right_table = NULL;
  bool is_table_valid = true;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (OB_ISNULL(joined_table) ||
             OB_ISNULL(joined_table->left_table_) ||
             OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid joined table", K(ret), K(joined_table));
  } else if (stmt->is_delete_stmt() || stmt->is_update_stmt()) {
    // transformation is not allowed if the updated/deleted table is 
    // on the right side of the join table
    ObDelUpdStmt *del_upd_stmt = static_cast<ObDelUpdStmt *>(stmt);
    ObSEArray<ObDmlTableInfo*, 2> table_infos;
    if (OB_FAIL(del_upd_stmt->get_dml_table_infos(table_infos))) {
      LOG_WARN("failed to get dml table info", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
      const ObDmlTableInfo *table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null table info", K(ret));
      } else if (right_table->is_joined_table()) {
        JoinedTable *right_joined_table = static_cast<JoinedTable *>(right_table);
        if (is_contain(right_joined_table->single_table_ids_, table_info->table_id_)) {
          is_table_valid = false;
          OPT_TRACE("right table is dml target table");
        }
      } else if (table_info->table_id_ == right_table->table_id_) {
        is_table_valid = false;
        OPT_TRACE("right table is dml target table");
      }
    }
  }
  if (OB_SUCC(ret) && is_table_valid) {
    bool contained = false;
    if (OB_FAIL(ObTransformUtils::check_table_contain_in_semi(stmt, right_table, contained))) {
      LOG_WARN("failed to check table contained in semi", K(ret));
    } else if (contained) {
      is_table_valid = false;
    }
  }
  // ObQueryRefRawExpr not support copy on replace in copier, disable this condition
  for (int64_t i = 0; OB_SUCC(ret) && is_table_valid &&
                      i < joined_table->get_join_conditions().count(); ++i) {
    if (OB_ISNULL(joined_table->get_join_conditions().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (joined_table->get_join_conditions().at(i)->has_flag(CNT_SUB_QUERY)) {
      is_table_valid = false;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_table_valid &&
                      i < cond_exprs.count(); ++i) {
    bool tmp_cond_valid = false;
    ObArray<ObRawExpr *> tmp_constraints;
    if (OB_FAIL(check_condition_expr_validity(cond_exprs.at(i),
                                              stmt,
                                              joined_table,
                                              tmp_constraints,
                                              tmp_cond_valid))) {
      LOG_WARN("fail to check condition expr validity", K(ret));
    } else if (!tmp_cond_valid) {
      // do nothing
    } else if (OB_FAIL(target_exprs.push_back(cond_exprs.at(i)))) {
      LOG_WARN("failed to push back target exprs", K(ret));
    } else if (OB_FAIL(append(constraints, tmp_constraints))) {
      LOG_WARN("failed to append constraints exprs", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_valid = !target_exprs.empty();
  }
  return ret;
}

int ObTransformLeftJoinToAnti::check_hint_valid(const ObDMLStmt &stmt,
                                                const TableItem &table,
                                                bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const ObQueryHint *query_hint = NULL; 
  const ObLeftToAntiHint *hint = static_cast<const ObLeftToAntiHint*>(get_hint(stmt.get_stmt_hint()));
  if (NULL == hint) {
    is_valid = true;
  } else if (OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(query_hint));
  } else {
    is_valid = hint->enable_left_to_anti(query_hint->cs_type_, table);
    LOG_TRACE("succeed to check left_to_anti hint valid", K(is_valid), K(table), K(*hint));
  }
  return ret;
}

int ObTransformLeftJoinToAnti::construct_trans_table_list(const ObDMLStmt *stmt,
                                                          const TableItem *table,
                                                          ObIArray<ObSEArray<TableItem *, 4>> &trans_tables)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableItem *, 4> trans_table;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table->is_joined_table()) {
    const JoinedTable *joined_table = static_cast<const JoinedTable *>(table);
    for (int64_t i = 0; OB_SUCC(ret) && i <
          joined_table->single_table_ids_.count(); ++i) {
      TableItem *table = stmt->get_table_item_by_id(joined_table->single_table_ids_.at(i));
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(trans_table.push_back(table))) {
        LOG_WARN("failed to push back trans table", K(ret));
      }
    }
  } else if (OB_FAIL(trans_table.push_back(const_cast<TableItem *>(table)))) {
    LOG_WARN("failed to push back table", K(ret));
  }
  if (OB_SUCC(ret) && OB_FAIL(trans_tables.push_back(trans_table))) {
    LOG_WARN("failed to push back trans tables", K(ret));
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
