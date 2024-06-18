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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_context.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/utility.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::share::schema;

int SelectItem::deep_copy(ObIRawExprCopier &expr_copier,
                          const SelectItem &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_copier.copy(other.expr_, expr_))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else {
    is_real_alias_ = other.is_real_alias_;
    alias_name_ = other.alias_name_;
    paramed_alias_name_ = other.paramed_alias_name_;
    expr_name_ = other.expr_name_;
    questions_pos_ = other.questions_pos_;
    params_idx_ = other.params_idx_;
    neg_param_idx_ = other.neg_param_idx_;
    esc_str_flag_ = other.esc_str_flag_;
    need_check_dup_name_ = other.need_check_dup_name_;
    implicit_filled_ = other.implicit_filled_;
    is_unpivot_mocked_column_ = other.is_unpivot_mocked_column_;
    is_implicit_added_ = other.is_implicit_added_;
    is_hidden_rowid_ = other.is_hidden_rowid_;
  }
  return ret;
}

const char* const ObSelectIntoItem::DEFAULT_LINE_TERM_STR = "\n";
const char* const ObSelectIntoItem::DEFAULT_FIELD_TERM_STR = "\t";
const char ObSelectIntoItem::DEFAULT_FIELD_ENCLOSED_CHAR = 0;
const bool ObSelectIntoItem::DEFAULT_OPTIONAL_ENCLOSED = false;
const bool ObSelectIntoItem::DEFAULT_SINGLE_OPT = true;
const int64_t ObSelectIntoItem::DEFAULT_MAX_FILE_SIZE = 256 * 1024 * 1024;
const char ObSelectIntoItem::DEFAULT_FIELD_ESCAPED_CHAR = '\\';

//对于select .. for update 也认为是被更改
int ObSelectStmt::check_table_be_modified(uint64_t ref_table_id, bool& is_exists) const
{
  int ret = OB_SUCCESS;
  is_exists = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_exists && i < table_items_.count(); ++i) {
    TableItem *table_item = table_items_.at(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("table item is NULL", K(ret), K(i), K(table_items_.count()));
    } else if (table_item->for_update_ && ref_table_id == table_item->ref_id_) {
      is_exists = true;
      LOG_DEBUG("duplicate table is used in select for update", K(is_exists), K(ref_table_id), K(table_items_.count()));
    }
  }
  if (OB_SUCC(ret) && !is_exists) {
    ObSEArray<ObSelectStmt*, 16> child_stmts;
    if (OB_FAIL(get_child_stmts(child_stmts))) {
      LOG_ERROR("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !is_exists && i < child_stmts.count(); ++i) {
        ObSelectStmt *sub_stmt = child_stmts.at(i);
        if (OB_ISNULL(sub_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("sub stmt is null", K(ret));
        } else if (OB_FAIL(SMART_CALL(sub_stmt->check_table_be_modified(ref_table_id, is_exists)))) {
          LOG_WARN("check sub stmt whether has select for update failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

bool ObSelectStmt::has_distinct_or_concat_agg() const
{
  bool has = false;
  for (int64_t i = 0; !has && i < get_aggr_item_size(); ++i) {
    const ObAggFunRawExpr *aggr = get_aggr_item(i);
    if (NULL != aggr) {
      has = aggr->is_param_distinct() ||
            T_FUN_GROUP_CONCAT == aggr->get_expr_type();
    }
  }
  return has;
}

int ObSelectStmt::add_window_func_expr(ObWinFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(win_func_exprs_.push_back(expr))) {
    LOG_WARN("failed to add expr", K(ret));
  } else {
    expr->set_explicited_reference();
  }
  return ret;
}

int ObSelectStmt::set_qualify_filters(common::ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(qualify_filters_.assign(exprs))) {
    LOG_WARN("failed to add expr", K(ret));
  }
  return ret;
}

int ObSelectStmt::remove_window_func_expr(ObWinFunRawExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < win_func_exprs_.count(); i++) {
      if (OB_ISNULL(win_func_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (expr == win_func_exprs_.at(i)) {
        ret = win_func_exprs_.remove(i);
        break;
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObSelectStmt::check_aggr_and_winfunc(ObRawExpr &expr)
{
  int ret = OB_SUCCESS;
  if (expr.is_aggr_expr() &&
      !ObRawExprUtils::find_expr(agg_items_, &expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("aggr expr does not exist in the stmt", K(agg_items_), K(expr), K(ret));
  } else if (expr.is_win_func_expr() &&
             !ObRawExprUtils::find_expr(win_func_exprs_, &expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("win func expr does not exist in the stmt", K(ret), K(expr));
  }
  return ret;
}

int ObSelectStmt::assign(const ObSelectStmt &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::assign(other))) {
    LOG_WARN("failed to copy stmt");
  } else if (OB_FAIL(select_items_.assign(other.select_items_))) {
    LOG_WARN("assign other select items failed", K(ret));
  } else if (OB_FAIL(group_exprs_.assign(other.group_exprs_))) {
    LOG_WARN("assign other group exprs failed", K(ret));
  } else if (OB_FAIL(rollup_exprs_.assign(other.rollup_exprs_))) {
    LOG_WARN("assign other rollup exprs failed", K(ret));
  } else if (OB_FAIL(grouping_sets_items_.assign(other.grouping_sets_items_))) {
    LOG_WARN("assgin other grouping sets items failed", K(ret));
  } else if (OB_FAIL(rollup_items_.assign(other.rollup_items_))) {
    LOG_WARN("assgin other rollup items failed", K(ret));
  } else if (OB_FAIL(cube_items_.assign(other.cube_items_))) {
    LOG_WARN("assgin other cube items failed", K(ret));
  } else if (OB_FAIL(having_exprs_.assign(other.having_exprs_))) {
    LOG_WARN("assign other having exprs failed", K(ret));
  } else if (OB_FAIL(agg_items_.assign(other.agg_items_))) {
    LOG_WARN("assign other aggr items failed", K(ret));
  } else if (OB_FAIL(start_with_exprs_.assign(other.start_with_exprs_))) {
    LOG_WARN("assign other start with failed", K(ret));
  } else if (OB_FAIL(win_func_exprs_.assign(other.win_func_exprs_))) {
    LOG_WARN("assign window function exprs failed", K(ret));
  } else if (OB_FAIL(qualify_filters_.assign(other.qualify_filters_))) {
    LOG_WARN("assign window function filter exprs failed", K(ret));
  } else if (OB_FAIL(connect_by_exprs_.assign(other.connect_by_exprs_))) {
    LOG_WARN("assign other connect by failed", K(ret));
  } else if (OB_FAIL(connect_by_prior_exprs_.assign(other.connect_by_prior_exprs_))) {
    LOG_WARN("assign other connect by prior failed", K(ret));
  } else if (OB_FAIL(cte_exprs_.assign(other.cte_exprs_))) {
    LOG_WARN("assign ctx_exprs failed", K(ret));
  } else if (OB_FAIL(cycle_by_items_.assign(other.cycle_by_items_))) {
    LOG_WARN("assign cycle by item failed", K(ret));
  } else if (OB_FAIL(rollup_directions_.assign(other.rollup_directions_))) {
    LOG_WARN("assign other rollup directions.", K(ret));
  } else if (OB_FAIL(search_by_items_.assign(other.search_by_items_))) {
    LOG_WARN("assign search by items failed", K(ret));
  } else if (OB_FAIL(sample_infos_.assign(other.sample_infos_))) {
    LOG_WARN("assign sample scan infos failed", K(ret));
  } else if (OB_FAIL(set_query_.assign(other.set_query_))) {
    LOG_WARN("assign set query failed", K(ret));
  } else if (OB_FAIL(for_update_dml_info_.assign(other.for_update_dml_info_))) {
    LOG_WARN("assign for update dml info failed", K(ret));
  } else {
    set_op_ = other.set_op_;
    is_recursive_cte_ = other.is_recursive_cte_;
    is_breadth_search_ = other.is_breadth_search_;
    is_distinct_ = other.is_distinct_;
    is_from_pivot_ = other.is_from_pivot_;
    is_view_stmt_ = other.is_view_stmt_;
    view_ref_id_ = other.view_ref_id_;
    is_match_topk_ = other.is_match_topk_;
    is_nocycle_ = other.is_nocycle_;
    is_set_distinct_ = other.is_set_distinct();
    show_stmt_ctx_.assign(other.show_stmt_ctx_);
    select_type_ = other.select_type_;
    into_item_ = other.into_item_;
    children_swapped_ = other.children_swapped_;
    check_option_ = other.check_option_;
    contain_ab_param_ = other.contain_ab_param_;
    is_order_siblings_ = other.is_order_siblings_;
    is_hierarchical_query_ = other.is_hierarchical_query_;
    has_prior_ = other.has_prior_;
    has_reverse_link_ = other.has_reverse_link_;
    is_expanded_mview_ = other.is_expanded_mview_;
    is_select_straight_join_ = other.is_select_straight_join_;
  }
  return ret;
}

int ObSelectStmt::deep_copy_stmt_struct(ObIAllocator &allocator,
                                        ObRawExprCopier &expr_copier,
                                        const ObDMLStmt &input)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt &other = static_cast<const ObSelectStmt &>(input);
  if (OB_UNLIKELY(!input.is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("input stmt is invalid", K(ret));
  } else if (OB_FAIL(set_query_.assign(other.set_query_))) {
    LOG_WARN("failed to assgin set query", K(ret));
  } else if (OB_FAIL(ObDMLStmt::deep_copy_stmt_struct(allocator, expr_copier, other))) {
    LOG_WARN("deep copy DML stmt failed", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ColumnItem>(expr_copier,
                                                        other.cycle_by_items_,
                                                        cycle_by_items_))) {
    LOG_WARN("deep copy cycle by item failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.group_exprs_, group_exprs_))) {
    LOG_WARN("deep copy group expr failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.rollup_exprs_, rollup_exprs_))) {
    LOG_WARN("deep copy rollup expr failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.having_exprs_, having_exprs_))) {
    LOG_WARN("deep copy having expr failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.agg_items_, agg_items_))) {
    LOG_WARN("deep copy agg item failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.win_func_exprs_, win_func_exprs_))) {
    LOG_WARN("deep copy window function expr failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.qualify_filters_, qualify_filters_))) {
    LOG_WARN("deep copy window function expr failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.start_with_exprs_, start_with_exprs_))) {
    LOG_WARN("deep copy start with exprs failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.connect_by_exprs_, connect_by_exprs_))) {
    LOG_WARN("deep copy connect by expr failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.connect_by_prior_exprs_,
                                      connect_by_prior_exprs_))) {
    LOG_WARN("deep copy connect by prior expr failed", K(ret));
  } else if (OB_FAIL(expr_copier.copy(other.cte_exprs_, cte_exprs_))) {
    LOG_WARN("deep copy ctx exprs failed", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<OrderItem>(expr_copier,
                                                       other.search_by_items_,
                                                       search_by_items_))) {
    LOG_WARN("deep copy search by items failed", K(ret));
  } else if (OB_FAIL(rollup_directions_.assign(other.rollup_directions_))) {
    LOG_WARN("assign rollup directions failed", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<SelectItem>(expr_copier,
                                                        other.select_items_,
                                                        select_items_))) {
    LOG_WARN("deep copy select items failed", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<OrderItem>(expr_copier,
                                                       other.order_items_,
                                                       order_items_))) {
    LOG_WARN("deep copy order items failed", K(ret));
  } else if (OB_FAIL(sample_infos_.assign(other.sample_infos_))) {
    LOG_WARN("failed to assign sample infos", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObGroupingSetsItem>(expr_copier,
                                                                other.grouping_sets_items_,
                                                                grouping_sets_items_))) {
    LOG_WARN("deep copy grouping sets items failed", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObRollupItem>(expr_copier,
                                                          other.rollup_items_,
                                                          rollup_items_))) {
    LOG_WARN("deep copy rollup items failed", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ObCubeItem>(expr_copier,
                                                        other.cube_items_,
                                                        cube_items_))) {
    LOG_WARN("deep copy cube items failed", K(ret));
  } else if (OB_FAIL(deep_copy_stmt_objects<ForUpdateDMLInfo>(allocator,
                                                              expr_copier,
                                                              other.for_update_dml_info_,
                                                              for_update_dml_info_))) {
    LOG_WARN("deep copy for update dml info failed", K(ret));
  } else {
    set_op_ = other.set_op_;
    is_recursive_cte_ = other.is_recursive_cte_;
    is_breadth_search_ = other.is_breadth_search_;
    is_distinct_ = other.is_distinct_;
    is_from_pivot_ = other.is_from_pivot_;
    is_nocycle_ = other.is_nocycle_;
    is_view_stmt_ = other.is_view_stmt_;
    view_ref_id_ = other.view_ref_id_;
    is_match_topk_ = other.is_match_topk_;
    is_set_distinct_ = other.is_set_distinct_;
    show_stmt_ctx_.assign(other.show_stmt_ctx_);
    select_type_ = other.select_type_;
    children_swapped_ = other.children_swapped_;
    is_fetch_with_ties_ = other.is_fetch_with_ties_;
    check_option_ = other.check_option_;
    contain_ab_param_ = other.contain_ab_param_;
    is_order_siblings_ = other.is_order_siblings_;
    is_hierarchical_query_ = other.is_hierarchical_query_;
    has_prior_ = other.has_prior_;
    has_reverse_link_ = other.has_reverse_link_;
    is_expanded_mview_ = other.is_expanded_mview_;
    is_select_straight_join_ = other.is_select_straight_join_;
    // copy insert into statement
    if (OB_SUCC(ret) && NULL != other.into_item_) {
      ObSelectIntoItem *temp_into_item = NULL;
      void *ptr = allocator.alloc(sizeof(ObSelectIntoItem));
      if (OB_ISNULL(ptr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate select into item", K(ret));
      } else {
        temp_into_item = new(ptr) ObSelectIntoItem();
        if (OB_FAIL(temp_into_item->assign(*other.into_item_))) {
          LOG_WARN("deep copy into item failed", K(ret));
        } else {
          into_item_ = temp_into_item;
        }
      }
    }
  }
  return ret;
}

int ObSelectStmt::create_select_list_for_set_stmt(ObRawExprFactory &expr_factory)
{
  int ret = OB_SUCCESS;
  SelectItem new_select_item;
  ObExprResType res_type;
  ObSelectStmt *child_stmt = NULL;
  if (OB_ISNULL(child_stmt = get_set_query(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret), K(child_stmt), K(get_set_op()));
  } else {
    int64_t num = child_stmt->get_select_item_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
      SelectItem &child_select_item = child_stmt->get_select_item(i);
      // unused
      // ObString set_column_name = left_select_item.alias_name_;
      ObItemType set_op_type = static_cast<ObItemType>(T_OP_SET + get_set_op());
      res_type.reset();
      new_select_item.alias_name_ = child_select_item.alias_name_;
      new_select_item.expr_name_ = child_select_item.expr_name_;
      new_select_item.is_real_alias_ = child_select_item.is_real_alias_ || child_select_item.expr_->is_column_ref_expr();
      res_type = child_select_item.expr_->get_result_type();
      if (OB_FAIL(ObRawExprUtils::make_set_op_expr(expr_factory, i, set_op_type, res_type,
                                                   NULL, new_select_item.expr_))) {
        LOG_WARN("create set op expr failed", K(ret));
      } else if (OB_FAIL(add_select_item(new_select_item))) {
        LOG_WARN("push back set select item failed", K(ret));
      } else if (OB_ISNULL(new_select_item.expr_) ||
                 OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
      }
    }
  }
  return ret;
}

int ObSelectStmt::update_stmt_table_id(ObIAllocator *allocator, const ObSelectStmt &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::update_stmt_table_id(allocator, other))) {
    LOG_WARN("failed to update stmt table id", K(ret));
  } else if (OB_UNLIKELY(set_query_.count() != other.set_query_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected child query count", K(ret), K(set_query_.count()),
                                                 K(other.set_query_.count()));
  } else {
    ObSelectStmt *child_query = NULL;
    ObSelectStmt *other_child_query = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < set_query_.count(); i++) {
      if (OB_ISNULL(other_child_query = other.set_query_.at(i))
          || OB_ISNULL(child_query = set_query_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null statement", K(ret), K(child_query), K(other_child_query));
      } else if (OB_FAIL(SMART_CALL(child_query->update_stmt_table_id(allocator, *other_child_query)))) {
        LOG_WARN("failed to update stmt table id", K(ret));
      } else { /* do nothing*/ }
    }
  }
  return ret;
}

int ObSelectStmt::iterate_stmt_expr(ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::iterate_stmt_expr(visitor))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); i++) {
    if (OB_FAIL(visitor.visit(select_items_.at(i).expr_, SCOPE_SELECT))) {
      LOG_WARN("failed to visit select exprs", K(ret));
    } else { /*do nothing*/ }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.visit(group_exprs_, SCOPE_GROUPBY))) {
      LOG_WARN("failed to visit group exprs", K(ret));
    } else if (OB_FAIL(visitor.visit(rollup_exprs_, SCOPE_GROUPBY))) {
      LOG_WARN("failed to visit rollup exprs", K(ret));
    } else if (OB_FAIL(visitor.visit(having_exprs_, SCOPE_HAVING))) {
      LOG_WARN("failed to visit having exprs", K(ret));
    } else if (OB_FAIL(visitor.visit(agg_items_, SCOPE_DICT_FIELDS))) {
      LOG_WARN("failed to visit aggr items", K(ret));
    } else if (OB_FAIL(iterate_rollup_items(rollup_items_, visitor))) {
      LOG_WARN("failed to iterate multi rollup items", K(ret));
    } else if (OB_FAIL(iterate_cube_items(cube_items_, visitor))) {
      LOG_WARN("failed to iterate multi cube items", K(ret));
    } else {/* do nothing */}
    for (int64_t i = 0; OB_SUCC(ret) && i < grouping_sets_items_.count(); i++) {
      if (OB_FAIL(iterate_group_items(grouping_sets_items_.at(i).grouping_sets_exprs_, visitor))) {
        LOG_WARN("failed to iterate grouping sets exprs", K(ret));
      } else if (OB_FAIL(iterate_rollup_items(grouping_sets_items_.at(i).rollup_items_, visitor))) {
        LOG_WARN("failed to iterate rollup items", K(ret));
      } else if (OB_FAIL(iterate_cube_items(grouping_sets_items_.at(i).cube_items_, visitor))) {
        LOG_WARN("failed to iterate cube items", K(ret));
      } else {/* do nothing */}
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.visit(win_func_exprs_, SCOPE_DICT_FIELDS))) {
      LOG_WARN("failed to visit winfunc exprs", K(ret));
    } else if (OB_FAIL(visitor.visit(cte_exprs_, SCOPE_DICT_FIELDS))) {
      LOG_WARN("failed to visit cte exprs", K(ret));
    } else if (OB_FAIL(visitor.visit(start_with_exprs_, SCOPE_START_WITH))) {
      LOG_WARN("failed to visit start with exprs", K(ret));
    } else if (OB_FAIL(visitor.visit(connect_by_exprs_, SCOPE_CONNECT_BY))) {
      LOG_WARN("failed to visit connect by exprs", K(ret));
    } else if (OB_FAIL(visitor.visit(connect_by_prior_exprs_, SCOPE_DICT_FIELDS))) {
      LOG_WARN("failed to visit connect by prior exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < search_by_items_.count(); i++) {
      if (OB_FAIL(visitor.visit(search_by_items_.at(i).expr_, SCOPE_DICT_FIELDS))) {
        LOG_WARN("failed to visit search by items", K(ret));
      } else { /* do nothing*/ }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(visitor.visit(qualify_filters_, SCOPE_QUALIFY_FILTER))) {
      LOG_WARN("failed to visit winfunc exprs", K(ret));
    }
  }
  if (OB_SUCC(ret) && NULL != into_item_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < into_item_->pl_vars_.count(); ++i) {
      if (OB_FAIL(visitor.visit(into_item_->pl_vars_.at(i), SCOPE_SELECT_INTO))) {
        LOG_WARN("failed to visit select into", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectStmt::iterate_rollup_items(ObIArray<ObRollupItem> &rollup_items,
                                       ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < rollup_items.count(); ++i) {
    if (OB_FAIL(iterate_group_items(rollup_items.at(i).rollup_list_exprs_, visitor))) {
      LOG_WARN("failed to visitor rollup list exprs", K(ret));
    }
  }
  return ret;
}

int ObSelectStmt::iterate_cube_items(ObIArray<ObCubeItem> &cube_items, ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cube_items.count(); ++i) {
    if (OB_FAIL(iterate_group_items(cube_items.at(i).cube_list_exprs_, visitor))) {
      LOG_WARN("failed to visitor cube list exprs", K(ret));
    }
  }
  return ret;
}

int ObSelectStmt::iterate_group_items(ObIArray<ObGroupbyExpr> &group_items,
                                      ObStmtExprVisitor &visitor)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < group_items.count(); ++i) {
    if (OB_FAIL(visitor.visit(group_items.at(i).groupby_exprs_, SCOPE_GROUPBY))) {
      LOG_WARN("failed to visit groupby exprs", K(ret));
    }
  }
  return ret;
}


ObSelectStmt::ObSelectStmt()
    : ObDMLStmt(stmt::T_SELECT)
{
  limit_count_expr_  = NULL;
  limit_offset_expr_ = NULL;
  is_distinct_ = false;
  is_nocycle_ = false;
  is_set_distinct_ = false;
  set_op_ = NONE;
  is_recursive_cte_ = false;
  is_breadth_search_ = true;
  is_view_stmt_ = false;
  view_ref_id_ = OB_INVALID_ID;
  select_type_ = AFFECT_FOUND_ROWS;
  into_item_ = NULL;
  is_match_topk_ = false;
  children_swapped_ = false;
  is_from_pivot_ = false;
  check_option_ = VIEW_CHECK_OPTION_NONE;
  contain_ab_param_ = false;
  is_order_siblings_ = false;
  is_hierarchical_query_ = false;
  has_prior_ = false;
  has_reverse_link_ = false;
  is_expanded_mview_ = false;
  is_select_straight_join_ = false;
}

ObSelectStmt::~ObSelectStmt()
{
}

const ObString* ObSelectStmt::get_select_alias(const char *col_name, uint64_t table_id, uint64_t col_id)
{
  int ret = OB_SUCCESS;
  const ObString *alias_name = NULL;
  UNUSED(col_name);
  bool hit = false;
  ObRawExpr *expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !hit && i < get_select_item_size(); i++) {
    expr = get_select_item(i).expr_;
    if (T_REF_ALIAS_COLUMN == expr->get_expr_type()) {
      expr = static_cast<ObAliasRefRawExpr*>(expr)->get_ref_expr();
    }

    if (T_REF_COLUMN != expr->get_expr_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("must be nake column", KPC(expr), K(ret));
    } else {
      ObColumnRefRawExpr *colexpr = static_cast<ObColumnRefRawExpr*>(expr);

      if (colexpr->get_column_id() == col_id && colexpr->get_table_id() == table_id) {
        hit = true;
        LOG_INFO("dump", K(get_select_item(i)));
        if (get_select_item(i).is_real_alias_) {
          alias_name = &get_select_item(i).alias_name_;
        } else {
          alias_name = &get_select_item(i).expr_name_;
        }
      }
    }
  }

  return alias_name;
}

int ObSelectStmt::add_select_item(SelectItem &item)
{
  int ret = OB_SUCCESS;
  if (item.expr_ != NULL) {
    if (item.is_real_alias_) {
      item.expr_->set_alias_column_name(item.alias_name_);
    }
    if (OB_FAIL(select_items_.push_back(item))) {
      LOG_WARN("push back new item failed", K(ret));
    }
  } else {
    ret = OB_ERR_ILLEGAL_ID;
  }
  return ret;
}

/**
 * clear and reset select items for JOIN...USING
 * @param[in] sorted_select_items
 * @return OB_SUCCESS succeed, others fail
 */
int ObSelectStmt::reset_select_item(const common::ObIArray<SelectItem> &sorted_select_items)
{
  int ret = OB_SUCCESS;
  select_items_.reset();
  for (int32_t i = 0; OB_SUCC(ret) && i < sorted_select_items.count(); ++i) {
    select_items_.push_back(sorted_select_items.at(i));
  }

  return ret;
}

int ObSelectStmt::get_child_stmt_size(int64_t &child_size) const
{
  int ret = OB_SUCCESS;
  int64_t tmp_size = 0;
  if (OB_FAIL(ObDMLStmt::get_child_stmt_size(tmp_size))) {
    LOG_WARN("failed to get child stmt size", K(ret));
  } else {
    child_size = tmp_size + set_query_.count();
  }
  return ret;
}

int ObSelectStmt::get_child_stmts(ObIArray<ObSelectStmt*> &child_stmts) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(child_stmts, set_query_))) {
    LOG_WARN("failed to append child query", K(ret));
  } else if (OB_FAIL(ObDMLStmt::get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  return ret;
}

int ObSelectStmt::set_child_stmt(const int64_t child_num, ObSelectStmt* child_stmt)
{
  int ret = OB_SUCCESS;
  if (child_num < set_query_.count()) {
    ret = set_set_query(child_num, child_stmt);
  } else if (OB_FAIL(ObDMLStmt::set_child_stmt(child_num - set_query_.count(), child_stmt))) {
    LOG_WARN("failed to set dml child stmt", K(ret));
  }
  return ret;
}

int ObSelectStmt::set_set_query(const int64_t index, ObSelectStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(index < 0 || index >= set_query_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to set child query", K(ret), K(index), K(set_query_.count()));
  } else {
    set_query_.at(index) = stmt;
  }
  return ret;
}

// input:: ((stmt1) union (stmt2)) union (stmt3)
// output:: stmt1
const ObSelectStmt* ObSelectStmt::get_real_stmt() const
{
  const ObSelectStmt *cur_stmt = this;
  while (OB_NOT_NULL(cur_stmt) && cur_stmt->is_set_stmt()) {
    cur_stmt = cur_stmt->get_set_query(0);
  }
  return cur_stmt;
}

int ObSelectStmt::get_from_subquery_stmts(ObIArray<ObSelectStmt*> &child_stmts) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_stmts.assign(set_query_))) {
    LOG_WARN("failed to assign child query", K(ret));
  } else if (OB_FAIL(ObDMLStmt::get_from_subquery_stmts(child_stmts))) {
    LOG_WARN("get from subquery stmts failed", K(ret));
  }
  return ret;
}

int64_t ObSelectStmt::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  UNUSED(SMART_CALL(do_to_string(buf, buf_len, pos)));
  return pos;
}

int ObSelectStmt::do_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  J_OBJ_START();
  ObArray<ObSelectStmt*> child_stmts;
  if (OB_FAIL(ObDMLStmt::get_child_stmts(child_stmts))) {
    databuff_printf(buf, buf_len, pos, "ERROR get child stmts failed");
  } else if (NONE == set_op_) {
    if (OB_ISNULL(query_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      databuff_printf(buf, buf_len, pos, "ERROR query context is null");
    } else {
      J_KV(
           N_STMT_TYPE, stmt_type_,
           K_(transpose_item),
           N_TABLE, table_items_,
           N_JOINED_TABLE, joined_tables_,
           N_SEMI_INFO, semi_infos_,
           N_PARTITION_EXPR, part_expr_items_,
           N_COLUMN, column_items_,
           N_SELECT, select_items_,
           //muhang
           //"recursive union", is_recursive_cte_,
           N_DISTINCT, is_distinct_,
           N_NOCYCLE, is_nocycle_,
           N_FROM, from_items_,
           N_START_WITH, start_with_exprs_,
           N_CONNECT_BY, connect_by_exprs_,
           //"cte exprs", cte_exprs_,
           N_WHERE, condition_exprs_,
           N_GROUP_BY, group_exprs_,
           N_ROLLUP, rollup_exprs_,
           N_HAVING, having_exprs_,
           N_AGGR_FUNC, agg_items_,
           N_ORDER_BY, order_items_,
           N_LIMIT, limit_count_expr_,
           N_WIN_FUNC, win_func_exprs_,
           N_OFFSET, limit_offset_expr_,
           N_SHOW_STMT_CTX, show_stmt_ctx_,
           N_STMT_HINT, stmt_hint_,
           N_USER_VARS, user_var_exprs_,
           N_QUERY_CTX, *query_ctx_,
           K_(pseudo_column_like_exprs),
           //K_(win_func_exprs),
           K(child_stmts),
           K_(is_hierarchical_query),
           K_(check_option),
           K_(dblink_id),
           K_(is_reverse_link),
           K_(is_expanded_mview)
             );
    }
  } else {
    J_KV(N_SET_OP, ((int)set_op_),
         //"recursive union", is_recursive_cte_,
         //"breadth search ", is_breadth_search_,
         N_DISTINCT, is_set_distinct_,
         K_(set_query),
         N_ORDER_BY, order_items_,
         N_LIMIT, limit_count_expr_,
         N_SELECT, select_items_,
         K(child_stmts),
         K_(dblink_id),
         K_(is_reverse_link));
  }
  J_OBJ_END();
  return ret;
}

int ObSelectStmt::check_and_get_same_aggr_item(ObRawExpr *expr,
                                               ObAggFunRawExpr *&same_aggr)
{
  int ret = OB_SUCCESS;
  same_aggr = NULL;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    bool is_existed = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < agg_items_.count(); ++i) {
      if (OB_ISNULL(agg_items_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (agg_items_.at(i)->same_as(*expr)) {
        is_existed = true;
        same_aggr = agg_items_.at(i);
      }
    }
  }
  return ret;
}

ObWinFunRawExpr *ObSelectStmt::get_same_win_func_item(const ObRawExpr *expr)
{
  ObWinFunRawExpr *win_expr = NULL;
  for (int64_t i = 0; i < win_func_exprs_.count(); ++i) {
    if (win_func_exprs_.at(i) != NULL && expr != NULL &&
        expr->same_as(*win_func_exprs_.at(i))) {
      win_expr = win_func_exprs_.at(i);
      break;
    }
  }
  return win_expr;
}

bool ObSelectStmt::has_for_update() const
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < table_items_.count(); ++i) {
    const TableItem *table_item = table_items_.at(i);
    if (table_item != NULL && table_item->for_update_) {
      bret = true;
    }
  }
  return bret;
}

bool ObSelectStmt::is_skip_locked() const
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < table_items_.count(); ++i) {
    const TableItem *table_item = table_items_.at(i);
    if (table_item != NULL && table_item->skip_locked_) {
      bret = true;
    }
  }
  return bret;
}

int ObSelectStmt::clear_sharable_expr_reference()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::clear_sharable_expr_reference())) {
    LOG_WARN("failed to clear sharable expr reference", K(ret));
  } else {
    ObRawExpr *expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_items_.count(); i++) {
      if (OB_ISNULL(expr = agg_items_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        expr->clear_explicited_referece();
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < win_func_exprs_.count(); i++) {
      if (OB_ISNULL(expr = win_func_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        expr->clear_explicited_referece();
      }
    }
  }
  return ret;
}

int ObSelectStmt::remove_useless_sharable_expr(ObRawExprFactory *expr_factory,
                                               ObSQLSessionInfo *session_info,
                                               bool explicit_for_col)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr_factory)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObDMLStmt::remove_useless_sharable_expr(expr_factory, session_info, explicit_for_col))) {
    LOG_WARN("failed to remove useless sharable expr", K(ret));
  } else {
    ObRawExpr *expr = NULL;
    const bool is_scala = is_scala_group_by();
    for (int64_t i = agg_items_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      if (OB_ISNULL(expr = agg_items_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (expr->is_explicited_reference()) {
        /*do nothing*/
      } else if (OB_FAIL(agg_items_.remove(i))) {
        LOG_WARN("failed to remove agg item", K(ret));
      } else {
        LOG_TRACE("succeed to remove agg items", K(*expr));
      }
    }
    for (int64_t i = win_func_exprs_.count() - 1; OB_SUCC(ret) && i >= 0; i--) {
      if (OB_ISNULL(expr = win_func_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (expr->is_explicited_reference()) {
        /*do nothing*/
      } else if (OB_FAIL(win_func_exprs_.remove(i))) {
        LOG_WARN("failed to remove win func expr", K(ret));
      } else {
        LOG_TRACE("succeed to remove win func exprs", K(*expr));
      }
    }
    if (OB_SUCC(ret) && is_scala && agg_items_.empty()) {
      ObAggFunRawExpr *aggr_expr = NULL;
      if (OB_FAIL(ObRawExprUtils::build_dummy_count_expr(*expr_factory, session_info, aggr_expr))) {
        LOG_WARN("failed to build a dummy expr", K(ret));
      } else if (OB_FAIL(agg_items_.push_back(aggr_expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(set_sharable_expr_reference(*aggr_expr,
                                                     ExplicitedRefType::REF_BY_NORMAL))) {
        LOG_WARN("failed to set sharable exprs reference", K(ret));
      } else {/* do nothing */}
    }
  }
  return ret;
}

const SampleInfo *ObSelectStmt::get_sample_info_by_table_id(uint64_t table_id) const
{
  const SampleInfo *sample_info = nullptr;
  int64_t num = sample_infos_.count();
  for (int64_t i = 0; i < num; ++i) {
    if (sample_infos_.at(i).table_id_ == table_id) {
      sample_info = &sample_infos_.at(i);
      break;
    }
  }
  return sample_info;
}

SampleInfo *ObSelectStmt::get_sample_info_by_table_id(uint64_t table_id)
{
  SampleInfo *sample_info = nullptr;
  int64_t num = sample_infos_.count();
  for (int64_t i = 0; i < num; ++i) {
    if (sample_infos_.at(i).table_id_ == table_id) {
      sample_info = &sample_infos_.at(i);
      break;
    }
  }
  return sample_info;
}

bool ObSelectStmt::is_spj() const
{
  bool has_rownum_expr = false;
  bool ret = !(has_distinct()
               || has_group_by()
               || is_set_stmt()
               || has_rollup()
               || has_order_by()
               || has_limit()
               || get_aggr_item_size() != 0
               || get_from_item_size() == 0
               || is_contains_assignment()
               || has_window_function()
               || has_sequence()
               || is_hierarchical_query());
  if (!ret) {
  } else if (OB_FAIL(has_rownum(has_rownum_expr))) {
    ret = false;
  } else {
    ret = !has_rownum_expr;
  }
  return ret;
}

int ObSelectStmt::get_select_exprs(ObIArray<ObRawExpr*> &select_exprs,
    const bool is_for_outout/* = false*/)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); ++i) {
    if (is_for_outout && select_items_.at(i).is_unpivot_mocked_column_) {
      //continue
    } else if (OB_ISNULL(expr = select_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null select expr", K(ret));
    } else if (OB_FAIL(select_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectStmt::get_select_exprs(ObIArray<ObRawExpr*> &select_exprs,
    const bool is_for_outout/* = false*/) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  LOG_DEBUG("before get_select_exprs", K(select_items_), K(table_items_), K(lbt()));
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); ++i) {
    if (is_for_outout && select_items_.at(i).is_unpivot_mocked_column_) {
      //continue
    } else if (OB_ISNULL(expr = select_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null select expr", K(ret));
    } else if (OB_FAIL(select_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/}
  }
  return ret;
}

int ObSelectStmt::get_select_exprs_without_lob(ObIArray<ObRawExpr*> &select_exprs) const
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); ++i) {
    if (OB_ISNULL(expr = select_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null select expr", K(ret));
    } else if (ObLongTextType == expr->get_data_type() || ObLobType == expr->get_data_type()) {
      /*do nothing*/
    } else if (OB_FAIL(select_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObSelectStmt::get_equal_set_conditions(ObIArray<ObRawExpr *> &conditions,
                                           const bool is_strict,
                                           const bool check_having) const
{
  int ret = OB_SUCCESS;
  if (!(check_having && has_rollup()) &&
      OB_FAIL(ObDMLStmt::get_equal_set_conditions(conditions, is_strict, check_having))) {
    LOG_WARN("failed to get equal set cond", K(ret));
  } else if (!check_having) {
    // do nothing
  } else if (OB_FAIL(append(conditions, having_exprs_))) {
    LOG_WARN("failed to append having exprs", K(ret));
  } else { /* do nothing */ }
  return ret;
}

int ObSelectStmt::get_set_stmt_size(int64_t &size) const
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObSelectStmt *, 8> set_stmts;
  if (is_set_stmt()) {
    for (int64_t i = -1; i < set_stmts.count(); ++i) {
      const ObSelectStmt *stmt = (i == -1) ? this : set_stmts.at(i);
      if (OB_ISNULL(stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret));
      } else if (!stmt->is_set_stmt()) {
        // do nothing
      } else if (OB_FAIL(append(set_stmts, stmt->set_query_))) {
        LOG_WARN("failed to append stmts", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      size = set_stmts.count() + 1;
    }
  }
  return ret;
}

bool ObSelectStmt::check_is_select_item_expr(const ObRawExpr *expr) const
{
  bool bret = false;
  for(int64_t i = 0; !bret && i < select_items_.count(); ++i) {
    if (expr == select_items_.at(i).expr_) {
      bret = true;
    }
  }
  return bret;
}

bool ObSelectStmt::contain_nested_aggr() const
{
  bool ret = false;
  for (int64_t i = 0; !ret && i < agg_items_.count(); i++) {
    if (agg_items_.at(i)->contain_nested_aggr()) {
      ret = true;
    } else { /*do nothing.*/ }
  }
  return ret;
}

int ForUpdateDMLInfo::assign(const ForUpdateDMLInfo& other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  base_table_id_ = other.base_table_id_;
  ref_table_id_ = other.ref_table_id_;
  rowkey_cnt_ = other.rowkey_cnt_;
  is_nullable_ = other.is_nullable_;
  for_update_wait_us_ = other.for_update_wait_us_;
  skip_locked_ = other.skip_locked_;
  if (OB_FAIL(unique_column_ids_.assign(other.unique_column_ids_))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ForUpdateDMLInfo::deep_copy(ObIRawExprCopier &expr_copier,
                                const ForUpdateDMLInfo &other)
{
  int ret = OB_SUCCESS;
  table_id_ = other.table_id_;
  base_table_id_ = other.base_table_id_;
  ref_table_id_ = other.ref_table_id_;
  rowkey_cnt_ = other.rowkey_cnt_;
  is_nullable_ = other.is_nullable_;
  for_update_wait_us_ = other.for_update_wait_us_;
  skip_locked_ = other.skip_locked_;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.unique_column_ids_.count(); ++i) {
    if (OB_FAIL(unique_column_ids_.push_back(other.unique_column_ids_.at(i)))) {
      LOG_WARN("failed to push back column ids", K(ret));
    }
  }
  return ret;
}

int ObGroupingSetsItem::assign(const ObGroupingSetsItem& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(grouping_sets_exprs_.assign(other.grouping_sets_exprs_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(rollup_items_.assign(other.rollup_items_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(cube_items_.assign(other.cube_items_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {/*do nothing*/}
  return ret;
}

int ObGroupingSetsItem::deep_copy(ObIRawExprCopier &expr_copier,
                                  const ObGroupingSetsItem &other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.grouping_sets_exprs_.count(); ++i) {
    ObGroupbyExpr groupby_expr;
    if (OB_FAIL(expr_copier.copy(other.grouping_sets_exprs_.at(i).groupby_exprs_,
                                 groupby_expr.groupby_exprs_))) {
      LOG_WARN("failed to copy exprs", K(ret));
    } else if (OB_FAIL(grouping_sets_exprs_.push_back(groupby_expr))) {
      LOG_WARN("failed to push back group by expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.rollup_items_.count(); ++i) {
    ObRollupItem rollup_item;
    if (OB_FAIL(rollup_item.deep_copy(expr_copier, other.rollup_items_.at(i)))) {
      LOG_WARN("failed to deep copy rollup item", K(ret));
    } else if (OB_FAIL(rollup_items_.push_back(rollup_item))) {
      LOG_WARN("failed to push back rollup item", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.cube_items_.count(); ++i) {
    ObCubeItem cube_item;
    if (OB_FAIL(cube_item.deep_copy(expr_copier, other.cube_items_.at(i)))) {
      LOG_WARN("failed to deep copy cube item", K(ret));
    } else if (OB_FAIL(cube_items_.push_back(cube_item))) {
      LOG_WARN("failed to push back cube item", K(ret));
    } else {/* do nothing */}
  }
  return ret;
}

int ObRollupItem::deep_copy(ObIRawExprCopier &expr_copier, const ObRollupItem &other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.rollup_list_exprs_.count(); ++i) {
    ObGroupbyExpr groupby_expr;
    if (OB_FAIL(expr_copier.copy(other.rollup_list_exprs_.at(i).groupby_exprs_,
                                 groupby_expr.groupby_exprs_))) {
      LOG_WARN("failed to copy exprs", K(ret));
    } else if (OB_FAIL(rollup_list_exprs_.push_back(groupby_expr))) {
      LOG_WARN("failed to push back group by expr", K(ret));
    }
  }
  return ret;
}

int ObCubeItem::deep_copy(ObIRawExprCopier &expr_copier, const ObCubeItem &other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.cube_list_exprs_.count(); ++i) {
    ObGroupbyExpr groupby_expr;
    if (OB_FAIL(expr_copier.copy(other.cube_list_exprs_.at(i).groupby_exprs_,
                                 groupby_expr.groupby_exprs_))) {
      LOG_WARN("failed to copy exprs", K(ret));
    } else if (OB_FAIL(cube_list_exprs_.push_back(groupby_expr))) {
      LOG_WARN("failed to push back group by expr", K(ret));
    }
  }
  return ret;
}

bool ObSelectStmt::is_expr_in_groupings_sets_item(const ObRawExpr *expr) const
{
  bool is_true = false;
  for (int64_t i = 0; !is_true && i < grouping_sets_items_.count(); ++i) {
    const ObIArray<ObGroupbyExpr> &grouping_sets_exprs =
                                                    grouping_sets_items_.at(i).grouping_sets_exprs_;
    for (int64_t j = 0; !is_true && j < grouping_sets_exprs.count(); ++j) {
      is_true = ObOptimizerUtil::find_item(grouping_sets_exprs.at(j).groupby_exprs_, expr);
    }
    if (!is_true) {
      const ObIArray<ObRollupItem> &rollup_items = grouping_sets_items_.at(i).rollup_items_;
      for (int64_t j = 0; !is_true && j < rollup_items.count(); ++j) {
        const ObIArray<ObGroupbyExpr> &rollup_list_exprs = rollup_items.at(j).rollup_list_exprs_;
        for (int64_t k = 0; !is_true && k < rollup_list_exprs.count(); ++k) {
          is_true = ObOptimizerUtil::find_item(rollup_list_exprs.at(k).groupby_exprs_, expr);
        }
      }
    }
    if (!is_true) {
      const ObIArray<ObCubeItem> &cube_items = grouping_sets_items_.at(i).cube_items_;
      for (int64_t j = 0; !is_true && j < cube_items.count(); ++j) {
        const ObIArray<ObGroupbyExpr> &cube_list_exprs = cube_items.at(j).cube_list_exprs_;
        for (int64_t k = 0; !is_true && k < cube_list_exprs.count(); ++k) {
          is_true = ObOptimizerUtil::find_equal_expr(cube_list_exprs.at(k).groupby_exprs_, expr);
        }
      }
    }
  }
  return is_true;
}

int ObSelectStmt::get_connect_by_root_exprs(ObIArray<ObRawExpr *> &connect_by_root_exprs) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> all_exprs;
  if (OB_FAIL(get_relation_exprs(all_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs.count(); ++i) {
    if (OB_FAIL(recursive_get_expr(all_exprs.at(i),
                                   connect_by_root_exprs,
                                   IS_CONNECT_BY_ROOT,
                                   CNT_CONNECT_BY_ROOT))) {
      LOG_WARN("failed to recursive get expr", K(ret));
    }
  }
  return ret;
}

int ObSelectStmt::get_sys_connect_by_path_exprs(ObIArray<ObRawExpr *> &sys_connect_by_path_exprs) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> all_exprs;
  if (OB_FAIL(get_relation_exprs(all_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs.count(); ++i) {
    if (OB_FAIL(recursive_get_expr(all_exprs.at(i),
                                   sys_connect_by_path_exprs,
                                   IS_SYS_CONNECT_BY_PATH,
                                   CNT_SYS_CONNECT_BY_PATH))) {
      LOG_WARN("failed to recursive get expr", K(ret));
    }
  }
  return ret;
}

int ObSelectStmt::get_prior_exprs(common::ObIArray<ObRawExpr *> &prior_exprs) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> all_exprs;
  if (OB_FAIL(get_relation_exprs(all_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs.count(); ++i) {
    if (OB_FAIL(recursive_get_expr(all_exprs.at(i),
                                   prior_exprs,
                                   IS_PRIOR,
                                   CNT_PRIOR))) {
      LOG_WARN("failed to recursive get expr", K(ret));
    }
  }
  return ret;
}

int ObSelectStmt::recursive_get_expr(ObRawExpr *expr,
                                     ObIArray<ObRawExpr *> &exprs,
                                     ObExprInfoFlag target_flag,
                                     ObExprInfoFlag search_flag) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->has_flag(target_flag)) {
    ret = exprs.push_back(expr);
  } else if (expr->has_flag(search_flag)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(recursive_get_expr(expr->get_param_expr(i),
                                                exprs,
                                                target_flag,
                                                search_flag)))) {
        LOG_WARN("failed to recursive get expr", K(ret));
      }
    }
  }
  return ret;
}

int ObSelectStmt::get_connect_by_pseudo_exprs(ObIArray<ObRawExpr*> &pseudo_exprs) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < pseudo_column_like_exprs_.count(); ++i) {
    ObRawExpr *cur_expr = pseudo_column_like_exprs_.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (cur_expr->is_pseudo_column_expr() &&
               static_cast<ObPseudoColumnRawExpr *>(cur_expr)->is_hierarchical_query_type()) {
      if (OB_FAIL(pseudo_exprs.push_back(cur_expr))) {
        LOG_WARN("fail to add pseudo column expr", KPC(cur_expr), K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

bool ObSelectStmt::is_expr_in_rollup_items(const ObRawExpr *expr) const
{
  bool is_true = false;
  for (int64_t i = 0; !is_true && i < rollup_items_.count(); ++i) {
    const ObIArray<ObGroupbyExpr> &rollup_list_exprs = rollup_items_.at(i).rollup_list_exprs_;
    for (int64_t j = 0; !is_true && j < rollup_list_exprs.count(); ++j) {
      is_true = ObOptimizerUtil::find_item(rollup_list_exprs.at(j).groupby_exprs_, expr);
    }
  }
  return is_true;
}

bool ObSelectStmt::is_expr_in_cube_items(const ObRawExpr *expr) const
{
  bool is_true = false;
  for (int64_t i = 0; !is_true && i < cube_items_.count(); ++i) {
    const ObIArray<ObGroupbyExpr> &cube_list_exprs = cube_items_.at(i).cube_list_exprs_;
    for (int64_t j = 0; !is_true && j < cube_list_exprs.count(); ++j) {
      is_true = ObOptimizerUtil::find_equal_expr(cube_list_exprs.at(j).groupby_exprs_, expr);
    }
  }
  return is_true;
}

int ObSelectStmt::contain_hierarchical_query(bool &contain_hie_query) const
{
  int ret = OB_SUCCESS;
  contain_hie_query |= is_hierarchical_query();
  if (!contain_hie_query) {
    ObSEArray<ObSelectStmt*, 16> child_stmts;
    if (OB_FAIL(get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && !contain_hie_query && i < child_stmts.count(); ++i) {
        ObSelectStmt *sub_stmt = child_stmts.at(i);
        if (OB_ISNULL(sub_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sub stmt is null", K(ret));
        } else if (OB_FAIL(SMART_CALL(sub_stmt->contain_hierarchical_query(contain_hie_query)))) {
          LOG_WARN("check sub stmt whether has is table failed", K(ret));
        }
      }
    }
  }
  return ret;
}

bool ObSelectStmt::has_hidden_rowid() const {
  bool res = false;
  for (int64_t i = 0; !res && i < get_select_item_size(); i++) {
    if (select_items_.at(i).is_hidden_rowid_) {
      res = true;
    }
  }
  return res;
}

bool ObSelectStmt::has_external_table() const {
  bool res = false;
  for (int i = 0; i < get_table_items().count(); i++) {
    if (OB_NOT_NULL(get_table_items().at(i))
        && EXTERNAL_TABLE == get_table_items().at(i)->table_type_) {
      res = true;
      break;
    }
  }
  return res;
}
int ObSelectStmt::get_pure_set_exprs(ObIArray<ObRawExpr*> &pure_set_exprs) const
{
  int ret = OB_SUCCESS;
  pure_set_exprs.reuse();
  if (is_set_stmt()) {
    ObRawExpr *select_expr = NULL;
    ObRawExpr *set_op_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < get_select_item_size(); ++i) {
      if (OB_ISNULL(select_expr = get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(i));
      } else if (!select_expr->has_flag(CNT_SET_OP)) {
        /* do nothing, for recursive union all, exists search/cycle pseudo columns*/
      } else if (OB_ISNULL(set_op_expr = get_pure_set_expr(select_expr))
                 || OB_UNLIKELY(!set_op_expr->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", K(ret), K(i), K(*select_expr));
      } else if (OB_FAIL(pure_set_exprs.push_back(set_op_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  return ret;
}

// cast sys functions above set op expr can be:
//   T_FUN_SYS_CAST/T_FUN_SYS_RAWTOHEX/T_FUN_SYS_TO_NCHAR/T_FUN_SYS_TO_CHAR
// use this function to get set op expr from expr child recursively
ObRawExpr* ObSelectStmt::get_pure_set_expr(ObRawExpr *expr)
{
  while (OB_NOT_NULL(expr) && !expr->is_set_op_expr() && 0 < expr->get_param_count()) {
    expr = expr->get_param_expr(0);
  }
  return expr;
}

int ObSelectStmt::get_all_group_by_exprs(ObIArray<ObRawExpr*> &group_by_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(group_by_exprs, group_exprs_))) {
    LOG_WARN("failed to append group exprs");
  } else if (OB_FAIL(append(group_by_exprs, rollup_exprs_))) {
    LOG_WARN("failed to append rollup exprs");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < grouping_sets_items_.count(); ++i) {
      const ObIArray<ObGroupbyExpr> &groupby_exprs = grouping_sets_items_.at(i).grouping_sets_exprs_;
      const ObIArray<ObRollupItem> &rollup_items = grouping_sets_items_.at(i).rollup_items_;
      const ObIArray<ObCubeItem> &cube_items = grouping_sets_items_.at(i).cube_items_;
      for (int64_t j = 0; OB_SUCC(ret) && j < groupby_exprs.count(); ++j) {
        if (OB_FAIL(append(group_by_exprs, groupby_exprs.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < rollup_items.count(); ++j) {
        for (int64_t k = 0; OB_SUCC(ret) && k < rollup_items.at(j).rollup_list_exprs_.count(); ++k) {
          if (OB_FAIL(append(group_by_exprs, rollup_items.at(j).rollup_list_exprs_.at(k).groupby_exprs_))) {
            LOG_WARN("failed to append exprs", K(ret));
          }
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < cube_items.count(); ++j) {
        for (int64_t k = 0; OB_SUCC(ret) && k < cube_items.at(j).cube_list_exprs_.count(); ++k) {
          if (OB_FAIL(append(group_by_exprs, cube_items.at(j).cube_list_exprs_.at(k).groupby_exprs_))) {
            LOG_WARN("failed to append exprs", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rollup_items_.count(); ++i) {
      const ObIArray<ObGroupbyExpr> &rollup_list_exprs = rollup_items_.at(i).rollup_list_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs.count(); ++j) {
        if (OB_FAIL(append(group_by_exprs, rollup_list_exprs.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < cube_items_.count(); ++i) {
      const ObIArray<ObGroupbyExpr> &cube_list_exprs = cube_items_.at(i).cube_list_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && j < cube_list_exprs.count(); ++j) {
        if (OB_FAIL(append(group_by_exprs, cube_list_exprs.at(j).groupby_exprs_))) {
          LOG_WARN("failed to append exprs", K(ret));
        }
      }
    }
  }
  LOG_TRACE("succeed to get all group by exprs", K(group_by_exprs));
  return ret;
}

int ObSelectStmt::check_is_simple_lock_stmt(bool &is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (get_from_item_size() == 0 &&
      get_subquery_expr_size() == 0 &&
      get_select_item_size() > 0 &&
      !has_distinct() &&
      !has_group_by() &&
      !is_set_stmt() &&
      !has_rollup() &&
      !has_order_by() &&
      get_aggr_item_size() == 0 &&
      !is_contains_assignment() &&
      !has_window_function() &&
      !has_sequence() &&
      !is_hierarchical_query()) {
    bool contain_lock_expr = false;
    for (int64_t i = 0; !contain_lock_expr && i < select_items_.count(); i ++) {
      if (OB_FAIL(ObRawExprUtils::check_contain_lock_exprs(select_items_.at(i).expr_, contain_lock_expr))) {
        LOG_WARN("failed to check contain lock exprs", K(ret));
      }
    }
    is_valid = contain_lock_expr;
  }
  return ret;
}