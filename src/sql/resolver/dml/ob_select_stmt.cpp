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

int SelectItem::deep_copy(ObRawExprFactory& expr_factory, const SelectItem& other, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  ObRawExpr* temp_expr = NULL;
  ObRawExpr* temp_default_value_expr = NULL;
  if (OB_FAIL(ObRawExprUtils::copy_expr(expr_factory, other.expr_, temp_expr, copy_types))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::copy_expr(
                 expr_factory, other.default_value_expr_, temp_default_value_expr, copy_types))) {
    LOG_WARN("failed to copy expr", K(ret));
  } else {
    expr_ = temp_expr;
    default_value_expr_ = temp_default_value_expr;
    is_real_alias_ = other.is_real_alias_;
    alias_name_ = other.alias_name_;
    paramed_alias_name_ = other.paramed_alias_name_;
    expr_name_ = other.expr_name_;
    default_value_ = other.default_value_;
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

bool ObSelectStmt::check_table_be_modified(uint64_t ref_table_id) const
{
  bool is_exists = false;
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && !is_exists && i < table_items_.count(); ++i) {
    TableItem* table_item = table_items_.at(i);
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
        ObSelectStmt* sub_stmt = child_stmts.at(i);
        if (OB_ISNULL(sub_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("sub stmt is null", K(ret));
        } else if (OB_FAIL(sub_stmt->check_table_be_modified(ref_table_id))) {
          LOG_WARN("check sub stmt whether has select for update failed", K(ret), K(i));
        }
      }
    }
  }
  return is_exists;
}

bool ObSelectStmt::has_distinct_or_concat_agg() const
{
  bool has = false;
  for (int64_t i = 0; !has && i < get_aggr_item_size(); ++i) {
    const ObAggFunRawExpr* aggr = get_aggr_item(i);
    if (NULL != aggr) {
      has = aggr->is_param_distinct() || T_FUN_GROUP_CONCAT == aggr->get_expr_type();
    }
  }
  return has;
}

int ObSelectStmt::add_window_func_expr(ObWinFunRawExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(win_func_exprs_.push_back(expr))) {
    LOG_WARN("failed to add expr", K(ret));
  } else {
    expr->set_expr_level(current_level_);
    expr->set_explicited_reference();
  }
  return ret;
}

int ObSelectStmt::remove_window_func_expr(ObWinFunRawExpr* expr)
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
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectStmt::assign(const ObSelectStmt& other)
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
    LOG_WARN("assgin other grouping sets expr failed", K(ret));
  } else if (OB_FAIL(multi_rollup_items_.assign(other.multi_rollup_items_))) {
    LOG_WARN("assgin other grouping sets expr failed", K(ret));
  } else if (OB_FAIL(having_exprs_.assign(other.having_exprs_))) {
    LOG_WARN("assign other having exprs failed", K(ret));
  } else if (OB_FAIL(agg_items_.assign(other.agg_items_))) {
    LOG_WARN("assign other aggr items failed", K(ret));
  } else if (OB_FAIL(start_with_exprs_.assign(other.start_with_exprs_))) {
    LOG_WARN("assign other start with failed", K(ret));
  } else if (OB_FAIL(win_func_exprs_.assign(other.win_func_exprs_))) {
    LOG_WARN("assign window function exprs failed", K(ret));
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
    LOG_WARN("assign sample scan infos failed", K(ret));
  } else {
    set_op_ = other.set_op_;
    is_recursive_cte_ = other.is_recursive_cte_;
    is_breadth_search_ = other.is_breadth_search_;
    generated_cte_count_ = other.generated_cte_count_;
    is_distinct_ = other.is_distinct_;
    has_rollup_ = other.has_rollup_;
    is_from_pivot_ = other.is_from_pivot_;
    has_hidden_rowid_ = other.has_hidden_rowid_;
    has_grouping_ = other.has_grouping_;
    has_grouping_sets_ = other.has_grouping_sets_;
    is_view_stmt_ = other.is_view_stmt_;
    view_ref_id_ = other.view_ref_id_;
    is_select_star_ = other.is_select_star_;
    is_match_topk_ = other.is_match_topk_;
    is_nocycle_ = other.is_nocycle_;
    is_parent_set_distinct_ = other.is_parent_set_distinct();
    is_set_distinct_ = other.is_set_distinct();
    show_stmt_ctx_.assign(other.show_stmt_ctx_);
    select_type_ = other.select_type_;
    base_table_id_ = other.base_table_id_;
    depend_table_id_ = other.depend_table_id_;
    having_has_self_column_ = other.having_has_self_column_;
    into_item_ = other.into_item_;
    children_swapped_ = other.children_swapped_;
    temp_table_info_ = other.temp_table_info_;
    need_temp_table_trans_ = other.need_temp_table_trans_;
    is_last_access_ = other.is_last_access_;
  }
  return ret;
}

int ObSelectStmt::deep_copy_stmt_struct(
    ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt& input)
{
  int ret = OB_SUCCESS;
  const ObSelectStmt& other = static_cast<const ObSelectStmt&>(input);
  if (OB_UNLIKELY(input.get_stmt_type() != get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt type does not match", K(ret));
  } else {
    set_query_.reuse();
    ObDMLStmt* child_query = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < other.set_query_.count(); i++) {
      if (OB_FAIL(ObTransformUtils::deep_copy_stmt(stmt_factory, expr_factory, other.set_query_.at(i), child_query))) {
        LOG_WARN("failed to deep copy stmt", K(ret));
      } else if (OB_FAIL(set_query_.push_back(static_cast<ObSelectStmt*>(child_query)))) {
        LOG_WARN("failed to push back stmt", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDMLStmt::deep_copy_stmt_struct(stmt_factory, expr_factory, other))) {
      LOG_WARN("deep copy DML stmt failed", K(ret));
    } else if (OB_FAIL(
                   ObTransformUtils::deep_copy_column_items(expr_factory, other.cycle_by_items_, cycle_by_items_))) {
      LOG_WARN("deep copy cycle by item failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.group_exprs_, group_exprs_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy group expr failed", K(ret));
    } else if (OB_FAIL(
                   ObRawExprUtils::copy_exprs(expr_factory, other.rollup_exprs_, rollup_exprs_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy rollup expr failed", K(ret));
    } else if (OB_FAIL(
                   ObRawExprUtils::copy_exprs(expr_factory, other.having_exprs_, having_exprs_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy having expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.agg_items_, agg_items_, COPY_REF_SHARED))) {
      LOG_WARN("deep copy agg item failed", K(ret));
    } else if (OB_FAIL(
                   ObRawExprUtils::copy_exprs(expr_factory, other.win_func_exprs_, win_func_exprs_, COPY_REF_SHARED))) {
      LOG_WARN("deep copy window function expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                   expr_factory, other.start_with_exprs_, start_with_exprs_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy start with exprs failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                   expr_factory, other.connect_by_exprs_, connect_by_exprs_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy connect by expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_exprs(
                   expr_factory, other.connect_by_prior_exprs_, connect_by_prior_exprs_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy connect by prior expr failed", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::copy_exprs(expr_factory, other.cte_exprs_, cte_exprs_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy ctx exprs failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_order_items(
                   expr_factory, other.search_by_items_, search_by_items_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy search by items failed", K(ret));
    } else if (OB_FAIL(rollup_directions_.assign(other.rollup_directions_))) {
      LOG_WARN("assign rollup directions failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_select_items(
                   expr_factory, other.select_items_, select_items_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy select items failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_order_items(
                   expr_factory, other.order_items_, order_items_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy order items failed", K(ret));
    } else if (OB_FAIL(sample_infos_.assign(other.sample_infos_))) {
      LOG_WARN("failed to assign sample infos", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_grouping_sets_items(
                   expr_factory, other.grouping_sets_items_, grouping_sets_items_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy grouping sets items failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::deep_copy_multi_rollup_items(
                   expr_factory, other.multi_rollup_items_, multi_rollup_items_, COPY_REF_DEFAULT))) {
      LOG_WARN("deep copy grouping sets items failed", K(ret));
    } else {
      set_op_ = other.set_op_;
      is_recursive_cte_ = other.is_recursive_cte_;
      is_breadth_search_ = other.is_breadth_search_;
      generated_cte_count_ = other.generated_cte_count_;
      is_distinct_ = other.is_distinct_;
      has_rollup_ = other.has_rollup_;
      is_from_pivot_ = other.is_from_pivot_;
      has_hidden_rowid_ = other.has_hidden_rowid_;
      has_grouping_sets_ = other.has_grouping_sets_;
      has_grouping_ = other.has_grouping_;
      is_nocycle_ = other.is_nocycle_;
      is_view_stmt_ = other.is_view_stmt_;
      view_ref_id_ = other.view_ref_id_;
      is_select_star_ = other.is_select_star_;
      is_match_topk_ = other.is_match_topk_;
      is_parent_set_distinct_ = other.is_parent_set_distinct_;
      is_set_distinct_ = other.is_set_distinct_;
      show_stmt_ctx_.assign(other.show_stmt_ctx_);
      base_table_id_ = other.base_table_id_;
      depend_table_id_ = other.depend_table_id_;
      having_has_self_column_ = other.having_has_self_column_;
      select_type_ = other.select_type_;
      children_swapped_ = other.children_swapped_;
      is_fetch_with_ties_ = other.is_fetch_with_ties_;
      temp_table_info_ = other.temp_table_info_;
      need_temp_table_trans_ = other.need_temp_table_trans_;
      is_last_access_ = other.is_last_access_;
      // copy insert into statement
      if (OB_SUCC(ret) && NULL != other.into_item_) {
        ObSelectIntoItem* temp_into_item = NULL;
        void* ptr = stmt_factory.get_allocator().alloc(sizeof(ObSelectIntoItem));
        if (OB_ISNULL(ptr)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate select into item", K(ret));
        } else {
          temp_into_item = new (ptr) ObSelectIntoItem();
          if (OB_FAIL(temp_into_item->assign(*other.into_item_))) {
            LOG_WARN("deep copy into item failed", K(ret));
          } else {
            into_item_ = temp_into_item;
          }
        }
      }
    }
  }
  return ret;
}

int ObSelectStmt::create_select_list_for_set_stmt(ObRawExprFactory& expr_factory)
{
  int ret = OB_SUCCESS;
  SelectItem new_select_item;
  ObExprResType res_type;
  ObSelectStmt* child_stmt = NULL;
  if (OB_ISNULL(child_stmt = get_set_query(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret), K(child_stmt), K(get_set_op()));
  } else {
    int64_t num = child_stmt->get_select_item_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < num; i++) {
      SelectItem& child_select_item = child_stmt->get_select_item(i);
      // unused
      // ObString set_column_name = left_select_item.alias_name_;
      ObItemType set_op_type = static_cast<ObItemType>(T_OP_SET + get_set_op());
      res_type.reset();
      new_select_item.alias_name_ = child_select_item.alias_name_;
      new_select_item.expr_name_ = child_select_item.expr_name_;
      new_select_item.default_value_ = child_select_item.default_value_;
      new_select_item.default_value_expr_ = child_select_item.default_value_expr_;
      new_select_item.is_real_alias_ = true;
      res_type = child_select_item.expr_->get_result_type();
      if (OB_FAIL(
              ObRawExprUtils::make_set_op_expr(expr_factory, i, set_op_type, res_type, NULL, new_select_item.expr_))) {
        LOG_WARN("create set op expr failed", K(ret));
      } else if (OB_FAIL(add_select_item(new_select_item))) {
        LOG_WARN("push back set select item failed", K(ret));
      } else if (OB_ISNULL(new_select_item.expr_) || OB_UNLIKELY(!new_select_item.expr_->is_set_op_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null or is not set op expr", "set op", PC(new_select_item.expr_));
      } else {
        new_select_item.expr_->set_expr_level(child_stmt->get_current_level());
      }
    }
  }
  return ret;
}

int ObSelectStmt::update_stmt_table_id(const ObSelectStmt& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::update_stmt_table_id(other))) {
    LOG_WARN("failed to update stmt table id", K(ret));
  } else if (OB_UNLIKELY(set_query_.count() != other.set_query_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected child query count", K(ret), K(set_query_.count()), K(other.set_query_.count()));
  } else {
    ObSelectStmt* child_query = NULL;
    ObSelectStmt* other_child_query = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < set_query_.count(); i++) {
      if (OB_ISNULL(other_child_query = other.set_query_.at(i)) || OB_ISNULL(child_query = set_query_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null statement", K(ret), K(child_query), K(other_child_query));
      } else if (OB_FAIL(SMART_CALL(child_query->update_stmt_table_id(*other_child_query)))) {
        LOG_WARN("failed to update stmt table id", K(ret));
      } else { /* do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectStmt::replace_inner_stmt_expr(
    const ObIArray<ObRawExpr*>& other_exprs, const ObIArray<ObRawExpr*>& new_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::replace_inner_stmt_expr(other_exprs, new_exprs))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, group_exprs_))) {
    LOG_WARN("failed to replace group exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, rollup_exprs_))) {
    LOG_WARN("failed to replace rollup exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, having_exprs_))) {
    LOG_WARN("failed to replace having exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, agg_items_))) {
    LOG_WARN("failed to replace aggregation items", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, win_func_exprs_))) {
    LOG_WARN("failed to replace window function exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, start_with_exprs_))) {
    LOG_WARN("failed to replace start with exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, connect_by_exprs_))) {
    LOG_WARN("failed to replace connect by exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, connect_by_prior_exprs_))) {
    LOG_WARN("failed to replace connect by prior exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(other_exprs, new_exprs, cte_exprs_))) {
    LOG_WARN("failed to replace ctx exprs", K(ret));
  } else if (OB_FAIL(replace_multi_rollup_items_expr(other_exprs, new_exprs, multi_rollup_items_))) {
    LOG_WARN("failed to replace multi rollup items expr", K(ret));
  } else {
    // replace exprs of gs exprs.
    for (int64_t i = 0; OB_SUCC(ret) && i < grouping_sets_items_.count(); i++) {
      ObIArray<ObGroupbyExpr>& grouping_sets = grouping_sets_items_.at(i).grouping_sets_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && j < grouping_sets.count(); j++) {
        ObGroupbyExpr& groupby_item = grouping_sets.at(j);
        for (int64_t k = 0; OB_SUCC(ret) && k < groupby_item.groupby_exprs_.count(); k++) {
          if (OB_ISNULL(groupby_item.groupby_exprs_.at(k))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null expr", K(ret));
          } else if (OB_FAIL(
                         ObTransformUtils::replace_expr(other_exprs, new_exprs, groupby_item.groupby_exprs_.at(k)))) {
            LOG_WARN("failed to replace column expr", K(ret));
          } else { /* do nothing*/
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(replace_multi_rollup_items_expr(
                other_exprs, new_exprs, grouping_sets_items_.at(i).multi_rollup_items_))) {
          LOG_WARN("failed to replace multi rollup items expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
    // replace column exprs of search by item
    for (int64_t i = 0; OB_SUCC(ret) && i < search_by_items_.count(); i++) {
      if (OB_ISNULL(search_by_items_.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, search_by_items_.at(i).expr_))) {
        LOG_WARN("failed to replace column expr", K(ret));
      } else { /* do nothing*/
      }
    }
    // replace columns exprs of select items
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); i++) {
      if (OB_ISNULL(select_items_.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, select_items_.at(i).expr_))) {
        LOG_WARN("failed to replace column expr", K(ret));
      } else if (OB_FAIL(
                     ObTransformUtils::replace_expr(other_exprs, new_exprs, select_items_.at(i).default_value_expr_))) {
        LOG_WARN("failed to replace column expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObSelectStmt::replace_multi_rollup_items_expr(const ObIArray<ObRawExpr*>& other_exprs,
    const ObIArray<ObRawExpr*>& new_exprs, ObIArray<ObMultiRollupItem>& multi_rollup_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_rollup_items.count(); i++) {
    ObIArray<ObGroupbyExpr>& rollup_list_exprs = multi_rollup_items.at(i).rollup_list_exprs_;
    for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs.count(); j++) {
      ObGroupbyExpr& groupby_item = rollup_list_exprs.at(j);
      for (int64_t k = 0; OB_SUCC(ret) && k < groupby_item.groupby_exprs_.count(); k++) {
        if (OB_ISNULL(groupby_item.groupby_exprs_.at(k))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, groupby_item.groupby_exprs_.at(k)))) {
          LOG_WARN("failed to replace column expr", K(ret));
        } else { /* do nothing*/
        }
      }
    }
  }
  return ret;
}

ObSelectStmt::ObSelectStmt::ObSelectStmt() : ObDMLStmt(stmt::T_SELECT)
{
  limit_count_expr_ = NULL;
  limit_offset_expr_ = NULL;
  is_distinct_ = false;
  has_rollup_ = false;
  has_grouping_ = false;
  has_grouping_sets_ = false;
  is_nocycle_ = false;
  is_parent_set_distinct_ = false;
  is_set_distinct_ = false;
  set_op_ = NONE;
  is_recursive_cte_ = false;
  is_breadth_search_ = true;
  generated_cte_count_ = 0;
  is_view_stmt_ = false;
  view_ref_id_ = OB_INVALID_ID;
  select_type_ = AFFECT_FOUND_ROWS;
  is_select_star_ = false;
  into_item_ = NULL;
  is_match_topk_ = false;
  base_table_id_ = OB_INVALID_ID;
  depend_table_id_ = OB_INVALID_ID;
  having_has_self_column_ = false;
  children_swapped_ = false;
  is_from_pivot_ = false;
  has_hidden_rowid_ = false;
  temp_table_info_ = NULL;
  need_temp_table_trans_ = false;
  is_last_access_ = false;
}

ObSelectStmt::~ObSelectStmt()
{}

const ObString* ObSelectStmt::get_select_alias(const char* col_name, uint64_t table_id, uint64_t col_id)
{
  int ret = OB_SUCCESS;
  const ObString* alias_name = NULL;
  UNUSED(col_name);
  bool hit = false;
  ObRawExpr* expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && !hit && i < get_select_item_size(); i++) {
    expr = get_select_item(i).expr_;
    if (T_REF_ALIAS_COLUMN == expr->get_expr_type()) {
      expr = static_cast<ObAliasRefRawExpr*>(expr)->get_ref_expr();
    }

    if (T_REF_COLUMN != expr->get_expr_type()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("must be nake column", K(ret));
    } else {
      ObColumnRefRawExpr* colexpr = static_cast<ObColumnRefRawExpr*>(expr);

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

int ObSelectStmt::add_select_item(SelectItem& item)
{
  int ret = OB_SUCCESS;
  if (item.expr_ != NULL) {
    if (item.expr_->get_expr_level() < 0) {
      item.expr_->set_expr_level(current_level_);
    }
    if (item.is_real_alias_) {
      item.expr_->set_alias_column_name(item.alias_name_);
    }
    ColumnItem* col_item = NULL;
    if (item.expr_->is_column_ref_expr()) {
      ObColumnRefRawExpr* col_ref = static_cast<ObColumnRefRawExpr*>(item.expr_);
      if (NULL != (col_item = get_column_item_by_id(col_ref->get_table_id(), col_ref->get_column_id()))) {
        item.default_value_ = col_item->default_value_;
        item.default_value_expr_ = col_item->default_value_expr_;
      }
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
int ObSelectStmt::reset_select_item(const common::ObIArray<SelectItem>& sorted_select_items)
{
  int ret = OB_SUCCESS;
  select_items_.reset();
  for (int32_t i = 0; OB_SUCC(ret) && i < sorted_select_items.count(); ++i) {
    select_items_.push_back(sorted_select_items.at(i));
  }

  return ret;
}

int ObSelectStmt::get_child_stmt_size(int64_t& child_size) const
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

int ObSelectStmt::get_child_stmts(ObIArray<ObSelectStmt*>& child_stmts) const
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

int ObSelectStmt::set_set_query(const int64_t index, ObSelectStmt* stmt)
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
  int ret = OB_SUCCESS;
  const ObSelectStmt* cur_stmt = this;
  while (OB_NOT_NULL(cur_stmt) && cur_stmt->has_set_op()) {
    cur_stmt = cur_stmt->get_set_query(0);
  }
  return cur_stmt;
}

int ObSelectStmt::get_from_subquery_stmts(ObIArray<ObSelectStmt*>& child_stmts) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(child_stmts.assign(set_query_))) {
    LOG_WARN("failed to assign child query", K(ret));
  } else if (OB_FAIL(ObDMLStmt::get_from_subquery_stmts(child_stmts))) {
    LOG_WARN("get from subquery stmts failed", K(ret));
  }
  return ret;
}

int64_t ObSelectStmt::to_string(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  SMART_CALL(do_to_string(buf, buf_len, pos));
  return pos;
}

int ObSelectStmt::do_to_string(char* buf, const int64_t buf_len, int64_t& pos) const
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
      J_KV(N_STMT_TYPE,
          stmt_type_,
          K_(transpose_item),
          N_TABLE,
          table_items_,
          N_JOINED_TABLE,
          joined_tables_,
          N_SEMI_INFO,
          semi_infos_,
          N_PARTITION_EXPR,
          part_expr_items_,
          N_COLUMN,
          column_items_,
          N_SELECT,
          select_items_,
          //"recursive union", is_recursive_cte_,
          N_DISTINCT,
          is_distinct_,
          N_ROLLUP,
          has_rollup_,
          N_NOCYCLE,
          is_nocycle_,
          N_FROM,
          from_items_,
          N_START_WITH,
          start_with_exprs_,
          N_CONNECT_BY,
          connect_by_exprs_,
          //"cte exprs", cte_exprs_,
          N_WHERE,
          condition_exprs_,
          N_GROUP_BY,
          group_exprs_,
          N_ROLLUP,
          rollup_exprs_,
          N_HAVING,
          having_exprs_,
          N_AGGR_FUNC,
          agg_items_,
          N_ORDER_BY,
          order_items_,
          N_LIMIT,
          limit_count_expr_,
          N_WIN_FUNC,
          win_func_exprs_,
          N_OFFSET,
          limit_offset_expr_,
          N_SHOW_STMT_CTX,
          show_stmt_ctx_,
          N_QUERY_HINT,
          stmt_hint_,
          N_USER_VARS,
          user_var_exprs_,
          N_QUERY_CTX,
          *query_ctx_,
          K_(current_level),
          K_(pseudo_column_like_exprs),
          // K_(win_func_exprs),
          K(child_stmts),
          K_(is_hierarchical_query));
    }
  } else {
    J_KV(N_SET_OP,
        ((int)set_op_),
        //"recursive union", is_recursive_cte_,
        //"breadth search ", is_breadth_search_,
        N_DISTINCT,
        is_set_distinct_,
        K_(set_query),
        N_ORDER_BY,
        order_items_,
        N_LIMIT,
        limit_count_expr_,
        N_SELECT,
        select_items_,
        K(child_stmts));
  }
  J_OBJ_END();
  return ret;
}

int ObSelectStmt::check_and_get_same_aggr_item(ObRawExpr* expr, ObAggFunRawExpr*& same_aggr)
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

ObWinFunRawExpr* ObSelectStmt::get_same_win_func_item(const ObRawExpr* expr)
{
  ObWinFunRawExpr* win_expr = NULL;
  for (int64_t i = 0; i < win_func_exprs_.count(); ++i) {
    if (win_func_exprs_.at(i) != NULL && expr != NULL && expr->same_as(*win_func_exprs_.at(i))) {
      win_expr = win_func_exprs_.at(i);
      break;
    }
  }
  return win_expr;
}

int ObSelectStmt::inner_get_relation_exprs(RelExprCheckerBase& expr_checker)
{
  int ret = OB_SUCCESS;
  // do not add agg expr and window function expr
  if (!expr_checker.is_ignore(RelExprCheckerBase::FIELD_LIST_SCOPE)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); ++i) {
      if (OB_FAIL(expr_checker.add_expr(select_items_.at(i).expr_))) {
        LOG_WARN("failed to add exprs", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::GROUP_SCOPE)) {
    if (OB_FAIL(expr_checker.add_exprs(group_exprs_))) {
      LOG_WARN("failed to add group by exprs", K(ret));
    } else if (OB_FAIL(expr_checker.add_exprs(rollup_exprs_))) {
      LOG_WARN("failed to add rollup exprs", K(ret));
    } else { /*do nothing*/
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < grouping_sets_items_.count(); i++) {
        ObIArray<ObGroupbyExpr>& grouping_sets = grouping_sets_items_.at(i).grouping_sets_exprs_;
        for (int64_t j = 0; OB_SUCC(ret) && j < grouping_sets.count(); j++) {
          if (OB_FAIL(expr_checker.add_exprs(grouping_sets.at(j).groupby_exprs_))) {
            LOG_WARN("failed to add expr to expr checker.", K(ret));
          } else { /*do nothing.*/
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(get_relation_exprs_from_multi_rollup_items(
                  expr_checker, grouping_sets_items_.at(i).multi_rollup_items_))) {
            LOG_WARN("failed to get relation exprs from multi rollup items.", K(ret));
          } else { /*do nothing*/
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(get_relation_exprs_from_multi_rollup_items(expr_checker, multi_rollup_items_))) {
          LOG_WARN("failed to get relation exprs from multi rollup items.", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }

  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::HAVING_SCOPE)) {
    if (OB_FAIL(expr_checker.add_exprs(having_exprs_))) {
      LOG_WARN("failed to add exprs", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::START_WITH_SCOPE)) {
    if (OB_FAIL(expr_checker.add_exprs(start_with_exprs_))) {
      LOG_WARN("failed to add exprs", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret) && !expr_checker.is_ignore(RelExprCheckerBase::CONNECT_BY_SCOPE)) {
    if (OB_FAIL(expr_checker.add_exprs(connect_by_exprs_))) {
      LOG_WARN("failed to add exprs", K(ret));
    } else { /*do nothing*/
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObDMLStmt::inner_get_relation_exprs(expr_checker))) {
      LOG_WARN("failed to add exprs", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSelectStmt::get_relation_exprs_from_multi_rollup_items(
    RelExprCheckerBase& expr_checker, ObIArray<ObMultiRollupItem>& multi_rollup_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_rollup_items.count(); i++) {
    ObIArray<ObGroupbyExpr>& rollup_list_exprs = multi_rollup_items.at(i).rollup_list_exprs_;
    for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs.count(); j++) {
      if (OB_FAIL(expr_checker.add_exprs(rollup_list_exprs.at(j).groupby_exprs_))) {
        LOG_WARN("faile dot add expr to expr checker.", K(ret));
      } else { /*do nothing.*/
      }
    }
  }
  return ret;
}

int ObSelectStmt::replace_expr_in_stmt(ObRawExpr* from, ObRawExpr* to)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); ++i) {
    if (select_items_.at(i).expr_ == from) {
      select_items_.at(i).expr_ = to;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < group_exprs_.count(); ++i) {
    if (group_exprs_.at(i) == from) {
      group_exprs_.at(i) = to;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs_.count(); ++i) {
    if (rollup_exprs_.at(i) == from) {
      rollup_exprs_.at(i) = to;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < grouping_sets_items_.count(); i++) {
    ObIArray<ObGroupbyExpr>& grouping_sets = grouping_sets_items_.at(i).grouping_sets_exprs_;
    for (int64_t j = 0; OB_SUCC(ret) && j < grouping_sets.count(); j++) {
      for (int64_t k = 0; OB_SUCC(ret) && k < grouping_sets.at(j).groupby_exprs_.count(); k++) {
        if (grouping_sets.at(j).groupby_exprs_.at(k) == from) {
          grouping_sets.at(j).groupby_exprs_.at(k) = to;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(replace_expr_in_multi_rollup_items(from, to, grouping_sets_items_.at(i).multi_rollup_items_))) {
        LOG_WARN("failed to replace expr in multi rollup items", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replace_expr_in_multi_rollup_items(from, to, multi_rollup_items_))) {
      LOG_WARN("failed to replace expr in multi rollup items", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs_.count(); ++i) {
    if (having_exprs_.at(i) == from) {
      having_exprs_.at(i) = to;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObDMLStmt::replace_expr_in_stmt(from, to))) {
    LOG_WARN("replace expr in stmt failed", K(ret));
  }
  return ret;
}

int ObSelectStmt::replace_expr_in_multi_rollup_items(
    ObRawExpr* from, ObRawExpr* to, ObIArray<ObMultiRollupItem>& multi_rollup_items)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < multi_rollup_items.count(); i++) {
    ObIArray<ObGroupbyExpr>& rollup_list_exprs = multi_rollup_items.at(i).rollup_list_exprs_;
    for (int64_t j = 0; OB_SUCC(ret) && j < rollup_list_exprs.count(); j++) {
      for (int64_t k = 0; OB_SUCC(ret) && k < rollup_list_exprs.at(j).groupby_exprs_.count(); k++) {
        if (rollup_list_exprs.at(j).groupby_exprs_.at(k) == from) {
          rollup_list_exprs.at(j).groupby_exprs_.at(k) = to;
        }
      }
    }
  }
  return ret;
}

int ObSelectStmt::adjust_view_parent_namespace_stmt(ObDMLStmt* new_parent)
{
  int ret = OB_SUCCESS;
  int32_t subquery_level = (new_parent != NULL ? new_parent->get_current_level() + 1 : 0);
  ObArray<ObSelectStmt*> view_stmts;
  set_parent_namespace_stmt(new_parent);
  set_current_level(subquery_level);
  if (OB_FAIL(get_from_subquery_stmts(view_stmts))) {
    LOG_WARN("get from subquery stmts failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < view_stmts.count(); ++i) {
    ObSelectStmt* view_stmt = view_stmts.at(i);
    if (OB_ISNULL(view_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_item is null", K(i));
    } else if (OB_FAIL(view_stmt->adjust_view_parent_namespace_stmt(new_parent))) {
      LOG_WARN("adjust view parent namespace stmt failed", K(ret));
    }
  }
  return ret;
}

bool ObSelectStmt::has_for_update() const
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < table_items_.count(); ++i) {
    const TableItem* table_item = table_items_.at(i);
    if (table_item != NULL && table_item->for_update_) {
      bret = true;
    }
  }
  return bret;
}

int ObSelectStmt::has_special_expr(const ObExprInfoFlag flag, bool& has) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObDMLStmt::has_special_expr(flag, has))) {
    LOG_WARN("failed to check rownum in stmt", K(ret));
  } else if (!has) {
    // select statement
    for (int64_t i = 0; OB_SUCC(ret) && (!has) && i < select_items_.count(); i++) {
      ObRawExpr* expr = select_items_.at(i).expr_;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select item expr is null", K(ret));
      } else if (expr->has_flag(flag)) {
        has = true;
      }
    }
    // group by statement
    for (int64_t i = 0; OB_SUCC(ret) && (!has) && i < group_exprs_.count(); i++) {
      ObRawExpr* expr = group_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group by expr is null", K(ret));
      } else if (expr->has_flag(flag)) {
        has = true;
      }
    }
    // rollup statement
    for (int64_t i = 0; OB_SUCC(ret) && (!has) && i < rollup_exprs_.count(); i++) {
      ObRawExpr* expr = rollup_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("rollup expr is null", K(ret));
      } else if (expr->has_flag(flag)) {
        has = true;
      }
    }
    // grouping sets statement
    for (int64_t i = 0; OB_SUCC(ret) && (!has) && i < grouping_sets_items_.count(); i++) {
      const ObIArray<ObGroupbyExpr>& grouping_sets_exprs = grouping_sets_items_.at(i).grouping_sets_exprs_;
      for (int64_t j = 0; OB_SUCC(ret) && (!has) && j < grouping_sets_exprs.count(); j++) {
        const ObGroupbyExpr& groupby_item = grouping_sets_exprs.at(j);
        for (int64_t k = 0; OB_SUCC(ret) && (!has) && k < groupby_item.groupby_exprs_.count(); k++) {
          const ObRawExpr* expr = groupby_item.groupby_exprs_.at(k);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("grouping sets expr is null", K(ret));
          } else if (expr->has_flag(flag)) {
            has = true;
          }
        }
      }
      if (OB_SUCC(ret) && !has) {
        if (OB_FAIL(
                has_special_expr_in_multi_rollup_items(grouping_sets_items_.at(i).multi_rollup_items_, flag, has))) {
          LOG_WARN("failed to has special expr in multi rollup items");
        }
      }
    }
    // multi rollup statement
    if (OB_SUCC(ret) && !has) {
      if (OB_FAIL(has_special_expr_in_multi_rollup_items(multi_rollup_items_, flag, has))) {
        LOG_WARN("failed to has special expr in multi rollup items");
      }
    }
    // having statement
    for (int64_t i = 0; OB_SUCC(ret) && (!has) && i < having_exprs_.count(); i++) {
      ObRawExpr* expr = having_exprs_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("having expr is null", K(ret));
      } else if (expr->has_flag(flag)) {
        has = true;
      }
    }
    // aggregation statement
    for (int64_t i = 0; OB_SUCC(ret) && (!has) && i < agg_items_.count(); i++) {
      ObAggFunRawExpr* expr = agg_items_.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("agg item expr is null", K(ret));
      } else if (expr->has_flag(flag)) {
        has = true;
      }
    }
  }
  return ret;
}

int ObSelectStmt::has_special_expr_in_multi_rollup_items(
    const ObIArray<ObMultiRollupItem>& multi_rollup_items, const ObExprInfoFlag flag, bool& has) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (!has) && i < multi_rollup_items.count(); i++) {
    const ObIArray<ObGroupbyExpr>& rollup_list_exprs = multi_rollup_items.at(i).rollup_list_exprs_;
    for (int64_t j = 0; OB_SUCC(ret) && (!has) && j < rollup_list_exprs.count(); j++) {
      const ObGroupbyExpr& groupby_item = rollup_list_exprs.at(j);
      for (int64_t k = 0; OB_SUCC(ret) && (!has) && k < groupby_item.groupby_exprs_.count(); k++) {
        const ObRawExpr* expr = groupby_item.groupby_exprs_.at(k);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("grouping sets expr is null", K(ret));
        } else if (expr->has_flag(flag)) {
          has = true;
        }
      }
    }
  }
  return ret;
}

int ObSelectStmt::clear_sharable_expr_reference()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::clear_sharable_expr_reference())) {
    LOG_WARN("failed to clear sharable expr reference", K(ret));
  } else {
    ObRawExpr* expr = NULL;
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

int ObSelectStmt::remove_useless_sharable_expr()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDMLStmt::remove_useless_sharable_expr())) {
    LOG_WARN("failed to remove useless sharable expr", K(ret));
  } else {
    ObRawExpr* expr = NULL;
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
  }
  return ret;
}

const SampleInfo* ObSelectStmt::get_sample_info_by_table_id(uint64_t table_id) const
{
  const SampleInfo* sample_info = nullptr;
  int64_t num = sample_infos_.count();
  for (int64_t i = 0; i < num; ++i) {
    if (sample_infos_.at(i).table_id_ == table_id) {
      sample_info = &sample_infos_.at(i);
      break;
    }
  }
  return sample_info;
}

SampleInfo* ObSelectStmt::get_sample_info_by_table_id(uint64_t table_id)
{
  SampleInfo* sample_info = nullptr;
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
  bool ret = !(has_distinct() || has_group_by() || has_set_op() || has_rollup() || has_order_by() || has_limit() ||
               get_aggr_item_size() != 0 || get_from_item_size() == 0 || is_contains_assignment() ||
               has_window_function() || has_sequence());
  if (!ret) {
  } else if (OB_FAIL(has_rownum(has_rownum_expr))) {
    ret = false;
  } else {
    ret = !has_rownum_expr;
  }
  return ret;
}

int ObSelectStmt::get_select_exprs(ObIArray<ObRawExpr*>& select_exprs, const bool is_for_outout /* = false*/)
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); ++i) {
    if (is_for_outout && select_items_.at(i).is_unpivot_mocked_column_) {
      // continue
    } else if (OB_ISNULL(expr = select_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null select expr", K(ret));
    } else if (OB_FAIL(select_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSelectStmt::get_select_exprs(ObIArray<ObRawExpr*>& select_exprs, const bool is_for_outout /* = false*/) const
{
  int ret = OB_SUCCESS;
  ObRawExpr* expr = NULL;
  LOG_DEBUG("before get_select_exprs", K(select_items_), K(table_items_), K(lbt()));
  for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); ++i) {
    if (is_for_outout && select_items_.at(i).is_unpivot_mocked_column_) {
      // continie
    } else if (OB_ISNULL(expr = select_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null select expr", K(ret));
    } else if (OB_FAIL(select_exprs.push_back(expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObSelectStmt::inner_get_share_exprs(ObIArray<ObRawExpr*>& candi_share_exprs) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  ObSEArray<ObRawExpr*, 4> general_exprs;
  /**
   * 1. column, aggr, winfunc, query ref can be shared
   * 2. select exprs, group by exprs, rollup exprs may be shared
   */
  if (OB_FAIL(ObDMLStmt::inner_get_share_exprs(candi_share_exprs))) {
    LOG_WARN("failed to inner mark share exprs", K(ret));
  } else if (OB_FAIL(get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(append(candi_share_exprs, get_aggr_items()))) {
    LOG_WARN("failed to append aggreagate exprs", K(ret));
  } else if (OB_FAIL(append(candi_share_exprs, get_window_func_exprs()))) {
    LOG_WARN("failed to append window function exprs", K(ret));
  } else if (OB_FAIL(append(general_exprs, select_exprs))) {
    LOG_WARN("failed to append select exprs", K(ret));
  } else if (OB_FAIL(append(general_exprs, get_group_exprs()))) {
    LOG_WARN("failed to append group by exprs", K(ret));
  } else if (OB_FAIL(append(general_exprs, get_rollup_exprs()))) {
    LOG_WARN("failed to append roll up exprs", K(ret));
  } else if (has_set_op()) {
    ObRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
      if (OB_ISNULL(expr = ObTransformUtils::get_expr_in_cast(select_exprs.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (expr != select_exprs.at(i) && OB_FAIL(candi_share_exprs.push_back(expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < general_exprs.count(); ++i) {
    ObRawExpr* expr = NULL;
    if (OB_ISNULL(expr = general_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("general expr is null", K(ret));
    } else if (ObRawExprUtils::is_sharable_expr(*expr) && expr->get_expr_level() != get_current_level()) {
      // a share expr of the upper stmt
    } else if (expr->has_flag(IS_EXEC_PARAM)) {
      // in static typing engine, exec param expr with different address will use different
      // frame, event they have same unknown value.
    } else if (OB_FAIL(candi_share_exprs.push_back(expr))) {
      LOG_WARN("failed to push back general expr", K(ret));
    }
  }
  return ret;
}

// remember to invoke formalize to generate expr_levels for all exprs
int ObSelectStmt::pullup_stmt_level()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_UNLIKELY(current_level_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt level", K(current_level_));
  } else if (FALSE_IT(--current_level_)) {
    // never reach
  } else if (OB_FAIL(set_sharable_exprs_level(current_level_))) {
    LOG_WARN("failed to set sharable exprs level", K(ret));
  } else if (OB_FAIL(get_child_stmts(child_stmts))) {
    LOG_WARN("failed to get child stmts", K(ret));
  }
  // pullup stmt level for children
  for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
    if (OB_ISNULL(child_stmts.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child stmt is null", K(ret));
    } else if (child_stmts.at(i)->get_current_level() <= current_level_) {
      // do nothing, this stmt may be just pushed down
      // there is no need to adjust
    } else if (OB_FAIL(child_stmts.at(i)->pullup_stmt_level())) {
      LOG_WARN("failed to pull stmt level", K(ret));
    }
  }
  return ret;
}

int ObSelectStmt::set_sharable_exprs_level(int32_t level)
{
  int ret = OB_SUCCESS;
  // for each columns
  for (int64_t i = 0; OB_SUCC(ret) && i < column_items_.count(); ++i) {
    if (OB_ISNULL(column_items_.at(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is null", K(ret));
    } else {
      column_items_.at(i).expr_->set_expr_level(level);
    }
  }
  // for each aggrs
  for (int64_t i = 0; OB_SUCC(ret) && i < agg_items_.count(); ++i) {
    if (OB_ISNULL(agg_items_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggr expr is null", K(ret));
    } else {
      agg_items_.at(i)->set_expr_level(level);
    }
  }
  // for each window function
  for (int64_t i = 0; OB_SUCC(ret) && i < win_func_exprs_.count(); ++i) {
    if (OB_ISNULL(win_func_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      win_func_exprs_.at(i)->set_expr_level(level);
      if (OB_NOT_NULL(win_func_exprs_.at(i)->get_agg_expr())) {
        win_func_exprs_.at(i)->get_agg_expr()->set_expr_level(level);
      }
    }
  }
  // for each query ref
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs_.count(); ++i) {
    if (OB_ISNULL(subquery_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subquery expr is null", K(ret));
    } else {
      subquery_exprs_.at(i)->set_expr_level(level);
    }
  }
  // for each set op
  if (is_set_stmt()) {
    ObRawExpr* expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items_.count(); ++i) {
      if (OB_ISNULL(expr = ObTransformUtils::get_expr_in_cast(select_items_.at(i).expr_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (expr->has_flag(IS_SET_OP)) {
        expr->set_expr_level(level);
      }
    }
  }
  return ret;
}

int ObSelectStmt::get_equal_set_conditions(
    ObIArray<ObRawExpr*>& conditions, const bool is_strict, const int check_scope)
{
  int ret = OB_SUCCESS;
  bool check_having = check_scope & SCOPE_HAVING;
  if (OB_FAIL(ObDMLStmt::get_equal_set_conditions(conditions, is_strict, check_scope))) {
    LOG_WARN("failed to get equal set cond", K(ret));
  } else if (check_having) {
    for (int64_t i = 0; OB_SUCC(ret) && i < having_exprs_.count(); ++i) {
      if (OB_FAIL(conditions.push_back(having_exprs_.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
  } else { /* do nothing */
  }
  return ret;
}

int ObSelectStmt::get_set_stmt_size(int64_t& size) const
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObSelectStmt*, 8> set_stmts;
  if (is_set_stmt()) {
    for (int64_t i = -1; i < set_stmts.count(); ++i) {
      const ObSelectStmt* stmt = (i == -1) ? this : set_stmts.at(i);
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

bool ObSelectStmt::contain_nested_aggr() const
{
  bool ret = false;
  for (int64_t i = 0; !ret && i < agg_items_.count(); i++) {
    if (agg_items_.at(i)->contain_nested_aggr()) {
      ret = true;
    } else { /*do nothing.*/
    }
  }
  return ret;
}

int ObGroupingSetsItem::assign(const ObGroupingSetsItem& other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(grouping_sets_exprs_.assign(other.grouping_sets_exprs_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(multi_rollup_items_.assign(other.multi_rollup_items_))) {
    LOG_WARN("failed to assign", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObGroupingSetsItem::deep_copy(
    ObRawExprFactory& expr_factory, const ObGroupingSetsItem& other, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.grouping_sets_exprs_.count(); ++i) {
    ObGroupbyExpr groupby_expr;
    if (OB_FAIL(ObRawExprUtils::copy_exprs(
            expr_factory, other.grouping_sets_exprs_.at(i).groupby_exprs_, groupby_expr.groupby_exprs_, copy_types))) {
      LOG_WARN("failed to copy exprs", K(ret));
    } else if (OB_FAIL(grouping_sets_exprs_.push_back(groupby_expr))) {
      LOG_WARN("failed to push back group by expr", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.multi_rollup_items_.count(); ++i) {
    ObMultiRollupItem multi_rollup_item;
    if (OB_FAIL(multi_rollup_item.deep_copy(expr_factory, other.multi_rollup_items_.at(i), copy_types))) {
      LOG_WARN("failed to deep copy multi rollup item", K(ret));
    } else if (OB_FAIL(multi_rollup_items_.push_back(multi_rollup_item))) {
      LOG_WARN("failed to push back group by expr", K(ret));
    }
  }
  return ret;
}

int ObMultiRollupItem::deep_copy(
    ObRawExprFactory& expr_factory, const ObMultiRollupItem& other, const uint64_t copy_types)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.rollup_list_exprs_.count(); ++i) {
    ObGroupbyExpr groupby_expr;
    if (OB_FAIL(ObRawExprUtils::copy_exprs(
            expr_factory, other.rollup_list_exprs_.at(i).groupby_exprs_, groupby_expr.groupby_exprs_, copy_types))) {
      LOG_WARN("failed to copy exprs", K(ret));
    } else if (OB_FAIL(rollup_list_exprs_.push_back(groupby_expr))) {
      LOG_WARN("failed to push back group by expr", K(ret));
    }
  }
  return ret;
}

bool ObSelectStmt::is_expr_in_groupings_sets_item(const ObRawExpr* expr) const
{
  bool is_true = false;
  for (int64_t i = 0; !is_true && i < grouping_sets_items_.count(); ++i) {
    const ObIArray<ObGroupbyExpr>& grouping_sets_exprs = grouping_sets_items_.at(i).grouping_sets_exprs_;
    for (int64_t j = 0; !is_true && j < grouping_sets_exprs.count(); ++j) {
      is_true = ObOptimizerUtil::find_equal_expr(grouping_sets_exprs.at(j).groupby_exprs_, expr);
    }
    if (!is_true) {
      const ObIArray<ObMultiRollupItem>& multi_rollup_items = grouping_sets_items_.at(i).multi_rollup_items_;
      for (int64_t j = 0; !is_true && j < multi_rollup_items.count(); ++j) {
        const ObIArray<ObGroupbyExpr>& rollup_list_exprs = multi_rollup_items.at(j).rollup_list_exprs_;
        for (int64_t k = 0; !is_true && k < rollup_list_exprs.count(); ++k) {
          is_true = ObOptimizerUtil::find_equal_expr(rollup_list_exprs.at(k).groupby_exprs_, expr);
        }
      }
    }
  }
  return is_true;
}

int ObSelectStmt::get_connect_by_root_exprs(ObIArray<ObRawExpr*>& connect_by_root_exprs) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> all_exprs;
  if (OB_FAIL(get_relation_exprs(all_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs.count(); ++i) {
    if (OB_FAIL(recursive_get_expr(all_exprs.at(i), connect_by_root_exprs, IS_CONNECT_BY_ROOT, CNT_CONNECT_BY_ROOT))) {
      LOG_WARN("failed to recursive get expr", K(ret));
    }
  }
  return ret;
}

int ObSelectStmt::get_sys_connect_by_path_exprs(ObIArray<ObRawExpr*>& sys_connect_by_path_exprs) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> all_exprs;
  if (OB_FAIL(get_relation_exprs(all_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs.count(); ++i) {
    if (OB_FAIL(recursive_get_expr(
            all_exprs.at(i), sys_connect_by_path_exprs, IS_SYS_CONNECT_BY_PATH, CNT_SYS_CONNECT_BY_PATH))) {
      LOG_WARN("failed to recursive get expr", K(ret));
    }
  }
  return ret;
}

int ObSelectStmt::recursive_get_expr(
    ObRawExpr* expr, ObIArray<ObRawExpr*>& exprs, ObExprInfoFlag target_flag, ObExprInfoFlag search_flag) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expr->has_flag(target_flag)) {
    ret = exprs.push_back(expr);
  } else if (expr->has_flag(search_flag)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(recursive_get_expr(expr->get_param_expr(i), exprs, target_flag, search_flag)))) {
        LOG_WARN("failed to recursive get expr", K(ret));
      }
    }
  }
  return ret;
}
bool ObSelectStmt::is_expr_in_multi_rollup_items(const ObRawExpr* expr) const
{
  bool is_true = false;
  for (int64_t i = 0; !is_true && i < multi_rollup_items_.count(); ++i) {
    const ObIArray<ObGroupbyExpr>& rollup_list_exprs = multi_rollup_items_.at(i).rollup_list_exprs_;
    for (int64_t j = 0; !is_true && j < rollup_list_exprs.count(); ++j) {
      is_true = ObOptimizerUtil::find_equal_expr(rollup_list_exprs.at(j).groupby_exprs_, expr);
    }
  }
  return is_true;
}
