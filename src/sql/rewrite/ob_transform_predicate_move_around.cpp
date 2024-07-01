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
#include "sql/rewrite/ob_transform_predicate_move_around.h"
//#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_predicate_deduce.h"
#include "share/schema/ob_table_schema.h"
#include "common/ob_smart_call.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;

/**
 *
 * FUTURE WORK
 * 1. deduce new join conditions
 * 2. cross 集合类 STMT 的推导
 * 3. 根据一个谓词推导新的谓词 (T1.C = 1 AND T2.C = 2) OR (T1.C = 2 AND T2.C = 3)，
 *    实际上是给定一个涉及多表的谓词，找出隐含的单表谓词
 * 4. PULLUP 是在推到 SELECT EXPR 之间的大小关系；目前我们只推导了
 *    column, aggr, winfunc 之间的大小关系，并向外层传递。未来可以尝试向上层传递更多的大小关系
 */

/**
 * @brief ObTransformPredicateMoveAround::transform_one_stmt
 * @param parent_stmts
 * @param stmt
 * @param trans_happened
 * @return
 */
int ObTransformPredicateMoveAround::transform_one_stmt(
    common::ObIArray<ObParentDMLStmt> &parent_stmts, ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  if (OB_FAIL(do_transform_predicate_move_around(stmt, trans_happened))) {
    LOG_WARN("failed to do transform_predicate_move_around", K(ret));
  } else if (transed_stmts_.empty() || !trans_happened) {
  // transform not happened actually
  } else if (OB_FAIL(adjust_transed_stmts())) {
    LOG_WARN("sort sort transed stmts failed", K(ret));
  } else if (OB_FAIL(add_transform_hint(*stmt, &transed_stmts_))) {
    LOG_WARN("add transform hint failed", K(ret));
  }
  transed_stmts_.reuse();
  return ret;
}

int ObTransformPredicateMoveAround::do_transform_predicate_move_around(ObDMLStmt *&stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_happened = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null stmt", K(ret));
  } else if (OB_FAIL(inner_do_transfrom(stmt, is_happened))) {
    LOG_WARN("failed to do predicate move around", K(ret));
  } else {
    trans_happened |= is_happened;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos_.count(); i ++) {
    is_happened = false;
    if (OB_FAIL(inner_do_transfrom(temp_table_infos_.at(i)->table_query_, is_happened))) {
      LOG_WARN("failed to do predicate move around", K(ret));
    } else {
      trans_happened |= is_happened;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(push_down_cte_filter(temp_table_infos_, is_happened))) {
      LOG_WARN("failed to push down filter", K(ret));
    } else {
      trans_happened |= is_happened;
      OPT_TRACE("push down common filter for temp table:", is_happened);
      LOG_TRACE("succeed to do push down common filter for temp table", K(temp_table_infos_),  K(is_happened));
    }
  }
  for (int64_t i = 0; i < temp_table_infos_.count(); i ++) {
    if (OB_NOT_NULL(temp_table_infos_.at(i))) {
      temp_table_infos_.at(i)->~ObSqlTempTableInfo();
    }
  }
  temp_table_infos_.reuse();
  return ret;
}

int ObTransformPredicateMoveAround::inner_do_transfrom(ObDMLStmt *stmt, bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> dummy_pullup;
  ObArray<ObRawExpr *> dummy_pushdown;
  ObArray<int64_t> dummy_list;
  real_happened_ = false;
  trans_happened = false;
  const ObQueryHint *query_hint = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(query_hint = stmt->get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt or hint is null", K(ret), K(stmt));
  } else if (is_normal_disabled_transform(*stmt)) {
    // do nothing
  } else if (!stmt_map_.created() && OB_FAIL(stmt_map_.create(20, ObModIds::OB_SQL_COMPILE))) {
    LOG_WARN("failed to create stmt map", K(ret));
  } else if (OB_FAIL(pullup_predicates(stmt, dummy_list, dummy_pullup))) {
    LOG_WARN("failed to pull up predicates", K(ret));
  } else if ((stmt->is_insert_stmt() || stmt->is_merge_stmt())
             && (OB_FAIL(create_equal_exprs_for_insert(static_cast<ObDelUpdStmt*>(stmt))))) {
    LOG_WARN("failed to create equal exprs for insert", K(ret));
  } else if (OB_FAIL(pushdown_predicates(stmt, dummy_pushdown))) {
    LOG_WARN("failed to push down predicates", K(ret));
  } else if (real_happened_ &&
         OB_FAIL(stmt->formalize_stmt_expr_reference(ctx_->expr_factory_, ctx_->session_info_))) {
    LOG_WARN("formalize stmt expr reference failed", K(ret));
  } else {
    trans_happened = real_happened_;
  }
  return ret;
}

const static auto cmp_func = [](ObDMLStmt* l_stmt, ObDMLStmt* r_stmt) {
  if (OB_ISNULL(l_stmt) || OB_ISNULL(r_stmt)) {
    return false;
  } else {
    return l_stmt->get_stmt_id() < r_stmt->get_stmt_id();
  }
};

int ObTransformPredicateMoveAround::adjust_transed_stmts()
{
  int ret = OB_SUCCESS;
  lib::ob_sort(transed_stmts_.begin(), transed_stmts_.end(), cmp_func);
  ObDMLStmt *stmt = NULL;
  for (int64_t i = transed_stmts_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
    if (OB_ISNULL(stmt = transed_stmts_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is null", K(ret), K(stmt));
    } else if (stmt->is_set_stmt() && OB_FAIL(transed_stmts_.remove(i))) {
      LOG_WARN("failed to remove set stmt", K(ret));
    }
  }
  return ret;
}

ObTransformPredicateMoveAround::ObTransformPredicateMoveAround(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::ROOT_ONLY, T_PRED_DEDUCE),
      allocator_("PredDeduce"),
      real_happened_(false)
{}

ObTransformPredicateMoveAround::~ObTransformPredicateMoveAround()
{
  for (int64_t i = 0; i < stmt_pullup_preds_.count(); ++i) {
    if (NULL != stmt_pullup_preds_.at(i)) {
      stmt_pullup_preds_.at(i)->~PullupPreds();
      stmt_pullup_preds_.at(i) = NULL;
    }
  }
}

/**
 * @brief ObTransformPredicateMoveAround::need_rewrite
 *   a stmt need rewrite if
 * 1. the stmt is not a generated table
 * @return
 */
int ObTransformPredicateMoveAround::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                   const int64_t current_level,
                                                   const ObDMLStmt &stmt,
                                                   bool &need_trans)
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = stmt.get_stmt_hint().query_hint_;
  need_trans = !is_normal_disabled_transform(stmt);
  ObDMLStmt *parent = NULL;
  temp_table_infos_.reuse();
  applied_hints_.reuse();
  if (!need_trans) {
    //do nothing
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!parent_stmts.empty() || current_level != 0) {
    need_trans = false;
  } else if (OB_FAIL(ObSqlTempTableInfo::collect_temp_tables(allocator_,
                                                             const_cast<ObDMLStmt &>(stmt),
                                                             temp_table_infos_,
                                                             NULL,
                                                             false))) {
    LOG_WARN("failed to add all temp tables", K(ret));
  } else if (check_outline_valid_to_transform(stmt, need_trans)) {
    LOG_WARN("failed to check outline", K(ret));
  }
  LOG_DEBUG("IF NO PRED DEDUCE", K(need_trans));
  return ret;
}

int ObTransformPredicateMoveAround::check_outline_valid_to_transform(const ObDMLStmt &stmt, 
                                                                     bool &need_trans)
{
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = stmt.get_stmt_hint().query_hint_;
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (need_trans && query_hint->has_outline_data()) {
    ObSEArray<ObDMLStmt *, 4> views; 
    bool need_outline_trans = false;
    int64_t i = 0; 
    if (OB_FAIL(get_stmt_to_trans(const_cast<ObDMLStmt *>(&stmt), views))) {
      LOG_WARN("get stmt to trans failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos_.count(); i ++) {
      if (OB_ISNULL(temp_table_infos_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null temp table info", K(ret));
      } else if (OB_FAIL(get_stmt_to_trans(temp_table_infos_.at(i)->table_query_, views))) {
        LOG_WARN("get stmt to trans failed", K(ret));
      }
    }
    lib::ob_sort(views.begin(), views.end(), cmp_func);
    int tmp_trans_list_loc = ctx_->trans_list_loc_;
    while (OB_SUCC(ret) && i <views.count()) {
      ObDMLStmt *view = views.at(i);
      const ObHint *trans_hint = NULL;
      if (OB_ISNULL(view)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("stmt is null", K(ret));
      } else if (NULL == (trans_hint = query_hint->get_outline_trans_hint(tmp_trans_list_loc))
        || !trans_hint->is_pred_deduce_hint()) {
        break;
      } else {
        bool is_valid = query_hint->is_valid_outline_transform(tmp_trans_list_loc, 
                                                                    get_hint(view->get_stmt_hint()));
        if (is_valid) {
          if (OB_FAIL(applied_hints_.push_back(const_cast<ObHint *>(trans_hint)))) {
            LOG_WARN("push back failed", K(ret));
          } else if (trans_hint->is_disable_hint()) {
          } else {
            need_outline_trans = true;
            tmp_trans_list_loc++;
            i = 0;
          }
        } else {
          i++;
        }
      }                                     
    }
    if (!need_outline_trans) {
      OPT_TRACE("outline reject transform");
    }
    need_trans = need_outline_trans;
  }
  return ret;
}

int ObTransformPredicateMoveAround::transform_one_stmt_with_outline(
                                                            ObIArray<ObParentDMLStmt> &parent_stmts,
                                                            ObDMLStmt *&stmt,
                                                            bool &trans_happened) {
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  if (OB_FAIL(do_transform_predicate_move_around(stmt, trans_happened))) {
    LOG_WARN("failed to do transform_predicate_move_around", K(ret));
  } else if (transed_stmts_.empty() || !trans_happened) {
    LOG_TRACE("outline data trans not happened");
  } else if (OB_FAIL(adjust_transed_stmts())) {
    LOG_WARN("sort sort transed stmts failed", K(ret));
  } else if (OB_FAIL(add_transform_hint(*stmt, &transed_stmts_))) {
    LOG_WARN("add transform hint failed", K(ret));
  } else {
    ctx_->trans_list_loc_ += transed_stmts_.count();
  }
  transed_stmts_.reuse();
  return ret;
}

int ObTransformPredicateMoveAround::get_stmt_to_trans(ObDMLStmt *stmt, ObIArray<ObDMLStmt *> &stmt_to_trans)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(stmt_to_trans.push_back(stmt))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(stmt->get_from_subquery_stmts(child_stmts))) {
    LOG_WARN("failed to get from subquery stmts", K(ret));
  } else {
    ObIArray<ObQueryRefRawExpr *> &subquery_exprs = stmt->get_subquery_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); i++) {
      if (OB_ISNULL(subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(get_stmt_to_trans(subquery_exprs.at(i)->get_ref_stmt(),
                                    stmt_to_trans)))) {
        LOG_WARN("get stmt to trans from subquery failed", K(ret));
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); i++) {
      if (OB_FAIL(SMART_CALL(get_stmt_to_trans(child_stmts.at(i), stmt_to_trans)))) {
        LOG_WARN("get stmt to trans failed", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pullup_predicates_from_set_stmt(ObDMLStmt *stmt,
                                                     ObIArray<int64_t> &sel_ids,
                                                     ObIArray<ObRawExpr *> &output_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
  ObSEArray<ObRawExpr *, 8> pullup_preds;
  ObIArray<ObRawExpr *> *input_pullup_preds = NULL;
  if (OB_ISNULL(sel_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_FAIL(SMART_CALL(pullup_predicates_from_set(sel_stmt,
                                                    pullup_preds)))) {
    LOG_WARN("failed to pull up predicates from set stmt", K(ret));
  } else if (OB_FAIL(generate_set_pullup_predicates(*sel_stmt,
                                                    sel_ids,
                                                    pullup_preds,
                                                    output_pullup_preds))) {
    LOG_WARN("generate set pullup preds failed", K(ret));
  } else if (OB_FAIL(acquire_transform_params(stmt, input_pullup_preds))) {
    LOG_WARN("failed to acquire pullup preds", K(ret));
  } else if (OB_ISNULL(input_pullup_preds)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to acquire transform params", K(ret));
  } else if (OB_FAIL(append(*input_pullup_preds, pullup_preds))) {
    LOG_WARN("assign pullup preds failed", K(ret));
  } else {/*do nothing*/}
  return ret;
}

//pullup predicates do not change stmt, need not hint check and allowed pullup pred from temp table
int ObTransformPredicateMoveAround::pullup_predicates(ObDMLStmt *stmt,
                                                      ObIArray<int64_t> &sel_ids,
                                                      ObIArray<ObRawExpr *> &output_pullup_preds)
{
  int ret = OB_SUCCESS;
  bool is_overflow = false;
  ObIArray<ObRawExpr *> *input_pullup_preds = NULL;
  OPT_TRACE_BEGIN_SECTION;
  OPT_TRACE("try to pullup predicates");
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (stmt->is_hierarchical_query()) {
    OPT_TRACE("can not pullup predicates for hierarchical query");
  } else if (OB_FAIL(check_stack_overflow(is_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_overflow));
  } else if (OB_FAIL(acquire_transform_params(stmt, input_pullup_preds))) {
    LOG_WARN("failed to acquire pullup preds", K(ret));
  } else if (OB_ISNULL(input_pullup_preds)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to acquire transform params", K(ret));
  } else if (stmt->is_set_stmt()) {
    if (OB_FAIL(pullup_predicates_from_set_stmt(stmt, sel_ids, output_pullup_preds))) {
      LOG_WARN("process set stmt failed", K(ret));
    }
  // } else if (OB_FAIL(generate_basic_table_pullup_preds(stmt, *input_pullup_preds))) {
  //   LOG_WARN("add stmt check constraints", K(ret));
  } else if (OB_FAIL(pullup_predicates_from_view(*stmt, sel_ids, *input_pullup_preds))) {
    LOG_WARN("failed to pullup predicates from view", K(ret));
  } else if (OB_FAIL(generate_pullup_predicates_for_subquery(*stmt, *input_pullup_preds))) {
    LOG_WARN("failed to pullup predicates from subquery", K(ret));
  } else if (!(stmt->is_select_stmt() || stmt->is_merge_stmt()) || sel_ids.empty()) {
    // do nothing
  } else if (OB_FAIL(generate_pullup_predicates(static_cast<ObSelectStmt &>(*stmt),
                                                sel_ids,
                                                *input_pullup_preds,
                                                output_pullup_preds))) {
    LOG_WARN("failed to generate pullup predicates", K(ret));
  }
  OPT_TRACE("pullup predicates:", output_pullup_preds);
  OPT_TRACE_END_SECTION;
  return ret;
}


int ObTransformPredicateMoveAround::pullup_predicates_from_view(
    ObDMLStmt &stmt, ObIArray<int64_t> &sel_ids, ObIArray<ObRawExpr *> &input_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> filter_columns;
  if (OB_FAIL(get_columns_in_filters(stmt, sel_ids, filter_columns))) {
    LOG_WARN("failed to get columns in filters", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
    TableItem *table_item = NULL;
    ObSEArray<ObRawExpr *, 4> view_preds;
    ObSEArray<int64_t, 4> view_sel_list;
    bool on_null_side = false;
    OPT_TRACE("try to pullup predicate from view:", stmt.get_table_items().at(i));
    if (OB_ISNULL(table_item = stmt.get_table_items().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (table_item->is_fake_cte_table()) {
      OPT_TRACE("cte table can not pullup predicate");
    } else if (!table_item->is_generated_table() &&
               !table_item->is_temp_table() &&
               !table_item->is_lateral_table()) {
      // do nothing
      OPT_TRACE("not view table, can not pullup predicate");
    } else if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(
                         &stmt, table_item->table_id_, on_null_side))) {
      LOG_WARN("failed to check is table on null side", K(ret));
    } else if (on_null_side) {
      // do nothing
      OPT_TRACE("table on null side, can not pullup predicate");
    } else if (OB_FAIL(choose_pullup_columns(*table_item, filter_columns, view_sel_list))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(SMART_CALL(pullup_predicates(table_item->ref_query_, view_sel_list, view_preds)))) {
      LOG_WARN("failed to pull up predicate", K(ret), K(view_sel_list), K(filter_columns), K(table_item->ref_query_->get_select_items()));
    } else if (table_item->is_lateral_table() &&
               OB_FAIL(filter_lateral_correlated_preds(*table_item, view_preds))) {
      LOG_WARN("failed to filter lateral correlated preds", K(ret));
    } else if (OB_FAIL(rename_pullup_predicates(
                         stmt, *table_item, view_sel_list, view_preds))) {
      LOG_WARN("failed to rename pullup predicates", K(ret));
    } else if (OB_FAIL(append(input_pullup_preds, view_preds))) {
      LOG_WARN("failed to append expr", K(ret));
    } else if (!table_item->ref_query_->is_set_stmt()) {
      // TODO: temp code for enable predicate dedue with `select const as c1 from table/dual`.
      //       remove this code after implement const folding.
      view_preds.reuse();
      if (OB_FAIL(generate_pullup_predicates_for_dual_stmt(stmt,
                                                           *table_item,
                                                           view_sel_list,
                                                           view_preds))) {
        LOG_WARN("failed to generate pullup predicates for dual stmt", K(ret));
      } else if (OB_FAIL(append(input_pullup_preds, view_preds))) {
        LOG_WARN("failed to append expr", K(ret));
      }
    }
    // select ... from (select c1 as a, c1 as b from ...),
    // potentially, we can pull up a predicate (a = b)
  }
  return ret;
}

int ObTransformPredicateMoveAround::update_current_property(ObDMLStmt &stmt,
                                                    ObIArray<ObRawExpr *> &exprs,
                                                    ObIArray<ObRawExpr *> &preds_for_subquery)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    if (OB_NOT_NULL(exprs.at(i)) && !exprs.at(i)->has_flag(CNT_SUB_QUERY)) {
      if (OB_FAIL(preds_for_subquery.push_back(exprs.at(i)))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  return ret;
}


int ObTransformPredicateMoveAround::update_subquery_pullup_preds(ObIArray<ObQueryRefRawExpr *> &subquery_exprs,
                                                                 ObIArray<ObRawExpr *> &current_property)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> *subquery_pullup_exprs = NULL;
  ObSEArray<ObRawExpr *, 4> renamed_preds;
  for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); i++) {
    ObQueryRefRawExpr *subquery = subquery_exprs.at(i);
    renamed_preds.reuse();
    if (OB_ISNULL(subquery)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sub query is null", K(ret));
    } else if (OB_FAIL(choose_and_rename_predicates_for_subquery(subquery, current_property, renamed_preds))) {
      LOG_WARN("rename predicates failed", K(ret));
    } else if (OB_FAIL(acquire_transform_params(subquery->get_ref_stmt(), subquery_pullup_exprs))) {
      LOG_WARN("failed to acquire pull up preds", K(ret));
    } else if (OB_ISNULL(subquery_pullup_exprs)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(append(*subquery_pullup_exprs, renamed_preds))) {
      LOG_WARN("push back failed", K(ret));
    }
  }

  return ret;
}

int ObTransformPredicateMoveAround::generate_pullup_predicates_for_subquery(ObDMLStmt &stmt,
                                                                    ObIArray<ObRawExpr *> &pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObQueryRefRawExpr *, 4> subquery_exprs;
  ObSEArray<ObRawExpr *, 4> current_property;
  ObSEArray<ObQueryRefRawExpr *, 4> processed_subquery;
  if (OB_FAIL(append(current_property, pullup_preds))) {
    LOG_WARN("append failed", K(ret));
  } else {
      // where condition
    if (OB_FAIL(update_current_property(stmt, stmt.get_condition_exprs(), current_property))) {
      LOG_WARN("get scope preds failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(stmt.get_condition_exprs(),
                                                                subquery_exprs, /*with_nest*/false))) {
      LOG_WARN("extract subquery failed", K(ret));
    } else if (OB_FAIL(ObTransformUtils::remove_dup_expr(processed_subquery, subquery_exprs))) {
        LOG_WARN("remove dup failed", K(ret));
    } else if (OB_FAIL(update_subquery_pullup_preds(subquery_exprs, current_property))) {
      LOG_WARN("update subquery pullup preds failed", K(ret));
    } else if (OB_FAIL(append_array_no_dup(processed_subquery, subquery_exprs))) {
      LOG_WARN("add var no dup failed", K(ret));
    }

    if (OB_SUCC(ret) && stmt.is_select_stmt()) {
      ObSelectStmt &sel_stmt = static_cast<ObSelectStmt &>(stmt);
      // group by
      subquery_exprs.reset();
      if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(sel_stmt.get_group_exprs(),
                                                                  subquery_exprs, /*with_nest*/false))) {
        LOG_WARN("extract subquery failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::remove_dup_expr(processed_subquery, subquery_exprs))) {
        LOG_WARN("remove dup failed", K(ret));
      } else if (OB_FAIL(update_subquery_pullup_preds(subquery_exprs, current_property))) {
        LOG_WARN("update subquery pullup preds failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(processed_subquery, subquery_exprs))) {
        LOG_WARN("add var no dup failed", K(ret));
      }

      // having
      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(subquery_exprs.reset())) {
      } else if (OB_FAIL(update_current_property(stmt, sel_stmt.get_having_exprs(),
                                                            current_property))) {
        LOG_WARN("get scope preds failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(sel_stmt.get_having_exprs(),
                                                                  subquery_exprs, /*with_nest*/false))) {
        LOG_WARN("extract subquery failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::remove_dup_expr(processed_subquery, subquery_exprs))) {
        LOG_WARN("remove dup failed", K(ret));
      } else if (OB_FAIL(update_subquery_pullup_preds(subquery_exprs, current_property))) {
        LOG_WARN("update subquery pullup preds failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(processed_subquery, subquery_exprs))) {
        LOG_WARN("add var no dup failed", K(ret));
      }


      //select
      ObSEArray<ObRawExpr *, 4> select_exprs;
      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(subquery_exprs.reset())) {
      } else if (OB_FAIL(sel_stmt.get_select_exprs(select_exprs))) {
        LOG_WARN("get sel expr failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(select_exprs,
                                                                  subquery_exprs, /*with_nest*/false))) {
        LOG_WARN("extract subquery failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::remove_dup_expr(processed_subquery, subquery_exprs))) {
        LOG_WARN("remove dup failed", K(ret));
      } else if (OB_FAIL(update_subquery_pullup_preds(subquery_exprs, current_property))) {
        LOG_WARN("update subquery pullup preds failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(processed_subquery, subquery_exprs))) {
        LOG_WARN("add var no dup failed", K(ret));
      }

      //order
      ObSEArray<ObRawExpr *, 4> order_exprs;
      if (OB_FAIL(ret)) {
      } else if (OB_FALSE_IT(subquery_exprs.reset())) {
      } else if (OB_FAIL(sel_stmt.get_order_exprs(order_exprs))) {
        LOG_WARN("get sel expr failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(order_exprs,
                                                                  subquery_exprs, /*with_nest*/false))) {
        LOG_WARN("extract subquery failed", K(ret));
      } else if (OB_FAIL(ObTransformUtils::remove_dup_expr(processed_subquery, subquery_exprs))) {
        LOG_WARN("remove dup failed", K(ret));
      } else if (OB_FAIL(update_subquery_pullup_preds(subquery_exprs, current_property))) {
        LOG_WARN("update subquery pullup preds failed", K(ret));
      } else if (OB_FAIL(append_array_no_dup(processed_subquery, subquery_exprs))) {
        LOG_WARN("add var no dup failed", K(ret));
      }
    }

    ObArray<int64_t> dummy_sels;
    ObArray<ObRawExpr *> dummy_expr;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_subquery_exprs().count(); i++) {
      ObQueryRefRawExpr *subquery = stmt.get_subquery_exprs().at(i);
      if (OB_ISNULL(subquery)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub query is null", K(ret));
      } else if (OB_FAIL(pullup_predicates(subquery->get_ref_stmt(), dummy_sels, dummy_expr))) {
        LOG_WARN("pull up preds failed", K(ret));
      }
    }

  }
  return ret;
}

//Be careful, this function may cause bugs.
//it should make sure that the check constraint result is not null or calculable, but now we can not guarantee it.
//may reopen it in future by case.
int ObTransformPredicateMoveAround::generate_basic_table_pullup_preds(ObDMLStmt *stmt, ObIArray<ObRawExpr *> &preds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_check_constraint_items().count(); i++) {
      ObDMLStmt::CheckConstraintItem &item = stmt->get_check_constraint_items().at(i);
      bool on_null_side = false;
      if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(
                        stmt, item.table_id_, on_null_side))) {
        LOG_WARN("check constraint expr valid", K(ret));
      } else if (on_null_side) {
        LOG_TRACE("table on null side", K(item));
        //the check expr is not maintance correctly.
        //add defense
      } else if (OB_ISNULL(stmt->get_table_item_by_id(item.table_id_))) {
        LOG_TRACE("check constraint not maintain correctly", K(item));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < item.check_constraint_exprs_.count(); j++) {
          ObRawExpr *check_constraint_expr = item.check_constraint_exprs_.at(j);
          if (OB_ISNULL(check_constraint_expr) ||
              OB_UNLIKELY(item.check_constraint_exprs_.count() != item.check_flags_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret), K(item));
          } else if (!(item.check_flags_.at(j) & ObDMLStmt::CheckConstraintFlag::IS_VALIDATE_CHECK) &&
                    !(item.check_flags_.at(j) & ObDMLStmt::CheckConstraintFlag::IS_RELY_CHECK)) {
            //do nothing
          } else if (OB_FAIL(preds.push_back(check_constraint_expr))) {
            LOG_WARN("push back failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::choose_and_rename_predicates_for_subquery(ObQueryRefRawExpr *subquery,
                                                                   ObIArray<ObRawExpr *> &preds,
                                                                   ObIArray<ObRawExpr *> &renamed_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> exec_params;
  ObSEArray<ObRawExpr *, 4> subquery_columns;
  if (OB_ISNULL(subquery) || OB_ISNULL(ctx_) ||
      OB_ISNULL(subquery->get_ref_stmt()) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub query is null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery->get_param_count(); i++) {
      if (OB_FAIL(subquery_columns.push_back(subquery->get_param_expr(i)))) {
        LOG_WARN("push back failed", K(ret));
      } else if (OB_FAIL(exec_params.push_back(subquery->get_exec_param(i)))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(copier.add_replaced_expr(subquery_columns, exec_params))) {
      LOG_WARN("add replaced expr failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); i++) {
      int64_t state = 0;
      ObRawExpr *new_pred = NULL;
      if (OB_FAIL(check_expr_pullup_validity(preds.at(i), subquery_columns, state))) {
        LOG_WARN("check expr pullup validity failed", K(ret));
      } else if (state != 1) {
        LOG_DEBUG("state is not 1");
      } else if (OB_FAIL(copier.copy(preds.at(i), new_pred))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_ISNULL(new_pred)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pred is null", K(ret));
      } else if (OB_FAIL(new_pred->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(new_pred->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (OB_FAIL(renamed_preds.push_back(new_pred))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
    LOG_TRACE("show renamed preds", K(preds), K(renamed_preds));
  }
  return ret;
}

int ObTransformPredicateMoveAround::generate_set_pullup_predicates(ObSelectStmt &stmt,
                                                                  ObIArray<int64_t> &select_list,
                                                                  ObIArray<ObRawExpr *> &input_pullup_preds,
                                                                  ObIArray<ObRawExpr *> &output_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> select_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_list.count(); ++i) {
    int64_t idx = select_list.at(i);
    ObRawExpr *sel_expr = NULL;
    if (OB_UNLIKELY(idx < 0 || idx >= stmt.get_select_item_size()) ||
        OB_ISNULL(sel_expr = stmt.get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret), K(sel_expr), K(idx));
    } else if (!sel_expr->is_set_op_expr()) {
    } else if (OB_FAIL(select_exprs.push_back(sel_expr))) {
      LOG_WARN("failed to push back select expr", K(ret));
    } else {/*do nothing*/}
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_pullup_preds.count(); ++i) {
    int64_t state = 0;
    if (OB_ISNULL(input_pullup_preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("input pullup predicate is null", K(ret));
    } else if (OB_FAIL(check_expr_pullup_validity(
                         input_pullup_preds.at(i), select_exprs, state))) {
      LOG_WARN("failed to check pullup validity", K(ret));
    } else if (state != 1) {
      // do nothing
    } else if (OB_FAIL(output_pullup_preds.push_back(input_pullup_preds.at(i)))) {
      LOG_WARN("failed to push back predicates", K(ret));
    } else {/*do nothing*/}
  }
  return ret;
}

int ObTransformPredicateMoveAround::pullup_predicates_from_set(ObSelectStmt *stmt,
                                                              ObIArray<ObRawExpr *> &pullup_preds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_UNLIKELY(!stmt->is_set_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (stmt->is_recursive_union()) {
    //recursive cte不能上拉谓词到外层
    ObArray<int64_t> dummy_sels;
    ObArray<ObRawExpr *> dummy_preds;
    OPT_TRACE("try to pullup predicate from recurisve union stmt");
    if (OB_FAIL(SMART_CALL(pullup_predicates(stmt->get_set_query(0), dummy_sels, dummy_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else if (OB_FAIL(SMART_CALL(pullup_predicates(stmt->get_set_query(1), dummy_sels, dummy_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else {/*do nothing*/}
  } else {
    ObIArray<ObSelectStmt *> &child_query = stmt->get_set_query();
    const int64_t child_num = child_query.count();
    ObSEArray<ObRawExpr *, 16> left_output_preds;
    ObSEArray<ObRawExpr *, 16> right_output_preds;
    ObSEArray<ObRawExpr *, 16> pullup_output_preds;
    //对于set stmt需要上拉所有select item相关的谓词
    ObArray<int64_t> all_sels;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_select_item_size(); ++i) {
      if (OB_FAIL(all_sels.push_back(i))) {
        LOG_WARN("push back select idx failed", K(ret));
      } else {/*do nothing*/}
    }
    OPT_TRACE("try to pullup predicate from set stmt");
    for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
      pullup_output_preds.reset();
      right_output_preds.reset();
      if (OB_ISNULL(child_query.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(SMART_CALL(pullup_predicates(child_query.at(i), all_sels,
                                                      right_output_preds)))) {
        LOG_WARN("pullup preds from set failed", K(ret));
      } else if (OB_FAIL(rename_set_op_predicates(*child_query.at(i), *stmt,
                                                  right_output_preds, true))) {
        LOG_WARN("rename predicates failed", K(ret));
      } else if (OB_FAIL(pullup_predicates_from_const_select(stmt, child_query.at(i),
                                                             right_output_preds))) {
        LOG_WARN("pullup preds from const select failed", K(ret));
      } else if (0 == i) {
        ret = left_output_preds.assign(right_output_preds);
      } else if (OB_FAIL(check_pullup_predicates(stmt, left_output_preds, right_output_preds,
                                                 pullup_output_preds))) {
        LOG_WARN("choose pullup predicates failed", K(ret));
      } else if (OB_FAIL(left_output_preds.assign(pullup_output_preds))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(pullup_preds.assign(pullup_output_preds))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_pullup_predicates(ObSelectStmt *stmt,
                                                            ObIArray<ObRawExpr *> &left_pullup_preds,
                                                            ObIArray<ObRawExpr *> &right_pullup_preds,
                                                            ObIArray<ObRawExpr *> &output_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObStmtCompareContext context;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(!stmt->is_set_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (OB_FALSE_IT(context.init(&stmt->get_query_ctx()->calculable_items_))) {
  } else if (stmt->get_set_op() == ObSelectStmt::UNION && !stmt->is_recursive_union()) {
    //找出同构谓词
    for (int64_t i = 0; OB_SUCC(ret) && i < left_pullup_preds.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < right_pullup_preds.count(); ++j) {
        if (left_pullup_preds.at(i)->same_as(*right_pullup_preds.at(j), &context)) {
          find = true;
        } else {/*do nothing*/}
      }
      if (OB_UNLIKELY(!find)) {
        /*do nothing*/
        OPT_TRACE(left_pullup_preds.at(i), "not find same expr in right query, can not pullup");
      } else if (OB_FAIL(output_pullup_preds.push_back(left_pullup_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_FAIL(append(ctx_->equal_param_constraints_, context.equal_param_info_))) {
      LOG_WARN("append equal param info failed", K(ret));
    } else {/*do nothing*/}
  } else if (stmt->get_set_op() == ObSelectStmt::INTERSECT) {
    //合并谓词
    for (int64_t i = 0; OB_SUCC(ret) && i < left_pullup_preds.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < output_pullup_preds.count(); ++j) {
        if (left_pullup_preds.at(i)->same_as(*output_pullup_preds.at(j), &context)) {
          find = true;
        } else {/*do nothing*/}
      }
      if (find) {
        /*do nothing*/
      } else if (OB_FAIL(output_pullup_preds.push_back(left_pullup_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else {/*do nothing*/}
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_pullup_preds.count(); ++i) {
      bool find = false;
      for (int64_t j = 0; OB_SUCC(ret) && !find && j < output_pullup_preds.count(); ++j) {
        if (right_pullup_preds.at(i)->same_as(*output_pullup_preds.at(j), &context)) {
          find = true;
        } else {/*do nothing*/}
      }
      if (find) {
        /*do nothing*/
      } else if (OB_FAIL(output_pullup_preds.push_back(right_pullup_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_FAIL(append(ctx_->equal_param_constraints_, context.equal_param_info_))) {
      LOG_WARN("append equal param info failed", K(ret));
    } else {/*do nothing*/}
  } else if (stmt->get_set_op() == ObSelectStmt::EXCEPT) {
    if (OB_FAIL(append(output_pullup_preds, left_pullup_preds))) {
      LOG_WARN("append pullup preds failed", K(ret));
    } else {/*do nothing*/}
  } else {/*do nothing*/}
  return ret;
}

int ObTransformPredicateMoveAround::choose_pullup_columns(TableItem &table,
                                                          ObIArray<ObRawExpr *> &columns,
                                                          ObIArray<int64_t> &view_sel_list)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
    if (OB_ISNULL(columns.at(i)) || OB_UNLIKELY(!columns.at(i)->is_column_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column exprs", K(ret));
    } else {
      ObColumnRefRawExpr *col = static_cast<ObColumnRefRawExpr *>(columns.at(i));
      if (col->get_table_id() != table.table_id_) {
        // do nothing
      } else if (ObOptimizerUtil::find_item(view_sel_list,
                                            col->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
        // do nothing
      } else if (OB_FAIL(view_sel_list.push_back(col->get_column_id() - OB_APP_MIN_COLUMN_ID))) {
        LOG_WARN("failed to push back select index", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::generate_pullup_predicates(
    ObSelectStmt &select_stmt,
    ObIArray<int64_t> &sel_ids,
    ObIArray<ObRawExpr *> &input_pullup_preds,
    ObIArray<ObRawExpr *> &output_pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> local_preds;
  ObSEArray<ObRawExpr *, 4> filter_preds;
  if (OB_FAIL(gather_pullup_preds_from_semi_outer_join(select_stmt, filter_preds))) {
    LOG_WARN("failed to pullup preds from semi outer join", K(ret));
  } else if (OB_FAIL(append(filter_preds, select_stmt.get_condition_exprs()))) {
    LOG_WARN("failed to append conditions", K(ret));
  } else if (OB_FAIL(append(filter_preds, select_stmt.get_having_exprs()))) {
    LOG_WARN("failed to append having conditions", K(ret));
  } else if (OB_FAIL(gather_basic_qualify_filter(select_stmt,  filter_preds))) {
     LOG_WARN("failed to gather qualify filters", K(ret));
  } else if (OB_FAIL(remove_simple_op_null_condition(select_stmt, filter_preds))) {
    LOG_WARN("fail to chck and remove const simple conditions", K(ret));
  } else if (OB_FAIL(append(local_preds, input_pullup_preds))) {
    LOG_WARN("failed to append pullup predicates", K(ret));
  } else if (OB_FAIL(append(local_preds, filter_preds))) {
    LOG_WARN("failed to append having and where conditions", K(ret));
  } else if (OB_FAIL(compute_pullup_predicates(
                       select_stmt, sel_ids, local_preds, output_pullup_preds))) {
    LOG_WARN("failed to deduce exported predicates", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::gather_basic_qualify_filter(ObSelectStmt &stmt,
                                                                ObIArray<ObRawExpr*> &preds)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> &qualify_filters = stmt.get_qualify_filters();
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_qualify_filters_count(); ++i) {
    ObRawExpr *expr = qualify_filters.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (!expr->has_flag(CNT_WINDOW_FUNC) && OB_FAIL(preds.push_back(expr))) {
      LOG_WARN("push back pred failed", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::gather_pullup_preds_from_semi_outer_join(ObDMLStmt &stmt,
                                                                             ObIArray<ObRawExpr*> &preds,
                                                                             bool remove_preds /* default false*/)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> left_rel_ids;
  SemiInfo *semi_info = NULL;
  ObRawExpr *expr = NULL;
  ObSEArray<ObRawExpr*, 4> new_semi_conditions;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_semi_infos().count(); ++i) {
    left_rel_ids.reuse();
    new_semi_conditions.reuse();
    if (OB_ISNULL(semi_info = stmt.get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is null", K(ret));
    } else if (semi_info->is_anti_join()) {
      // do not pull up anti conditions to upper conds
    } else if (OB_FAIL(stmt.get_table_rel_ids(semi_info->left_table_ids_, left_rel_ids))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < semi_info->semi_conditions_.count(); ++j) {
        if (OB_ISNULL(expr = semi_info->semi_conditions_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (!expr->has_flag(CNT_COLUMN) || !expr->get_relation_ids().is_subset(left_rel_ids)) {
          if (remove_preds && OB_FAIL(new_semi_conditions.push_back(expr))) {
            LOG_WARN("failed to push back expr", K(ret));
          }
        } else if (OB_FAIL(preds.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        }
      }
      if (OB_SUCC(ret) && remove_preds && semi_info->semi_conditions_.count() != new_semi_conditions.count()
          && OB_FAIL(semi_info->semi_conditions_.assign(new_semi_conditions))) {
        LOG_WARN("failed to assign semi conditions", K(ret));
      }
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_joined_tables().count(); ++i) {
    if (OB_FAIL(gather_pullup_preds_from_join_table(stmt.get_joined_tables().at(i), preds, remove_preds))) {
      LOG_WARN("failed to pullup preds from joined table", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::gather_pullup_preds_from_join_table(TableItem *table,
                                                                        ObIArray<ObRawExpr*> &preds,
                                                                        bool remove_preds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (table->is_joined_table()) {
    JoinedTable *join_table = static_cast<JoinedTable*>(table);
    if (join_table->is_inner_join() && OB_FAIL(append(preds, join_table->join_conditions_))) {
      LOG_WARN("failed to append conditions", K(ret));
    } else if ((join_table->is_inner_join() || join_table->is_left_join())
               && OB_FAIL(SMART_CALL(gather_pullup_preds_from_join_table(join_table->left_table_, preds, remove_preds)))) {
      LOG_WARN("failed to pullup preds from joined table", K(ret));
    } else if ((join_table->is_inner_join() || join_table->is_right_join())
               && OB_FAIL(SMART_CALL(gather_pullup_preds_from_join_table(join_table->right_table_, preds, remove_preds)))) {
      LOG_WARN("failed to pullup preds from joined table", K(ret));
    } else if (join_table->is_inner_join() && remove_preds) {
      join_table->join_conditions_.reset();
    }
  }
  return ret;
}

// be careful about anti join right table,
// do not pullup a filter which does not contain any column of the right table.
int ObTransformPredicateMoveAround::compute_pullup_predicates(
    ObSelectStmt &view,
    const ObIArray<int64_t> &select_list,
    ObIArray<ObRawExpr *> &local_preds,
    ObIArray<ObRawExpr *> &pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> select_exprs;
  ObSEArray<ObRawExpr *, 4> deduced_preds;
  pullup_preds.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < select_list.count(); ++i) {
    int64_t idx = select_list.at(i);
    ObRawExpr *sel_expr = NULL;
    if (OB_UNLIKELY(idx < 0 || idx >= view.get_select_item_size()) ||
        OB_ISNULL(sel_expr = view.get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret), K(sel_expr), K(idx));
    } else if (!sel_expr->is_column_ref_expr() &&
               !sel_expr->is_win_func_expr() &&
               !sel_expr->is_aggr_expr()) {
    } else if (OB_FAIL(select_exprs.push_back(sel_expr))) {
      LOG_WARN("failed to push back select expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(transform_predicates(
                  view, local_preds, select_exprs, deduced_preds, true))) {
      LOG_WARN("failed to deduce predicates", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < deduced_preds.count(); ++i) {
    //  0 for neither use valid select expr nor use invalid ones
    //  1 for only use valid select expr
    // -1 for use invalid select expr
    int64_t state = 0;
    if (OB_ISNULL(deduced_preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deduced predicate is null", K(ret));
    } else if ((!ObPredicateDeduce::is_simple_condition(deduced_preds.at(i)->get_expr_type()) &&
                T_OP_IS != deduced_preds.at(i)->get_expr_type() &&
                !ObPredicateDeduce::is_general_condition(deduced_preds.at(i)->get_expr_type())) ||
               deduced_preds.at(i)->is_const_expr() ||
               deduced_preds.at(i)->has_flag(CNT_SUB_QUERY)) {
      // do nothing
    } else if (OB_FAIL(check_expr_pullup_validity(
                         deduced_preds.at(i), select_exprs, state))) {
      LOG_WARN("failed to check pullup validity", K(ret));
    } else if (state != 1) {
      // do nothing
    } else if (OB_FAIL(pullup_preds.push_back(deduced_preds.at(i)))) {
      LOG_WARN("failed to push back predicates", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_expr_pullup_validity(
    ObRawExpr *expr, const ObIArray<ObRawExpr *> &pullup_list, int64_t &state)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> parent_exprs;
  if (OB_FAIL(recursive_check_expr_pullup_validity(expr, pullup_list, parent_exprs, state))) {
    LOG_WARN("failed to check pullup validity", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::recursive_check_expr_pullup_validity(
    ObRawExpr *expr,
    const ObIArray<ObRawExpr *> &pullup_list,
    ObIArray<ObRawExpr *> &parent_exprs,
    int64_t &state)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_aggr_expr() ||
             expr->is_win_func_expr() ||
             expr->is_column_ref_expr() ||
             expr->is_set_op_expr()) {
    bool can_replace = false;
    if (OB_FAIL(ObTransformUtils::check_can_replace(expr, parent_exprs, false, can_replace))) {
      LOG_WARN("failed to check can replace expr", K(ret));
    } else if (!can_replace) {
      state = -1;
    } else {
      state = ObOptimizerUtil::find_item(pullup_list, expr) ? 1 : -1;
    }
  } else if (expr->is_generalized_column()) {
    state = -1;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && state >= 0 && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(parent_exprs.push_back(expr))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(SMART_CALL(recursive_check_expr_pullup_validity(expr->get_param_expr(i),
                                                                  pullup_list,
                                                                  parent_exprs,
                                                                  state)))) {
        LOG_WARN("failed to check pullup validity", K(ret));
      }
      if (OB_SUCC(ret)) {
        parent_exprs.pop_back();
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::rename_pullup_predicates(
    ObDMLStmt &stmt,
    TableItem &view,
    const ObIArray<int64_t> &sel_ids,
    ObIArray<ObRawExpr *> &preds)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(view_stmt = view.ref_query_) || OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else {
    ObRawExprCopier copier(*expr_factory);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_ids.count(); ++i) {
      ObRawExpr *sel_expr = NULL;
      ObColumnRefRawExpr *col_expr = NULL;
      int64_t idx = sel_ids.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= view_stmt->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select index is invalid", K(ret), K(idx));
      } else if (OB_ISNULL(sel_expr = view_stmt->get_select_item(idx).expr_) ||
                 OB_ISNULL(col_expr = stmt.get_column_expr_by_id(
                             view.table_id_, idx + OB_APP_MIN_COLUMN_ID))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr/ column expr is not found", K(ret), K(sel_expr), K(col_expr));
      } else if (copier.is_existed(sel_expr)) {
        // do nothing
      } else if (OB_FAIL(copier.add_replaced_expr(sel_expr, col_expr))) {
        LOG_WARN("failed to add replace expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
      ObRawExpr *new_pred = NULL;
      if (OB_FAIL(copier.copy_on_replace(preds.at(i), new_pred))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_FAIL(new_pred->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(new_pred->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else {
        preds.at(i) = new_pred;
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::rename_pullup_predicates(
    ObDMLStmt &stmt,
    TableItem &view,
    ObIArray<ObRawExpr *> &preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 8> sel_exprs;
  ObSEArray<ObRawExpr *, 8> col_exprs;
  ObRawExprFactory *expr_factory = NULL;
  if (preds.empty()) {
    // do nothing
  } else if (OB_ISNULL(ctx_) ||
             OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
             OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret));
  } else if (OB_FAIL(stmt.get_view_output(view, sel_exprs, col_exprs))) {
    LOG_WARN("failed to get view output", K(ret));
  } else {
    ObRawExprCopier copier(*expr_factory);
    if (OB_FAIL(copier.add_replaced_expr(sel_exprs, col_exprs))) {
      LOG_WARN("failed to add replaced expr");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
      ObRawExpr *new_pred = NULL;
      if (OB_FAIL(copier.copy_on_replace(preds.at(i), new_pred))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_FAIL(new_pred->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(new_pred->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else {
        preds.at(i) = new_pred;
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::pushdown_predicates
 * @param stmt
 * @param pushdown_preds
 * the push down preds are execute upon the output of the stmt
 * -  push down predicates
 * -  limit                 不能下推过 limit
 * -  order by              可以下推过 order by
 * -  sequence              不能下推过 sequence
 * -  distinct              可以下推过 distinct
 * -  window function       当谓词关联的列是 partition by 列的子集，可以下推
 * -  groupby/having        当谓词关联的列是 group by 列的子集，可以下推 (有rollup时不检查rollup列，谓词关联的列为 group by 列的子集时下推)
 * -  rownum                不能下推过 rownum
 * -  where                 下推谓词加入到 where conditions
 * -  from-join             where 谓词强化 joined-on 条件，并尝试下推到 VIEW 中
 * -  table-access          do nothing
 *
 * we would like to push down predicates as far as possible
 *
 * 有 limit, sequence, rownum, user variable assignment 的时候，谓词可能无法下推，这种情况下，要加回到上层
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_predicates(
    ObDMLStmt *stmt, ObIArray<ObRawExpr *> &pushdown_preds)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_group = false;
  bool has_winfunc = false;
  ObSEArray<ObRawExpr *, 4> candi_preds;
  ObIArray<ObRawExpr *> *pullup_preds = NULL;
  ObSelectStmt *sel_stmt = static_cast<ObSelectStmt *>(stmt);
  bool enable_no_pred_deduce = false;
  OPT_TRACE_BEGIN_SECTION;
  OPT_TRACE("try pushdown predicates:", pushdown_preds);
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret), K(stmt));
  } else if (OB_FAIL(acquire_transform_params(stmt, pullup_preds))) {
    LOG_WARN("failed to acquire pull up preds", K(ret));
  } else if (OB_FAIL(check_enable_no_pred_deduce(*stmt, enable_no_pred_deduce))) {
    LOG_WARN("check_enable_no_pred_deduce failed", K(ret));
  } else if (OB_ISNULL(pullup_preds)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pullup predicate array is null", K(ret));
  } else if (stmt->is_select_stmt() &&
             static_cast<ObSelectStmt *>(stmt)->contain_ab_param()) {
    // do nothing
  } else if (stmt->is_set_stmt()) {
    ObSelectStmt *sel_stmt = static_cast<ObSelectStmt*>(stmt);
    if (OB_FAIL(pushdown_into_set_stmt(sel_stmt,
                                       *pullup_preds,
                                       pushdown_preds))) {
      LOG_WARN("recursive pushdown preds into set stmt failed", K(ret));
    } else {/*do nothing*/}
  } else if (stmt->is_hierarchical_query()) {
    // do not transform for current level stmt, but need call function to transform child query
    ObArray<ObRawExpr *> dummy_preds;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_size(); ++i) {
      TableItem *table = stmt->get_table_item(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (!table->is_generated_table() &&
                 !table->is_lateral_table()) {
        //do nothing
      } else if (OB_FAIL(SMART_CALL(pushdown_predicates(table->ref_query_, dummy_preds)))) {
        LOG_WARN("failed to push down predicates", K(ret));
      } else {/*do nothing*/}
    }
  } else if (enable_no_pred_deduce) {
    // do not transform for current level stmt, but need call function to transform child query
    LOG_TRACE("NO PRED DEDUCE");
    OPT_TRACE("hint disable transform");
    if (OB_FAIL(pushdown_into_tables_skip_current_level_stmt(*stmt))) {
      LOG_WARN("failed to pushdown predicates into tables skip current level stmt", K(ret));
    }
  } else {
    const uint64_t pushdown_pred_count = pushdown_preds.count();
    bool is_happened = false;
    bool has_distinct = false;
    if (OB_FAIL(stmt->has_rownum(has_rownum))) {
      LOG_WARN("failed to check stmt has rownum", K(ret));
    } else if (stmt->is_select_stmt()) {
      has_group = sel_stmt->has_group_by();
      has_winfunc = sel_stmt->has_window_function();
      has_distinct = sel_stmt->has_distinct();
    }
    if (OB_SUCC(ret)) {
      if (stmt->has_limit() || stmt->has_sequence() ||
          stmt->is_contains_assignment()) {
        // no exprs can be pushed down
        OPT_TRACE("stmt has limit/sequence/assignmen, can not pushdown any pred");
      } else if (has_rownum && !has_group) {
        // predicates can only be pushed into where
        // but with rownum, it is impossible
        OPT_TRACE("stmt has rownum, can not pushdown into where");
      } else if (has_winfunc) {
        if (!has_distinct && !sel_stmt->is_dblink_stmt()
            && OB_FAIL(pushdown_into_qualify_filter(pushdown_preds, *sel_stmt, is_happened))) {
          LOG_WARN("extract winfunc topn exprs failed", K(ret));
        } else if (OB_FAIL(pushdown_through_winfunc(*sel_stmt, pushdown_preds, candi_preds))) {
          LOG_WARN("failed to push down predicates throught winfunc", K(ret));
        }
      } else if (OB_FAIL(candi_preds.assign(pushdown_preds))) {
        LOG_WARN("failed to assign push down predicates", K(ret));
      } else {
        pushdown_preds.reset();
      }
      if (OB_SUCC(ret) && pushdown_preds.count() != pushdown_pred_count) {
        is_happened = true;
      }
    }
    if (OB_SUCC(ret) && has_group) {
      ObSEArray<ObRawExpr*, 4> old_having_exprs;
      if (OB_FAIL(old_having_exprs.assign(sel_stmt->get_having_exprs()))) {
        LOG_WARN("failed to assign having exprs", K(ret));
      } else if (OB_FAIL(pushdown_into_having(*sel_stmt, *pullup_preds, candi_preds))) {
        LOG_WARN("failed to push down predicates into having", K(ret));
      } else if (sel_stmt->get_group_exprs().empty() || has_rownum) {
        candi_preds.reset();
        OPT_TRACE("can not pushdown preds into scalar/rollup gorup by");
      } else if (OB_FAIL(pushdown_through_groupby(*sel_stmt, candi_preds))) {
        LOG_WARN("failed to pushdown predicate", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(check_conds_deduced(old_having_exprs, sel_stmt->get_having_exprs(), is_happened))) {
        LOG_WARN("failed to check transform happened", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      typedef ObSEArray<ObSEArray<ObRawExpr*, 16>, 4> PredsArray;
      SMART_VAR(PredsArray, all_old_preds) {
        ObIArray<FromItem> &from_items = stmt->get_from_items();
        ObIArray<SemiInfo*> &semi_infos = stmt->get_semi_infos();
        if (OB_FAIL(store_all_preds(*stmt, all_old_preds))) {
          LOG_WARN("failed to store all preds", K(ret));
        }  else if (OB_FAIL(gather_pullup_preds_from_semi_outer_join(*stmt, stmt->get_condition_exprs(), true))) {
          LOG_WARN("failed to pullup preds from semi outer join", K(ret));
        } else if (OB_FAIL(pushdown_into_where(*stmt, *pullup_preds, candi_preds))) {
          LOG_WARN("failed to push down predicates into where", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
          ObSEArray<ObRawExprCondition *, 4> pred_conditions;
          if (OB_FAIL(append_condition_array(pred_conditions,
                                              stmt->get_condition_size(),
                                              &stmt->get_condition_exprs()))) {
            LOG_WARN("failed to prepare allocate", K(ret));
          } else if (OB_FAIL(pushdown_into_table(stmt, stmt->get_table_item(from_items.at(i)),
                                          *pullup_preds, stmt->get_condition_exprs(), pred_conditions))) {
            LOG_WARN("failed to push down predicates", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
          if (OB_FAIL(pushdown_into_semi_info(stmt, semi_infos.at(i), *pullup_preds,
                                              stmt->get_condition_exprs()))) {
            LOG_WARN("failed to push down into semi info", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(check_transform_happened(all_old_preds, *stmt, is_happened))) {
          LOG_WARN("failed to check transform happened", K(ret));
        } else if (is_happened && OB_FAIL(add_var_to_array_no_dup(transed_stmts_, stmt))) {
          LOG_WARN("append transed stmt failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !stmt->is_hierarchical_query()) {
    ObArray<ObRawExpr *> dummy_expr;
    ObIArray<ObQueryRefRawExpr *> &subquery_exprs = stmt->get_subquery_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < subquery_exprs.count(); i++) {
      if (OB_ISNULL(subquery_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(pushdown_predicates(subquery_exprs.at(i)->get_ref_stmt(), dummy_expr))) {
        LOG_WARN("push down predicate failed", K(ret));
      }
    }
  }
  OPT_TRACE_END_SECTION;
  return ret;
}

// when hint disabled transform for current level stmt, need try transform child stmt in table item in specific ordering.
int ObTransformPredicateMoveAround::pushdown_into_tables_skip_current_level_stmt(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> dummy_preds;
  TableItem *table_item = NULL;
  ObIArray<FromItem> &from_items = stmt.get_from_items();
  ObIArray<SemiInfo*> &semi_infos = stmt.get_semi_infos();
  for (int64_t i = 0; OB_SUCC(ret) && i < from_items.count(); ++i) {
    if (OB_ISNULL(table_item = stmt.get_table_item(from_items.at(i)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params have null", K(ret), K(table_item));
    } else if (table_item->is_generated_table() ||
               table_item->is_lateral_table()) {
      if (OB_FAIL(SMART_CALL(pushdown_predicates(table_item->ref_query_, dummy_preds)))) {
        LOG_WARN("failed to push down predicates", K(ret));
      }
    } else if (!table_item->is_joined_table()) {
      /* do nothing */
    } else if (OB_FAIL(pushdown_into_joined_table_skip_current_level_stmt(table_item))) {
      LOG_WARN("failed to push down predicates", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    if (OB_ISNULL(semi_infos.at(i)) ||
        OB_ISNULL(table_item = stmt.get_table_item_by_id(semi_infos.at(i)->right_table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params have null", K(ret), K(i), K(table_item), K(semi_infos));
    } else if (!table_item->is_generated_table()) {
      /* do nothing */
    } else if (OB_FAIL(SMART_CALL(pushdown_predicates(table_item->ref_query_, dummy_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_into_joined_table_skip_current_level_stmt(TableItem *table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(table_item));
  } else if (table_item->is_generated_table() ||
             table_item->is_lateral_table()) {
    ObArray<ObRawExpr *> dummy_preds;
    if (OB_FAIL(SMART_CALL(pushdown_predicates(table_item->ref_query_, dummy_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    }
  } else if (!table_item->is_joined_table()) {
    /* do nothing */
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table_item);
    if (OB_FAIL(SMART_CALL(pushdown_into_joined_table_skip_current_level_stmt(joined_table->left_table_)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else if (OB_FAIL(SMART_CALL(pushdown_into_joined_table_skip_current_level_stmt(joined_table->right_table_)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::store_all_preds(const ObDMLStmt &stmt,
                                                    ObIArray<ObSEArray<ObRawExpr*, 16>> &all_preds)
{
  int ret = OB_SUCCESS;
  const ObIArray<JoinedTable*> &join_tables = stmt.get_joined_tables();
  const ObIArray<SemiInfo*> &semi_infos = stmt.get_semi_infos();
  ObIArray<ObRawExpr*> *preds = NULL;
  all_preds.reuse();
  if (OB_ISNULL(preds = all_preds.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Allocate ObRawExpr* ObSEArray from array error", K(ret));
  } else if (OB_FAIL(preds->assign(stmt.get_condition_exprs()))) {
    LOG_WARN("failed to assign conditions", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < join_tables.count(); ++i) {
    if (OB_FAIL(store_join_conds(join_tables.at(i), all_preds))) {
      LOG_WARN("failed to store join conds", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_infos.count(); ++i) {
    if (OB_ISNULL(semi_infos.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_ISNULL(preds = all_preds.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate ObRawExpr* ObSEArray from array error", K(ret));
    } else if (OB_FAIL(preds->assign(semi_infos.at(i)->semi_conditions_))) {
      LOG_WARN("failed to assign conditions", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::store_join_conds(const TableItem *table,
                                                     ObIArray<ObSEArray<ObRawExpr*, 16>> &all_preds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (table->is_joined_table()) {
    const JoinedTable *join_table = static_cast<const JoinedTable*>(table);
    ObIArray<ObRawExpr*> *preds = NULL;
    if (OB_ISNULL(preds = all_preds.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate ObRawExpr* ObSEArray from array error", K(ret));
    } else if (OB_FAIL(preds->assign(join_table->join_conditions_))) {
      LOG_WARN("failed to assign conditions", K(ret));
    } else if (OB_FAIL(SMART_CALL(store_join_conds(join_table->left_table_, all_preds)))) {
      LOG_WARN("failed to pullup preds from joined table", K(ret));
    } else if (OB_FAIL(SMART_CALL(store_join_conds(join_table->right_table_, all_preds)))) {
      LOG_WARN("failed to pullup preds from joined table", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_transform_happened(const ObIArray<ObSEArray<ObRawExpr*, 16>> &all_preds,
                                                             ObDMLStmt &stmt,
                                                             bool &is_happened)
{
  int ret = OB_SUCCESS;
  ObIArray<JoinedTable*> &join_tables = stmt.get_joined_tables();
  ObIArray<SemiInfo*> &semi_infos = stmt.get_semi_infos();
  bool real_happened = is_happened && real_happened_;
  if (OB_UNLIKELY(all_preds.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty", K(ret), K(all_preds.count()));
  } else if (OB_FAIL(check_conds_deduced(all_preds.at(0), stmt.get_condition_exprs(), real_happened))) {
    LOG_WARN("failed to push back conditions", K(ret));
  } else {
    uint64_t idx = 1;
    for (int64_t i = 0; !real_happened && OB_SUCC(ret) && i < join_tables.count(); ++i) {
      if (OB_FAIL(check_join_conds_deduced(all_preds, idx, join_tables.at(i), real_happened))) {
        LOG_WARN("failed to check join conds deduced", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(semi_infos.count() + idx > all_preds.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", K(ret), K(semi_infos.count()), K(idx), K(all_preds.count()));
    }
    for (int64_t i = 0; !real_happened && OB_SUCC(ret) && i < semi_infos.count(); ++i) {
      if (OB_ISNULL(semi_infos.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (OB_FAIL(check_conds_deduced(all_preds.at(idx),
                                             semi_infos.at(i)->semi_conditions_,
                                             real_happened))) {
        LOG_WARN("failed to check conds deduced", K(ret));
      } else {
        ++idx;
      }
    }
    if (OB_SUCC(ret)) {
      is_happened |= real_happened;
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_join_conds_deduced(
                                               const ObIArray<ObSEArray<ObRawExpr*, 16>> &all_preds,
                                               uint64_t &idx,
                                               TableItem *table,
                                               bool &is_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!is_happened && table->is_joined_table()) {
    JoinedTable *join_table = static_cast<JoinedTable*>(table);
    if (OB_UNLIKELY(all_preds.count() <= idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx", K(ret), K(idx), K(all_preds.count()));
    } else if (OB_FAIL(check_conds_deduced(all_preds.at(idx), join_table->join_conditions_, is_happened))) {
      LOG_WARN("failed to check conds deduced", K(ret));
    } else {
      ++idx;
    }
    if (OB_FAIL(ret) || is_happened) {
    } else if (OB_FAIL(SMART_CALL(check_join_conds_deduced(all_preds, idx, join_table->left_table_, is_happened)))) {
      LOG_WARN("failed to check join conds deduced", K(ret));
    } else if (OB_FAIL(SMART_CALL(check_join_conds_deduced(all_preds, idx, join_table->right_table_, is_happened)))) {
      LOG_WARN("failed to check join conds deduced", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_conds_deduced(const ObIArray<ObRawExpr *> &old_conditions,
                                                        ObIArray<ObRawExpr *> &new_conditions,
                                                        bool &is_happened)
{
  int ret = OB_SUCCESS;
  bool happened = false;
  uint64_t new_conditions_count = new_conditions.count();
  uint64_t old_conditions_count = old_conditions.count();
  if (new_conditions_count != old_conditions_count) {
    happened = true;
  } else {
    for (int64_t i = 0; !happened && i < new_conditions_count; ++i) {
      if (!ObPredicateDeduce::find_equal_expr(old_conditions, new_conditions.at(i)) ||
          !ObPredicateDeduce::find_equal_expr(new_conditions, old_conditions.at(i))) {
        happened = true;
      }
    }
  }
  if (happened) {
    is_happened = true;
    real_happened_ = true;
  } else if (OB_FAIL(new_conditions.assign(old_conditions))) {
    LOG_WARN("failed to assign exprs", K(ret));
  }
  LOG_DEBUG("check transform happened", K(old_conditions), K(new_conditions), K(happened));
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_into_set_stmt(ObSelectStmt *stmt,
                                                          ObIArray<ObRawExpr *> &pullup_preds,
                                                          ObIArray<ObRawExpr *> &pushdown_preds)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSelectStmt*> &child_query = stmt->get_set_query();
  const int64_t child_num = child_query.count();
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (OB_UNLIKELY(!stmt->is_set_stmt()) || OB_UNLIKELY(child_num < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (stmt->is_recursive_union() || stmt->has_limit()) {
    ObArray<ObRawExpr *> dummy_preds;
    OPT_TRACE("try to pushdown into recursive union`s child query");
    for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
      ret = SMART_CALL(pushdown_predicates(child_query.at(i), dummy_preds));
    }
  } else {
    ObSEArray<ObRawExpr *, 16> last_input_preds;
    ObSEArray<ObRawExpr *, 16> input_preds;
    ObSEArray<ObRawExpr *, 16> cur_pushdown_preds;
    ObSEArray<ObRawExpr *, 16> dummy_pullup_preds;
    bool false_cond_exists = false;
    OPT_TRACE("try to pushdown into set stmt`s child query");
    for (int64_t i = 0; OB_SUCC(ret) && i < child_num; ++i) {
      if (OB_FAIL(input_preds.assign(pushdown_preds))) {
        LOG_WARN("assign right input preds failed", K(ret));
      } else if (OB_FAIL(check_false_condition(child_query.at(i),
                                               false_cond_exists))) {
        LOG_WARN("failed to check false condition", K(ret));
      } else if (!false_cond_exists && OB_FAIL(pushdown_into_set_stmt(child_query.at(i),
                                                stmt->get_set_op() == ObSelectStmt::UNION ?
                                                dummy_pullup_preds : pullup_preds,
                                                input_preds, stmt))) {
        LOG_WARN("failed to pushdown into set stmt", K(ret));
      } else if (0 == i) {
        ret = last_input_preds.assign(input_preds);
      } else if (OB_FAIL(check_pushdown_predicates(stmt, last_input_preds,
                                                   input_preds, cur_pushdown_preds))) {
        LOG_WARN("choose pushdown predicates failed", K(ret));
      } else if (OB_FAIL(last_input_preds.assign(cur_pushdown_preds))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(pushdown_preds.assign(cur_pushdown_preds))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_pushdown_predicates(ObSelectStmt *stmt,
                                                              ObIArray<ObRawExpr *> &left_pushdown_preds,
                                                              ObIArray<ObRawExpr *> &right_pushdown_preds,
                                                              ObIArray<ObRawExpr *> &output_pushdown_preds)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (!stmt->is_set_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is not a set stmt", K(ret));
  } else if (stmt->is_recursive_union()) {
    /*do nothing*/
  } else if (stmt->get_set_op() == ObSelectStmt::UNION) {
    //对于未成功下推的谓词，需要合并加回上层
    for (int64_t i = 0; OB_SUCC(ret) && i < right_pushdown_preds.count(); ++i) {
      if (ObPredicateDeduce::find_equal_expr(left_pushdown_preds, right_pushdown_preds.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(left_pushdown_preds.push_back(right_pushdown_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else {/*do nothing*/}
    }
    if (OB_SUCC(ret) && OB_FAIL(output_pushdown_preds.assign(left_pushdown_preds))) {
      LOG_WARN("assign predicated failed", K(ret));
    } else {/*do nothing*/}
  } else if (stmt->get_set_op() == ObSelectStmt::INTERSECT) {
    //对于左右两侧均未成功下推的谓词，需要加回上层
    output_pushdown_preds.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < right_pushdown_preds.count(); ++i) {
      if (!ObPredicateDeduce::find_equal_expr(left_pushdown_preds, right_pushdown_preds.at(i))) {
        /*do nothing*/
      } else if (OB_FAIL(output_pushdown_preds.push_back(right_pushdown_preds.at(i)))) {
        LOG_WARN("push back preds failed", K(ret));
      } else {/*do nothing*/}
    }
  } else if (stmt->get_set_op() == ObSelectStmt::EXCEPT) {
    //对于左侧未成功下推的谓词，需要加回上层
    if (OB_FAIL(output_pushdown_preds.assign(left_pushdown_preds))) {
      LOG_WARN("assign predicated failed", K(ret));
    } else {/*do nothing*/}
  } else {/*do nothing*/}
  return ret;
}

int ObTransformPredicateMoveAround::remove_useless_equal_const_preds(ObSelectStmt *stmt, 
                                                                     ObIArray<ObRawExpr *> &exprs,
                                                                     ObIArray<ObRawExpr *> &equal_const_preds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObRawExpr *cur_expr = exprs.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret), K(cur_expr));
    } else if (T_OP_EQ == cur_expr->get_expr_type()) {
      ObRawExpr *param_1 = cur_expr->get_param_expr(0);
      ObRawExpr *param_2 = cur_expr->get_param_expr(1);
      bool is_not_null = false;
      if (OB_ISNULL(param_1) || OB_ISNULL(param_2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param", K(ret));
      } else if (!param_1->is_const_expr() ||
                 !param_2->is_const_expr() ||
                 !param_1->same_as(*param_2)) {
        
      } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(
                        ctx_, stmt, param_1, NULLABLE_SCOPE::NS_TOP, is_not_null))) {
        LOG_WARN("failed to check expr not null", K(ret));
      } else if (!is_not_null) {

      } else if (OB_FAIL(equal_const_preds.push_back(cur_expr))) {
        LOG_WARN("failed to push back into useless equal const preds", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObOptimizerUtil::remove_item(exprs, equal_const_preds))) {
    LOG_WARN("failed to remove equal const from exprs", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_into_set_stmt(ObSelectStmt *stmt,
                                                          ObIArray<ObRawExpr *> &pullup_preds,
                                                          ObIArray<ObRawExpr *> &pushdown_preds,
                                                          ObSelectStmt *parent_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(parent_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(stmt), K(parent_stmt), K(ret));
  } else {
    ObSEArray<ObRawExpr*, 16> subquery_preds;
    ObSEArray<ObRawExpr*, 16> valid_preds;  //存储能够下推的谓词
    ObSEArray<ObRawExpr*, 16> rename_preds; //存储重命名为当前stmt的select expr的谓词
    ObSEArray<ObRawExpr*, 16> candi_preds;  //返回未成功下推的谓词
    ObSEArray<ObRawExpr*, 16> equal_const_preds;
    ObSEArray<ObRawExpr*, 16> invalid_pushdown_preds;
    ObSEArray<ObRawExpr*, 16> invalid_pullup_preds;
    const int64_t pushdown_preds_cnt = pushdown_preds.count();
    if (OB_FAIL(extract_valid_preds(parent_stmt, stmt, pushdown_preds, valid_preds, invalid_pushdown_preds))
        || OB_FAIL(extract_valid_preds(parent_stmt, stmt, pullup_preds, valid_preds, invalid_pullup_preds))) {
      LOG_WARN("failed to check push down", K(ret));
    } else if (OB_FAIL(rename_preds.assign(valid_preds))) {
      LOG_WARN("failed to assign rename preds", K(ret));
    } else if (OB_FAIL(rename_set_op_predicates(*stmt, *parent_stmt, rename_preds, false))) {
      LOG_WARN("rename pushdown predicates failed", K(ret));
    } else if (OB_FAIL(candi_preds.assign(rename_preds))) {
      LOG_WARN("failed to assign filters", K(ret));
    } else if (OB_FAIL(remove_useless_equal_const_preds(stmt, candi_preds, equal_const_preds))) {
      LOG_WARN("failed to remove useless equal const preds", K(ret));
    } else if (OB_FAIL(SMART_CALL(pushdown_predicates(stmt, candi_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else if (OB_FAIL(append(candi_preds, equal_const_preds))) {
      LOG_WARN("failed to add back equal const preds to candi array", K(ret));
    } else {
      ObSEArray<ObRawExpr*, 16> output_preds;  //返回未成功下推的原始谓词
      //把未下推的谓词转换为原始谓词返回
      for (int64_t i = 0; OB_SUCC(ret) && i < rename_preds.count(); ++i) {
        if (!ObPredicateDeduce::find_equal_expr(candi_preds, rename_preds.at(i))) {
          /*成功下推的谓词，不需要返回*/
        } else if (!ObPredicateDeduce::find_equal_expr(pushdown_preds, valid_preds.at(i))) {
          /*pullup的谓词没有成功下推，不需要返回*/
        } else if (OB_FAIL(output_preds.push_back(valid_preds.at(i)))) {
          LOG_WARN("push back predicate failed", K(ret));
        } else {/*do nothing*/}
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(pushdown_preds.assign(output_preds))) {
        LOG_WARN("assign preds failed", K(ret));
      } else if (OB_FAIL(append(pushdown_preds, invalid_pushdown_preds))) {
        LOG_WARN("failed to append no push down preds", K(ret));
      } else if ((pushdown_preds.count() != pushdown_preds_cnt
                 || (stmt->is_set_stmt() && ObOptimizerUtil::find_item(transed_stmts_, stmt)))
                 && OB_FAIL(add_var_to_array_no_dup(transed_stmts_, static_cast<ObDMLStmt*>(parent_stmt)))) {
        LOG_WARN("append transed stmt failed", K(ret));
      } else {/*do nothing*/}
    }
  }
  return ret;
}

/**
 * @brief 
 *  predicates that contains subquery flag or string type set op expr
 *  are now allowed to be pushed down into set stmt
 * @param all_preds 
 * @param valid_preds 
 * @param invalid_preds 
 * @return int 
 */
int ObTransformPredicateMoveAround::extract_valid_preds(ObSelectStmt *stmt,
                                                        ObSelectStmt *child_stmt,
                                                        ObIArray<ObRawExpr *> &all_preds,
                                                        ObIArray<ObRawExpr *> &valid_preds,
                                                        ObIArray<ObRawExpr *> &invalid_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> parent_set_exprs;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(stmt->get_pure_set_exprs(parent_set_exprs))) {
    LOG_WARN("failed to get parent set exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_preds.count(); ++i) {
    bool is_valid = true;
    bool is_subquery = false;
    ObRawExpr *expr = all_preds.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null expr", K(ret));
    } else if (expr->has_flag(CNT_SUB_QUERY) ||
               expr->has_flag(CNT_ONETIME)) {
      is_valid = false;
    }
    if (OB_SUCC(ret) && is_valid) {
      if (OB_FAIL(ObTransformUtils::check_pushdown_into_set_valid(child_stmt,
                                                                  expr,
                                                                  parent_set_exprs,
                                                                  is_valid))) {
        LOG_WARN("failed to check expr pushdown validity", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_valid) {
        if (OB_FAIL(invalid_preds.push_back(expr))) {
          LOG_WARN("failed to push back no push down preds", K(ret));
        }
      } else {
        if (OB_FAIL(valid_preds.push_back(expr))) {
          LOG_WARN("failed to push back no push down preds", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pullup_predicates_from_const_select(ObSelectStmt *parent_stmt,
                                                                        ObSelectStmt *child_stmt,
                                                                        ObIArray<ObRawExpr*> &pullup_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> child_select_list;
  ObSEArray<ObRawExpr *, 4> parent_select_list;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param", K(ret));
  } else if (OB_FAIL(child_stmt->get_select_exprs(child_select_list))) {
    LOG_WARN("get child stmt select exprs failed", K(ret));
  } else if (OB_FAIL(parent_stmt->get_select_exprs(parent_select_list))) {
    LOG_WARN("get parent stmt select exprs failed", K(ret));
  } else if (child_select_list.count() != parent_select_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid select list", K(child_select_list.count()), K(parent_select_list), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < parent_select_list.count(); ++i) {
      ObRawExpr *child_expr = NULL;
      ObRawExpr *parent_expr = parent_select_list.at(i);
      const ObRawExpr *real_parent_expr = parent_select_list.at(i);
      ObRawExpr *generated_expr = NULL;
      bool is_not_null = false;
      int64_t child_idx = OB_INVALID_ID;
      if (OB_ISNULL(parent_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::get_real_expr_without_cast(parent_expr, real_parent_expr))) {
        LOG_WARN("fail to get real expr", K(ret));
      } else if (OB_ISNULL(real_parent_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr", K(ret), K(real_parent_expr));
      } else if (!real_parent_expr->is_set_op_expr()) {
        // do nothing
      } else if (FALSE_IT(child_idx = static_cast<const ObSetOpRawExpr *>(real_parent_expr)->get_idx())) {
      } else if (OB_UNLIKELY(child_idx < 0 || child_idx >= child_select_list.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("set op index is invalid", K(ret), K(child_idx));
      } else if (OB_ISNULL(child_expr = child_select_list.at(child_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid expr", K(ret));
      } else if (!child_expr->is_const_expr()) {
        // do nothing
      } else if (child_expr->get_result_type().is_ext() ||
                 real_parent_expr->get_result_type().is_ext()) {
        // OP_EQ between udt not supported
      } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(
                           ctx_, child_stmt, child_expr, NULLABLE_SCOPE::NS_TOP, is_not_null))) {
        LOG_WARN("failed to check expr not null", K(ret));
      } else if (!is_not_null) {
        // do nothing
        ObObj result;
        bool got_result = false;
        if (!child_expr->is_static_scalar_const_expr()) {
          //do nothing
        } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                              child_expr,
                                                              result,
                                                              got_result,
                                                              *ctx_->allocator_))) {
          LOG_WARN("failed to calc const or caculable expr", K(ret));
        } else if (!got_result || (!result.is_null()
                  && !(lib::is_oracle_mode() && result.is_null_oracle()))) {
          //do nothing
        } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_,
                                                                  const_cast<ObRawExpr*>(real_parent_expr),
                                                                  false,
                                                                  generated_expr))) {
          LOG_WARN("fail to build is null expr", K(ret), K(real_parent_expr));
        } else if (OB_ISNULL(generated_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("is_null expr is null", K(ret));
        } else if (OB_FAIL(generated_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("formalize equal expr failed", K(ret));
        } else {}
      } else if (OB_FAIL(ObRawExprUtils::create_double_op_expr(
                           *(ctx_->expr_factory_), ctx_->session_info_, T_OP_EQ,
                           generated_expr, const_cast<ObRawExpr*>(real_parent_expr), child_expr))) {
        LOG_WARN("failed to create double op expr", K(ret));
      }
      if (OB_FAIL(ret) || NULL == generated_expr) {
        //do nothing
      } else if (OB_FAIL(generated_expr->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (OB_FAIL(pullup_preds.push_back(generated_expr))) {
        LOG_WARN("failed to push back generated expr", K(ret));
      } else {
        LOG_TRACE("generate expr for set op", K(*generated_expr));
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_false_condition(ObSelectStmt *stmt,
                                                          bool &false_cond_exists)
{
  int ret = OB_SUCCESS;
  false_cond_exists = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !false_cond_exists &&
         i < stmt->get_condition_exprs().count(); i ++) {
      ObRawExpr *expr = stmt->get_condition_exprs().at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pred found", K(ret));
      } else if (T_BOOL == expr->get_expr_type() &&
                !static_cast<const ObConstRawExpr*>(expr)->get_value().get_bool()) {
        false_cond_exists = true;
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::rename_set_op_predicates(ObSelectStmt &child_stmt,
                                                            ObSelectStmt &parent_stmt,
                                                            ObIArray<ObRawExpr *> &preds,
                                                            bool is_pullup)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> child_select_list;
  ObSEArray<ObRawExpr *, 4> parent_set_exprs;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_) || 
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param has null", K(ctx_), K(ret));
  } else if (OB_FAIL(child_stmt.get_select_exprs(child_select_list))) {
    LOG_WARN("get child stmt select exprs failed", K(ret));
  } else if (OB_FAIL(parent_stmt.get_pure_set_exprs(parent_set_exprs))) {
    LOG_WARN("failed to get parent set exprs", K(ret));
  } else if (child_select_list.count() != parent_set_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child stmt select exprs size is incorrect", K(child_select_list),
                                                          K(parent_set_exprs), K(ret));
  } else {
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (is_pullup) {
      if (OB_FAIL(copier.add_replaced_expr(child_select_list, parent_set_exprs))) {
        LOG_WARN("failed to add replace expr", K(ret));
      }
    } else {
      if (OB_FAIL(copier.add_replaced_expr(parent_set_exprs, child_select_list))) {
        LOG_WARN("failed to add replace expr", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
      ObRawExpr *new_pred = NULL;
      ObRawExpr *pred = preds.at(i);
      if (OB_FAIL(copier.copy_on_replace(pred, new_pred))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_FAIL(new_pred->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(new_pred->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else {
        preds.at(i) = new_pred;
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_into_having(
    ObSelectStmt &sel_stmt,
    ObIArray<ObRawExpr *> &pullup_preds,
    ObIArray<ObRawExpr *> &pushdown_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> new_having_exprs;
  ObSEArray<ObRawExpr *, 4> target_exprs;
  ObSEArray<ObRawExpr *, 4> input_preds;
  ObSEArray<ObRawExpr *, 4> all_columns;
  ObSEArray<ObRawExpr *, 4> columns;
  ObSqlBitSet<> table_set;
  OPT_TRACE("try to pushdown",pushdown_preds, "into having");
  if (sel_stmt.get_having_exprs().empty() && pushdown_preds.empty()) {
    // do nothing
  } else if (OB_FAIL(sel_stmt.get_column_exprs(all_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(sel_stmt.get_from_tables(table_set))) {
    LOG_WARN("failed to get from items rel ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(sel_stmt, all_columns,
                                                           table_set, columns))) {
    LOG_WARN("failed to get related columns", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(sel_stmt, pullup_preds,
                                                           table_set, input_preds))) {
    LOG_WARN("failed to get related pullup preds", K(ret));
  } else if (OB_FAIL(get_exprs_cnt_exec(sel_stmt, pullup_preds, input_preds))) { //exec param stored in pullup is useful certainly.
    LOG_WARN("get exec expr failed", K(ret));
  } else if (OB_FAIL(append(input_preds, sel_stmt.get_having_exprs()))) {
    LOG_WARN("failed to append having predicates", K(ret));
  } else if (OB_FAIL(append(input_preds, pushdown_preds))) {
    LOG_WARN("failed to append push down predicates", K(ret));
  } else if (OB_FAIL(append(target_exprs, columns))) {
    LOG_WARN("failed to append column exprs", K(ret));
  } else if (OB_FAIL(append(target_exprs, sel_stmt.get_aggr_items()))) {
    LOG_WARN("failed to append aggregation items", K(ret));
  } else if (OB_FAIL(transform_predicates(sel_stmt,
                                          input_preds,
                                          target_exprs,
                                          new_having_exprs))) {
    LOG_WARN("failed to transform having predicates", K(ret));
  } else if (OB_FAIL(accept_predicates(sel_stmt,
                                       sel_stmt.get_having_exprs(),
                                       pullup_preds,
                                       new_having_exprs))) {
    LOG_WARN("failed to check different", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::get_exprs_cnt_exec(ObDMLStmt &stmt,
                                                       ObIArray<ObRawExpr *> &pullup_preds,
                                                       ObIArray<ObRawExpr *> &conds)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> right_rel_ids;
  SemiInfo *semi_info = NULL;
  int32_t idx = OB_INVALID_INDEX;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_semi_infos().count(); ++i) {
    if (OB_ISNULL(semi_info = stmt.get_semi_infos().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("semi info is null", K(ret));
    } else if (OB_FALSE_IT(idx = stmt.get_table_bit_index(semi_info->right_table_id_))) {
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == idx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect idx", K(ret));
    } else if (OB_FAIL(right_rel_ids.add_member(idx))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < pullup_preds.count(); i++)  {
    ObRawExpr *expr = pullup_preds.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (expr->get_relation_ids().overlap(right_rel_ids)) {
    } else if (!expr->has_flag(CNT_DYNAMIC_PARAM)) {
      //do nothing
    } else if (OB_FAIL(conds.push_back(expr))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::pushdown_into_where(ObDMLStmt &stmt,
                                                        ObIArray<ObRawExpr *> &pullup_preds,
                                                        ObIArray<ObRawExpr *> &predicates)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> new_conds;
  ObSEArray<ObRawExpr *, 4> all_conds;
  ObSEArray<ObRawExpr *, 4> all_columns;
  ObSEArray<ObRawExpr *, 4> columns;
  ObSqlBitSet<> table_set;
  OPT_TRACE("try to transform where condition");
  ObIArray<ObRawExpr *> &conditions = (stmt.is_insert_stmt() || stmt.is_merge_stmt())
                                      ? static_cast<ObInsertStmt &>(stmt).get_sharding_conditions()
                                      : stmt.get_condition_exprs();
  if (conditions.empty() && predicates.empty()) {
    // do nothing
  } else if (OB_FAIL(stmt.get_column_exprs(all_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(stmt.get_from_tables(table_set))) {
    LOG_WARN("failed to get from items rel ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(stmt, all_columns,
                                                           table_set, columns))) {
    LOG_WARN("failed to get related columns", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(stmt, pullup_preds,
                                                           table_set, all_conds))) {
    LOG_WARN("failed to get related pullup preds", K(ret));
  } else if (OB_FAIL(get_exprs_cnt_exec(stmt, pullup_preds, all_conds))) { //exec param stored in pullup is useful.
    LOG_WARN("get exec expr failed", K(ret));
  } else if (OB_FAIL(append(all_conds, predicates))) {
    LOG_WARN("failed to append push down predicates", K(ret));
  } else if (OB_FAIL(append(all_conds, conditions))) {
    LOG_WARN("failed to append where conditions", K(ret));
  } else if (OB_FAIL(transform_predicates(stmt, all_conds, columns, new_conds))) {
    LOG_WARN("failed to transform non-anti conditions", K(ret));
  } else if (OB_FAIL(accept_predicates(stmt,
                                       conditions,
                                       pullup_preds,
                                       new_conds))) {
    LOG_WARN("failed to accept predicate", K(ret));
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::check_pushdown_validity_against_winfunc
 * 检查一个谓词能够压过 window function
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_through_winfunc(
    ObSelectStmt &sel_stmt, ObIArray<ObRawExpr *> &predicates, ObIArray<ObRawExpr *> &down_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> common_part_exprs;
  ObSEArray<ObRawExpr *, 4> remain_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < sel_stmt.get_window_func_count(); ++i) {
    ObWinFunRawExpr *win_expr = NULL;
    if (OB_ISNULL(win_expr = sel_stmt.get_window_func_expr(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("window function expr is null", K(ret));
    } else if (i == 0) {
      if (OB_FAIL(common_part_exprs.assign(win_expr->get_partition_exprs()))) {
        LOG_WARN("failed to assign partition exprs", K(ret));
      }
    } else if (OB_FAIL(ObOptimizerUtil::intersect_exprs(common_part_exprs,
                                                        win_expr->get_partition_exprs(),
                                                        common_part_exprs))) {
      LOG_WARN("failed to intersect expr array", K(ret));
    } else if (common_part_exprs.empty()) {
      break;
    }
  }
  OPT_TRACE("try to pushdown", predicates, "though windown function");
  for (int64_t i = 0; OB_SUCC(ret) && !common_part_exprs.empty() && i < predicates.count(); ++i) {
    ObRawExpr *pred = NULL;
    ObSEArray<ObRawExpr *, 4> column_exprs;
    bool pushed = false;
    if (OB_ISNULL(pred = predicates.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (pred->has_flag(CNT_WINDOW_FUNC)) {
      // do nothing
      OPT_TRACE(pred, "countain win func, can not pushdown");
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(pred, column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(column_exprs, common_part_exprs)) {
      // do nothing
      OPT_TRACE(pred, "contain none partition by expr, can not pushdown");
    } else if (OB_FAIL(down_exprs.push_back(pred))) {
      LOG_WARN("failed to push back predicate", K(ret));
    } else {
      pushed = true;
    }
    if (OB_SUCC(ret) && !pushed) {
      if (OB_FAIL(remain_exprs.push_back(pred))) {
        LOG_WARN("failed to push back predicate", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && !down_exprs.empty()) {
    if (OB_FAIL(predicates.assign(remain_exprs))) {
      LOG_WARN("failed to assign remain exprs", K(ret));
    }
  }
  return ret;
}

//extract topn filters for ranking window functions and pushdown into qualify_filters_
int ObTransformPredicateMoveAround::pushdown_into_qualify_filter(ObIArray<ObRawExpr *> &predicates, ObSelectStmt &sel_stmt, bool &is_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> qualify_filters;
  ObSEArray<ObRawExpr *, 4> remain_exprs;
  if (OB_ISNULL(ctx_) && OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_3_0_0) {
    //do nothing
  } else if (!ctx_->session_info_->is_qualify_filter_enabled()) {
    //do nothing
  } else if (OB_FAIL(qualify_filters.assign(sel_stmt.get_qualify_filters()))) {
    LOG_WARN("assign window function filter expr failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < predicates.count(); ++i) {
      ObRawExpr *pred = NULL;
      bool is_topn_pred = false;
      ObRawExpr *dummy_expr = NULL;
      ObWinFunRawExpr *dummy_win_expr = NULL;
      bool dummy_flag;
      if (OB_ISNULL(pred = predicates.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("predicate is null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_winfunc_topn_filter(sel_stmt.get_window_func_exprs(), pred,
                                              is_topn_pred, dummy_expr, dummy_flag, dummy_win_expr))) {
        LOG_WARN("check whether the filter is a winfunc topn filter failed", K(ret));
      } else if (is_topn_pred) {
        if (ObPredicateDeduce::find_equal_expr(qualify_filters, pred)) {
          // the condition has been ensured
        } else if (OB_FAIL(qualify_filters.push_back(pred))) {
          LOG_WARN("push back topn filter failed", K(ret));
        } else {
          is_happened = true;
        }
      } else if (OB_FAIL(remain_exprs.push_back(pred))) {
        LOG_WARN("push back filter failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sel_stmt.set_qualify_filters(qualify_filters))) {
        LOG_WARN("assign topn filters failed", K(ret));
      } else if (OB_FAIL(predicates.assign(remain_exprs))) {
        LOG_WARN("assign remain filters failed", K(ret));
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::check_pushdown_validity
 * 检查一个谓词能否压过 group by
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_through_groupby(
    ObSelectStmt &stmt, ObIArray<ObRawExpr *> &output_predicates)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> new_having_exprs;
  output_predicates.reuse();
  OPT_TRACE("try to pushdown having conditions into where");
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_having_expr_size(); ++i) {
    ObRawExpr *pred = NULL;
    ObSEArray<ObRawExpr *, 4> generalized_columns;
    bool pushed = false;
    bool has_ref_assign_user_var = false;
    if (OB_ISNULL(pred = stmt.get_having_exprs().at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (pred->has_flag(CNT_AGG)) {
      ObRawExpr *new_pred = NULL;
      if (stmt.get_aggr_item_size() == 1 &&
          pred->get_expr_type() >= T_OP_LE &&
          pred->get_expr_type() <= T_OP_GT) {
        if (OB_FAIL(deduce_param_cond_from_aggr_cond(pred->get_expr_type(),
                                                     pred->get_param_expr(0),
                                                     pred->get_param_expr(1),
                                                     new_pred))) {
          LOG_WARN("failed to deduce param condition from aggr cond", K(ret));
        } else if (NULL == new_pred) {
          // do nothing
        } else if (OB_FAIL(output_predicates.push_back(new_pred))) {
          LOG_WARN("failed to push back predicate", K(ret));
        } else {
          pushed = true;
          OPT_TRACE(pred, "deduce a new pred:", new_pred);
        }
      }
    } else if (OB_FAIL(extract_generalized_column(pred, generalized_columns))) {
      LOG_WARN("failed to extract generalized columns", K(ret));
    } else if (!ObOptimizerUtil::subset_exprs(
                 generalized_columns, stmt.get_group_exprs())) {
      // do nothing
      OPT_TRACE(pred, "has none group by expr, can not pushdown");
    } else if (ObOptimizerUtil::overlap_exprs(
                 generalized_columns, stmt.get_rollup_exprs())) {
      //do nothing
      OPT_TRACE(pred, "has rollup expr in mysql mode, can not pushdown");
    } else if (pred->has_flag(CNT_SUB_QUERY) &&
               OB_FAIL(ObOptimizerUtil::check_subquery_has_ref_assign_user_var(
                          pred, has_ref_assign_user_var))) {
      LOG_WARN("failed to check subquery has ref assign user var", K(ret));
    } else if (has_ref_assign_user_var) {
      // do nothing
      OPT_TRACE(pred, "has user var, can not pushdown");
    } else if (OB_FAIL(output_predicates.push_back(pred))) {
      LOG_WARN("failed to push back predicate", K(ret));
    } else {
      pushed = true;
    }
    if (OB_SUCC(ret) && !pushed) {
      if (T_OP_OR == pred->get_expr_type() && !ObOptimizerUtil::find_equal_expr(ctx_->push_down_filters_, pred)) {
        //对于having c1 > 1 or (c1 < 0 and count(*) > 1)
        //可以拆分出c1 > 1 or c1 < 0下推过group by
        ObRawExpr *new_pred = NULL;
        if (OB_FAIL(split_or_having_expr(stmt, *static_cast<ObOpRawExpr*>(pred), new_pred))) {
          LOG_WARN("failed to split or having expr", K(ret));
        } else if (NULL == new_pred) {
          //do nothing
        } else if (OB_FAIL(output_predicates.push_back(new_pred))) {
          LOG_WARN("failed to push back predicate", K(ret));
        } else {
          OPT_TRACE(pred, "deduce a new pred:", new_pred);
        }
      }
    }
    if (OB_SUCC(ret) && !pushed) {
      if (OB_FAIL(new_having_exprs.push_back(pred))) {
        LOG_WARN("failed to push back new having expr", K(ret));
      }
    }
    //no matter new_pred is valid, pred not need to generate new_pred and try push again
    if (OB_FAIL(ret)) {
      //do nothing
    } else if (OB_FAIL(ctx_->push_down_filters_.push_back(pred))) {
      LOG_WARN("failed to append table filters", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(stmt.get_having_exprs().assign(new_having_exprs))) {
      LOG_WARN("failed to assign new having exprs", K(ret));
    }
  }
  return ret;

// reminder: 测试一下 win func 谓词的下推过程。理论上是不能从上层 stmt 下推下来的
}

/**
 * @brief ObTransformPredicateMoveAround::deduce_param_cond_from_aggr_cond
 * select * from t group by c1 having max(c1) < 10
 * =>
 * select * from t where c1 < 10 group by c1 having max(c1) < 10
 * @return
 */
int ObTransformPredicateMoveAround::deduce_param_cond_from_aggr_cond(
    ObItemType expr_type, ObRawExpr *first, ObRawExpr *second, ObRawExpr *&new_predicate)
{
  int ret = OB_SUCCESS;
  bool flag = false;
  if (OB_ISNULL(first) || OB_ISNULL(second) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("param exprs are null", K(ret), K(first), K(second), K(ctx_));
  } else if (expr_type == T_OP_GT) {
    ret = deduce_param_cond_from_aggr_cond(T_OP_LT, second, first, new_predicate);
  } else if (expr_type == T_OP_GE) {
    ret = deduce_param_cond_from_aggr_cond(T_OP_LE, second, first, new_predicate);
  } else if ((first->get_expr_type() == T_FUN_MIN && second->is_const_expr())) {
    // min(c) < const_val => c < const_val
    if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                      ctx_->session_info_,
                                                      expr_type,
                                                      new_predicate,
                                                      first->get_param_expr(0),
                                                      second))) {
      LOG_WARN("fail create compare expr", K(ret));
    } else {
      flag = true;
    }
  } else if (first->is_const_expr() && second->get_expr_type() == T_FUN_MAX) {
    // const_val < max(c) => const_val < c
    if (OB_FAIL(ObRawExprUtils::create_double_op_expr(*ctx_->expr_factory_,
                                                      ctx_->session_info_,
                                                      expr_type,
                                                      new_predicate,
                                                      first,
                                                      second->get_param_expr(0)))) {
      LOG_WARN("fail create compare expr", K(ret));
    } else {
      flag = true;
    }
  }
  if (OB_SUCC(ret) && NULL != new_predicate && flag) {
    if (OB_FAIL(new_predicate->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize expr", K(ret));
    } else if (OB_FAIL(new_predicate->pull_relation_id())) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::split_or_having_expr(ObSelectStmt &stmt,
                                                        ObOpRawExpr &or_qual,
                                                        ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  new_expr = NULL;
  ObSEArray<ObSEArray<ObRawExpr *, 16>, 8> sub_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < or_qual.get_param_count(); ++i) {
    ObSEArray<ObRawExpr *, 16> exprs;
    if (OB_FAIL(sub_exprs.push_back(exprs))) {
      LOG_WARN("failed to push back se array", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_having_expr(stmt, or_qual, sub_exprs, is_valid))) {
    LOG_WARN("failed to check having expr", K(ret));
  } else if (!is_valid) {
    /* do nothing */
  } else if (OB_FAIL(inner_split_or_having_expr(stmt, sub_exprs, new_expr))) {
    LOG_WARN("failed to split or expr", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_having_expr(ObSelectStmt &stmt,
                                                      ObOpRawExpr &or_qual,
                                                      ObIArray<ObSEArray<ObRawExpr *, 16> > &sub_exprs,
                                                      bool &all_contain)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> generalized_columns;
  ObSEArray<ObRawExpr *, 4> param_preds;
  all_contain = true;
  for (int64_t i = 0; OB_SUCC(ret) && all_contain && i < or_qual.get_param_count(); ++i) {
    ObRawExpr *cur_expr = or_qual.get_param_expr(i);
    generalized_columns.reuse();
    param_preds.reuse();
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in or expr is null", K(ret));
    } else if (T_OP_AND == cur_expr->get_expr_type()) {
      ObOpRawExpr *and_pred = static_cast<ObOpRawExpr *>(cur_expr);
      if (OB_FAIL(param_preds.assign(and_pred->get_param_exprs()))) {
        LOG_WARN("failed to assgin predicates", K(ret));
      }
    } else {
      if (OB_FAIL(param_preds.push_back(cur_expr))) {
        LOG_WARN("failed to push back predicate", K(ret));
      }
    }
    // and expr 中要求至少有一个子expr只涉及到该表的列
    for (int64_t j = 0; OB_SUCC(ret) && j < param_preds.count(); ++j) {
      ObRawExpr *cur_and_expr = param_preds.at(j);
      generalized_columns.reuse();
      bool contain_op_row = false;
      if (OB_ISNULL(cur_and_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr in and expr is null", K(ret));
      } else if (cur_and_expr->has_flag(CNT_SUB_QUERY)) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::check_contain_op_row_expr(cur_and_expr, contain_op_row))) {
        LOG_WARN("fail to check contain op row", K(ret));
      } else if (contain_op_row) {
        // do nothing
      } else if (OB_FAIL(extract_generalized_column(cur_and_expr, generalized_columns))) {
        LOG_WARN("failed to extract generalized columns", K(ret));
      } else if (!ObOptimizerUtil::subset_exprs(
                   generalized_columns, stmt.get_group_exprs())) {
        // do nothing
      } else if (OB_FAIL(sub_exprs.at(i).push_back(cur_and_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /* do nothing */ }
    }
    all_contain = all_contain && !sub_exprs.at(i).empty();
  }
  return ret;
}

int ObTransformPredicateMoveAround::inner_split_or_having_expr(ObSelectStmt &stmt,
                                                              ObIArray<ObSEArray<ObRawExpr *, 16> > &sub_exprs,
                                                              ObRawExpr *&new_expr)
{
  int ret = OB_SUCCESS;
  new_expr = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObRawExpr *new_and_expr = NULL;
  ObSEArray<ObRawExpr*, 4> or_exprs;
  if (OB_ISNULL(ctx_) || 
      OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(session_info), K(expr_factory));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < sub_exprs.count(); ++i) {
    ObIArray<ObRawExpr *> &cur_exprs = sub_exprs.at(i);
    if (OB_FAIL(ObRawExprUtils::build_and_expr(*expr_factory,
                                                cur_exprs,
                                                new_and_expr))) {
      LOG_WARN("failed to to build and expr", K(ret));
    } else if (OB_FAIL(or_exprs.push_back(new_and_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::build_or_exprs(*expr_factory, or_exprs, new_expr))) {
      LOG_WARN("failed to build or expr", K(ret));
    } else if (OB_ISNULL(new_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (OB_FAIL(new_expr->formalize(session_info))) {
      LOG_WARN("failed to formalize and expr", K(ret));
    } else if (OB_FAIL(new_expr->pull_relation_id())) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::pushdown_into_joined_table
 * @param stmt
 * @param joined_table
 * @param preds
 *   the preds are executed upon the results of joined table
 *   consider how to execute these predicates before join
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_into_joined_table(
    ObDMLStmt *stmt,
    JoinedTable *joined_table,
    ObIArray<ObRawExpr *> &pullup_preds,
    ObIArray<ObRawExpr *> &pushdown_preds,
    ObIArray<ObRawExprCondition *> &pred_conditions)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> all_preds;
  /// STEP 1. deduce new join conditions
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(joined_table->left_table_) ||
      OB_ISNULL(joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params are invalid", K(ret), K(stmt), K(joined_table));
  } else if (joined_table->is_left_join() ||
             joined_table->is_right_join() ||
             joined_table->is_inner_join()) {
    // given {preds, join conditions}, try to deduce new predicates
    // inner join, deduce predicates for both left, right table
    // left join,  deduce predicates for only right table
    // right join, deduce predicates for only left  table
    ObSEArray<ObRawExpr *, 4> cols;
    ObSEArray<ObRawExpr *, 4> all_cols;
    ObSEArray<ObRawExpr *, 4> new_preds;
    TableItem *filterable_table = NULL;
    ObSqlBitSet <>filter_table_set;

    if (joined_table->is_left_join()) {
      filterable_table = joined_table->right_table_;
    } else if (joined_table->is_right_join()) {
      filterable_table = joined_table->left_table_;
    } else if (joined_table->is_inner_join()) {
      filterable_table = joined_table;
    }
    if (NULL != filterable_table) {
      if (OB_FAIL(stmt->get_table_rel_ids(*filterable_table, filter_table_set))) {
        LOG_WARN("failed to get table relation ids", K(ret));
      }
    }

    ObSEArray<ObRawExpr *,4 > properites;
    if (FAILEDx(append(properites, pullup_preds))) {
      LOG_WARN("failed to push back predicates", K(ret));
    } else if (OB_FAIL(append(properites, pushdown_preds))) {
      LOG_WARN("failed to append predicates", K(ret));
    } else if (OB_FAIL(append(all_preds, properites))) {
      LOG_WARN("failed to append predicates", K(ret));
    } else if (OB_FAIL(append(all_preds, joined_table->join_conditions_))) {
      LOG_WARN("failed to append join conditions", K(ret));
    } else if (OB_FAIL(stmt->get_column_exprs(all_cols))) {
      LOG_WARN("failed to get all column exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(
                         *stmt, all_cols, *filterable_table, cols))) {
      LOG_WARN("failed to get related columns", K(ret));
    } else if (OB_FAIL(transform_predicates(*stmt, all_preds, cols, new_preds))) {
      LOG_WARN("failed to deduce predicates", K(ret));
    } else if (joined_table->is_inner_join()) {
      if (OB_FAIL(accept_predicates(*stmt,
                                    joined_table->join_conditions_,
                                    properites,
                                    new_preds))) {
        LOG_WARN("failed to accept predicate for joined table", K(ret));
      }
    } else {
      ObSEArray<ObRawExpr*, 8> chosen_preds;
      for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
        if (ObPredicateDeduce::find_equal_expr(chosen_preds, joined_table->join_conditions_.at(i))) {
          //do nothing
        } else if (OB_FAIL(chosen_preds.push_back(joined_table->join_conditions_.at(i)))) {
          LOG_WARN("push back join condition failed", K(ret));
        } else {/*do nothing*/}
      }
      if (OB_SUCC(ret) && OB_FAIL(joined_table->join_conditions_.assign(chosen_preds))) {
        LOG_WARN("assign join conditions failed", K(ret));
      } else {/*do nothing*/}
      for (int64_t i = 0; OB_SUCC(ret) && i < new_preds.count(); ++i) {
        if (OB_ISNULL(new_preds.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("new predicate is null", K(ret));
        } else if (!new_preds.at(i)->has_flag(CNT_COLUMN) ||
                   !new_preds.at(i)->get_relation_ids().is_subset2(filter_table_set)) {
          // do nothing
        } else if (ObPredicateDeduce::find_equal_expr(all_preds, new_preds.at(i))) {
          // do nothing
        } else if (OB_FAIL(joined_table->join_conditions_.push_back(new_preds.at(i)))) {
          LOG_WARN("failed to push back new predicate", K(ret));
        }
      }
    }
  }
  /// STEP 2: push down predicates
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*,8> left_down;
    ObSEArray<ObRawExpr*,8> right_down;
    ObSEArray<ObRawExprCondition *, 8> left_conditions;
    ObSEArray<ObRawExprCondition *, 8> right_conditions;
    // consider the left table of the joined table
    // full outer join, can not push anything down
    // left outer join, push preds down
    // right outer join, push join conditions down, [potentially, we can also push preds down]
    // inner join,push preds and join condition down
    if (joined_table->is_left_join()) {
      if (OB_FAIL(append(left_down, pushdown_preds))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(right_down, joined_table->join_conditions_))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(left_conditions, pred_conditions))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append_condition_array(right_conditions,
                                                 joined_table->join_conditions_.count(),
                                                 &joined_table->join_conditions_))) {
        LOG_WARN("failed to prepare allocate", K(ret));
      }
    } else if (joined_table->is_right_join()) {
      if (OB_FAIL(append(left_down, joined_table->join_conditions_))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(right_down, pushdown_preds))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append_condition_array(left_conditions,
                                                 joined_table->join_conditions_.count(),
                                                 &joined_table->join_conditions_))) {
        LOG_WARN("failed to prepare allocate", K(ret));
      } else if (OB_FAIL(append(right_conditions, pred_conditions))) {
        LOG_WARN("failed to append preds", K(ret));
      }
    } else if (joined_table->is_inner_join()) {
      if (OB_FAIL(append(left_down, pushdown_preds))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(left_down, joined_table->join_conditions_))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(right_down, left_down))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(left_conditions, pred_conditions))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append_condition_array(left_conditions,
                                                joined_table->join_conditions_.count(),
                                                &joined_table->join_conditions_))) {
        LOG_WARN("failed to append preds", K(ret));
      } else if (OB_FAIL(append(right_conditions, left_conditions))) {
        LOG_WARN("failed to append preds", K(ret));
      }
    } else {
      //can pushdown nothing
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(SMART_CALL(pushdown_into_table(stmt,
                                           joined_table->left_table_,
                                           pullup_preds,
                                           left_down,
                                           left_conditions)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else if (OB_FAIL(SMART_CALL(pushdown_into_table(stmt,
                                           joined_table->right_table_,
                                           pullup_preds,
                                           right_down,
                                           right_conditions)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else {
      //删除下推的谓词
      if (joined_table->is_left_join()) {
        if (OB_FAIL(pushdown_preds.assign(left_down))) {
          LOG_WARN("failed to assign preds", K(ret));
        } else if (OB_FAIL(joined_table->join_conditions_.assign(right_down))) {
          LOG_WARN("failed to assign preds", K(ret));
        }
      } else if (joined_table->is_right_join()) {
        if (OB_FAIL(joined_table->join_conditions_.assign(left_down))) {
          LOG_WARN("failed to assign preds", K(ret));
        } else if (OB_FAIL(pushdown_preds.assign(right_down))) {
          LOG_WARN("failed to assign preds", K(ret));
        }
      } else if (joined_table->is_inner_join()) {
        ObSEArray<ObRawExpr*,8> new_pushdown_preds;
        ObSEArray<ObRawExpr*,8> new_join_conditions;
        for (int64_t i = 0; OB_SUCC(ret) && i < pushdown_preds.count(); ++i) {
          if (!ObPredicateDeduce::find_equal_expr(left_down, pushdown_preds.at(i)) ||
              !ObPredicateDeduce::find_equal_expr(right_down, pushdown_preds.at(i))) {
            //成功下推到左侧或右侧
          } else if (OB_FAIL(new_pushdown_preds.push_back(pushdown_preds.at(i)))) {
            LOG_WARN("failed to push back pred", K(ret));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < joined_table->join_conditions_.count(); ++i) {
          if (!ObPredicateDeduce::find_equal_expr(left_down, joined_table->join_conditions_.at(i)) ||
              !ObPredicateDeduce::find_equal_expr(right_down, joined_table->join_conditions_.at(i))) {
            //成功下推到左侧或右侧
          } else if (OB_FAIL(new_join_conditions.push_back(joined_table->join_conditions_.at(i)))) {
            LOG_WARN("failed to push back pred", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(pushdown_preds.assign(new_pushdown_preds))) {
          LOG_WARN("failed to assign preds", K(ret));
        } else if (OB_FAIL(joined_table->join_conditions_.assign(new_join_conditions))) {
          LOG_WARN("failed to assign preds", K(ret));
        }
      } else {
        //do nothing for full join
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::get_pushdown_predicates(
    ObDMLStmt &stmt, TableItem &table,
    ObIArray<ObRawExpr *> &preds,
    ObIArray<ObRawExpr *> &table_filters,
    ObIArray<ObRawExprCondition *> *pred_conditions,
    ObIArray<ObRawExprCondition *> *table_conditions)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  TableItem *target_table = NULL;
  if (table.is_joined_table()) {
    JoinedTable &joined_table = static_cast<JoinedTable &>(table);
    if (joined_table.is_left_join()) {
      target_table = joined_table.left_table_;
    } else if (joined_table.is_right_join()) {
      target_table = joined_table.right_table_;
    } else if (joined_table.is_inner_join()) {
      target_table = &joined_table;
    }
  } else if (table.is_generated_table() || table.is_lateral_table() || table.is_temp_table()) {
    target_table = &table;
  }
  if (OB_FAIL(ret) || NULL == target_table) {
  } else if (OB_FAIL(stmt.get_table_rel_ids(*target_table, table_set))) {
    LOG_WARN("failed to get table set", K(ret));
  } else if (OB_FAIL(get_pushdown_predicates(stmt, table_set, preds, table_filters,
                                             pred_conditions, table_conditions))) {
    LOG_WARN("failed to get push down predicates", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::get_pushdown_predicates(ObDMLStmt &stmt,
                                                            ObSqlBitSet<> &table_set,
                                                            ObIArray<ObRawExpr *> &preds,
                                                            ObIArray<ObRawExpr *> &table_filters,
                                                            ObIArray<ObRawExprCondition *> *pred_conditions,
                                                            ObIArray<ObRawExprCondition *> *table_conditions)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
    if (OB_ISNULL(preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret));
    } else if (ObPredicateDeduce::contain_special_expr(*preds.at(i))) {
      // do nothing
    } else if (!table_set.is_superset2(preds.at(i)->get_relation_ids())) {
      // shall we push down a predicate containing exec param ?
      // a subquery may not be unnested after pushing down such a predicate.
      // do nothing
    } else if (OB_FAIL(table_filters.push_back(preds.at(i)))) {
      LOG_WARN("failed to push back predicate", K(ret));
    } else if (NULL != pred_conditions &&
               OB_UNLIKELY(i >= pred_conditions->count() || NULL == pred_conditions->at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pred conditions", K(ret), KPC(pred_conditions));
    } else if (NULL != pred_conditions && NULL != table_conditions &&
               OB_FAIL(table_conditions->push_back(pred_conditions->at(i)))) {
      LOG_WARN("failed to push back conditions", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::pushdown_into_semi_info
 * @param stmt
 * @param table_item
 * @param pullup_preds
 * @param preds
 * pushdown_preds is only used to deduce new preds
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_into_semi_info(ObDMLStmt *stmt,
                                                            SemiInfo *semi_info,
                                                            ObIArray<ObRawExpr *> &pullup_preds,
                                                            ObIArray<ObRawExpr *> &pushdown_preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> all_preds;
  ObSqlBitSet<> left_rel_ids;
  ObSqlBitSet<> right_rel_ids;
  TableItem *right_table = NULL;
  ObSEArray<ObRawExpr*, 4 > properites;
  ObSEArray<ObRawExpr*, 4> cols;
  ObSEArray<ObRawExpr*, 4> all_cols;
  ObSEArray<ObRawExpr*, 4> new_preds;
  ObSEArray<ObRawExpr *, 16, common::ModulePageAllocator, true> empty;
  ObSEArray<ObRawExprCondition *, 4> pred_conditions;
  OPT_TRACE("try to transform semi conditions");
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info) ||
      OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(semi_info), K(right_table));
  } else if (right_table->is_values_table()) {
    /* not allow predicate moving into values table */
    OPT_TRACE("right table is values table");
  } else if (OB_FAIL(stmt->get_table_rel_ids(semi_info->left_table_ids_, left_rel_ids))) {
    LOG_WARN("failed to get left rel ids", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(semi_info->right_table_id_, right_rel_ids))) {
    LOG_WARN("failed to get right semi rel ids", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, pullup_preds, left_rel_ids,
                                                           properites)) ||
             OB_FAIL(get_pushdown_predicates(*stmt, left_rel_ids, pushdown_preds, properites))) {
    LOG_WARN("failed to extract left table filters", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, pullup_preds, right_rel_ids,
                                                           properites))) {
    LOG_WARN("failed to extract right table filters", K(ret));
  } else if (OB_FAIL(all_preds.assign(properites))) {
    LOG_WARN("failed to assign predicates", K(ret));
  } else if (OB_FAIL(append(all_preds, semi_info->semi_conditions_))) {
    LOG_WARN("failed to append join conditions", K(ret));
  } else if (OB_FAIL(stmt->get_column_exprs(all_cols))) {
    LOG_WARN("failed to get all column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt, all_cols, right_rel_ids, cols))) {
    LOG_WARN("failed to get related columns", K(ret));
  } else if (OB_FAIL(transform_predicates(*stmt, all_preds, cols, new_preds))) {
    LOG_WARN("failed to deduce predicates", K(ret));
  } else if (OB_FAIL(accept_predicates(*stmt, semi_info->semi_conditions_,
                                       properites, new_preds))) {
    LOG_WARN("failed to check different", K(ret));
  } else if (OB_FAIL(append_condition_array(pred_conditions,
                                            semi_info->semi_conditions_.count(),
                                            &semi_info->semi_conditions_))) {
    LOG_WARN("failed to append preds", K(ret));
  } else if (OB_FAIL(pushdown_into_table(stmt, right_table, pullup_preds,
                                         semi_info->semi_conditions_, pred_conditions))) {
    LOG_WARN("failed to push down predicates", K(ret));
  } else if (OB_FAIL(pushdown_semi_info_right_filter(stmt, ctx_, semi_info, pullup_preds))) {
    LOG_WARN("failed to pushdown semi info right filter", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::extract_semi_right_table_filter(ObDMLStmt *stmt,
                                                                    SemiInfo *semi_info,
                                                                    ObIArray<ObRawExpr *> &right_filters)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> right_table_set;
  if (OB_ISNULL(stmt) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(semi_info));
  } else if (OB_FAIL(stmt->get_table_rel_ids(semi_info->right_table_id_,
                                             right_table_set))) {
    LOG_WARN("failed to get right table set", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < semi_info->semi_conditions_.count(); ++i) {
    ObRawExpr *expr = semi_info->semi_conditions_.at(i);
    bool has = false;
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("source expr shoud not be null", K(ret));
    } else if (expr->get_relation_ids().is_empty()) {
      // do nothing
    } else if (!right_table_set.is_superset2(expr->get_relation_ids())) {
      /* do nothing */
    } else if (OB_FAIL(check_has_shared_query_ref(expr, has))) {
      LOG_WARN("failed to check has shared query ref", K(ret));
    } else if (has) {
      // do nothing
    } else if (OB_FAIL(right_filters.push_back(expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_has_shared_query_ref(ObRawExpr *expr, bool &has)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret), K(expr));
  } else if (expr->is_query_ref_expr() && expr->get_ref_count() > 1) {
    has = true;
  } else if (expr->has_flag(CNT_SUB_QUERY)) {
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(check_has_shared_query_ref(expr->get_param_expr(i), has)))) {
        LOG_WARN("failed to check has shared query ref", K(ret));
      }
    }
  }
  return ret;
}

// pushdown right table filter in semi condition:
// 1. if right table is a basic table, create a generate table.
// 2. pushdown the right table filters into the generate table.
int ObTransformPredicateMoveAround::pushdown_semi_info_right_filter(ObDMLStmt *stmt,
                                                                    ObTransformerCtx *ctx,
                                                                    SemiInfo *semi_info,
                                                                    ObIArray<ObRawExpr *> &pullup_preds)
{
  int ret = OB_SUCCESS;
  TableItem *right_table = NULL;
  TableItem *view_table = NULL;
  bool can_push = false;
  ObSEArray<ObRawExpr*, 16> right_filters;
  ObSEArray<ObRawExpr*, 4> column_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx) || OB_ISNULL(semi_info) || OB_ISNULL(ctx->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(ctx), K(semi_info), K(ctx->expr_factory_));
  } else if (OB_ISNULL(right_table = stmt->get_table_item_by_id(semi_info->right_table_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(right_table));
  } else if (OB_FAIL(extract_semi_right_table_filter(stmt, semi_info, right_filters))) {
    LOG_WARN("failed to extract table exprs", K(ret));
  } else if (right_filters.empty()) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::can_push_down_filter_to_table(*right_table, can_push))) {
    LOG_WARN("failed to check can push down", K(ret), K(*right_table));
  } else if (can_push) {
    // if a right filter can push down to right_table, has pushdown in pushdown_into_table,
    // do not reture error here
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(semi_info->semi_conditions_, right_filters))) {
    LOG_WARN("failed to remove right filters", K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(transed_stmts_, stmt))) {
    LOG_WARN("append transed stmt failed", K(ret));
  } else if (OB_FAIL(ObTransformUtils::get_explicated_ref_columns(semi_info->right_table_id_,
                                                                  stmt, column_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               stmt,
                                                               view_table,
                                                               right_table))) {
    LOG_WARN("failed to create empty view table", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          stmt,
                                                          view_table,
                                                          right_table,
                                                          &right_filters,
                                                          NULL,
                                                          &column_exprs))) {
    LOG_WARN("failed to create view with table", K(ret));
  } else if (OB_FAIL(rename_pullup_predicates(*stmt, *view_table, pullup_preds))) {
    LOG_WARN("failed to rename pullup preds", K(ret));
  } else {
    real_happened_ = true;
    for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos_.count(); i ++) {
      ObSqlTempTableInfo *info = NULL;
      if (OB_ISNULL(info = temp_table_infos_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected temp table info", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < info->table_infos_.count(); ++j) {
        if (OB_ISNULL(info->table_infos_.at(j).table_item_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error", K(ret));
        } else if (info->table_infos_.at(j).table_item_->table_id_ == right_table->table_id_) {
          ObDMLStmt *&upper_stmt = info->table_infos_.at(j).upper_stmt_;
          if (OB_UNLIKELY(upper_stmt != stmt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          } else {
            upper_stmt = view_table->ref_query_;
          }
        }
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::pushdown_into_table
 * @param stmt
 * @param table_item
 * @param pullup_preds
 * @param preds
 * if a predicate in preds is pushed down into the table item,
 * it is removed from the preds
 * @return
 */
int ObTransformPredicateMoveAround::pushdown_into_table(ObDMLStmt *stmt,
                                                        TableItem *table_item,
                                                        ObIArray<ObRawExpr *> &pullup_preds,
                                                        ObIArray<ObRawExpr *> &preds,
                                                        ObIArray<ObRawExprCondition *> &pred_conditions)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> rename_preds;
  ObSEArray<ObRawExpr*, 4> table_preds;
  ObSEArray<ObRawExpr*, 4> candi_preds;
  ObSEArray<ObRawExpr*, 4> table_pullup_preds;
  ObSEArray<ObRawExprCondition*, 4> table_conditions;
  OPT_TRACE("try to pushdown preds into", table_item);
  if (OB_ISNULL(stmt) || OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(table_item));
  } else if (OB_FAIL(get_pushdown_predicates(
                       *stmt, *table_item, preds, table_preds, &pred_conditions, &table_conditions))) {
    LOG_WARN("failed to get push down predicates", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null after create_view_with_table", K(ret), K(table_item));
  } else if (!table_item->is_joined_table() &&
             !table_item->is_generated_table() &&
             !table_item->is_temp_table() &&
             !table_item->is_lateral_table()) {
    // do nothing
  } else if (table_item->is_generated_table() &&
             NULL != table_item->ref_query_ &&
             table_item->ref_query_->is_values_table_query()) {
    /* not allow predicate moving into values table query */
  } else if (OB_FAIL(rename_preds.assign(table_preds))) {
    LOG_WARN("failed to assgin exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::extract_table_exprs(*stmt,
                                                           pullup_preds,
                                                           *table_item,
                                                           table_pullup_preds))) {
    LOG_WARN("failed to extract table predicates", K(ret));
  }
  if (OB_SUCC(ret) && (table_item->is_generated_table() || table_item->is_lateral_table())) {
    // if predicates are pushed into the view, we can remove them from the upper stmt
    ObSEArray<ObRawExpr *, 8> invalid_preds;
    uint64_t old_candi_preds_count = 0;
    if (OB_FAIL(rename_pushdown_predicates(*stmt, *table_item, rename_preds))) {
      LOG_WARN("failed to rename predicates", K(ret));
    } else if (OB_FAIL(choose_pushdown_preds(rename_preds, invalid_preds, candi_preds))) {
      LOG_WARN("failed to choose predicates for pushdown", K(ret));
    } else if (OB_FALSE_IT(old_candi_preds_count = candi_preds.count())) {
    } else if (OB_FAIL(SMART_CALL(pushdown_predicates(table_item->ref_query_, candi_preds)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    } else if ((candi_preds.count() != old_candi_preds_count
                || (table_item->ref_query_->is_set_stmt() &&
                    ObOptimizerUtil::find_item(transed_stmts_, table_item->ref_query_)))
               && OB_FAIL(add_var_to_array_no_dup(transed_stmts_, stmt))) {
      LOG_WARN("append transed stmt failed", K(ret));
    } else if (OB_FAIL(append(candi_preds, invalid_preds))) {
      LOG_WARN("failed to append predicates", K(ret));
    }
  }
  if (OB_SUCC(ret) && table_item->is_joined_table()) {
    if (OB_FAIL(candi_preds.assign(rename_preds))) {
      LOG_WARN("failed to assgin exprs", K(ret));
    } else if (OB_FAIL(SMART_CALL(pushdown_into_joined_table(
                         stmt,
                         static_cast<JoinedTable*>(table_item),
                         table_pullup_preds,
                         candi_preds,
                         table_conditions)))) {
      LOG_WARN("failed to push down predicates", K(ret));
    }
  }
  if (OB_SUCC(ret) && table_item->is_temp_table()) {
    bool find_temp_table_info = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find_temp_table_info && i < temp_table_infos_.count(); i ++) {
      ObSqlTempTableInfo *temp_table_info = NULL;
      if (OB_ISNULL(temp_table_info = temp_table_infos_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected temp table info", K(ret));
      } else if (temp_table_info->table_query_ == table_item->ref_query_) {
        for (int64_t j = 0; OB_SUCC(ret) && j < temp_table_info->table_infos_.count(); j ++) {
          TableInfo &table_info = temp_table_info->table_infos_.at(j);
          if (table_info.table_item_ != table_item) {
            // do nothing
          } else if (OB_FAIL(append(table_info.table_filters_, rename_preds))) {
            LOG_WARN("failed to append", K(ret));
          } else if (OB_FAIL(append(table_info.filter_conditions_, table_conditions))) {
            LOG_WARN("failed to append", K(ret));
          } else if (OB_UNLIKELY(table_info.table_filters_.count() != table_info.filter_conditions_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected filter count",
              K(table_info.table_filters_.count()), K(table_info.filter_conditions_.count()));
          } else {
            break;
          }
        }
        find_temp_table_info = true;;
      }
    }
  }
  if (OB_SUCC(ret) && (table_item->is_generated_table() ||
                       table_item->is_lateral_table() ||
                       table_item->is_joined_table())) {
    // remove a pred from preds if it is pushed into a joined table or a generated table
    for (int64_t i = 0; OB_SUCC(ret) && i < rename_preds.count(); ++i) {
      // check whether a table filter is pushed into a view
      if (ObPredicateDeduce::find_equal_expr(candi_preds, rename_preds.at(i))) {
        // the filter is not pushed down
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(preds, table_preds.at(i)))) {
        LOG_WARN("failed to remove pushed filter from preds", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::choose_pushdown_preds(
    ObIArray<ObRawExpr *> &preds,
    ObIArray<ObRawExpr *> &invalid_preds,
    ObIArray<ObRawExpr *> &valid_preds)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
    ObRawExpr *expr = NULL;
    if (OB_ISNULL(expr = preds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("predicate is null", K(ret), K(expr));
    } else if (expr->has_flag(CNT_SUB_QUERY) ||
               expr->has_flag(CNT_DYNAMIC_USER_VARIABLE)) {
      // push down a exec param may not a good idea
      ret = invalid_preds.push_back(expr);
    } else {
      ret = valid_preds.push_back(expr);
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::rename_pushdown_predicates(ObDMLStmt &stmt,
                                                               TableItem &view,
                                                               ObIArray<ObRawExpr *> &preds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColumnRefRawExpr *, 4> table_columns;
  ObSelectStmt *view_stmt = NULL;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(view_stmt = view.ref_query_) || OB_ISNULL(ctx_)
      || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("view stmt is null", K(ret));
  } else if (OB_FAIL(stmt.get_column_exprs(view.table_id_, table_columns))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else {
    ObRawExprCopier copier(*expr_factory);
    for (int64_t i = 0; OB_SUCC(ret) && i < table_columns.count(); ++i) {
      ObRawExpr *sel_expr = NULL;
      ObColumnRefRawExpr *col_expr = table_columns.at(i);
      int64_t idx = -1;
      if (OB_ISNULL(col_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret), K(col_expr));
      } else if (FALSE_IT(idx = col_expr->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
        // do nothing
      } else if (OB_UNLIKELY(idx < 0 || idx >= view_stmt->get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select index is invalid", K(ret), K(idx));
      } else if (OB_ISNULL(sel_expr = view_stmt->get_select_item(idx).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr expr is not found", K(ret), K(sel_expr));
      } else if (OB_FAIL(copier.add_replaced_expr(col_expr, sel_expr))) {
        LOG_WARN("failed to add replace pair", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < preds.count(); ++i) {
      ObRawExpr *new_pred = NULL;
      if (OB_FAIL(copier.copy_on_replace(preds.at(i), new_pred))) {
        LOG_WARN("failed to copy on replace expr", K(ret));
      } else if (OB_FAIL(new_pred->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      } else if (OB_FAIL(new_pred->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else {
        preds.at(i) = new_pred;
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::transform_predicates
 * 将 input_preds 转换成一个等价的 output_preds，这里两个谓词集合语义上必须是等价的，
 * 给定 p \in input_preds, p 一定可以由 output_preds 推导得到
 * 给定 p \in output_preds, p 一定也可以由 input_presd 推导得到
 * @return
 */
int ObTransformPredicateMoveAround::transform_predicates(
    ObDMLStmt &stmt,
    common::ObIArray<ObRawExpr *> &input_preds,
    common::ObIArray<ObRawExpr *> &target_exprs,
    common::ObIArray<ObRawExpr *> &output_preds,
    bool is_pullup /*= false*/)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> valid_preds;
  ObSEArray<ObRawExpr *, 4> other_preds;
  ObSEArray<ObRawExpr *, 4> simple_preds;
  ObSEArray<ObRawExpr *, 4> general_preds;
  ObSEArray<ObRawExpr *, 4> aggr_bound_preds;
  ObSEArray<std::pair<ObRawExpr*, ObRawExpr*>, 4> lossless_cast_preds;
  ObSqlBitSet<> visited;
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transform context is null", K(ret), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < input_preds.count(); ++i) {
    bool is_valid = false;
    if (OB_FAIL(ObPredicateDeduce::check_deduce_validity(input_preds.at(i), is_valid))) {
      LOG_WARN("failed to check condition validity", K(ret));
    } else if (is_valid) {
      ObRawExpr *cast_expr = NULL;
      if (OB_FAIL(valid_preds.push_back(input_preds.at(i)))) {
        LOG_WARN("failed to push back predicates for deduce", K(ret));
      } else if (OB_FAIL(ObPredicateDeduce::check_lossless_cast_table_filter(input_preds.at(i),
                                                                             cast_expr,
                                                                             is_valid))) {
        LOG_WARN("failed to check lossless cast table filter", K(ret));
      } else if (!is_valid) {
        // do nothing
      } else if (OB_FAIL(lossless_cast_preds.push_back(std::pair<ObRawExpr*, ObRawExpr*>(
                                                  cast_expr, input_preds.at(i))))) {
        LOG_WARN("failed to push back preds", K(ret));
      }
    } else if (OB_FAIL(other_preds.push_back(input_preds.at(i)))) {
      LOG_WARN("failed to push back complex predicates", K(ret));
    }
  }
  while (OB_SUCC(ret) && visited.num_members() < valid_preds.count()) {
    // build a graph for comparing expr with the same comparing type
    ObPredicateDeduce deducer(stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_preds.count(); ++i) {
      bool is_added = false;
      if (visited.has_member(i)) {
        // do nothing
      } else if (OB_FAIL(deducer.add_predicate(valid_preds.at(i), is_added))) {
        LOG_WARN("failed to add predicate into deducer", K(ret));
      } else if (is_added && OB_FAIL(visited.add_member(i))) {
        LOG_WARN("failed to mark predicate is deduced", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(deducer.deduce_simple_predicates(*ctx_, simple_preds))) {
        LOG_WARN("failed to deduce predicates for target", K(ret));
      } else if (OB_FAIL(deducer.deduce_general_predicates(
                           *ctx_, target_exprs, other_preds,
                           lossless_cast_preds, general_preds))) {
        LOG_WARN("failed to deduce special predicates", K(ret));
      } else if (!is_pullup) {
        // do nothing
      } else if (stmt.is_select_stmt() && static_cast<ObSelectStmt &>(stmt).is_scala_group_by()) {
        // do nothing
      } else if (OB_FAIL(deducer.deduce_aggr_bound_predicates(
                           *ctx_, target_exprs, aggr_bound_preds))) {
        LOG_WARN("faield to deduce semantic predicates", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(output_preds.assign(simple_preds))) {
      LOG_WARN("failed to assign result", K(ret));
    } else if (OB_FAIL(append(output_preds, other_preds))) {
      LOG_WARN("failed to append other predicates", K(ret));
    } else if (OB_FAIL(append(output_preds, general_preds))) {
      LOG_WARN("failed to append speical predicates", K(ret));
    } else if (OB_FAIL(append(output_preds, aggr_bound_preds))) {
      LOG_WARN("failed to deduce aggr bound predicates", K(ret));
    } else if (!input_preds.empty()) {
      OPT_TRACE(input_preds);
      OPT_TRACE("deduce to:");
      OPT_TRACE(output_preds);
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::accept_predicates(ObDMLStmt &stmt,
                                                      ObIArray<ObRawExpr *> &conds,
                                                      ObIArray<ObRawExpr *> &properties,
                                                      ObIArray<ObRawExpr *> &new_conds)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> chosen_preds;
  ObExprParamCheckContext context;
  ObSEArray<ObPCParamEqualInfo, 4> equal_param_constraints;
  if (OB_ISNULL(stmt.get_query_ctx()) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init param check context failed", K(ret));
  } else if (OB_FAIL(equal_param_constraints.assign(stmt.get_query_ctx()->all_equal_param_constraints_))
             || OB_FAIL(append(equal_param_constraints, ctx_->equal_param_constraints_))) {
    LOG_WARN("failed to fill equal param constraints", K(ret));
  } else {
    context.init(&stmt.get_query_ctx()->calculable_items_, &equal_param_constraints);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < new_conds.count(); ++i) {
    if (OB_ISNULL(new_conds.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new condition is null", K(ret));
    } else if (ObPredicateDeduce::find_equal_expr(properties, new_conds.at(i), NULL, &context)) {
      // the condition has been ensured
    } else if (ObPredicateDeduce::find_equal_expr(chosen_preds, new_conds.at(i), NULL, &context)) {
      // the condition has been chosen
    } else if (OB_FAIL(chosen_preds.push_back(new_conds.at(i)))) {
      LOG_WARN("failed to push back new condition", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(conds.assign(chosen_preds))) {
    LOG_WARN("failed to assign new conditions", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::extract_generalized_column(ObRawExpr *expr,
                                                               ObIArray<ObRawExpr *> &output)
{
  int ret = OB_SUCCESS;
  ObArray<ObRawExpr *> queue;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(queue.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < queue.count(); ++i) {
    ObRawExpr *cur = queue.at(i);
    if (!cur->is_set_op_expr()) {
      for (int64_t j = 0; OB_SUCC(ret) && j < cur->get_param_count(); ++j) {
        ObRawExpr *param = NULL;
        if (OB_ISNULL(param = cur->get_param_expr(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param expr is null", K(ret), K(param));
        } else if (!param->has_flag(CNT_COLUMN) &&
                   !param->has_flag(CNT_AGG) &&
                   !param->has_flag(CNT_WINDOW_FUNC)) {
          // do nothing
        } else if (OB_FAIL(queue.push_back(param))) {
          LOG_WARN("failed to push back param expr", K(ret));
        }
      }
      if (OB_SUCC(ret) && (cur->is_column_ref_expr() ||
                           cur->is_aggr_expr() ||
                           cur->is_win_func_expr())) {
        if (OB_FAIL(output.push_back(cur))) {
          LOG_WARN("failed to push back current expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::acquire_transform_params(ObDMLStmt *stmt,
                                                             ObIArray<ObRawExpr *> *&preds)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  preds = NULL;
  const uint64_t key = reinterpret_cast<const uint64_t>(stmt);
  if (OB_SUCCESS != (ret = stmt_map_.get_refactored(key, index))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else if (OB_UNLIKELY(index >= stmt_pullup_preds_.count() || index < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("does not find pullup predicates", K(ret), K(index));
  } else {
    preds = stmt_pullup_preds_.at(index);
  }
  if (OB_SUCC(ret) && NULL == preds) {
    PullupPreds *new_preds = NULL;
    index = stmt_pullup_preds_.count();
    if (OB_ISNULL(new_preds = (PullupPreds *) allocator_.alloc(sizeof(PullupPreds)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate pullup predicates array", K(ret));
    } else {
      new_preds = new (new_preds) PullupPreds();
      if (OB_FAIL(stmt_pullup_preds_.push_back(new_preds))) {
        LOG_WARN("failed to push back predicates", K(ret));
      } else if (OB_FAIL(stmt_map_.set_refactored(key, index))) {
        LOG_WARN("failed to add entry info hash map", K(ret));
      } else {
        preds = new_preds;
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::get_columns_in_filters(
    ObDMLStmt &stmt, ObIArray<int64_t> &sel_items, ObIArray<ObRawExpr *> &columns)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 4> filter_exprs;
  ObSEArray<ObRawExpr *, 4> tmp_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_joined_tables().count(); ++i) {
    if (OB_FAIL(ObTransformUtils::get_on_condition(stmt.get_joined_tables().at(i),
                                                   filter_exprs))) {
      LOG_WARN("failed to get on conditions", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::get_semi_conditions(stmt.get_semi_infos(), filter_exprs))) {
    LOG_WARN("failed to get semi conditions", K(ret));
  } else if (OB_FAIL(append(filter_exprs, stmt.get_condition_exprs()))) {
    LOG_WARN("failed to append condition exprs", K(ret));
  } else if (stmt.is_select_stmt()) {
    ObSelectStmt &sel_stmt = static_cast<ObSelectStmt &>(stmt);
    if (OB_FAIL(append(filter_exprs, sel_stmt.get_having_exprs()))) {
      LOG_WARN("failed to append having exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_items.count(); ++i) {
      int64_t idx = sel_items.at(i);
      if (OB_UNLIKELY(idx < 0 || idx >= sel_stmt.get_select_item_size())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid select index", K(ret), K(idx));
      } else if (OB_FAIL(filter_exprs.push_back(sel_stmt.get_select_item(idx).expr_))) {
        LOG_WARN("failed to push back select expr", K(ret));
      }
    }
  } else if (stmt.is_insert_stmt() || stmt.is_merge_stmt()) {
    ObDelUpdStmt &delupd_stmt = static_cast<ObDelUpdStmt &>(stmt);
    if (OB_FAIL(extract_filter_column_exprs_for_insert(delupd_stmt, columns))){
      LOG_WARN("failed to extract column exprs", K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < filter_exprs.count(); ++i) {
    tmp_exprs.reuse();
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(filter_exprs.at(i), tmp_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(append_array_no_dup(columns, tmp_exprs))) {
      LOG_WARN("failed to append array without duplicate", K(ret));
    }
  }
  return ret;
}

/**
 * @brief ObTransformPredicateMoveAround::deduce_part_prune_filters
 * deduce partition pruning filters for the insert stmt
 * @param preds
 * @return
 */
int ObTransformPredicateMoveAround::create_equal_exprs_for_insert(ObDelUpdStmt *del_upd_stmt)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr * ,4> part_exprs;
  ObSEArray<ObRawExpr *, 4> target_exprs;
  ObSEArray<ObRawExpr *, 4> source_exprs;
  ObPhysicalPlanCtx *plan_ctx = NULL;
  ObSEArray<ObDmlTableInfo*, 2> dml_table_infos;
  if (OB_ISNULL(del_upd_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->exec_ctx_) ||
      OB_ISNULL(ctx_->allocator_) ||
      OB_ISNULL(plan_ctx = ctx_->exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("insert stmt is null", K(ret), K(del_upd_stmt));
  } else if (!((del_upd_stmt->is_insert_stmt() &&
                static_cast<ObInsertStmt*>(del_upd_stmt)->value_from_select()) || 
               del_upd_stmt->is_merge_stmt()) ||
             !del_upd_stmt->get_sharding_conditions().empty()) {
    // do nothing
  } else if (OB_FAIL(del_upd_stmt->get_value_exprs(source_exprs))) {
    LOG_WARN("failed to get source exprs", K(ret));
  } else if (OB_FAIL(del_upd_stmt->get_dml_table_infos(dml_table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else if (OB_UNLIKELY(dml_table_infos.count() != 1) || OB_ISNULL(dml_table_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dml table info", K(ret), K(dml_table_infos));
  } else if (OB_FAIL(append(target_exprs, dml_table_infos.at(0)->column_exprs_))) {
    LOG_WARN("failed to get target exprs", K(ret));
  } else if (target_exprs.count() != source_exprs.count()) {
    if (OB_UNLIKELY(!del_upd_stmt->is_merge_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array size does not match", K(ret), K(target_exprs.count()), K(source_exprs.count()));
    } else {
      // merge stmt may has no insert clause, do nothing
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < del_upd_stmt->get_part_exprs().count(); ++i) {
      ObRawExpr *expr = NULL;
      if (NULL != (expr = del_upd_stmt->get_part_exprs().at(i).part_expr_)) {
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, part_exprs))) {
          LOG_WARN("failed to extract column exprs", K(ret));
        }
      }
      if (OB_SUCC(ret) && NULL != (expr = del_upd_stmt->get_part_exprs().at(i).subpart_expr_)) {
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, part_exprs))) {
          LOG_WARN("failed to extract column exprs", K(ret));
        }
      }
    }

    // 1. check a target expr is a partition expr
    // 2. check the comparison meta between target and source expr
    // 3. mock equal expr between target and source
    ObNotNullContext not_null_context(*ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < target_exprs.count(); ++i) {
      ObRawExpr *ret_expr = NULL;
      bool type_safe = false;
      bool is_not_null = true;
      bool column_is_null = false;
      ObArray<ObRawExpr *> constraints;
      if (OB_ISNULL(target_exprs.at(i)) || OB_ISNULL(source_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param exprs are null", K(ret));
      } else if (!source_exprs.at(i)->is_const_expr()) {
        ObRawExpr *col_expr = source_exprs.at(i);
        if (T_FUN_COLUMN_CONV == col_expr->get_expr_type() && col_expr->get_param_count() > 5) {
          col_expr = col_expr->get_param_expr(4);
        }
        if (T_FUN_SYS_CAST == col_expr->get_expr_type() && col_expr->get_param_count() > 1) {
          col_expr = col_expr->get_param_expr(0);
        }
        if (!col_expr->is_column_ref_expr()) {
          // do nothing
        } else if (OB_FAIL(is_column_expr_null(del_upd_stmt,
                                       static_cast<const ObColumnRefRawExpr *>(col_expr),
                                       column_is_null,
                                       constraints))) {
          LOG_WARN("fail to check if column expr is null", K(ret));
        } else {
          is_not_null = !column_is_null;
        }
      } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_context,
                                                            source_exprs.at(i),
                                                            is_not_null,
                                                            &constraints))) {
        LOG_WARN("failed to check expr not null", K(ret));
      } else if (!is_not_null && source_exprs.at(i)->is_static_scalar_const_expr()) {
        // check default value
        ObObj result;
        bool got_result = false;
        if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                              source_exprs.at(i),
                                                              result,
                                                              got_result,
                                                              *ctx_->allocator_))) {
          LOG_WARN("failed to calc const or caculable expr", K(ret));
        } else if (!got_result || (!result.is_null()
                  && !(lib::is_oracle_mode() && result.is_null_oracle()))) {
          //do nothing
        } else {
          column_is_null = true;
          constraints.reset();
        }
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (source_exprs.at(i)->has_flag(CNT_SUB_QUERY)) {
        //do nothing
      } else if (!ObOptimizerUtil::find_item(part_exprs, target_exprs.at(i))) {
        // do nothing
      } else if (!is_not_null) {
        //build is null expr
        if (!column_is_null) {
          // do nothing
        } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_,
                                          target_exprs.at(i),
                                          false,
                                          ret_expr))) {
          LOG_WARN("fail to build is null expr", K(ret), K(*target_exprs.at(i)));
        } else if (OB_ISNULL(ret_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("is_null expr is null", K(ret));
        } else if (OB_FAIL(ret_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("formalize equal expr failed", K(ret));
        } else {
          for (int64_t j = 0; j < constraints.count(); ++j) {
            if (OB_FAIL(ObTransformUtils::add_param_null_constraint(*ctx_, constraints.at(j)))) {
              LOG_WARN("failed to add param null constraint", K(ret));
            }
          }
        }
      } else if (OB_FAIL(ObRelationalExprOperator::is_equivalent(
                          target_exprs.at(i)->get_result_type(),
                          target_exprs.at(i)->get_result_type(),
                          source_exprs.at(i)->get_result_type(),
                          type_safe))) {
        LOG_WARN("failed to check is type safe", K(ret));
      } else if (!type_safe) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_,
                                                          ctx_->session_info_,
                                                          target_exprs.at(i),
                                                          source_exprs.at(i),
                                                          ret_expr))) {
        LOG_WARN("failed to create equal exprs", K(ret));
      } else if (OB_ISNULL(ret_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("equal expr is null", K(ret));
      }
      ObRawExpr *sharding_expr = NULL;
      ObRawExprCopier copier(*ctx_->expr_factory_);
      ObArray<ObRawExpr *> column_exprs;
      if (OB_FAIL(ret) || NULL == ret_expr) {
        //do nothing
      } else if (OB_FAIL(ret_expr->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(ret_expr, column_exprs))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else if (OB_FAIL(copier.add_skipped_expr(column_exprs))) {
        LOG_WARN("add skipped expr failed", K(ret));
      } else if (OB_FAIL(copier.copy(ret_expr, sharding_expr))) {
        LOG_WARN("failed to copy expr", K(ret));
      } else if (OB_FAIL(del_upd_stmt->get_sharding_conditions().push_back(sharding_expr))) {
        LOG_WARN("failed to add condition expr", K(ret));
      } else if (is_not_null && OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
        LOG_WARN("failed to add param not null constraint", K(ret));
      } else if (OB_FAIL(add_var_to_array_no_dup(transed_stmts_, static_cast<ObDMLStmt *>(del_upd_stmt)))) {
        LOG_WARN("append failed", K(ret));
      } else {
        real_happened_ = true;
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::generate_pullup_predicates_for_dual_stmt(
    ObDMLStmt &stmt,
    TableItem &view,
    const ObIArray<int64_t> &sel_ids,
    ObIArray<ObRawExpr *> &preds)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *child_stmt = view.ref_query_;
  ObRawExpr *sel_expr = NULL;
  ObRawExpr *column_expr = NULL;
  ObRawExpr *equal_expr = NULL;
  if (OB_ISNULL(child_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(stmt.get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child_stmt), K(ctx_));
  } else {
    ObNotNullContext not_null_context(*ctx_, child_stmt);
    for (int64_t i = 0; OB_SUCC(ret) && i < sel_ids.count(); ++i) {
      int64_t idx = sel_ids.at(i);
      bool is_not_null = false;
      ObArray<ObRawExpr *> constraints;
      if (OB_UNLIKELY(idx < 0 || idx > child_stmt->get_select_item_size()) ||
          OB_ISNULL(sel_expr = child_stmt->get_select_item(idx).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", K(ret), K(idx), K(sel_expr));
      } else if (!sel_expr->is_const_expr() || sel_expr->get_result_type().is_lob()) {
        // do nothing
      } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_context,
                                                            sel_expr,
                                                            is_not_null,
                                                            &constraints))) {
        LOG_WARN("failed to check expr not null", K(ret));
      } else if (!is_not_null) {
        // do nothing
        ObObj result;
        bool got_result = false;
        if (!sel_expr->is_static_scalar_const_expr()) {
          //do nothing
        } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                              sel_expr,
                                                              result,
                                                              got_result,
                                                              *ctx_->allocator_))) {
          LOG_WARN("failed to calc const or caculable expr", K(ret));
        } else if (!got_result || (!result.is_null()
                  && !(lib::is_oracle_mode() && result.is_null_oracle()))) {
          //do nothing
        } else if (OB_ISNULL(column_expr = stmt.get_column_expr_by_id(view.table_id_,
                                                                    OB_APP_MIN_COLUMN_ID + idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected column expr", K(ret), K(view.table_id_), K(idx));
        } else if (OB_FAIL(ObRawExprUtils::build_is_not_null_expr(*ctx_->expr_factory_,
                                                                  column_expr,
                                                                  false,
                                                                  equal_expr))) {
          LOG_WARN("fail to build is null expr", K(ret), K(column_expr));
        } else if (OB_ISNULL(equal_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("is_null expr is null", K(ret));
        } else if (OB_FAIL(equal_expr->formalize(ctx_->session_info_))) {
          LOG_WARN("formalize equal expr failed", K(ret));
        } else if (!sel_expr->is_const_raw_expr() && OB_FAIL(ObTransformUtils::add_param_null_constraint(*ctx_, sel_expr))) {
          LOG_WARN("failed to add param null constraint", K(ret));
        } else {}
      } else if (OB_ISNULL(column_expr = stmt.get_column_expr_by_id(view.table_id_,
                                                                    OB_APP_MIN_COLUMN_ID + idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected column expr", K(ret), K(view.table_id_), K(idx));
      } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*ctx_->expr_factory_,
                                                         ctx_->session_info_,
                                                         column_expr,
                                                         sel_expr,
                                                         equal_expr))) {
        LOG_WARN("failed to create equal exprs", K(ret));
      } else if (OB_ISNULL(equal_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("equal expr is null", K(ret));
      } else if (OB_FAIL(equal_expr->formalize(ctx_->session_info_))) {
        LOG_WARN("failed to formalize expr", K(ret));
      }
      if (OB_FAIL(ret) || NULL == equal_expr) {
        //do nothing
      } else if (OB_FAIL(equal_expr->pull_relation_id())) {
        LOG_WARN("failed to pull relation id and levels", K(ret));
      } else if (OB_FAIL(preds.push_back(equal_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (is_not_null && OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
        LOG_WARN("failed to add param not null constraint", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::check_enable_no_pred_deduce(ObDMLStmt &stmt, bool &enable_no_pred_deduce) {
  int ret = OB_SUCCESS;
  const ObQueryHint *query_hint = stmt.get_stmt_hint().query_hint_;
  const ObTransHint *hint = static_cast<const ObTransHint *>(get_hint(stmt.get_stmt_hint()));
  if (OB_ISNULL(query_hint)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (query_hint->has_outline_data()) {
    bool has_hint = ObOptimizerUtil::find_item(applied_hints_, hint);
    enable_no_pred_deduce = (hint == NULL ? true : (has_hint ? hint->is_disable_hint() : true));
  } else {
    const ObHint *no_rewrite = stmt.get_stmt_hint().get_no_rewrite_hint();
    enable_no_pred_deduce = (hint == NULL ? no_rewrite != NULL : hint->is_disable_hint());
  }
  LOG_DEBUG("check enable no pred deduce", K(ret), K(enable_no_pred_deduce),
                                           K(stmt.get_stmt_hint().get_no_rewrite_hint()));
  return ret;
}

int ObTransformPredicateMoveAround::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObIArray<ObDMLStmt*> *transed_stmts = static_cast<ObIArray<ObDMLStmt*>*>(trans_params);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(transed_stmts)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->add_src_hash_val(ctx_->src_qb_name_))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else {
    ObTransHint *hint = NULL;
    ObString qb_name;
    ObDMLStmt *cur_stmt = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < transed_stmts->count(); i++) {
      if (OB_ISNULL(cur_stmt = transed_stmts->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("transed_stmt is null", K(ret), K(i));
      } else if (OB_FAIL(ctx_->add_used_trans_hint(get_hint(cur_stmt->get_stmt_hint())))) {
        LOG_WARN("failed to add used hint", K(ret));
      } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, get_hint_type(), hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      } else if (OB_FAIL(cur_stmt->get_qb_name(qb_name))) {
        LOG_WARN("failed to get qb name", K(ret));
      } else if (OB_FAIL(ctx_->add_src_hash_val(qb_name))) {
        LOG_WARN("failed to add src hash val", K(ret));
      } else if (OB_FAIL(cur_stmt->adjust_qb_name(ctx_->allocator_,
                                                  qb_name,
                                                  ctx_->src_hash_val_))) {
        LOG_WARN("adjust stmt id failed", K(ret));
      } else {
        hint->set_qb_name(qb_name);
        OPT_TRACE("transformed query blocks:", qb_name);
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::remove_simple_op_null_condition(ObSelectStmt &stmt,
                                                                  ObIArray<ObRawExpr *> &pullup_preds)
{
  int ret = OB_SUCCESS;
  ObNotNullContext not_null_context(*ctx_, &stmt);
  ObSEArray<ObRawExpr*, 16> const_preds;
  for (int64_t i = 0; OB_SUCC(ret) && i < pullup_preds.count(); i++) {
    ObRawExpr *cur_expr = pullup_preds.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret), K(cur_expr));
    } else if (ObPredicateDeduce::is_simple_condition(cur_expr->get_expr_type())) {
      ObRawExpr *param_1 = cur_expr->get_param_expr(0);
      ObRawExpr *param_2 = cur_expr->get_param_expr(1);
      ObRawExpr *const_expr = NULL;
      bool is_not_null = false;
      ObArray<ObRawExpr *> constraints;
      if (OB_ISNULL(param_1) || OB_ISNULL(param_2)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param", K(ret));
      } else if (param_1->is_static_scalar_const_expr() && param_2->is_static_scalar_const_expr()) {
        //do nothing
      } else if (param_1->is_static_scalar_const_expr()) {
        const_expr = param_1;
      } else if (param_2->is_static_scalar_const_expr()) {
        const_expr = param_2;
      }
      if (OB_FAIL(ret) || NULL == const_expr) {
      } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(not_null_context,
                                                            const_expr,
                                                            is_not_null,
                                                            &constraints))) {
        LOG_WARN("failed to check expr not null", K(ret));
      } else if (!is_not_null && OB_FAIL(const_preds.push_back(cur_expr))) {
        LOG_WARN("failed to push back into useless const preds", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ObOptimizerUtil::remove_item(pullup_preds, const_preds))) {
    LOG_WARN("failed to remove simple op null conditions from pullup_preds", K(ret));
  }
  return ret;
}

int ObTransformPredicateMoveAround::is_column_expr_null(ObDMLStmt *stmt,
                                              const ObColumnRefRawExpr *expr,
                                              bool &is_null,
                                              ObIArray<ObRawExpr *> &constraints)
{
  int ret = OB_SUCCESS;
  const TableItem *table = NULL;
  is_null = false;

  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(table = stmt->get_table_item_by_id(expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(expr), K(table));
  } else if (table->is_basic_table()) {
    //do nothing
  } else if (table->is_generated_table() || table->is_lateral_table() || table->is_temp_table()) {
    int64_t idx = expr->get_column_id() - OB_APP_MIN_COLUMN_ID;
    ObRawExpr *child_expr = NULL;
    ObSelectStmt *child_stmt = table->ref_query_;
    if (OB_ISNULL(child_stmt) ||
      OB_UNLIKELY(idx < 0 || idx >= child_stmt->get_select_item_size()) ||
      OB_ISNULL(child_expr = child_stmt->get_select_item(idx).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt is invalid", K(ret), K(idx));
    } else if (child_expr->is_static_scalar_const_expr()) {
      //case: insert into t1 select null from dual;
      ObObj result;
      bool got_result = false;
      if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(ctx_->exec_ctx_,
                                                            child_expr,
                                                            result,
                                                            got_result,
                                                            *ctx_->allocator_))) {
        LOG_WARN("failed to calc const or caculable expr", K(ret));
      } else if (got_result && (result.is_null()
                || (lib::is_oracle_mode() && result.is_null_oracle()))) {
        is_null = true;
        if (!child_expr->is_const_raw_expr() && OB_FAIL(constraints.push_back(child_expr))) {
          LOG_WARN("fail to push back constraint", K(ret));
        }
      }
    } else {
      //cases: insert into t1 select c1 from t2 where c1 is null;
      //       insert into t1 select c1 from (select null as c1 from t1 union select null as c2 from t2);
      ObIArray<ObRawExpr *> *pullup_preds;
      ObSEArray<ObRawExpr *, 4> filter_preds;
      if (OB_FAIL(acquire_transform_params(child_stmt, pullup_preds))) {
        LOG_WARN("failed to acquire pull up preds", K(ret));
      } else if (OB_FAIL(append(filter_preds, *pullup_preds))) {
        LOG_WARN("failed to append conditions", K(ret));
      } else if (OB_FAIL(append(filter_preds, child_stmt->get_condition_exprs()))) {
        LOG_WARN("failed to append conditions", K(ret));
      } else if (OB_FAIL(append(filter_preds, child_stmt->get_having_exprs()))) {
        LOG_WARN("failed to append having conditions", K(ret));
      }
      for (int i = 0; OB_SUCC(ret) && !is_null && i < filter_preds.count(); ++i) {
        if (OB_ISNULL(filter_preds.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(filter_preds.at(i)));
        } else if (T_OP_IS == filter_preds.at(i)->get_expr_type() &&
                   child_expr == filter_preds.at(i)->get_param_expr(0)) {
          is_null = true;
        }
      }
    }
  }

  return ret;
}

int ObTransformPredicateMoveAround::extract_filter_column_exprs_for_insert(ObDelUpdStmt &del_upd_stmt, ObIArray<ObRawExpr *> &columns)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr * ,4> part_exprs;
  ObSEArray<ObRawExpr *, 4> target_exprs;
  ObSEArray<ObRawExpr *, 4> source_exprs;
  ObSEArray<ObDmlTableInfo*, 2> dml_table_infos;
  ObSEArray<ObRawExpr *, 4> tmp_exprs;
  if (!((del_upd_stmt.is_insert_stmt() &&
        static_cast<ObInsertStmt&>(del_upd_stmt).value_from_select()) ||
        del_upd_stmt.is_merge_stmt())) {
    // do nothing
  } else if (OB_FAIL(del_upd_stmt.get_value_exprs(source_exprs))) {
    LOG_WARN("failed to get source exprs", K(ret));
  } else if (OB_FAIL(del_upd_stmt.get_dml_table_infos(dml_table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else if (OB_UNLIKELY(dml_table_infos.count() != 1) || OB_ISNULL(dml_table_infos.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected dml table info", K(ret), K(dml_table_infos));
  } else if (OB_FAIL(append(target_exprs, dml_table_infos.at(0)->column_exprs_))) {
    LOG_WARN("failed to get target exprs", K(ret));
  } else if (target_exprs.count() != source_exprs.count()) {
    if (OB_UNLIKELY(!del_upd_stmt.is_merge_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array size does not match", K(ret), K(target_exprs.count()), K(source_exprs.count()));
    } else {
      // merge stmt may has no insert clause, do nothing
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < del_upd_stmt.get_part_exprs().count(); ++i) {
      ObRawExpr *expr = NULL;
      if (NULL != (expr = del_upd_stmt.get_part_exprs().at(i).part_expr_)) {
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, part_exprs))) {
          LOG_WARN("failed to extract column exprs", K(ret));
        }
      }
      if (OB_SUCC(ret) && NULL != (expr = del_upd_stmt.get_part_exprs().at(i).subpart_expr_)) {
        if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, part_exprs))) {
          LOG_WARN("failed to extract column exprs", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < target_exprs.count(); ++i) {
      tmp_exprs.reuse();
      if (OB_ISNULL(target_exprs.at(i)) || OB_ISNULL(source_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param exprs are null", K(ret));
      } else if (!ObOptimizerUtil::find_item(part_exprs, target_exprs.at(i))) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(target_exprs.at(i), tmp_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(source_exprs.at(i), tmp_exprs))) {
        LOG_WARN("failed to extract column exprs", K(ret));
      } else if (OB_FAIL(append_array_no_dup(columns, tmp_exprs))) {
        LOG_WARN("failed to append array without duplicate", K(ret));
      } else {}
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::push_down_cte_filter(ObIArray<ObSqlTempTableInfo *> &temp_table_info,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  real_happened_ = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < temp_table_info.count(); ++i) {
    ObSqlTempTableInfo *info = temp_table_info.at(i);
    uint64_t filter_count = 0;
    bool have_common_filter = true;
    bool is_valid = true;
    ObSEArray<ObRawExpr *, 4> common_filters;
    bool enable_no_pred_deduce = false;
    if (OB_ISNULL(info) || OB_ISNULL(info->table_query_) ||
        OB_ISNULL(info->table_query_->get_query_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null ref query", K(ret));
    } else if (OB_FAIL(check_enable_no_pred_deduce(*info->table_query_, enable_no_pred_deduce))) {
      LOG_WARN("check_enable_no_pred_deduce failed", K(ret));
    } else if (enable_no_pred_deduce) {
      OPT_TRACE("hint reject push down filter into ", info->table_name_);
      continue;
    }
    for (int64_t j = 0; OB_SUCC(ret) && have_common_filter && j < info->table_infos_.count(); ++j) {
      ObIArray<ObRawExpr *> &table_filters = info->table_infos_.at(j).table_filters_;
      if (table_filters.empty()) {
        have_common_filter = false;
      } else if (j == 0) {
        for (int64_t k = 0; OB_SUCC(ret) && k < table_filters.count(); ++k) {
          if (table_filters.at(k)->has_flag(CNT_DYNAMIC_PARAM)) {
            //exec param can not push into temp table
          } else if (OB_FAIL(common_filters.push_back(table_filters.at(k)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        if (OB_SUCC(ret) && common_filters.empty()) {
          have_common_filter = false;
        }
      } else {
        ObSEArray<ObRawExpr *, 4> new_common_filters;
        ObTempTableColumnCheckContext check_context;
        check_context.init(info->table_infos_.at(0).table_item_->table_id_,
                           info->table_infos_.at(j).table_item_->table_id_,
                           &info->table_query_->get_query_ctx()->calculable_items_);
        for (int64_t k = 0; OB_SUCC(ret) && k < common_filters.count(); ++k) {
          bool find = false;
           if (common_filters.at(k)->has_flag(CNT_DYNAMIC_PARAM)) {
            //exec param can not push into temp table
          } else if (OB_FAIL(ObTransformUtils::find_expr(table_filters, common_filters.at(k), find, &check_context))) {
            LOG_WARN("failed to find expr", K(ret));
          } else if (find && OB_FAIL(new_common_filters.push_back(common_filters.at(k)))) {
            LOG_WARN("failed to push back", K(ret));
          }
        }
        have_common_filter = !new_common_filters.empty();
        if (OB_SUCC(ret) && have_common_filter && OB_FAIL(common_filters.assign(new_common_filters))) {
          LOG_WARN("failed to assign", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && have_common_filter) {
      OPT_TRACE("try to pushdown filter into temp table:", info->table_query_);
      //当所有的引用表都有可以相同的下推的谓词时才下推谓词
      ObDMLStmt *orig_stmt = info->table_query_;
      if (OB_FAIL(inner_push_down_cte_filter(*info, common_filters))) {
        LOG_WARN("failed to pushdown preds into temp table", K(ret));
      } else {
        trans_happened = true;
        real_happened_ = true;
      }
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::inner_push_down_cte_filter(ObSqlTempTableInfo& info, ObIArray<ObRawExpr *> &filters)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObSEArray<ObRawExpr *, 8> rename_exprs;
  if (OB_ISNULL(info.table_query_) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(info.table_query_->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(info), K(expr_factory), K(ret));
  } else if (OB_FAIL(add_var_to_array_no_dup(transed_stmts_, static_cast<ObDMLStmt *>(info.table_query_)))) {
    LOG_WARN("append transed stmt failed", K(ret));
  } else if (!info.table_query_->is_spj() && OB_FAIL(ObTransformUtils::pack_stmt(ctx_, info.table_query_))) {
    LOG_WARN("failed to create spj", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::rename_pushdown_filter(*info.table_infos_.at(0).upper_stmt_,
                                                             *info.table_query_,
                                                             info.table_infos_.at(0).table_item_->table_id_,
                                                             session_info,
                                                             *expr_factory,
                                                             filters,
                                                             rename_exprs))) {
    LOG_WARN("failed to rename push down preds", K(ret));
  } else if (OB_FAIL(append(info.table_query_->get_condition_exprs(), rename_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  }

  // remove the filter in upper stmts
  for (int64_t i = 0; OB_SUCC(ret) && i < info.table_infos_.count(); i ++) {
    ObIArray<ObRawExpr *> &table_filters = info.table_infos_.at(i).table_filters_;
    ObIArray<ObRawExprCondition *> &filter_conditions = info.table_infos_.at(i).filter_conditions_;
    ObTempTableColumnCheckContext check_context;
    check_context.init(info.table_infos_.at(0).table_item_->table_id_,
                       info.table_infos_.at(i).table_item_->table_id_,
                       &info.table_query_->get_query_ctx()->calculable_items_);
    if (OB_UNLIKELY(table_filters.count() != filter_conditions.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected filter count", K(table_filters.count()), K(filter_conditions.count()));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < table_filters.count(); j ++) {
      bool find = false;
      if (OB_FAIL(ObTransformUtils::find_expr(filters, table_filters.at(j), find, &check_context))) {
        LOG_WARN("failed to find expr", K(ret));
      } else if (!find) {
        // do nothing
      } else if (OB_ISNULL(filter_conditions.at(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpecte null filter conditions", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::remove_item(*filter_conditions.at(j), table_filters.at(j)))) {
        LOG_WARN("failed to remove condition", K(ret));
      } else {
        OPT_TRACE("succeed to remove filter in upper stmt : ", table_filters.at(j));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_var_to_array_no_dup(transed_stmts_, info.table_infos_.at(i).upper_stmt_))) {
      LOG_WARN("append transed stmt failed", K(ret));
    } else if (OB_FAIL(append(ctx_->equal_param_constraints_, check_context.equal_param_info_))) {
      LOG_WARN("failed to append equal param constraints", K(ret));
    }
  }
  return ret;
}

int ObTransformPredicateMoveAround::append_condition_array(ObIArray<ObRawExprCondition *> &conditions,
                                                           int count, ObRawExprCondition *value)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    if (OB_FAIL(conditions.push_back(value))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

bool ObTempTableColumnCheckContext::compare_column(const ObColumnRefRawExpr &left,
                                                                         const ObColumnRefRawExpr &right)
{
  bool bret = false;
  if (left.get_expr_type() != right.get_expr_type()) {
  } else if (left.get_column_id() != right.get_column_id()) {
  } else if (left.get_table_id() == right.get_table_id() ||
             (left.get_table_id() == first_temp_table_id_ && right.get_table_id() == second_temp_table_id_) ||
             (left.get_table_id() == second_temp_table_id_ && right.get_table_id() == first_temp_table_id_)) {
    bret = true;
  }
  return bret;
}

int ObTransformPredicateMoveAround::filter_lateral_correlated_preds(TableItem &table_item,
                                                                    ObIArray<ObRawExpr*> &preds)
{
  int ret = OB_SUCCESS;
  if (table_item.is_lateral_table()) {
    for (int64_t i = preds.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      bool contains = false;
      if (OB_FAIL(ObOptimizerUtil::check_contain_my_exec_param(preds.at(i),
                                                               table_item.exec_params_,
                                                               contains))) {
        LOG_WARN("failed to check contain my exec param", K(ret));
      } else if (!contains) {
        // do nothing
      } else if (OB_FAIL(preds.remove(i))) {
        LOG_WARN("failed to remove predicate", K(ret), K(i));
      }
    }
  }
  return ret;
}
