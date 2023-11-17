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
#include "sql/rewrite/ob_transform_decorrelate.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
namespace oceanbase {
namespace sql {
/**
 * @brief decorrelate lateral derived table
 *
 * 1. decorrelate normal lateral derived table or can create spj stmt
 * select t1.*, v.* from t1, lateral (select * from t2 where t1.a = t2.a) v;
 *   =>
 * select t1.*, v.* from t1, (select * from t2) v where t1.a = v.a;
 *
 * select  t1.*,v.* from t1, lateral (select count(t2.c1), t2.c2 from t2 group by c2 having t2.c2 = t1.c1 ) v;
 *   =>
 * select t1.*,v.* from test.t1,(select * from (select count(c1),c2 from t2 group by c2) VIEW1) v where (v.c2 = t1.c1);
 *
 * 2. decorrelate aggr lateral derived table
 * select * from t1, lateral (select  sum(b) from t2 where t1.a = t2.a);
 *   =>
 * select * from t1 left join (select sum(b) from t2 group by t2.a) v on v.a = t1.a;
 *
 * select * from t1, lateral (select  count(b) from t2 where t1.a = t2.a);
 *   =>
 * select t1.*, case when v.a is null then 0 else v.`count(b)` from
 *     t1 left join (select a, count(b) from t2 group by t2.a) v on v.a = t1.a;
 *
 * select * from t3,
*      lateral (select  sum(b) from t2 where t2.a = t3.c1 group by b) v;
 *   =>
 *  select * from t3, (select sum(b), a from t2 group by b,a) v where v.a = t3.c1;
 */
int ObTransformDecorrelate::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                               ObDMLStmt *&stmt,
                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSelectStmt*, 4> decorrelate_stmts;
  bool is_lateral_trans_happened = false;
  bool is_aggr_lateral_trans_happened = false;
  trans_happened = false;
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(decorrelate_lateral_derived_table(stmt,
                                                       decorrelate_stmts,
                                                       is_lateral_trans_happened))) {
    LOG_WARN("failed to decorrlate lateral derived table", K(ret));
  } else if (OB_FAIL(decorrelate_aggr_lateral_derived_table(stmt,
                                                            decorrelate_stmts,
                                                            is_aggr_lateral_trans_happened))) {
    LOG_WARN("failed to decorrelate aggr lateral derived table", K(ret));
  } else if (!is_lateral_trans_happened && !is_aggr_lateral_trans_happened) {
    // do nothing
  } else if (OB_FAIL(stmt->formalize_query_ref_exprs())) {
    LOG_WARN("failed to formalize query ref exprs", K(ret));
  } else if (OB_FAIL(add_transform_hint(*stmt, &decorrelate_stmts))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else {
    trans_happened = true;
    LOG_TRACE("succeed to do decorrelate lateral derived table");
  }
  return ret;
}

int ObTransformDecorrelate::transform_one_stmt_with_outline(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                            ObDMLStmt *&stmt,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  bool is_happened = false;
  ObSEArray<ObSelectStmt*, 4> decorrelate_stmts;
  do {
    is_happened = false;
    if (OB_FAIL(decorrelate_lateral_derived_table(stmt,
                                                  decorrelate_stmts,
                                                  is_happened))) {
      LOG_WARN("failed to decorrlate lateral derived table", K(ret));
    } else if (!is_happened &&
               OB_FAIL(decorrelate_aggr_lateral_derived_table(stmt,
                                                              decorrelate_stmts,
                                                              is_happened))) {
      LOG_WARN("failed to decorrelate aggr lateral derived table", K(ret));
    } else if (!is_happened) {
      LOG_TRACE("can not do decorrelate with outline", K(ctx_->src_qb_name_));
    } else {
      ++ctx_->trans_list_loc_;
      trans_happened = true;
      LOG_TRACE("succeed to do decorrelate with outline", K(ctx_->src_qb_name_));
    }
  } while (OB_SUCC(ret) && is_happened);

  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(stmt->formalize_query_ref_exprs())) {
      LOG_WARN("failed to fromalize query ref exprs", K(ret));
    } else if (OB_FAIL(add_transform_hint(*stmt, &decorrelate_stmts))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  return ret;
}

int ObTransformDecorrelate::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObIArray<ObSelectStmt*> *decorrelate_stmts = NULL;
  UNUSED(stmt);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(trans_params)
      || OB_ISNULL(decorrelate_stmts = static_cast<ObIArray<ObSelectStmt*>*>(trans_params))
      || OB_UNLIKELY(decorrelate_stmts->empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(trans_params));
  } else {
    ObHint *hint = NULL;
    ObDMLStmt *child_stmt = NULL;
    ObString child_qb_name;
    const ObHint *myhint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < decorrelate_stmts->count(); ++i) {
      if (OB_ISNULL(child_stmt = decorrelate_stmts->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(child_stmt));
      } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_DECORRELATE, hint))) {
        LOG_WARN("failed to create hint", K(ret));
      } else if (OB_FAIL(child_stmt->get_qb_name(child_qb_name))) {
        LOG_WARN("failed to get qb name", K(ret), K(child_stmt->get_stmt_id()));
      } else if (OB_FAIL(ctx_->add_src_hash_val(child_qb_name))) {
        LOG_WARN("failed to add src hash val", K(ret));
      } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
        LOG_WARN("failed to push back hint", K(ret));
      } else if (NULL != (myhint = get_hint(child_stmt->get_stmt_hint()))
                 && myhint->is_enable_hint()
                 && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
        LOG_WARN("failed to add used trans hint", K(ret));
      } else {
        hint->set_qb_name(child_qb_name);
      }
    }
  }
  return ret;
}

int ObTransformDecorrelate::decorrelate_lateral_derived_table(ObDMLStmt *stmt,
                                                              ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                                              bool &trans_happened)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_items().count(); ++i) {
    TableItem *table_item = stmt->get_table_item(stmt->get_from_item(i));
    bool is_happened = false;
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (table_item->is_joined_table()) {
      if (OB_FAIL(transform_joined_table(stmt,
                                         static_cast<JoinedTable*>(table_item),
                                         true,
                                         decorrelate_stmts,
                                         is_happened))) {
        LOG_WARN("failed to transform joined table", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do decorrelate for joined table", K(is_happened),
                  K(table_item->table_id_));
      }
    } else if (table_item->is_lateral_table()) {
      if (OB_FAIL(transform_lateral_inline_view(stmt,
                                                table_item,
                                                decorrelate_stmts,
                                                true,
                                                NULL,
                                                is_happened))) {
        LOG_WARN("failed to transform lateral table item", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do decorrelate for basic lateral derived table",
                  K(is_happened),
                  K(table_item->table_id_));
      }
    }
  }
  return ret;
}

int ObTransformDecorrelate::transform_joined_table(ObDMLStmt *parent_stmt,
                                                   JoinedTable *joined_table,
                                                   bool parent_can_push_where,
                                                   ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  TableItem *left_table = NULL;
  TableItem *right_table = NULL;
  trans_happened = false;
  if (OB_ISNULL(parent_stmt) ||
      OB_ISNULL(joined_table) ||
      OB_ISNULL(left_table = joined_table->left_table_) ||
      OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parent_stmt), K(joined_table));
  } else if (joined_table->joined_type_ == CONNECT_BY_JOIN) {
    // do nothing
  } else {
    bool can_push_where = true;
    bool can_be_decorrelate = true;
    bool is_happened = false;
    if (!parent_can_push_where ||
        joined_table->joined_type_ == FULL_OUTER_JOIN ||
        joined_table->joined_type_ == RIGHT_OUTER_JOIN) {
      can_push_where = false;
      if (joined_table->joined_type_ == LEFT_OUTER_JOIN ||
          joined_table->joined_type_ == INNER_JOIN ||
          joined_table->joined_type_ == FULL_OUTER_JOIN) {
        can_be_decorrelate = false;
      }
    }
    if (left_table->is_joined_table()) {
      JoinedTable *j_table = static_cast<JoinedTable*>(left_table);
      if (OB_FAIL(SMART_CALL(transform_joined_table(parent_stmt,
                                                    j_table,
                                                    can_push_where,
                                                    decorrelate_stmts,
                                                    is_happened)))) {
        LOG_WARN("failed to transform joined table", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    } else if (!can_be_decorrelate) {
      // do nothing
    } else if (!left_table->is_lateral_table()) {
      // do nothing
    } else if (OB_FAIL(transform_lateral_inline_view(parent_stmt,
                                                     left_table,
                                                     decorrelate_stmts,
                                                     can_push_where,
                                                     joined_table,
                                                     is_happened))) {
      LOG_WARN("failed to transform basic table", K(ret));
    } else {
      trans_happened |= is_happened;
    }
    //process right table
    if (OB_SUCC(ret)) {
      can_push_where = true;
      can_be_decorrelate = true;
      if (!parent_can_push_where ||
          joined_table->joined_type_ == FULL_OUTER_JOIN ||
          joined_table->joined_type_ == LEFT_OUTER_JOIN) {
        can_push_where = false;
        if (joined_table->joined_type_ == RIGHT_OUTER_JOIN ||
            joined_table->joined_type_ == INNER_JOIN ||
            joined_table->joined_type_ == FULL_OUTER_JOIN) {
          can_be_decorrelate = false;
        }
      }
      if (right_table->is_joined_table()) {
        JoinedTable *j_table = static_cast<JoinedTable*>(right_table);
        if (OB_FAIL(SMART_CALL(transform_joined_table(parent_stmt,
                                                      j_table,
                                                      can_push_where,
                                                      decorrelate_stmts,
                                                      is_happened)))) {
          LOG_WARN("failed to transform joined table", K(ret));
        } else {
          trans_happened |= is_happened;
        }
      } else if (!can_be_decorrelate) {
        // do nothing
      } else if (!right_table->is_lateral_table()) {
        // do nothing
      } else if (OB_FAIL(transform_lateral_inline_view(parent_stmt,
                                                       right_table,
                                                       decorrelate_stmts,
                                                       can_push_where,
                                                       joined_table,
                                                       is_happened))) {
        LOG_WARN("failed to transform basic table", K(ret));
      } else {
        trans_happened |= is_happened;
      }
    }
  }
  return ret;
}

int ObTransformDecorrelate::transform_lateral_inline_view(ObDMLStmt *parent_stmt,
                                                          TableItem *table_item,
                                                          ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                                          bool can_push_where,
                                                          JoinedTable *joined_table,
                                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  bool need_create_spj = false;
  ObSelectStmt *ref_query = NULL;
  if (OB_ISNULL(parent_stmt) ||
      OB_ISNULL(table_item) ||
      OB_ISNULL(ref_query = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parent_stmt), K(table_item));
  } else if (OB_FAIL(check_transform_validity(parent_stmt,
                                              table_item->ref_query_,
                                              table_item,
                                              can_push_where,
                                              joined_table,
                                              is_valid,
                                              need_create_spj))) {
    LOG_WARN("failed to check transform validaity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(do_transform_lateral_inline_view(parent_stmt,
                                                      table_item->ref_query_,
                                                      table_item,
                                                      can_push_where,
                                                      joined_table,
                                                      need_create_spj))) {
    LOG_WARN("failed to do transform lateral inline view", K(ret));
  } else if (OB_FAIL(decorrelate_stmts.push_back(ref_query))) {
    LOG_WARN("failed to push back to array", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformDecorrelate::check_transform_validity(ObDMLStmt *stmt,
                                                     ObSelectStmt *ref_query,
                                                     TableItem *table_item,
                                                     bool can_push_where,
                                                     JoinedTable *joined_table,
                                                     bool &is_valid,
                                                     bool &need_create_spj)
{
  int ret = OB_SUCCESS;
  bool check_status = false;
  bool can_be_decorrelate_direct = true;
  bool has_rownum = false;
  bool is_ref_outer = false;
  is_valid = true;
  need_create_spj = false;
  OPT_TRACE("try to decorrelate lateral inline view :", ref_query);
  if (OB_ISNULL(stmt) || OB_ISNULL(ref_query) ||
      OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret), K(ref_query));
  } else if (OB_FAIL(check_hint_allowed_decorrelate(*stmt, *ref_query, is_valid))) {
    LOG_WARN("failed to check hint", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("hint reject decorrelate");
  } else if (!can_push_where &&
             OB_FAIL(ObTransformUtils::check_lateral_ref_outer_table(stmt,
                                                                     joined_table,
                                                                     table_item,
                                                                     is_ref_outer))) {
    LOG_WARN("failed to check lateral ref outer table", K(ret));
  } else if (is_ref_outer) {
    is_valid = false;
    OPT_TRACE("lateral ref outer table, cannot decorrelate");
  } else if (ref_query->is_set_stmt()) {
    ObIArray<ObSelectStmt*> &set_queries = ref_query->get_set_query();
    for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < set_queries.count(); ++i) {
      if (OB_FAIL(check_lateral_inline_view_validity(table_item,
                                                     set_queries.at(i),
                                                     is_valid))) {
        LOG_WARN("failed to check subquery valid", K(ret));
      }
    }
  } else if (OB_FAIL(check_lateral_inline_view_validity(table_item,
                                                        ref_query,
                                                        is_valid))) {
    LOG_WARN("failed to check subquery valid", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (!is_valid) {
    // do nothing
  } else if (ref_query->has_distinct()
             || ref_query->is_hierarchical_query()
             || ref_query->has_group_by()
             || ref_query->has_window_function()
             || ref_query->is_set_stmt()) {
    can_be_decorrelate_direct = false;
  } else if (OB_FAIL(ref_query->has_rownum(has_rownum))) {
    LOG_WARN("failed to check has rownum expr", K(ret));
  } else if (has_rownum) {
    can_be_decorrelate_direct = false;
  } else if (ref_query->has_limit()) {
    can_be_decorrelate_direct = false;
  }
  if (OB_FAIL(ret)) {
  } else if (!is_valid) {
    // do nothing
  } else if (can_be_decorrelate_direct) {
    // do nothing
  } else if (OB_FAIL(ObTransformUtils::check_correlated_exprs_can_pullup(table_item->exec_params_,
                                                                         *ref_query,
                                                                         is_valid))) {
    LOG_WARN("failed to check can unnest with spj", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
    OPT_TRACE("lateral inline view cannot be pullup, will not transform");
  } else {
    need_create_spj = true;
  }
  return ret;
}

int ObTransformDecorrelate::check_hint_allowed_decorrelate(ObDMLStmt &stmt,
                                                           ObDMLStmt &ref_query,
                                                           bool &allowed)
{
  int ret = OB_SUCCESS;
  allowed = true;
  const ObQueryHint *query_hint = NULL;
  const ObHint *myhint = get_hint(ref_query.get_stmt_hint());
  bool is_disable = NULL != myhint && myhint->is_disable_hint();
  const ObHint *no_rewrite1 = stmt.get_stmt_hint().get_no_rewrite_hint();
  const ObHint *no_rewrite2 = ref_query.get_stmt_hint().get_no_rewrite_hint();
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (query_hint->has_outline_data()) {
    // outline data allowed decorrelate
    allowed = query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, myhint);
  } else if (NULL != myhint && myhint->is_enable_hint()) {
    allowed = true;
  } else if (is_disable || NULL != no_rewrite1 || NULL != no_rewrite2) {
    allowed = false;
    if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite1))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (OB_FAIL(ctx_->add_used_trans_hint(no_rewrite2))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    } else if (is_disable && OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
      LOG_WARN("failed to add used trans hint", K(ret));
    }
  }
  return ret;
}

int ObTransformDecorrelate::check_lateral_inline_view_validity(TableItem *table_item,
                                                               ObSelectStmt *ref_query,
                                                               bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool check_status = false;
  bool is_where_cond_correlated = false;
  bool is_having_correlated = false;
  is_valid = true;
  if (OB_ISNULL(table_item) ||
      OB_ISNULL(ref_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_item), K(ref_query));
  } else if (OB_FAIL(ref_query->has_ref_assign_user_var(check_status))) {
    LOG_WARN("failed to check stmt has assignment ref user var", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("lateral inline view has user var");
  } else if (OB_FAIL(ObTransformUtils::is_correlated_exprs(table_item->exec_params_,
                                                           ref_query->get_condition_exprs(),
                                                           is_where_cond_correlated))) {
    LOG_WARN("failed to check correlated exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::is_correlated_exprs(table_item->exec_params_,
                                                           ref_query->get_having_exprs(),
                                                           is_having_correlated))) {
    LOG_WARN("failed to check correlated exprs", K(ret));
  } else if (!is_where_cond_correlated && !is_having_correlated) {
    is_valid = false;
    OPT_TRACE("lateral inline view no need decorrelate");
  } else if (OB_FAIL(ObTransformUtils::is_select_item_contain_subquery(ref_query, check_status))) {
    LOG_WARN("failed to check select item contain subquery", K(ref_query), K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("lateral inline view select expr contain subquery");
  } else if (OB_FAIL(ObTransformUtils::is_join_conditions_correlated(table_item->exec_params_,
                                                                     ref_query,
                                                                     check_status))) {
    LOG_WARN("failed to is joined table conditions correlated", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("lateral inline view contain correlated on condition");
  } else if (OB_FAIL(ObTransformUtils::is_table_item_correlated(table_item->exec_params_,
                                                                *ref_query,
                                                                check_status))) {
    LOG_WARN("failed to check if subquery contain correlated subquery", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("lateral inline view contain correlated table item");
  } else if (OB_FAIL(ObTransformUtils::is_where_subquery_correlated(table_item->exec_params_,
                                                                    *ref_query,
                                                                    check_status))) {
    LOG_WARN("failed to check where condition subquery is correlated", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("lateral inline view where condition contain correlated subquery");
  } else if (OB_FAIL(ObTransformUtils::is_select_item_correlated(table_item->exec_params_,
                                                                 *ref_query,
                                                                 check_status))) {
    LOG_WARN("failed to check select item is correlated", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("lateral inline view select item is correlated");
  } else if (OB_FAIL(ObTransformUtils::is_orderby_correlated(table_item->exec_params_,
                                                             *ref_query,
                                                             check_status))) {
    LOG_WARN("failed to check order by is correlated", K(ret));
  } else if (check_status) {
    is_valid = false;
    OPT_TRACE("lateral inline view order by is correlated");
  }
  return ret;
}

int ObTransformDecorrelate::do_transform_lateral_inline_view(ObDMLStmt *stmt,
                                                             ObSelectStmt *ref_query,
                                                             TableItem *table_item,
                                                             bool can_push_where,
                                                             JoinedTable *joined_table,
                                                             bool need_create_spj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(ref_query) ||
      OB_ISNULL(table_item) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->expr_factory_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(stmt), K(ref_query), K(table_item), K(ctx_));
  } else if (!can_push_where && OB_ISNULL(joined_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(can_push_where), K(joined_table));
  } else if (need_create_spj) {
    if (OB_FAIL(ObTransformUtils::create_spj_and_pullup_correlated_exprs(table_item->exec_params_,
                                                                         ref_query,
                                                                         ctx_))) {
      LOG_WARN("failed to create spj and pullup correlated exprs", K(ret));
    } else {
      table_item->ref_query_ = ref_query;
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<ObRawExpr*, 4> candi_pullup_conds;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    ObSEArray<ObRawExpr*, 4> upper_column_exprs;
    ObSEArray<ObRawExpr*, 4> select_exprs;
    ObSEArray<ObRawExpr*, 4> new_column_exprs;
    ObSEArray<ObRawExpr*, 4> pullup_conds;
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_FAIL(ObTransformUtils::get_correlated_conditions(table_item->exec_params_,
                                                            ref_query->get_condition_exprs(),
                                                            candi_pullup_conds))) {
      LOG_WARN("failed to get semi conditions", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(candi_pullup_conds,
                                                            column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(ref_query->get_condition_exprs(),
                                                    candi_pullup_conds))) {
      LOG_WARN("failed to remove condition expr", K(ret));
    } else if (OB_FAIL(ref_query->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::except_exprs(column_exprs,
                                                     select_exprs,
                                                     new_column_exprs))) {
      LOG_WARN("failed to except exprs", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                            new_column_exprs,
                                                            ref_query))) {
      LOG_WARN("failed to create select item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *table_item, stmt,
                                                                 upper_column_exprs))) {
      LOG_WARN("failed to create columns for view", K(ret));
    } else if (OB_FALSE_IT(select_exprs.reuse())) {
      // do nothing
    } else if (OB_FAIL(ref_query->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(copier.add_replaced_expr(select_exprs, upper_column_exprs))) {
      LOG_WARN("failed to add replaced expr", K(ret));
    } else if (OB_FAIL(copier.copy_on_replace(candi_pullup_conds, pullup_conds))) {
      LOG_WARN("failed to copy on replace expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::decorrelate(pullup_conds, table_item->exec_params_))) {
      LOG_WARN("failed to decorrelate pullup conds", K(ret));
    } else if (can_push_where && OB_FAIL(append(stmt->get_condition_exprs(), pullup_conds))) {
      LOG_WARN("failed to append condition exprs", K(ret));
    } else if (!can_push_where && OB_FAIL(append(joined_table->get_join_conditions(), pullup_conds))) {
      LOG_WARN("failed to append on condition exprs", K(ret));
    } else if (OB_FAIL(stmt->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(ref_query->adjust_subquery_list())) {
      LOG_WARN("failed to adjust subquery list", K(ret));
    } else if (OB_FAIL(ref_query->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("formalize child stmt failed", K(ret));
    }
  }
  return ret;
}

int ObTransformDecorrelate::check_hint_status(const ObDMLStmt &stmt, bool &need_trans)
{
  int ret = OB_SUCCESS;
  need_trans = false;
  const ObQueryHint *query_hint = NULL;
  const ObHint *cur_trans_hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (!query_hint->has_outline_data()) {
    need_trans = true;
  } else if (NULL == (cur_trans_hint = query_hint->get_outline_trans_hint(ctx_->trans_list_loc_)) ||
             !cur_trans_hint->is_decorrelate_hint()) {
    /*do nothing*/
  } else {
    ObSelectStmt* select_stmt = NULL;
    const TableItem *table_item = NULL;
    for (int64_t i = 0; !need_trans && OB_SUCC(ret) && i < stmt.get_table_size(); ++i) {
      if (OB_ISNULL(table_item = stmt.get_table_item(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!table_item->is_lateral_table()) {
        // do nothing
      } else if (OB_ISNULL(select_stmt = table_item->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        need_trans = query_hint->is_valid_outline_transform(ctx_->trans_list_loc_,
                                                            get_hint(select_stmt->get_stmt_hint()));
      }
    }
  }
  return ret;
}

int ObTransformDecorrelate::decorrelate_aggr_lateral_derived_table(ObDMLStmt *stmt,
                                                                   ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  ObSEArray<FromItem, 4> from_item_list;
  ObSEArray<JoinedTable*, 4> joined_table_list;
  trans_happened = false;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ctx_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_items().count(); ++i) {
    TableItem *table_item = stmt->get_table_item(stmt->get_from_item(i));
    bool is_happened = false;
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (table_item->is_joined_table()) {
      if (OB_FAIL(joined_table_list.push_back(static_cast<JoinedTable*>(table_item)))) {
        LOG_WARN("failed to push back joined table list", K(ret));
      }
    } else if (table_item->is_lateral_table()) {
      if (OB_FAIL(transform_aggr_lateral_inline_view(stmt,
                                                     table_item,
                                                     decorrelate_stmts,
                                                     from_item_list,
                                                     joined_table_list,
                                                     is_happened))) {
        LOG_WARN("failed to transform aggr lateral inline view", K(ret));
      } else {
        trans_happened |= is_happened;
        LOG_TRACE("succeed to do decorrelate for aggr lateral derived table",
                  K(is_happened),
                  K(table_item->table_id_));
      }
    }
    if (OB_SUCC(ret) && !is_happened &&
        OB_FAIL(from_item_list.push_back(stmt->get_from_item(i)))) {
      LOG_WARN("failed to push back array", K(ret));
    }
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(stmt->reset_from_item(from_item_list))) {
      LOG_WARN("failed to reset table_items", K(ret));
    } else if (OB_FAIL(stmt->get_joined_tables().assign(joined_table_list))) {
      LOG_WARN("failed to reset joined table container", K(ret));
    } else if (OB_FAIL(stmt->formalize_stmt(ctx_->session_info_))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      LOG_TRACE("succ to to do decorrelate aggr lateral inline view");
    }
  }
  return ret;
}

int ObTransformDecorrelate::transform_aggr_lateral_inline_view(ObDMLStmt *parent_stmt,
                                                               TableItem *table_item,
                                                               ObIArray<ObSelectStmt*> &decorrelate_stmts,
                                                               ObIArray<FromItem> &from_item_list,
                                                               ObIArray<JoinedTable*> &joined_table_list,
                                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSEArray<ObRawExpr*, 4> pullup_conds;
  ObSelectStmt *ref_query = NULL;
  if (OB_ISNULL(parent_stmt) ||
      OB_ISNULL(table_item) ||
      OB_ISNULL(ref_query = table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(parent_stmt), K(table_item));
  } else if (OB_FAIL(check_transform_aggr_validity(parent_stmt,
                                                   table_item->ref_query_,
                                                   table_item,
                                                   pullup_conds,
                                                   is_valid))) {
    LOG_WARN("failed to check transform validaity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(do_transform_aggr_lateral_inline_view(parent_stmt,
                                                           table_item->ref_query_,
                                                           table_item,
                                                           pullup_conds,
                                                           from_item_list,
                                                           joined_table_list))) {
    LOG_WARN("failed to do transform aggr lateral inline view", K(ret));
  } else if (OB_FAIL(decorrelate_stmts.push_back(ref_query))) {
    LOG_WARN("failed to push back to array", K(ret));
  } else {
    trans_happened = true;
  }
  return ret;
}

int ObTransformDecorrelate::check_transform_aggr_validity(ObDMLStmt *stmt,
                                                          ObSelectStmt *ref_query,
                                                          TableItem *table_item,
                                                          ObIArray<ObRawExpr*> &pullup_conds,
                                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool has_rownum = false;
  bool has_ref_assign_user_var = false;
  OPT_TRACE("try to decorrelate aggregate lateral inline view :", ref_query);
  if (OB_ISNULL(stmt) || OB_ISNULL(ref_query) ||
      OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subquery is null", K(ret), K(ref_query));
  } else if (OB_FAIL(check_hint_allowed_decorrelate(*stmt, *ref_query, is_valid))) {
    LOG_WARN("failed to check hint", K(ret));
  } else if (!is_valid) {
    // do nothing
    OPT_TRACE("hint reject decorrelate");
  } else if (!is_valid_group_by(*ref_query)) {
    is_valid = false;
    OPT_TRACE("ref query is valid group by");
  } else if (ref_query->has_rollup() ||
             ref_query->has_having() ||
             NULL != ref_query->get_limit_percent_expr() ||
             NULL != ref_query->get_offset_expr() ||
             ref_query->has_window_function() ||
             ref_query->has_sequence() ||
             ref_query->is_set_stmt() ||
             ref_query->is_hierarchical_query()) {
    is_valid = false;
    OPT_TRACE("ref query has rollup/having/limit offset/limit percent/win_func/sequence");
  } else if (OB_FAIL(ref_query->has_rownum(has_rownum))) {
    LOG_WARN("failed to check ref query has rownum", K(ret));
  } else if (has_rownum) {
    is_valid = false;
    OPT_TRACE("ref query has rownum");
  } else if (OB_FAIL(ObTransformUtils::check_is_basic_aggr_item(*ref_query, is_valid))) {
    LOG_WARN("failed to check subquery select item", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("has not sum/count/min/max aggregation");
  } else if (OB_FAIL(check_lateral_inline_view_validity(table_item,
                                                        ref_query,
                                                        is_valid))) {
    LOG_WARN("failed to check lateral inline view validity", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("ref query has no correlate expr");
  } else if (OB_FAIL(check_transform_aggr_condition_validity(stmt,
                                                             ref_query,
                                                             table_item,
                                                             pullup_conds,
                                                             is_valid))) {
    LOG_WARN("failed to check transform aggr condition validity", K(ret));
  } else if (!is_valid) {
    OPT_TRACE("ref query condition is not valid");
  }
  return ret;
}

int ObTransformDecorrelate::check_transform_aggr_condition_validity(ObDMLStmt *stmt,
                                                                    ObSelectStmt *ref_query,
                                                                    TableItem *table_item,
                                                                    ObIArray<ObRawExpr*> &pullup_conds,
                                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(ref_query) ||
      OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt), K(ref_query), K(table_item));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < ref_query->get_condition_size(); ++i) {
      ObRawExpr *cond = NULL;
      bool is_correlated = false;
      if (OB_ISNULL(cond = ref_query->get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("condition expr is null", K(ret));
      } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(table_item->exec_params_,
                                                              cond,
                                                              is_correlated))) {
        LOG_WARN("failed to check is correlated expr", K(ret));
      } else if (!is_correlated) {
        // do nothing
      } else if (!cond->has_flag(CNT_COLUMN) || cond->has_flag(CNT_SUB_QUERY)) {
        is_valid = false;
        OPT_TRACE("ref query condition has subquery");
      } else if (OB_FAIL(ObTransformUtils::is_equal_correlation(table_item->exec_params_,
                                                                cond,
                                                                is_valid))) {
        LOG_WARN("failed to check is equal correlation", K(ret));
      } else if (!is_valid) {
        OPT_TRACE("ref query correlated condition is not equal cond");
      } else if (OB_FAIL(pullup_conds.push_back(cond))) {
        LOG_WARN("failed to push back nested conditions", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformDecorrelate::do_transform_aggr_lateral_inline_view(
                            ObDMLStmt *stmt,
                            ObSelectStmt *ref_query,
                            TableItem *table_item,
                            ObIArray<ObRawExpr*> &pullup_conds,
                            ObIArray<FromItem> &from_item_list,
                            ObIArray<JoinedTable*> &joined_table_list)
{
  int ret = OB_SUCCESS;
  ObRawExpr* not_null_expr = NULL;
  ObSEArray<ObRawExpr *, 4> view_columns;
  ObSEArray<ObRawExpr *, 4> select_exprs;
  ObSEArray<ObRawExpr *, 4> real_values;
  ObSEArray<bool, 4> is_null_prop;
  int64_t idx = 0;
  if (OB_ISNULL(stmt) ||
      OB_ISNULL(ref_query) ||
      OB_ISNULL(table_item) ||
      OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (table_item->table_name_.empty() &&
             OB_FAIL(stmt->generate_view_name(*ctx_->allocator_,
                                              table_item->table_name_))) {
    LOG_WARN("failed to generate view name", K(ret));
  } else if (table_item->alias_name_.empty() &&
             OB_FALSE_IT(table_item->alias_name_ = table_item->table_name_)) {
    // do nothing
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < pullup_conds.count(); ++i) {
    ObRawExpr *cond_expr = pullup_conds.at(i);
    bool left_is_correlated = false;
    if (OB_ISNULL(cond_expr) || OB_UNLIKELY(cond_expr->get_expr_type() != T_OP_EQ) ||
        OB_ISNULL(cond_expr->get_param_expr(0)) ||
        OB_ISNULL(cond_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nested expr is invalid", K(ret), K(cond_expr));
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(ref_query->get_condition_exprs(),
                                                    cond_expr))) {
      LOG_WARN("failed to remove expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::is_correlated_expr(table_item->exec_params_,
                                                            cond_expr->get_param_expr(0),
                                                            left_is_correlated))) {
      LOG_WARN("failed to check is left correlated", K(ret));
    } else {
      // construct a pullup condition from a nested condition
      int64_t inner_param_id = left_is_correlated ? 1 : 0;
      ObRawExpr *group_expr = cond_expr->get_param_expr(inner_param_id);
      if (group_expr->has_flag(IS_CONST)) {
        // do nothing for const expr
      } else if (ObOptimizerUtil::find_item(ref_query->get_group_exprs(),
                                            group_expr)) {
        // do nothing
      } else if (OB_FAIL(ref_query->add_group_expr(group_expr))) {
        LOG_WARN("failed to add group expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                              group_expr,
                                                              ref_query))) {
        LOG_WARN("failed to create select item", K(ret));
      } else if (not_null_expr == NULL) {
        not_null_expr = group_expr;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(
                     ctx_, *table_item, stmt, view_columns))) {
    LOG_WARN("failed to create columns for view stmt", K(ret));
  } else if (OB_FAIL(ref_query->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (ObOptimizerUtil::find_item(select_exprs, not_null_expr, &idx)) {
    not_null_expr = view_columns.at(idx);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(gather_select_item_null_propagate(ref_query, is_null_prop))) {
    LOG_WARN("failed to gather select item null propagate", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deduce_query_values(*ctx_,
                                                           *stmt,
                                                           is_null_prop,
                                                           not_null_expr,
                                                           true,
                                                           select_exprs,
                                                           view_columns,
                                                           real_values))) {
    LOG_WARN("failed to deduce query values", K(ret));
  } else if (OB_FAIL(stmt->replace_relation_exprs(view_columns, real_values))) {
    LOG_WARN("failed to replace inner stmt expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_exprs(select_exprs, view_columns, pullup_conds))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::decorrelate(pullup_conds, table_item->exec_params_))) {
    LOG_WARN("failed to decorrelation", K(ret));
  } else if (OB_FAIL(transform_from_list(*stmt,
                                         table_item,
                                         ref_query->is_scala_group_by(),
                                         pullup_conds,
                                         from_item_list,
                                         joined_table_list))) {
    LOG_WARN("failed to transform from list", K(ret));
  }
  return ret;
}

int ObTransformDecorrelate::gather_select_item_null_propagate(ObSelectStmt *ref_query,
                                                              ObIArray<bool> &is_null_prop)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObRawExpr*, 4> vars;
  bool is_scala_group_by = false;
  if (OB_ISNULL(ref_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(is_null_prop.prepare_allocate(ref_query->get_select_item_size()))) {
    LOG_WARN("failed to prepare allocate case when array", K(ret));
  } else {
    is_scala_group_by = ref_query->is_scala_group_by();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ref_query->get_select_item_size(); ++i) {
    ObRawExpr *expr = NULL;
    vars.reuse();
    is_null_prop.at(i) = false;
    if (!is_scala_group_by) {
      is_null_prop.at(i) = true;
    } else if (OB_ISNULL(expr = ref_query->get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is null", K(ret), K(expr));
    } else if (OB_FAIL(ObTransformUtils::extract_nullable_exprs(expr, vars))) {
      LOG_WARN("failed to extract nullable exprs", K(ret));
    } else if (vars.count() <= 0) {
      // do nothing
    } else if (OB_FAIL(ObTransformUtils::is_null_propagate_expr(expr, vars, is_null_prop.at(i)))) {
      LOG_WARN("failed to check is null propagate expr", K(ret));
    }
  }
  return ret;
}

int ObTransformDecorrelate::transform_from_list(ObDMLStmt &stmt,
                                                TableItem *view_table_item,
                                                bool use_outer_join,
                                                const ObIArray<ObRawExpr *> &joined_conds,
                                                ObIArray<FromItem> &from_item_list,
                                                ObIArray<JoinedTable*> &joined_table_list)
{
  int ret = OB_SUCCESS;
  if (!use_outer_join) {
    if (OB_FAIL(stmt.add_condition_exprs(joined_conds))) {
      LOG_WARN("failed to add condition exprs", K(ret));
    } else {
      FromItem item;
      item.table_id_ = view_table_item->table_id_;
      item.is_joined_ = false;
      ret = from_item_list.push_back(item);
    }
  } else {
    TableItem *joined_table = NULL;
    if (OB_FAIL(ObTransformUtils::merge_from_items_as_inner_join(
                ctx_, stmt, from_item_list, joined_table_list, joined_table))) {
      LOG_WARN("failed to merge from items as inner join", K(ret));
    } else if (OB_FAIL(ObTransformUtils::add_new_joined_table(ctx_,
                                                              stmt,
                                                              LEFT_OUTER_JOIN,
                                                              joined_table,
                                                              view_table_item,
                                                              joined_conds,
                                                              joined_table,
                                                              false))) {
      LOG_WARN("failed to add new joined table", K(ret));
    } else if (OB_ISNULL(joined_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("joined table is null", K(ret));
    } else if (OB_FAIL(joined_table_list.push_back(static_cast<JoinedTable*>(joined_table)))) {
      LOG_WARN("failed to push back joined table list", K(ret));
    } else {
      FromItem item;
      item.table_id_ = joined_table->table_id_;
      item.is_joined_ = true;
      ret = from_item_list.push_back(item);
    }
  }
  return ret;
}

bool ObTransformDecorrelate::is_valid_group_by(const ObSelectStmt &ref_query)
{
  bool is_valid = false;
  if (ref_query.is_scala_group_by()) {
    is_valid = true;
  } else if (ref_query.get_group_expr_size() > 0 &&
             ref_query.get_rollup_expr_size() == 0) {
    is_valid = true;
  }
  return is_valid;
}

}
}