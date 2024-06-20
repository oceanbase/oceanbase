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
#include "sql/rewrite/ob_transform_query_push_down.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

/**
 * @brief ObTransformQueryPushDown::transform_one_stmt 将部分可以下压的算子下压，同时去掉外层subplan
 * 1.where condition
 * select * from (select * from t1 order by c3) where c2 > 2;
 * ==>
 * select * from t1 where c2 > 2 order by c3;
 *
 * select c2, c3 from (select c2, c3 from t1 group by c2, c3) where c2 > 2;
 * ==>
 * select c2, c3 from t1 group by c2, c3 having c2 > 2;
 *
 * 2.group by(having)
 * select c2 from (select c2 from t1) group by c2;
 * ==>
 * select c2 from t1 group by c2;
 * select c3 from (select c3 from t1) group by c3 having c3 > 5;
 * ==>
 * select c3 from t1 group by c3 having c3 > 5;
 *
 * 3.window function
 * select row_number() OVER() from (select * from t1);
 * ==>
 * select row_number() OVER() from t1;
 *
 * 4.aggr
 * select sum(c3) from (select * from t1) group by c3;
 * ==>
 * select sum(c3) from t1 group by c3;
 *
 * 5.distinct
 * select distinct * from (select * from t1 order by c2);
 * ==>
 * select distinct * from t1 order by c2;
 *
 * 6.order by
 * select * from (select * from t1 order by c2) order by c3;
 * ==>
 * select * from t1 order by c3;
 *
 * 7.limit
 * select * from (select t1.a, sum(b) from t1 group by t1.a) limit 5
 * ==>
 * select t1.a, sum(b) from t1 group by t1.a limit 5
 *
 * 8.rownum
 * select * from (select * from t1 where rownum = 1)
 * ==>
 * select * from t1 where rownum = 1
 *
 */
int ObTransformQueryPushDown::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                 ObDMLStmt *&stmt,
                                                 bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  trans_happened = false;
  ObSelectStmt *select_stmt = NULL; //outer select stmt
  ObSelectStmt *view_stmt = NULL; //inner select stmt
  bool can_transform = false; //can rewrite
  bool need_distinct = false; //need distinct
  bool transform_having = false; //can push where condition to having condition
  ObSEArray<int64_t, 4> select_offset; //record select item offset postition，used to adjust set-op stmt output postition
  ObSEArray<SelectItem, 4> const_select_items;//record const select item, used to adjust set-op stmt output postition
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
    OPT_TRACE("not select stmt, can not transform");
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    // do nothing
  } else if (1 == select_stmt->get_from_item_size() &&  // only one table reference
             !select_stmt->get_from_item(0).is_joined_ &&
             select_stmt->get_semi_infos().empty()) {
    const FromItem &cur_from = select_stmt->get_from_item(0);
    const TableItem *view_table_item = NULL;
    if (OB_ISNULL(view_table_item = select_stmt->get_table_item_by_id(cur_from.table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get table item", K(ret), K(cur_from));
    } else if (!view_table_item->is_generated_table()) {
      // do nothing
      OPT_TRACE("table item is not view, can not pushdown query");
    } else if (OB_ISNULL(view_stmt = view_table_item->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view stmt is null", K(ret));
    } else if (OB_FAIL(check_transform_validity(select_stmt,
                                                view_stmt,
                                                can_transform,
                                                need_distinct,
                                                transform_having,
                                                select_offset,
                                                const_select_items))) {
      LOG_WARN("gather transform information failed", K(ret));
    } else if (!can_transform) {
      /*do nothing*/
      OPT_TRACE("can not pushdown query");
    } else if (OB_FAIL(do_transform(select_stmt,
                                    view_stmt,
                                    need_distinct,
                                    transform_having,
                                    view_table_item,
                                    select_offset,
                                    const_select_items))) {
      LOG_WARN("do transform failed", K(ret), K(stmt));
    } else if (OB_ISNULL(stmt = view_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view stmt is null", K(ret), K(view_stmt));
    } else if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    } else {
      trans_happened = true;
    }
  } else {
    /*do nothing*/
    OPT_TRACE("not simple stmt, can not transform");
  }
  LOG_TRACE("succeed to push query down", K(trans_happened));
  return ret;
}

int ObTransformQueryPushDown::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                             const int64_t current_level,
                                             const ObDMLStmt &stmt,
                                             bool &need_trans)
{
  int ret = OB_SUCCESS;
  need_trans = false;
  UNUSED(parent_stmts);
  UNUSED(current_level);
  const ObQueryHint *query_hint = NULL;
  const ObHint *trans_hint = NULL;
  if (!stmt.is_select_stmt() || is_normal_disabled_transform(stmt)) {
    need_trans = false;
  } else if (OB_ISNULL(ctx_) || OB_ISNULL(query_hint = stmt.get_stmt_hint().query_hint_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(query_hint));
  } else if (stmt.get_table_size() == 1) {
    const TableItem *table = NULL;
    const ObViewMergeHint *myhint = NULL;
    if (OB_ISNULL(table = stmt.get_table_item(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table item is null", K(ret));
    } else if (!table->is_generated_table()) {
      /*do nothing*/
    } else if (OB_ISNULL(table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(*table));
    } else if (!query_hint->has_outline_data()) {
      if (OB_FAIL(check_hint_allowed_query_push_down(stmt, *table->ref_query_, need_trans))) {
        LOG_WARN("failed to check hint allowed query push down", K(ret));
      } else if (!need_trans) {
        OPT_TRACE("hint reject transform");
      }
    } else if (OB_FALSE_IT(myhint =
                           static_cast<const ObViewMergeHint*>(get_hint(table->ref_query_->get_stmt_hint())))) {
      // do nothing
    } else if (myhint != NULL &&
               query_hint->is_valid_outline_transform(ctx_->trans_list_loc_, myhint) &&
               myhint->enable_query_push_down(ctx_->src_qb_name_)) {
      need_trans = true;
    } else {
      OPT_TRACE("outline reject transform");
    }
  }
  return ret;
}

int ObTransformQueryPushDown::check_hint_allowed_query_push_down(const ObDMLStmt &stmt,
                                                                 const ObSelectStmt &ref_query,
                                                                 bool &allowed)
{
  int ret = OB_SUCCESS;
  allowed = true;
  const ObViewMergeHint *myhint = static_cast<const ObViewMergeHint*>(get_hint(ref_query.get_stmt_hint()));
  bool is_disable = (NULL != myhint && myhint->enable_no_query_push_down());
  const ObHint *no_rewrite1 = stmt.get_stmt_hint().get_no_rewrite_hint();
  const ObHint *no_rewrite2 = ref_query.get_stmt_hint().get_no_rewrite_hint();
  if (OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (NULL != myhint && myhint->enable_query_push_down(ctx_->src_qb_name_)) {
    // enable transform hint added after transform in construct_transform_hint()
    allowed = true;
  } else if (is_disable || NULL != no_rewrite1 || NULL != no_rewrite2) {
    // add disable transform hint here
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

/**
 * @brief check_transform_validity 检查改写是否满足相关条件:
 * 1.判断是否有cte等(外层含有sequence,内层必须为spj)
 * 2.判断set-op能否被下压 -->放在最先判断因为不支持外层set-op往里压
 * 3.判断rownum能否被下压
 * 4.判断select item能否被下压
 * 5.判断where condition能否被下压 -->可能被下压为having expr
 * 6.判断group by、aggr、having condition能否被下压
 * 7.判断window function能否被下压
 * 8.判断distinct 能否被下压
 * 9.判断order by能否被下压
 * 10.判断limit能否被下压
 */
int ObTransformQueryPushDown::check_transform_validity(ObSelectStmt *select_stmt,
                                                       ObSelectStmt *view_stmt,
                                                       bool &can_transform,
                                                       bool &need_distinct,
                                                       bool &transform_having,
                                                       ObIArray<int64_t> &select_offset,
                                                       ObIArray<SelectItem> &const_select_items)
{
  int ret = OB_SUCCESS;
  bool check_status = false;
  can_transform = false;
  need_distinct = false;
  transform_having = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt) ||
      OB_ISNULL(select_stmt->get_query_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (select_stmt->is_recursive_union() ||
             view_stmt->is_hierarchical_query() ||
             (view_stmt->is_recursive_union() && !select_stmt->is_spj()) ||
             select_stmt->is_set_stmt() ||
             (select_stmt->has_sequence() && !view_stmt->is_spj()) ||
             view_stmt->has_ora_rowscn()) {//判断1, 2
    can_transform = false;
    OPT_TRACE("stmt is not spj");
  } else if (select_stmt->get_query_ctx()->optimizer_features_enable_version_ < COMPAT_VERSION_4_3_2 &&
             view_stmt->is_values_table_query()) {
    can_transform = false;
    OPT_TRACE("stmt is not spj");
  } else if (OB_FAIL(check_rownum_push_down(select_stmt, view_stmt, check_status))) {//判断3
    LOG_WARN("check rownum push down failed", K(check_status), K(ret));
  } else if (!check_status) {
    can_transform = false;
    OPT_TRACE("can not pushdwon rownum");
  } else if (OB_FAIL(check_select_item_push_down(select_stmt,
                                                 view_stmt,
                                                 select_offset,
                                                 const_select_items,
                                                 check_status))) {//判断4
    LOG_WARN("check select item push down failed");
  } else if (!check_status) {
    can_transform = false;
    OPT_TRACE("can not pushdown select expr");
  } else if (select_stmt->get_condition_size() > 0 &&
             OB_FAIL(check_where_condition_push_down(select_stmt,
                                                     view_stmt,
                                                     transform_having,
                                                     check_status))) {//判断5
    LOG_WARN("check where condition push down failed", K(check_status), K(ret));
  } else if (!check_status) {
    can_transform = false;
    OPT_TRACE("can not pushdown where condition");
  } else if ((select_stmt->has_group_by() || select_stmt->has_rollup())
             && !view_stmt->is_spj()) {//判断6
    can_transform = false;
    OPT_TRACE("stmt has group by, but view is not spj");
  } else if (select_stmt->has_window_function() &&
             OB_FAIL(check_window_function_push_down(view_stmt, check_status))) {//判断7
    LOG_WARN("check window function push down failed", K(check_status), K(ret));
  } else if (!check_status) {
    can_transform = false;
    OPT_TRACE("can not pushdown windown function");
  } else if (select_stmt->has_distinct() &&
             OB_FAIL(check_distinct_push_down(view_stmt, need_distinct, check_status))) {//判断8
    LOG_WARN("check distinct push down failed", K(check_status), K(ret));
  } else if (!check_status) {
    can_transform = false;
    OPT_TRACE("can not pushdown distinct");
  } else if (select_stmt->has_order_by() && view_stmt->has_limit()) {//判断9
    can_transform = false;
    OPT_TRACE("stmt has order by, but view has limit ,can not pushdown");
  } else if (select_stmt->has_limit()) {//判断10
    can_transform = (!select_stmt->is_calc_found_rows()
                     && !view_stmt->is_contains_assignment()
                     && can_limit_merge(*select_stmt, *view_stmt));
  } else {
    can_transform = true;
  }
  return ret;
}

//在 upper_stmt 与 view_stmt limit 在以下两种情况可以合并:
// 1. view_stmt 无 limit, 直接使用上层 limit
// 2. upper_stmt 与 view_stmt 均无 fetch with ties, limit_percent, 合并 limit 和 offset:
//  select ... from (select ... limit a1 offset b1) limit a2 offset b2;
//  select ... from ... limit min(a1 - b2, a2) offset b1 + b2;
bool ObTransformQueryPushDown::can_limit_merge(ObSelectStmt &upper_stmt,
                                               ObSelectStmt &view_stmt)
{
  bool can_merge = false;
  if (!view_stmt.has_limit()) {
    can_merge = true;
  } else if (NULL == upper_stmt.get_limit_percent_expr()
             && NULL == view_stmt.get_limit_percent_expr()
             && !upper_stmt.is_fetch_with_ties()
             && !view_stmt.is_fetch_with_ties()) {
    can_merge = true;
  } else {
    can_merge = false;
  }
  return can_merge;
}

int ObTransformQueryPushDown::do_limit_merge(ObSelectStmt &upper_stmt,
                                             ObSelectStmt &view_stmt)
{
  int ret = OB_SUCCESS;
  ObRawExpr *limit_expr = NULL;
  ObRawExpr *offset_expr = NULL;
  if (!upper_stmt.has_limit()) {
    /*do nothing*/
  } else if (!can_limit_merge(upper_stmt, view_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt input", K(ret), K(upper_stmt), K(view_stmt));
  } else if (!view_stmt.has_limit()) {
    view_stmt.set_limit_offset(upper_stmt.get_limit_expr(), upper_stmt.get_offset_expr());
    view_stmt.set_limit_percent_expr(upper_stmt.get_limit_percent_expr());
    view_stmt.set_fetch_with_ties(upper_stmt.is_fetch_with_ties());
    view_stmt.set_has_fetch(upper_stmt.has_fetch());
  } else if (OB_FAIL(ObTransformUtils::merge_limit_offset(ctx_, view_stmt.get_limit_expr(),
                                                          upper_stmt.get_limit_expr(),
                                                          view_stmt.get_offset_expr(),
                                                          upper_stmt.get_offset_expr(),
                                                          limit_expr, offset_expr))) {
    LOG_WARN("failed to merge limit offset", K(ret));
  } else {
    //这里 upper_stmt 无 percent expr / with ties, 直接舍弃了 upper stmt 的 has fetch 标识
    view_stmt.set_limit_offset(limit_expr, offset_expr);
  }
  return ret;
}

int ObTransformQueryPushDown::is_select_item_same(ObSelectStmt *select_stmt,
                                                  ObSelectStmt *view_stmt,
                                                  bool &is_same,
                                                  ObIArray<int64_t> &select_offset,
                                                  ObIArray<SelectItem> &const_select_items)
{
  int ret = OB_SUCCESS;
  is_same = false;
  if (OB_ISNULL(select_stmt)|| OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (select_stmt->get_select_item_size() < view_stmt->get_select_item_size()) {
    is_same = false;
  } else {
    is_same = true;
    ObBitSet<> column_id;
    bool is_same_exactly = true;
    int64_t column_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && is_same && i < select_stmt->get_select_item_size(); ++i) {
      const ObRawExpr *sel_expr = NULL;
      const ObColumnRefRawExpr *column_expr = NULL;
      if (OB_ISNULL(sel_expr = select_stmt->get_select_item(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is null", K(ret), K(sel_expr));
      } else if (sel_expr->is_const_expr()) {
        if (OB_FAIL(const_select_items.push_back(select_stmt->get_select_item(i)))) {
          LOG_WARN("failed to push back select item");
        } else if (OB_FAIL(select_offset.push_back(-1))) {//-1 meanings const expr
          LOG_WARN("failed to push back select offset");
        } else {
          is_same_exactly = false;
        }
      } else if (!sel_expr->is_column_ref_expr()) {
        is_same = false;
      } else if (OB_FAIL(select_offset.push_back(static_cast<const ObColumnRefRawExpr *>(sel_expr)
                                                   ->get_column_id() - OB_APP_MIN_COLUMN_ID))) {
        LOG_WARN("push back location offset failed", K(ret));
      } else {
        ++ column_count;
        column_expr = static_cast<const ObColumnRefRawExpr *>(sel_expr);
        if (column_id.has_member(column_expr->get_column_id())) {
          is_same = false;
          is_same_exactly = false;
        } else {
          column_id.add_member(column_expr->get_column_id());
        }
        is_same_exactly &= (static_cast<const ObColumnRefRawExpr *>(sel_expr)->get_column_id()
                            == i + OB_APP_MIN_COLUMN_ID);
      }
    }
    if (OB_SUCC(ret) && is_same) {
      if (column_count == view_stmt->get_select_item_size()) {
        /*do nothing*/
      //if outer output is const expr, then inner output must be const expr, eg:select 1 from (select 2 from t1)
      } else if (OB_FAIL(check_set_op_expr_reference(select_stmt, view_stmt,
                                                     select_offset, is_same))) {
        LOG_WARN("failed to check select item reference", K(ret));
      } else if (!is_same) {
        // view stmt is set stmt and outer output not contain all set op exprs in relation exprs of view
        // eg: select 1, c1 from (select 2 c1, 3 c2 from t1 union all select 4, 5 from t2 order by union[2]) v
      } else if (const_select_items.count() == select_stmt->get_select_item_size()) {
        for (int64_t i = 0; OB_SUCC(ret) && is_same && i < view_stmt->get_select_item_size(); ++i) {
          ObRawExpr *select_expr = NULL;
          if (OB_ISNULL(select_expr = view_stmt->get_select_item(i).expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("select expr is null", K(ret), K(select_expr));
          } else if (select_expr->is_set_op_expr()) {
            if (OB_FAIL(check_set_op_is_const_expr(view_stmt, select_expr, is_same))) {
              LOG_WARN("failed to check set op is const expr", K(ret));
            }
          } else {
            is_same = select_expr->is_const_expr();
          }
        }
      } else {
        is_same = false;
      }
      if (OB_SUCC(ret) && is_same && is_same_exactly) {
        select_offset.reset();
        const_select_items.reset();
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::check_set_op_expr_reference(ObSelectStmt *select_stmt,
                                                          ObSelectStmt *view_stmt,
                                                          ObIArray<int64_t> &select_offset,
                                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(select_stmt)|| OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (!view_stmt->is_set_stmt()) {
    // do nothing
  } else {
    ObSEArray<ObRawExpr*, 4> relation_exprs;
    ObSEArray<ObRawExpr*, 4> set_op_exprs;
    ObSEArray<ObRawExpr*, 4> column_exprs;
    ObBitSet<> selected_set_op_idx;
    ObStmtExprGetter visitor;
    visitor.set_relation_scope();
    visitor.remove_scope(SCOPE_SELECT);
    if (OB_FAIL(view_stmt->get_relation_exprs(relation_exprs, visitor))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_set_op_exprs(relation_exprs,
                                                            set_op_exprs))) {
      LOG_WARN("failed to extract set op exprs", K(ret));
    } else if (FALSE_IT(relation_exprs.reuse())) {
    } else if (OB_FAIL(select_stmt->get_relation_exprs(relation_exprs, visitor))) {
      LOG_WARN("failed to get relation exprs", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(relation_exprs,
                                                            column_exprs))) {
      LOG_WARN("failed to extract column exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < select_offset.count(); ++i) {
      if (select_offset.at(i) != -1) {
        selected_set_op_idx.add_member(select_offset.at(i));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < set_op_exprs.count(); ++i) {
      ObSetOpRawExpr *expr = static_cast<ObSetOpRawExpr*>(set_op_exprs.at(i));
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret), KPC(set_op_exprs.at(i)));
      } else if (!selected_set_op_idx.has_member(expr->get_idx())) {
        is_valid = false;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < column_exprs.count(); ++i) {
      ObColumnRefRawExpr *expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(i));
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret), KPC(column_exprs.at(i)));
      } else if (!selected_set_op_idx.has_member(expr->get_column_id() - OB_APP_MIN_COLUMN_ID)) {
        is_valid = false;
      }
    }
  }
  return ret;
}

/*@brief check_rownum_push_down检查含有rownum能否被下压
* 1.外层存在rownum时
* 如果外层存在rownum,内层是非spj时算子基本都不支持下压，如:
*  1)select rowum from (select distinct c1 from t); ==>直接下压，先执行排序编号，在去重，语义错误
*  2)select rowum from (select c2 from t group by c2); ==>直接下压语法错误
*  3)select rowum from (select c2 from t order by c2); ==>直接下压，无法保证rownum和order by执行先后顺序，
*    语义错误
* 而如果内层是spj则可以在view merge处做统一处理，因此这里统一不支持外层有rownum算子的下压
* 2.内层存在rownum时
* 而如果内层存在rownum，如果外层有其他算子下压，同内层有rownum情况一样，即使只有where condition,也会出现不能下压，
* 如:
* select * from (select pk1, rownum rn from t1 where pk1>0) t where t.rn>1 and t.rn<=10;
* 因此这目前只支持以下两种情况下压内层有:
*  1)外层仅仅只有distinct任何算子: select distinct * from (select * from t1 where rownum = 1);
*  2)外层没有任何算子:select * from (select * from t1 where rownum = 1);
 */
int ObTransformQueryPushDown::check_rownum_push_down(ObSelectStmt *select_stmt,
                                                     ObSelectStmt *view_stmt,
                                                     bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  bool select_has_rownum = false;
  bool view_has_rownum = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (OB_FAIL(select_stmt->has_rownum(select_has_rownum))) {
    LOG_WARN("check stmt has rownum failed", K(select_stmt), K(ret));
  } else if (OB_FAIL(view_stmt->has_rownum(view_has_rownum))) {
    LOG_WARN("check stmt has rownum failed", K(view_stmt), K(ret));
  } else if (select_has_rownum) { //判断1
    can_be = false;
  } else if (view_has_rownum &&
             (select_stmt->get_condition_size() > 0 ||
              select_stmt->has_group_by() ||
              select_stmt->has_rollup() ||
              select_stmt->has_window_function() ||
              select_stmt->is_set_stmt() ||
              select_stmt->has_order_by())) { //判断2
    can_be = false;
  } else {
    can_be = true;
  }
  return ret;
}

/*@brief check_select_item_push_down检查select item能否被下压
* 1.首先判断内外select item是否相同，如果相同，证明select item肯定能被下压,而不同需要考虑以下情况:
*   2.1 内层含有aggr, 这个时候虽然aggr先执行，但是aggr和select item相关, 如果直接把select item压下去，则有
*       可能引起语义错误，如:eg: select 1 from (select sum(c1) from t1); 但是如果是含有group by时是没问题的
*   2.2 内层含有distinct, 这个也是显然不行的，因为distinct后于select item执行, 直接压下去会有语义错误
*   2.3 内层含有set-op, 这个就必须要求内外的select item相同
*   2.4 内层含有用户变量赋值, 直接下压可能会消掉这个赋值. 
*       e.g. select f1 from (select @a := c1 as f1, @b := @b +1 from t1);
*/
int ObTransformQueryPushDown::check_select_item_push_down(ObSelectStmt *select_stmt,
                                                          ObSelectStmt *view_stmt,
                                                          ObIArray<int64_t> &select_offset,
                                                          ObIArray<SelectItem> &const_select_items,
                                                          bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  bool check_status = false;
  bool is_select_expr_valid = false;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  bool has_assign = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(view_stmt), K(ret));
  } else if (OB_FAIL(check_select_item_subquery(*select_stmt, *view_stmt, check_status))) {
    LOG_WARN("failed to check select item has subquery", K(ret));
  } else if (!check_status) {
    can_be = false;
    OPT_TRACE("view`s select expr has subquery");
  } else if (OB_FAIL(ObTransformUtils::check_has_assignment(*view_stmt, has_assign))) {
    LOG_WARN("check has assign failed", K(ret));
  } else if (has_assign) {
    can_be = false;
  } else if (OB_FAIL(ObTransformUtils::check_has_assignment(*select_stmt, has_assign))) {
    LOG_WARN("check has assign failed", K(ret));
  } else if (has_assign) {
    can_be = false;
  } else if (OB_FAIL(view_stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::check_expr_valid_for_stmt_merge(select_exprs,
                                                                       is_select_expr_valid))) {
    LOG_WARN("failed to check select expr valid", K(ret));
  } else if (!is_select_expr_valid) {
    can_be = false;
    OPT_TRACE("stmt or view has assignment");
  } else if(OB_FAIL(is_select_item_same(select_stmt,
                                        view_stmt,
                                        check_status,
                                        select_offset,
                                        const_select_items))) {
    LOG_WARN("check select item same failed", K(ret));
  } else if (check_status && !view_stmt->is_recursive_union()) {//is_recursive_union需要完全一致
    can_be = true;
  } else if (view_stmt->is_scala_group_by() ||
             view_stmt->has_distinct() ||
             (view_stmt->is_recursive_union() && (!check_status || !select_offset.empty())) ||
             (view_stmt->is_set_stmt() && !view_stmt->is_recursive_union())) {
    can_be = false;
    OPT_TRACE("view is scalary group or view has distinct");
  } else {
    can_be = true;
  }
  if (OB_SUCC(ret) && select_stmt->has_rollup()) {
    for (int64_t i = 0; OB_SUCC(ret) && can_be && i < select_exprs.count(); ++i) {
      if (OB_ISNULL(select_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("select expr is null", K(ret));
      } else {
        can_be = !select_exprs.at(i)->is_const_expr() &&
                 !select_exprs.at(i)->has_flag(CNT_SUB_QUERY);
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::check_select_item_subquery(ObSelectStmt &select_stmt,
                                                         ObSelectStmt &view,
                                                         bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = true;
  ObSEArray<ObRawExpr*, 4> column_exprs_from_subquery;
  ObSEArray<ObQueryRefRawExpr*, 4> query_ref_exprs;
  ObRawExpr *expr = NULL;
  TableItem *table = NULL;
  ObSqlBitSet<> table_set;
  if (OB_UNLIKELY(1 != select_stmt.get_table_items().count())
      || OB_ISNULL(table = select_stmt.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect select stmt", K(ret), K(select_stmt.get_from_item_size()), K(table));
  }
  for (int64_t i = 0; OB_SUCC(ret) && can_be && i < view.get_select_item_size(); ++i) {
    if (OB_ISNULL(expr = view.get_select_item(i).expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!expr->has_flag(CNT_SUB_QUERY)) {
      /* do nothing */
    } else if (expr->has_flag(IS_WITH_ANY) || expr->has_flag(IS_WITH_ALL) ||
               expr->get_expr_type() == T_OP_EXISTS || expr->get_expr_type() == T_OP_NOT_EXISTS) {
      /*
      * Disable query pushdown when a select item of view is `any/all/exists subquery` form.
      * As for the query below, after pushing down, v.c2 will be used for both projection and filtering,
      * which may result in an incorrect stmt status in the subsequent rewrite loop.
      * e.g. select * from
      * (select t1.c1, (exists(select 1 from t2 where (t1.c1 = t2.c1))) as c2 from t1) v
      * where v.c2 is true;
      */
      can_be = false;
    } else if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(expr, query_ref_exprs, true))) {
      LOG_WARN("failed to extract query ref exprs", K(ret));
    } else if (OB_ISNULL(expr = select_stmt.get_column_expr_by_id(table->table_id_,
                                                                  i + OB_APP_MIN_COLUMN_ID))) {
      /* do nothing */
    } else if (OB_FAIL(column_exprs_from_subquery.push_back(expr))) {
      LOG_WARN("failed to push back column expr", K(ret));
    }
  }

  // check query ref exprs of view's select items
  if (OB_FAIL(ret) || !can_be) {
    /* do nothing */
  } else if (query_ref_exprs.count() > 0) {
    for (int64_t i = 0; OB_SUCC(ret) && can_be && i < query_ref_exprs.count(); i++) {
      ObQueryRefRawExpr* query_ref = NULL;
      if (OB_ISNULL(query_ref = query_ref_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null pointer", K(ret));
      } else if (query_ref->is_set() || query_ref->get_output_column() != 1) {
        can_be = false;
      }
    }
  }

  // check query ref exprs of upper select stmt
  if (OB_FAIL(ret) || !can_be) {
  } else if (column_exprs_from_subquery.empty()) {
    /* do nothing */
  } else if (OB_FAIL(select_stmt.get_table_rel_ids(*table, table_set))) {
    LOG_WARN("failed to get rel ids", K(ret));
  } else {
    ObIArray<ObQueryRefRawExpr*> &subquery_exprs = select_stmt.get_subquery_exprs();
    ObSEArray<ObRawExpr*, 4> column_exprs;
    for (int64_t i = 0; OB_SUCC(ret) && can_be && i < subquery_exprs.count(); ++i) {
      column_exprs.reuse();
      if(OB_FAIL(ObRawExprUtils::extract_column_exprs(subquery_exprs.at(i),
                                                      column_exprs))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else if (ObOptimizerUtil::overlap(column_exprs, column_exprs_from_subquery)) {
        can_be = false;
      }
    }
  }
  return ret;
}

/*@brief check_where_condition_push_down 判断外层有condition条件时，能否下压到内层, 需要注意的是:
* 如果内层为group by/aggr等时，where condition需要下压为having condition,但是需要注意的是有如果where
* condition包含子查询时，继续下压到having condition中可能会造成性能回退，因此不能下含有子查询的where condition
* 到having condition中
*/
int ObTransformQueryPushDown::check_where_condition_push_down(ObSelectStmt *select_stmt,
                                                              ObSelectStmt *view_stmt,
                                                              bool &transform_having,
                                                              bool &can_be)
{
  int ret = OB_SUCCESS;
  bool has_assign = false;
  can_be = false;
  transform_having = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(view_stmt), K(ret));
  } else if (view_stmt->is_set_stmt() ||
             view_stmt->has_limit() ||
             view_stmt->has_window_function()) {
    can_be = false;
  } else if (OB_FAIL(ObTransformUtils::check_has_assignment(*view_stmt, has_assign))) {
    LOG_WARN("check has assign failed", K(ret));
  } else if (has_assign) {
    can_be = false;
  } else if (view_stmt->has_group_by() || view_stmt->has_rollup()) {
    bool is_invalid = false;
    for (int64_t i = 0;
         OB_SUCC(ret) && !is_invalid && i < select_stmt->get_condition_size();
         ++i) {
      const ObRawExpr *cond_expr = NULL;
      if (OB_ISNULL(cond_expr = select_stmt->get_condition_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret));
      } else if (cond_expr->has_flag(CNT_SUB_QUERY)) {
        is_invalid = true;
        can_be = false;
      } else {
        /*do nothing*/
      }
    }
    if (OB_SUCC(ret) && !is_invalid) {
      transform_having = true;
      can_be = true;
    }
  } else {
    can_be = true;
  }
  return ret;
}

/*@brief check_window_function_push_down 判断外层有window function条件时，能否下压到内层
* 内层含有窗口函数，如果直接下压也会存在问题,如:
* SELECT t.*, SUM(t.`rank`) OVER (ROWS UNBOUNDED PRECEDING) FROM
    (SELECT sex, id, date, ROW_NUMBER() OVER w AS row_no, RANK() OVER w AS `rank` FROM t1,t2
    WHERE t1.id=t2.user_id WINDOW w AS (PARTITION BY date ORDER BY id)) AS t;
 */
int ObTransformQueryPushDown::check_window_function_push_down(ObSelectStmt *view_stmt,
                                                              bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  if (OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(view_stmt), K(ret));
  } else if (view_stmt->has_window_function() ||
             view_stmt->has_distinct() || 
             view_stmt->is_set_stmt() || 
             view_stmt->has_order_by() || 
             view_stmt->has_limit() ||
             view_stmt->is_contains_assignment()) {
    can_be = false;
  } else {
    can_be = true;
  }
  return ret;
}

/*@brief check_distinct_push_down 判断外层有distinct条件时，能否下压到内层
 */
int ObTransformQueryPushDown::check_distinct_push_down(ObSelectStmt *view_stmt,
                                                       bool &need_distinct,
                                                       bool &can_be)
{
  int ret = OB_SUCCESS;
  can_be = false;
  if (OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(view_stmt), K(ret));
  } else if (view_stmt->has_limit()) {
    can_be = false;
  } else if (view_stmt->is_set_stmt()) {
    need_distinct = true;
    can_be = true;
  } else {
    can_be = true;
  }
  return ret;
}

int ObTransformQueryPushDown::do_transform(ObSelectStmt *select_stmt,
                                           ObSelectStmt *view_stmt,
                                           bool need_distinct,
                                           bool transform_having,
                                           const TableItem *view_table_item,
                                           ObIArray<int64_t> &select_offset,
                                           ObIArray<SelectItem> &const_select_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt) ||
      OB_ISNULL(view_table_item) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(select_stmt), K(view_stmt),
                                 K(view_table_item), K(ctx_), K(ret));
    // adjsut hint, replace name after merge stmt hint.
  } else if (OB_FAIL(view_stmt->get_stmt_hint().merge_stmt_hint(select_stmt->get_stmt_hint(),
                                                                LEFT_HINT_DOMINATED))) {
    LOG_WARN("failed to merge stmt hint", K(ret));
  } else if (OB_FAIL(view_stmt->get_stmt_hint().replace_name_for_single_table_view(ctx_->allocator_,
                                                                                   *select_stmt,
                                                                                   *view_table_item))) {
      LOG_WARN("failed to replace name for single table view", K(ret));
  } else if (OB_FAIL(replace_stmt_exprs(select_stmt,
                                        view_stmt,
                                        view_table_item->table_id_))) {
    LOG_WARN("failed to replace stmt exprs", K(ret));
  } else if (OB_FAIL(push_down_stmt_exprs(select_stmt,
                                          view_stmt,
                                          need_distinct,
                                          transform_having,
                                          select_offset,
                                          const_select_items))) {
    LOG_WARN("push down stmt exprs failed", K(ret));
  } else {
    /* do nothing*/
  }
  return ret;
}

int ObTransformQueryPushDown::push_down_stmt_exprs(ObSelectStmt *select_stmt,
                                                   ObSelectStmt *view_stmt,
                                                   bool need_distinct,
                                                   bool transform_having,
                                                   ObIArray<int64_t> &select_offset,
                                                   ObIArray<SelectItem> &const_select_items)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(view_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(select_stmt), K(view_stmt), K(ret));
  } else if (OB_ISNULL(select_stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null query ctx", K(ret));
  } else if (!transform_having && OB_FAIL(append(view_stmt->get_condition_exprs(),
                                                 select_stmt->get_condition_exprs()))) {
    LOG_WARN("append select_stmt condition exprs to view stmt failed", K(ret));
  } else if (transform_having && OB_FAIL(append(view_stmt->get_having_exprs(),
                                                select_stmt->get_condition_exprs()))) {
    LOG_WARN("append select_stmt condition exprs to view stmt having expr failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_group_exprs(),
                            select_stmt->get_group_exprs()))) {
    LOG_WARN("append select_stmt window func exprs to view stmt window func exprs failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_rollup_exprs(),
                            select_stmt->get_rollup_exprs()))) {
    LOG_WARN("append select_stmt rollup exprs failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_rollup_dirs(),
                            select_stmt->get_rollup_dirs()))) {
    LOG_WARN("append select_stmt rollup directions failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_aggr_items(),
                            select_stmt->get_aggr_items()))) {
    LOG_WARN("append aggr items failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_having_exprs(),
                            select_stmt->get_having_exprs()))) {
    LOG_WARN("append select_stmt having exprs to view stmt having exprs failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_window_func_exprs(),
                            select_stmt->get_window_func_exprs()))) {
    LOG_WARN("append select_stmt window func exprs to view stmt window func exprs failed", K(ret));
  } else if (OB_FAIL(append(view_stmt->get_qualify_filters(),
                            select_stmt->get_qualify_filters()))) {
    LOG_WARN("append select_stmt window func filters to view stmt window func filters failed", K(ret));
  } else {
    if (!view_stmt->is_from_pivot()) {
      view_stmt->set_from_pivot(select_stmt->is_from_pivot());
    }
    if (need_distinct && !view_stmt->is_set_distinct()) {//说明内层为set-op,且为union all
      view_stmt->assign_set_distinct();
    } else if (select_stmt->has_distinct() && !view_stmt->has_distinct()) {
      view_stmt->assign_distinct();
    }
    if (select_stmt->has_select_into()) {
      view_stmt->set_select_into(select_stmt->get_select_into());
    }
    if (select_stmt->has_order_by()) {
      view_stmt->get_order_items().reset();
      if (OB_FAIL(append(view_stmt->get_order_items(), select_stmt->get_order_items()))) {
         LOG_WARN("append select_stmt order items to view stmt order items failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (select_stmt->has_limit() && OB_FAIL(do_limit_merge(*select_stmt, *view_stmt))) {
      LOG_WARN("failed to merge limit", K(ret));
    } else if (select_stmt->has_sequence() && //处理sequence
               OB_FAIL(append(view_stmt->get_nextval_sequence_ids(),
                              select_stmt->get_nextval_sequence_ids()))) {
      LOG_WARN("failed to append nextval sequence ids", K(ret));
    } else if (select_stmt->has_sequence()
               && OB_FAIL(append(view_stmt->get_currval_sequence_ids(),
                                 select_stmt->get_currval_sequence_ids()))) {
      LOG_WARN("failed to append currval sequence ids", K(ret));
    } else if (view_stmt->is_set_stmt()) {
      if (select_offset.empty()){
        /*do nothing*/
      } else if (OB_FAIL(recursive_adjust_select_item(view_stmt,
                                                      select_offset,
                                                      const_select_items))) {
        LOG_WARN("recursive adjust select item location failed", K(ret));
      } else {
        /*do nothing*/
      }
    } else {
      for (int64_t i = 0 ; i < view_stmt->get_select_item_size(); i ++) {
        ObRawExpr *expr = NULL;
        if (OB_ISNULL(expr = view_stmt->get_select_item(i).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else {
          expr->set_alias_column_name(ObString::make_empty_string());
        }
      }
      if (OB_SUCC(ret)) {
        view_stmt->get_select_items().reset();
        if (OB_FAIL(append(view_stmt->get_select_items(), select_stmt->get_select_items()))) {
          LOG_WARN("view stmt replace select items failed", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append(view_stmt->get_subquery_exprs(),
                       select_stmt->get_subquery_exprs()))) {
      LOG_WARN("view stmt append subquery failed", K(ret));
    } else {
      //bug20488629, 备库普通租户SHOW database语句执行超时，始终要求选择Leader副本
      //view pull后需要继承原show db信息
      bool is_from_show_stmt = select_stmt->is_from_show_stmt();
      stmt::StmtType literal_stmt_type = select_stmt->get_query_ctx()->get_literal_stmt_type();
      view_stmt->get_query_ctx()->set_literal_stmt_type(literal_stmt_type);
      view_stmt->set_is_from_show_stmt(is_from_show_stmt);
    }
  }
  return ret;
}

int ObTransformQueryPushDown::replace_stmt_exprs(ObDMLStmt *parent_stmt,
                                                 ObSelectStmt *child_stmt,
                                                 uint64_t table_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 16> old_column_exprs;
  ObSEArray<ObRawExpr*, 16> new_column_exprs;
  ObSEArray<ObColumnRefRawExpr *, 16> temp_exprs;
  if (OB_ISNULL(parent_stmt) || OB_ISNULL(child_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(parent_stmt->get_column_exprs(table_id, temp_exprs))) {
    LOG_WARN("failed to get column exprs", K(ret));
  } else if (OB_FAIL(append(old_column_exprs, temp_exprs))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(ObTransformUtils::convert_column_expr_to_select_expr(
                                       old_column_exprs,
                                       *child_stmt,
                                       new_column_exprs))) {
    LOG_WARN("failed to convert column expr to select expr", K(ret));
  } else if (OB_FAIL(parent_stmt->replace_relation_exprs(old_column_exprs,
                                                         new_column_exprs))) {
    LOG_WARN("failed to replace relation exprs", K(ret));
  }
  return ret;
}

int ObTransformQueryPushDown::recursive_adjust_select_item(ObSelectStmt *select_stmt,
                                                           ObIArray<int64_t> &select_offset,
                                                           ObIArray<SelectItem> &const_select_items)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(ctx_), K(ctx_->expr_factory_), K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("check stack overflow failed", K(is_stack_overflow), K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(is_stack_overflow), K(ret));
  } else if (select_stmt->is_set_stmt()) {
    ObIArray<ObSelectStmt*> &child_stmts = static_cast<ObSelectStmt*>(select_stmt)->get_set_query();
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      ret = SMART_CALL(recursive_adjust_select_item(child_stmts.at(i), select_offset, const_select_items));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(reset_set_stmt_select_list(select_stmt, select_offset))) {
        LOG_WARN("failed to reset set stmt select list", K(ret));
      }
    }
  } else {
    ObSEArray<SelectItem, 1> new_select_item;
    ObSEArray<SelectItem, 1> old_select_item;
    ObSEArray<SelectItem, 1> new_const_select_items;
    ObRawExprCopier copier(*ctx_->expr_factory_);
    //copy一份select item进行处理
    if (OB_FAIL(old_select_item.assign(select_stmt->get_select_items()))) {
      LOG_WARN("failed to assign a new select item", K(ret));
    } else if (OB_FAIL(deep_copy_stmt_objects(copier,
                                              const_select_items,
                                              new_const_select_items))) {
      LOG_WARN("deep copy select items failed", K(ret));
    } else {
      int64_t k = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < select_offset.count(); ++i) {
        if (select_offset.at(i) == -1) {//-1 meanings upper stmt has const select item
          if (OB_UNLIKELY(k >= new_const_select_items.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret), K(k), K(new_const_select_items.count()));
          } else if (OB_FAIL(new_select_item.push_back(new_const_select_items.at(k)))) {
            LOG_WARN("push back select item error", K(ret));
          } else {
            ++ k;
          }
        } else if (OB_UNLIKELY(select_offset.at(i) < 0 ||
                               select_offset.at(i) >= old_select_item.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(select_offset.at(i)),
                                           K(old_select_item.count()), K(ret));
        } else if (OB_FAIL(new_select_item.push_back(old_select_item.at(select_offset.at(i))))) {
          LOG_WARN("push back select item error", K(ret));
        } else {/*do nothing*/}
      }
      if (OB_SUCC(ret)) {
        select_stmt->get_select_items().reset();
        if (OB_FAIL(append(select_stmt->get_select_items(), new_select_item))) {
          LOG_WARN("view stmt replace select items failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::reset_set_stmt_select_list(ObSelectStmt *select_stmt,
                                                         ObIArray<int64_t> &select_offset)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 4> old_select_exprs;
  ObSEArray<ObRawExpr*, 4> adjust_old_select_exprs;
  ObSEArray<ObRawExpr*, 4> new_select_exprs;
  ObSEArray<ObRawExpr*, 4> adjust_new_select_exprs;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stmt is NULL", K(select_stmt), K(ctx_), K(ret));
  } else if (!select_stmt->is_set_stmt()) {
    /*do nothing*/
  } else if (OB_FAIL(select_stmt->get_select_exprs(old_select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else {
    select_stmt->get_select_items().reset();
    if (OB_FAIL(ObOptimizerUtil::gen_set_target_list(ctx_->allocator_,
                                                     ctx_->session_info_,
                                                     ctx_->expr_factory_,
                                                     select_stmt))) {
      LOG_WARN("failed to create select list for union", K(ret));
    } else if (OB_FAIL(select_stmt->get_select_exprs(new_select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_offset.count(); ++i) {
        if (select_offset.at(i) == -1) {//-1 meanings upper stmt has const select item
          /*do nothing */
        } else if (OB_UNLIKELY(select_offset.at(i) < 0 ||
                               select_offset.at(i) >= old_select_exprs.count() ||
                               i >= new_select_exprs.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected error", K(select_offset.at(i)), K(old_select_exprs.count()),
                                           K(new_select_exprs.count()), K(i), K(ret));
        } else if (OB_FAIL(adjust_old_select_exprs.push_back(old_select_exprs.at(select_offset.at(i)))) ||
                   OB_FAIL(adjust_new_select_exprs.push_back(new_select_exprs.at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else {/*do nothing*/}
      }
      if (OB_SUCC(ret) && adjust_old_select_exprs.count() > 0) {
        if (OB_FAIL(select_stmt->replace_relation_exprs(adjust_old_select_exprs,
                                                        adjust_new_select_exprs))) {
          LOG_WARN("failed to replace relation exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::check_set_op_is_const_expr(ObSelectStmt *select_stmt,
                                                         ObRawExpr *expr,
                                                         bool &is_const)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || OB_ISNULL(select_stmt) ||
      OB_UNLIKELY(!expr->is_set_op_expr() || !select_stmt->is_set_stmt() ||
                  select_stmt->get_set_query().empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(expr), K(select_stmt));
  } else if (!is_const) {
    /*do nothing*/
  } else if (select_stmt->is_set_distinct()) {
    is_const = false;
  } else {
    int64_t idx = static_cast<ObSetOpRawExpr*>(expr)->get_idx();
    for (int64_t i = 0; OB_SUCC(ret) && is_const && i < select_stmt->get_set_query().count(); ++i) {
      ObRawExpr *select_expr = NULL;
      if (OB_ISNULL(select_stmt->get_set_query(i)) ||
          OB_UNLIKELY(idx < 0 || idx >= select_stmt->get_set_query(i)->get_select_item_size()) ||
          OB_ISNULL(select_expr = select_stmt->get_set_query(i)->get_select_item(idx).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(i), K(select_stmt->get_set_query(i)),
                                        K(idx), K(select_expr), K(ret));
      } else if (select_expr->is_set_op_expr()) {
        if (OB_FAIL(SMART_CALL(check_set_op_is_const_expr(select_stmt->get_set_query(i),
                                                          select_expr,
                                                          is_const)))) {
          LOG_WARN("failed to check set op is const expr", K(ret));
        }
      } else {
        is_const &= select_expr->is_const_expr();
      }
    }
  }
  return ret;
}

int ObTransformQueryPushDown::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  UNUSED(trans_params);
  const ObQueryCtx *query_ctx = NULL;
  ObViewMergeHint *hint = NULL;
  ObString qb_name;;
  const ObViewMergeHint *myhint = static_cast<const ObViewMergeHint*>(get_hint(stmt.get_stmt_hint()));
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, get_hint_type(), hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(stmt.get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else if (OB_FAIL(ctx_->add_src_hash_val(qb_name))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (NULL != myhint && myhint->enable_query_push_down(ctx_->src_qb_name_) &&
             OB_FAIL(ctx_->add_used_trans_hint(myhint))) {
    LOG_WARN("failed to add used trans hint", K(ret));
  } else if (OB_FAIL(stmt.adjust_qb_name(ctx_->allocator_,
                                         ctx_->src_qb_name_,
                                         ctx_->src_hash_val_))) {
    LOG_WARN("failed to adjust statement id", K(ret));
  } else {
    hint->set_parent_qb_name(ctx_->src_qb_name_);
    hint->set_is_used_query_push_down(true);
    hint->set_qb_name(qb_name);
  }
  return ret;
}

}
}
