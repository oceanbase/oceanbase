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
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
#include "sql/rewrite/ob_transform_join_limit_pushdown.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObTransformJoinLimitPushDown::LimitPushDownHelper::assign(const LimitPushDownHelper &other)
{
  int ret = OB_SUCCESS;
  view_table_ = other.view_table_;
  pushdown_conds_.reset();
  pushdown_order_items_.reset();
  pushdown_semi_infos_.reset();
  expr_relation_ids_.reset();
  if (OB_FAIL(pushdown_conds_.assign(other.pushdown_conds_))) {
    LOG_WARN("failed to assign extracted conditions", K(ret));
  } else if (OB_FAIL(pushdown_order_items_.assign(other.pushdown_order_items_))) {
    LOG_WARN("failed to assign order items", K(ret));
  } else if (OB_FAIL(pushdown_semi_infos_.assign(other.pushdown_semi_infos_))) {
    LOG_WARN("failed to assign semi infos", K(ret));
  } else if (OB_FAIL(pushdown_tables_.assign(other.pushdown_tables_))) {
    LOG_WARN("failed to assign pushdown tables", K(ret));
  } else if (OB_FAIL(lazy_join_tables_.assign(other.lazy_join_tables_))) {
    LOG_WARN("failed to assign lazy join tables", K(ret));
  } else if (OB_FAIL(expr_relation_ids_.add_members(other.expr_relation_ids_))) {
    LOG_WARN("failed to assign rel ids", K(ret));
  } else {
    view_table_ = other.view_table_;
    all_lazy_join_is_unique_join_ = other.all_lazy_join_is_unique_join_;
  }
  return ret;
}

bool ObTransformJoinLimitPushDown::LimitPushDownHelper::is_table_lazy_join(TableItem* table)
{
  bool find = false;
  for (int64_t i = 0; !find && i < lazy_join_tables_.count(); ++i) {
    if (table == lazy_join_tables_.at(i).right_table_) {
      find = true;
    }
  }
  return find;
}

 int ObTransformJoinLimitPushDown::LimitPushDownHelper::alloc_helper(ObIAllocator &allocator, LimitPushDownHelper* &helper)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (NULL == (buf = allocator.alloc(sizeof(LimitPushDownHelper)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret));
  } else {
    helper = new(buf)LimitPushDownHelper();
  }
  return ret;
}

uint64_t ObTransformJoinLimitPushDown::LimitPushDownHelper::get_max_table_id() const
{
  uint64_t max_table_id = OB_INVALID_ID;
  const TableItem *table = NULL;
  for (int64_t i = 0; i < pushdown_tables_.count(); ++i) {
    if (OB_NOT_NULL(table = pushdown_tables_.at(i))
        && (max_table_id < table->table_id_ || OB_INVALID_ID == max_table_id)) {
      max_table_id = table->table_id_;
    }
  }
  return max_table_id;
}

/**
 * @brief ObTransformJoinLimitPushDown::transform_one_stmt
 * Pushdown limit/orderby/where conditions/semi infos/distinct into left/right join and cartesian join
 * 
 * Scenarios:
 * 1. left outer join + limit: push down limit into left table
 * 2. left outer join + order by left table column + limit: push down order by limit into left table
 * 3. cartesian nlj join + limit: push down limit into all the caretesian tables,
 *    orderby/filter infos/distinct are also pushed down if necessary
 * NOTE: The rule only supports LEFT outer join by converting right to left at the very beginning. 
 * The mixed join type, such as left-inner-left, is NOT supported. Besides, rownum also is expected to 
 * be transformed to limit before entering into this rule, and those remained will NOT be supported.
 * 
 * Order suggestion with other transformation:
 *  [after]  simplify : unnecessary order by removal
 *  [after]  set_op : (order by) limit pushing down set op
 *  [before] or_expansion : new set op generated
 * 
 */
int ObTransformJoinLimitPushDown::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                                     ObDMLStmt *&stmt,
                                                     bool &trans_happened)
{
  int ret = OB_SUCCESS;
  UNUSED(parent_stmts);
  bool is_valid = false;
  ObSEArray<LimitPushDownHelper*, 4> helpers;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret));
  } else if (!stmt->is_select_stmt()) {
    // do nothing
  } else if (OB_FAIL(check_stmt_validity(stmt, helpers, is_valid))) {
    LOG_WARN("failed to check stmt validity", K(ret));
  } else if (!is_valid) {
    // do nothing
  } else if (OB_FAIL(sort_pushdown_helpers(helpers))) {
    LOG_WARN("failed to sort pushdown helpers", K(ret));
  } else if (is_valid) {
    LOG_TRACE("start to pushdown limit into join", K(helpers));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < helpers.count(); ++i) {
    if (OB_ISNULL(helpers.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null helper", K(ret));
    } else if (OB_FAIL(do_transform(static_cast<ObSelectStmt *>(stmt), *helpers.at(i)))) {
      LOG_WARN("failed to push limit before join", K(ret));
    } else {
      trans_happened = true;
    }
  }
  if (OB_SUCC(ret) && trans_happened) {
    if (OB_FAIL(stmt->rebuild_tables_hash())) {
      LOG_WARN("failed to rebuild table hash", K(ret));
    } else if (OB_FAIL(stmt->update_column_item_rel_id())) {
      LOG_WARN("failed to update column rel ids", K(ret));
    } else if (OB_FAIL(add_transform_hint(*stmt))) {
      LOG_WARN("failed to add transform hint", K(ret));
    }
  }
  //destruct helpers
  for (int64_t i = 0; i < helpers.count(); ++i) {
    if (NULL != helpers.at(i)) {
      helpers.at(i)->~LimitPushDownHelper();
      helpers.at(i) = NULL;
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::sort_pushdown_helpers(ObSEArray<LimitPushDownHelper*, 4> &helpers)
{
  int ret = OB_SUCCESS;
  auto cmp_func = [](LimitPushDownHelper* l_helper, LimitPushDownHelper* r_helper) {
    if (OB_ISNULL(l_helper) || OB_ISNULL(r_helper)) {
      return false;
    } else {
      return l_helper->get_max_table_id() > r_helper->get_max_table_id();
    }
  };
  std::sort(helpers.begin(), helpers.end(), cmp_func);
  return ret;
}

int ObTransformJoinLimitPushDown::check_stmt_validity(ObDMLStmt *stmt,
                                                      ObIArray<LimitPushDownHelper*> &helpers,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  bool has_cartesian = false;
  bool has_rownum = false;
  bool is_valid_limit = false;
  ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", K(ret));
  } else if (!stmt->is_select_stmt()) {
    is_valid = false;
  } else if (FALSE_IT(select_stmt = static_cast<ObSelectStmt *>(stmt))) {
  } else if (select_stmt->is_set_stmt()) {
    // self will be ignored and children will be handled recursively
    is_valid = false;
  } else if (!select_stmt->has_limit() ||
             select_stmt->is_hierarchical_query() ||
             select_stmt->is_calc_found_rows() ||
             select_stmt->has_group_by() ||
             select_stmt->has_having() ||
             select_stmt->has_rollup() ||
             select_stmt->has_window_function() ||
             select_stmt->has_sequence() ||
             select_stmt->has_distinct()) {
    is_valid = false;
  } else if (OB_FAIL(select_stmt->has_rownum(has_rownum))) {
    LOG_WARN("failed to check stmt has rownum", K(ret));
  } else if (has_rownum) {
    // rownum should be replaced by preprocess stage.
    // NOT support those left.
    is_valid = false;
  } else if (OB_FAIL(check_limit(select_stmt, is_valid_limit))) {
    LOG_WARN("failed to check the validity of limit expr", K(ret));
  } else if (!is_valid_limit) {
    is_valid = false;
  } else if (OB_FAIL(split_cartesian_tables(select_stmt, 
                                            helpers, 
                                            is_valid,
                                            has_cartesian))) {
    LOG_WARN("failed to check cartesian product", K(ret));
  } else if (!is_valid) {
    //do nothing
  } else {
    for (int i = 0; OB_SUCC(ret) && i < helpers.count(); ++i) {
      LimitPushDownHelper *helper = helpers.at(i);
      bool is_all_unique_join = false;
      if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null helper", K(ret));
      } else if (OB_FAIL(ObTransformUtils::get_lazy_left_join(stmt, 
                                                              helper->pushdown_tables_,
                                                              helper->expr_relation_ids_,
                                                              helper->lazy_join_tables_))) {
        LOG_WARN("failed to get lazy left join", K(ret));
      } else if (OB_FAIL(check_lazy_join_is_unique(helper->lazy_join_tables_, 
                                                   stmt,
                                                   is_all_unique_join))) {
        LOG_WARN("failed to check lazy join is unique", K(ret));
      } else {
        helper->all_lazy_join_is_unique_join_ &= is_all_unique_join;
      }
    }
    //check for no cartesian
    if (OB_SUCC(ret) && !has_cartesian && 1 == helpers.count()) {
      LimitPushDownHelper *helper = helpers.at(0);
      if (OB_ISNULL(helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null helper", K(ret));
      } else if (helper->lazy_join_tables_.empty()) {
        //no valid lazy left join
        is_valid = false;
      }
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::check_lazy_join_is_unique(ObIArray<ObTransformUtils::LazyJoinInfo> &lazy_join, 
                                                            ObDMLStmt *stmt,
                                                            bool &is_unique_join)
{
  int ret = OB_SUCCESS;
  is_unique_join = true;
  bool is_simply_join = false;
  ObSqlBitSet<> right_table_ids;
  ObSEArray<ObRawExpr*, 4> join_keys;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_unique_join && i < lazy_join.count(); ++i) {
    right_table_ids.reuse();
    join_keys.reuse();
    TableItem *right_table = lazy_join.at(i).right_table_;
    if (OB_ISNULL(right_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (OB_FAIL(stmt->get_table_rel_ids(*right_table, right_table_ids))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    } else if (OB_FAIL(ObTransformUtils::get_join_keys(lazy_join.at(i).join_conditions_, 
                                                      right_table_ids, 
                                                      join_keys, 
                                                      is_simply_join))) {
      LOG_WARN("failed to get right table join keys", K(ret));
    } else if (!is_simply_join) {
      is_unique_join = false;
    } else if (OB_FAIL(ObTransformUtils::check_exprs_unique(*stmt, 
                                                            lazy_join.at(i).right_table_, 
                                                            join_keys,
                                                            ctx_->session_info_,
                                                            ctx_->schema_checker_, 
                                                            is_unique_join))) {
      LOG_WARN("failed to check exprs unique", K(ret));
    }
  }
  return ret;
}

/**
 * @brief 
 * To check if the sql have cartesian tables
 * Brief introduction to the algorithm:
 * 
 * Consider a sql contains n tables in inner join, T = {t1, t2, ..., tn}, where n >= 1, 
 *  m WHERE conditions, C = {c1, c2, ..., cm}, where m >= 0.
 *  p order by items. O = {o1, o2, ..., op}, where p >= 0
 * The type of each table in T can be basic table, generated table or joined table.
 * 
 * For each condition ci 、in C, where i = {1, ..., m}, ci joined k tables within T,
 *  where k = {0, 1, ..., n}.
 * Finally, condition set C split T into r clusters ST, 
 *  where ST = {S1, S2, ..., Sr}, r = {1, ..., n}, respectively. 
 * Each element in ST is a non-empty subset of T.
 * 
 * If r = 1, then it means that all the tables are connected by C, 
 *  and there is no cartesian joins at all. In this case we won't push down limit.
 * 
 * If r > 1, then we can say that Si \in ST is cartesian joined with Sj \in ST, 
 *  where i = {1, ..., n}, j = {1, ..., n}, i ≠ j. 
 * We will try to push down limit into all the elements in ST.
 * @param select_stmt 
 * @param helpers 
 * @param is_valid 
 * @return int 
 */
int ObTransformJoinLimitPushDown::split_cartesian_tables(ObSelectStmt *select_stmt,
                                                         ObIArray<LimitPushDownHelper*> &helpers,
                                                         bool &is_valid,
                                                         bool &has_cartesian)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  has_cartesian = false;
  bool is_contain = false;
  // 1. check is function table valid
  // 2. check is cartesian valid
  // 3. collect cartesian infos
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(check_contain_correlated_function_table(select_stmt, is_contain))) {
    LOG_WARN("failed to check contain correlated function table", K(ret));
  } else if (OB_FAIL(check_contain_correlated_json_table(select_stmt, is_contain))) {
    LOG_WARN("failed to check contain correlated function table", K(ret));
  } else if (is_contain) {
    //do nothing
  } else {
    int64_t N = select_stmt->get_from_item_size();
    UnionFind uf(N);
    if (OB_FAIL(uf.init())) {
      LOG_WARN("failed to initialize union find", K(ret));
    } else if (OB_FAIL(check_cartesian(select_stmt, uf, is_valid))) {
      LOG_WARN("failed to check cartesian", K(ret));
    } else if (!is_valid) {
    } else if (OB_FAIL(collect_cartesian_infos(select_stmt,
                                               uf,
                                               helpers))) {
      LOG_WARN("failed to generate cartesian infos", K(ret));
    } else {
      has_cartesian = uf.count_ > 1;
      is_valid = !helpers.empty();
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::check_contain_correlated_function_table(ObDMLStmt *stmt, bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  for (int i = 0; OB_SUCC(ret) && !is_contain && i < stmt->get_table_items().count(); ++i) {
    TableItem *table = stmt->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_function_table()) {
      //do nothing
    } else if (OB_ISNULL(table->function_table_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!table->function_table_expr_->get_relation_ids().is_empty()) {
      is_contain = true;
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::check_contain_correlated_json_table(ObDMLStmt *stmt, bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  for (int i = 0; OB_SUCC(ret) && !is_contain && i < stmt->get_table_items().count(); ++i) {
    TableItem *table = stmt->get_table_item(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else if (!table->is_json_table()) {
      //do nothing
    } else if (OB_ISNULL(table->json_table_def_) || OB_ISNULL(table->json_table_def_->doc_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!table->json_table_def_->doc_expr_->get_relation_ids().is_empty()) {
      is_contain = true;
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::check_cartesian(ObSelectStmt *stmt,
                                                  UnionFind &uf,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  ObSEArray<TableItem *, 8> from_tables;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    // 1. get FROM ITEMS tables
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_from_item_size(); ++i) {
      TableItem *cur_table = stmt->get_table_item(stmt->get_from_item(i));
      if (OB_ISNULL(cur_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(from_tables.push_back(cur_table))) {
        LOG_WARN("failed to push back table id", K(ret));
      }
    }
    bool is_cond_valid = true;
    // 2. collect connect infos according to the conditions
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
      ObRawExpr *cond = stmt->get_condition_expr(i);
      ObSEArray<uint64_t, 4> where_table_ids;
      if (OB_ISNULL(cond)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (cond->has_flag(CNT_SUB_QUERY) ||
                 cond->has_flag(CNT_RAND_FUNC) ||
                 cond->has_flag(CNT_STATE_FUNC)) {
        is_cond_valid = false;
        OPT_TRACE("condition has subquery/rand func/state func");
      } else if (OB_FAIL(ObRawExprUtils::extract_table_ids(cond,
                                                          where_table_ids))) {
        LOG_WARN("failed to extract table ids", K(ret));
      } else if (OB_FAIL(connect_tables(stmt, where_table_ids, from_tables, uf))) {
        LOG_WARN("failed to connect tables", K(ret));
      }
    }
    // 3. collect connect infos according to the semi infos
    for (int64_t i = 0; OB_SUCC(ret) && is_cond_valid &&
                        i < stmt->get_semi_infos().count(); ++i) {
      SemiInfo *semi = stmt->get_semi_infos().at(i);
      if (OB_ISNULL(semi)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(connect_tables(stmt, semi->left_table_ids_, from_tables, uf))) {
        LOG_WARN("failed to connect tables", K(ret));
      }
    }
    // 4. collect connect infos according to the order by items
    bool is_orderby_valid = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_cond_valid && is_orderby_valid &&
                        i < stmt->get_order_item_size(); ++i) {
      ObRawExpr *expr = stmt->get_order_items().at(i).expr_;
      ObSEArray<uint64_t, 8> orderby_table_ids;
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid orderby expr", K(ret));
      } else if (expr->has_flag(CNT_RAND_FUNC) ||
                 expr->has_flag(CNT_STATE_FUNC) ||
                 expr->has_flag(CNT_SUB_QUERY)) {
        // avoid pushing down non-deterministic func and subquery
        is_orderby_valid = false;
        OPT_TRACE("order by has subquery or special expr");
      } else if (!expr->has_flag(CNT_COLUMN)) {
        // do nothing
      } else if (OB_FAIL(ObRawExprUtils::extract_table_ids(expr, orderby_table_ids))) {
        LOG_WARN("failed to collect orderby table sets", K(ret));
      }  else if (OB_FAIL(connect_tables(stmt, orderby_table_ids, from_tables, uf))) {
        LOG_WARN("failed to connect tables", K(ret));
      }
    }
    is_valid = OB_SUCC(ret) && is_cond_valid && is_orderby_valid;
  }
  return ret;
}

int ObTransformJoinLimitPushDown::connect_tables(ObSelectStmt *stmt,
                                                 const ObIArray<uint64_t> &table_ids,
                                                 const ObIArray<TableItem *> &from_tables,
                                                 UnionFind &uf)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> indices;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(get_idx_from_table_ids(stmt,
                                            table_ids,
                                            from_tables,
                                            indices))) {
    LOG_WARN("failed to get indices of from table ids", K(ret));
  } else if (indices.count() <= 1) {
    // do nothing
  } else {
    // connect tables appeared in a condition
    // we store table indices instead of table item to leverage the consumption
    for (int64_t i = 1; OB_SUCC(ret) && i < indices.count(); ++i) {
      if (OB_FAIL(uf.connect(indices.at(0), indices.at(i)))) {
        LOG_WARN("failed to connect nodes", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::get_idx_from_table_ids(ObSelectStmt *stmt,
                                                         const ObIArray<uint64_t> &src_table_ids,
                                                         const ObIArray<TableItem *> &target_tables,
                                                         ObIArray<int64_t> &indices)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < target_tables.count(); ++i) {
    TableItem *target_table = target_tables.at(i);
    if (OB_ISNULL(target_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < src_table_ids.count(); ++j) {
      if (src_table_ids.at(j) == target_table->table_id_) {
        if (OB_FAIL(indices.push_back(i))) {
          LOG_WARN("failed to push back index", K(ret));
        }
        break;
      } else if (target_table->is_joined_table()) {
        if (is_contain(static_cast<JoinedTable *>(target_table)->single_table_ids_,
                       src_table_ids.at(j))) {
          if (!is_contain(indices, i) && OB_FAIL(indices.push_back(i))) {
            LOG_WARN("failed to push back index", K(ret));
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::collect_cartesian_infos(ObSelectStmt *stmt,
                                                          UnionFind &uf,
                                                          ObIArray<LimitPushDownHelper*> &helpers)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 8> visited;
  LimitPushDownHelper *helper = NULL;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    int64_t N = stmt->get_from_item_size();
    ObSEArray<TableItem *, 8> connected_tables;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      bool is_visited = is_contain(visited, i);
      connected_tables.reuse();
      TableItem *table_item1 = stmt->get_table_item(stmt->get_from_item(i));
      if (is_visited) {
        // do nothing
      } else if (OB_ISNULL(table_item1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(connected_tables.push_back(table_item1))) {
        LOG_WARN("failed to push back table item", K(ret));
      } else {
        for (int64_t j = i + 1; OB_SUCC(ret) && j < N; ++j) {
          bool connected = false;
          TableItem *table_item2 = stmt->get_table_item(stmt->get_from_item(j));
          if (OB_ISNULL(table_item2)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpeceted null table item", K(ret));
          } else if (OB_FAIL(uf.is_connected(i, j, connected))) {
            LOG_WARN("failed to check is connected", K(ret), K(i), K(j));
          } else if (!connected) {
            // do nothing
          } else if (OB_FAIL(connected_tables.push_back(table_item2))) {
            LOG_WARN("failed to push back table item", K(ret));
          } else if (OB_FAIL(visited.push_back(j))) {
            LOG_WARN("failed to push back visited info", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          bool is_valid = false;
          if (OB_FAIL(check_table_validity(connected_tables, is_valid))) {
            LOG_WARN("failed to check table is valid", K(ret));
          } else if (!is_valid) {
            //do nothing
          } else if (OB_FAIL(LimitPushDownHelper::alloc_helper(*ctx_->allocator_, helper))) {
            LOG_WARN("failed to allocate helper", K(ret));
          } else if (OB_ISNULL(helper)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect null helper", K(ret));
          } else if (OB_FAIL(helper->pushdown_tables_.assign(connected_tables))) {
            LOG_WARN("failed to assign pushdown tables", K(ret));
          } else if (OB_FAIL(collect_cartesian_exprs(stmt, helper))) {
            LOG_WARN("failed to collect cartesian exprs", K(ret));
          } else if (OB_FAIL(helpers.push_back(helper))) {
            LOG_WARN("failed to push back helper", K(ret));
          } else {
            helper->all_lazy_join_is_unique_join_ = uf.count_ == 1;
          }
        }
      }
    }
  }
  return ret;
}


/**
 * @brief 
 * only when all the target tables are generated tables with 
 *  limit/distinct/orderby/scalar group by,
 * the limit pushdown is not allowed to be done. eg:
 * Q1: select * from (select * from t1 limit 10) V1, 
 *                   t2
 *                   where V1.c1 > 0 limit 10;
 * Q2: select * from (select * from t1 limit 10) V1, 
 *                   (select * from t2 limit 10) V2
 *                   where V1.c1 > 0 limit 10;
 * Q2 is invalid since both [V1, V2] already contain limit
 * @param target_tables 
 * @param is_valid 
 * @return int 
 */
int ObTransformJoinLimitPushDown::check_table_validity(const ObIArray<TableItem *> &target_tables,
                                                       bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (target_tables.count() > 1) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < target_tables.count(); ++i) {
      TableItem *target_table = target_tables.at(i);
      if (OB_ISNULL(target_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret));
      } else if (target_table->is_basic_table() ||
                target_table->is_joined_table()) {
        is_valid = true;
      } else if (target_table->is_generated_table()) {
        bool has_rownum = false;
        ObSelectStmt *ref_query = NULL;
        if (OB_ISNULL(ref_query = target_table->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid target table ref query", K(ret));
        } else if (ref_query->has_limit() ||
                  ref_query->is_calc_found_rows() ||
                  ref_query->has_order_by() ||
                  ref_query->is_scala_group_by() ||
                  ref_query->has_distinct()) {
          // ignore push down when ref_query has 
          // distinct/orderby/limit/rownum/scalar group by          
        } else if (OB_FAIL(ref_query->has_rownum(has_rownum))) {
          LOG_WARN("failed to check stmt has rownum", K(ret));
        } else if (has_rownum) {
        } else {
          is_valid = true;
        }
      }
    }
  } else {
    //check whether been already rewritten
    TableItem *table = target_tables.at(0);
    while (NULL != table && table->is_joined_table()) {
      JoinedTable *joined_table = static_cast<JoinedTable*>(table);
      if (LEFT_OUTER_JOIN == joined_table->joined_type_) {
        table = joined_table->left_table_;
      } else {
        break;
      }
    }
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table", K(ret));
    }  else if (table->is_generated_table()) {
      ObSelectStmt *ref_query = NULL;
      if (OB_ISNULL(ref_query = table->ref_query_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid target table ref query", K(ret));
      } else if (ref_query->has_limit() ||
                 ref_query->is_scala_group_by() ||
                 ref_query->is_calc_found_rows()) {
        //do nothing       
      } else {
        is_valid = true;
      }
    } else {
      is_valid = true;
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::collect_cartesian_exprs(ObSelectStmt *stmt,
                                                         LimitPushDownHelper *helper)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_rel_ids;
  if (OB_ISNULL(stmt) || OB_ISNULL(helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(helper->pushdown_tables_,
                                             table_rel_ids))) {
    LOG_WARN("failed to get table rel ids", K(ret));
  }
  // 1. extract condition infos
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_condition_size(); ++i) {
    ObRawExpr *cond = stmt->get_condition_expr(i);
    if (OB_ISNULL(cond)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (!cond->get_relation_ids().overlap(table_rel_ids)) {
      // do nothing
    } else if (OB_FAIL(helper->pushdown_conds_.push_back(cond))) {
      LOG_WARN("failed to push back conditions", K(ret));
    } else if (OB_FAIL(helper->expr_relation_ids_.add_members(cond->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  // 2. extract semi infos
  ObSqlBitSet<> left_table_ids;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_semi_info_size(); ++i) {
    SemiInfo *semi_info = stmt->get_semi_infos().at(i);
    left_table_ids.reuse();
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::get_left_rel_ids_from_semi_info(stmt, 
                                                                         semi_info, 
                                                                         left_table_ids))) {
      LOG_WARN("failed to get left table rel ids", K(ret));
    } else if (!left_table_ids.overlap(table_rel_ids)) {
      //do nothing
    } else if (OB_FAIL(helper->pushdown_semi_infos_.push_back(semi_info))) {
      LOG_WARN("failed to push back semi info", K(ret));
    } else if (OB_FAIL(helper->expr_relation_ids_.add_members(left_table_ids))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  // 3. extract order item
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_order_item_size(); ++i) {
    OrderItem item = stmt->get_order_item(i);
    if (OB_ISNULL(item.expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is invalid", K(ret));
    } else if (!item.expr_->get_relation_ids().overlap(table_rel_ids)) {
      // do nothing
    } else if (OB_FAIL(helper->pushdown_order_items_.push_back(item))) {
      LOG_WARN("failed to push back order item", K(ret));
    } else if (OB_FAIL(helper->expr_relation_ids_.add_members(item.expr_->get_relation_ids()))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::check_limit(ObSelectStmt *select_stmt, 
                                              bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt found", K(ret));
  } else if (!select_stmt->has_limit()) {
    // do nothing
    OPT_TRACE("stmt do not have limit");
  } else if (OB_NOT_NULL(select_stmt->get_limit_percent_expr())) {
    is_valid = false;
    OPT_TRACE("can not pushdown limit percent expr");
  } else if (select_stmt->is_fetch_with_ties() ||
             OB_ISNULL(select_stmt->get_limit_expr())) {
    is_valid = false;
    OPT_TRACE("can not pushdown fetch with ties");
  } else {
    ObRawExpr *offset_expr = select_stmt->get_offset_expr();
    ObRawExpr *limit_expr = select_stmt->get_limit_expr();
    bool is_offset_valid;
    bool is_limit_valid;
    if (OB_FAIL(check_offset_limit_expr(limit_expr, is_limit_valid))) {
      LOG_WARN("failed to check limit expr", K(ret));
    } else if (!is_limit_valid) {
      is_valid = false;
      OPT_TRACE("limit value is invalid");
    } else if (OB_NOT_NULL(offset_expr) &&
               OB_FAIL(check_offset_limit_expr(offset_expr, is_offset_valid))) {
      LOG_WARN("failed to check offset expr", K(ret));
    } else if (OB_NOT_NULL(offset_expr) && !is_offset_valid) {
      is_valid = false;
      OPT_TRACE("offset value is invalid");
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::check_offset_limit_expr(ObRawExpr *offset_limit_expr,
                                                          bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (OB_ISNULL(offset_limit_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("illegal limit expr", K(ret));
  } else if (T_NULL == offset_limit_expr->get_expr_type() ||
             T_QUESTIONMARK == offset_limit_expr->get_expr_type()) {
    // do nothing
  } else if (offset_limit_expr->is_const_raw_expr()) {
    const ObObj &value = static_cast<const ObConstRawExpr*>(offset_limit_expr)->get_value();
    if (value.is_invalid_type() || !value.is_integer_type()) {
      is_valid = false;
    } else if (value.get_int() < 0) {
      is_valid = false;
    }
  } else {
    // ignore cast format introduced by rownum
  }
  return ret;
}

/**
 * @brief do limit pushdown
 * step:
 * - merge cartesian tables for cartesian join
 * - collect infos that need to push down:
 *      where conditions, semi infos, order by items
 * - remove filter infos from stmt and copy them:
 *      where conditions and semi infos
 * - copy orderby items
 * - create a view if needed
 * - pushdown infos into the view
 * @param select_stmt 
 * @param helper 
 * @param trans_happened 
 * @return int 
 */
int ObTransformJoinLimitPushDown::do_transform(ObSelectStmt *select_stmt,
                                               LimitPushDownHelper &helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!helper.lazy_join_tables_.empty() && 
             OB_FAIL(remove_lazy_left_join(select_stmt, helper))) {
    LOG_WARN("failed to remove lazy left join table", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(select_stmt->get_condition_exprs(),
                                                  helper.pushdown_conds_))) {
    LOG_WARN("failed to remove conditions from stmt", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(select_stmt->get_semi_infos(),
                                                  helper.pushdown_semi_infos_))) {
    LOG_WARN("failed to remove semi infos from stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_new_table_item(ctx_,
                                                  select_stmt,
                                                  NULL,
                                                  helper.view_table_))) {
    LOG_WARN("failed to create table item", K(ret));
  } else if (OB_FAIL(select_stmt->add_from_item(helper.view_table_->table_id_, false))) {
    LOG_WARN("failed to add from item", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < helper.pushdown_tables_.count(); ++i) {
    TableItem *table = helper.pushdown_tables_.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is null", K(ret), K(table));
    } else if (OB_FAIL(ObTransformUtils::replace_table_in_semi_infos(
                         select_stmt, helper.view_table_, table))) {
      LOG_WARN("failed to replace table in semi infos", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_table_in_joined_tables(
                         select_stmt, helper.view_table_, table))) {
      LOG_WARN("failed to replace table in joined tables", K(ret));
    }
  }

  if (OB_SUCC(ret) && !helper.lazy_join_tables_.empty()) {
    if (OB_FAIL(build_lazy_left_join(select_stmt, helper))) {
      LOG_WARN("failed to build lazy left join table", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          select_stmt,
                                                          helper.view_table_,
                                                          helper.pushdown_tables_,
                                                          &helper.pushdown_conds_,
                                                          &helper.pushdown_semi_infos_,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          NULL,
                                                          &helper.pushdown_order_items_))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (OB_FAIL(add_limit_for_view(helper.view_table_->ref_query_,
                                        select_stmt,
                                        helper.all_lazy_join_is_unique_join_))) {
    LOG_WARN("failed to add order by limit for view", K(ret));
  }
  return ret;
}

int ObTransformJoinLimitPushDown::remove_lazy_left_join(ObDMLStmt *stmt,
                                                        LimitPushDownHelper &helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < helper.pushdown_tables_.count(); ++i) {
    TableItem *table = helper.pushdown_tables_.at(i);
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table", K(ret));
    } else if (OB_FAIL(stmt->remove_from_item(table->table_id_))) {
      LOG_WARN("failed to remove from item", K(ret));
    } else if (table->is_joined_table() && 
               OB_FAIL(stmt->remove_joined_table_item(static_cast<JoinedTable*>(table)))) {
      LOG_WARN("failed to remove joined table item", K(ret));
    } else if (OB_FAIL(inner_remove_lazy_left_join(table, helper))) {
      LOG_WARN("failed to remove lazy left join", K(ret));
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table", K(ret));
    } else if (table->is_joined_table() &&
               OB_FAIL(ObTransformUtils::adjust_single_table_ids(static_cast<JoinedTable*>(table)))) {
      LOG_WARN("failed to adjust single table ids", K(ret));
    } else {
      helper.pushdown_tables_.at(i) = table;
      LOG_TRACE("succeed to remove lazy left join", KPC(table));
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::inner_remove_lazy_left_join(TableItem* &table,
                                                              LimitPushDownHelper &helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (!table->is_joined_table()) {
    //do nothing
  } else {
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    if (OB_FAIL(SMART_CALL(inner_remove_lazy_left_join(joined_table->left_table_, helper)))) {
      LOG_WARN("failed to remove lazy left join", K(ret));
    } else if (OB_FAIL(SMART_CALL(inner_remove_lazy_left_join(joined_table->right_table_, helper)))) {
      LOG_WARN("failed to remove lazy left join", K(ret));
    } else if (helper.is_table_lazy_join(joined_table->right_table_)) {
      //remove it
      table = joined_table->left_table_;
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::build_lazy_left_join(ObDMLStmt *stmt,
                                                       LimitPushDownHelper &helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(helper.view_table_) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (helper.lazy_join_tables_.empty()) {
    //do nothing
  } else if (OB_FAIL(stmt->remove_from_item(helper.view_table_->table_id_))) {
    LOG_WARN("failed to remove from item", K(ret));
  } else {
    TableItem *left_table = NULL;
    TableItem *right_table = NULL;
    JoinedTable *tmp_joined_table = NULL;
    TableItem *cur_table = helper.view_table_;
    void *buf = NULL;
    for (int64_t i = helper.lazy_join_tables_.count()-1; OB_SUCC(ret) && i >= 0; --i) {
      ObTransformUtils::LazyJoinInfo &lazy_join = helper.lazy_join_tables_.at(i);
      if (OB_ISNULL(left_table = cur_table) || OB_ISNULL(right_table = lazy_join.right_table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(left_table), K(right_table));
      } else if (OB_ISNULL(buf = ctx_->allocator_->alloc(sizeof(JoinedTable)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else {
        tmp_joined_table = new (buf) JoinedTable();
        tmp_joined_table->type_ = TableItem::JOINED_TABLE;
        tmp_joined_table->table_id_ = stmt->get_query_ctx()->available_tb_id_--;
        tmp_joined_table->joined_type_ = LEFT_OUTER_JOIN;
        tmp_joined_table->left_table_ = left_table;
        tmp_joined_table->right_table_ = right_table;
        cur_table = tmp_joined_table;
        if (OB_FAIL(tmp_joined_table->join_conditions_.assign(lazy_join.join_conditions_))) {
          LOG_WARN("failed to push back join conditions", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransformUtils::adjust_single_table_ids(tmp_joined_table))) {
      LOG_WARN("failed to adjust single table ids", K(ret));
    } else if (OB_FAIL(stmt->add_joined_table(tmp_joined_table))) {
      LOG_WARN("failed to add joined table into stmt", K(ret));
    } else if (OB_FAIL(stmt->add_from_item(tmp_joined_table->table_id_, true))) {
      LOG_WARN("failed to add from item", K(ret));
    } else {
      LOG_TRACE("succeed to build lazy left join table", KPC(tmp_joined_table));
    }
  }
  return ret;
}

int ObTransformJoinLimitPushDown::add_limit_for_view(ObSelectStmt *generated_view,
                                                     ObSelectStmt *upper_stmt,
                                                     bool pushdown_offset)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(generated_view) || OB_ISNULL(upper_stmt) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->session_info_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parameter", K(ret), K(generated_view), K(upper_stmt), K(ctx_));
  } else {
    ObRawExpr *offset_expr = upper_stmt->get_offset_expr();
    ObRawExpr *limit_expr = upper_stmt->get_limit_expr();
    if (OB_ISNULL(limit_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("illegal limit expr", K(ret));
    } else if (pushdown_offset) {
      generated_view->set_limit_offset(limit_expr, offset_expr);
      upper_stmt->set_limit_offset(NULL, NULL);
    } else if (NULL == offset_expr) {
      generated_view->set_limit_offset(limit_expr, NULL);
    } else {
      ObRawExpr *new_limit_count_expr = NULL;
      // need to cast result to integer in static typing engine
      if (OB_FAIL(ObTransformUtils::make_pushdown_limit_count(*ctx_->expr_factory_,
                                                              *ctx_->session_info_,
                                                              limit_expr,
                                                              offset_expr,
                                                              new_limit_count_expr))) {
        LOG_WARN("make pushdown limit expr failed", K(ret));
      } else {
        generated_view->set_limit_offset(new_limit_count_expr, NULL);
      }
      generated_view->set_limit_percent_expr(NULL);
      generated_view->set_fetch_with_ties(false);
      generated_view->set_has_fetch(upper_stmt->has_fetch());
    }
  } 
  return ret;
}

}
}
