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

#ifndef _OB_TRANSFORM_JOIN_LIMIT_PUSHDOWN_H
#define _OB_TRANSFORM_JOIN_LIMIT_PUSHDOWN_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_union_find.h"

namespace oceanbase
{

namespace common
{
class ObIAllocator;
}

namespace sql
{

class ObRawExpr;

class ObTransformJoinLimitPushDown : public ObTransformRule
{
private:
  struct LimitPushDownHelper {
    LimitPushDownHelper()
      : view_table_(NULL),
        pushdown_conds_(),
        pushdown_order_items_(),
        pushdown_semi_infos_(),
        pushdown_tables_(),
        lazy_join_tables_(),
        expr_relation_ids_(),
        inner_is_not_null_exprs_(),
        all_lazy_join_is_unique_join_(true) {}
    virtual ~LimitPushDownHelper() {};

    void reset() {
      view_table_ = NULL;
      pushdown_conds_.reset();
      pushdown_order_items_.reset();
      pushdown_semi_infos_.reset();
      pushdown_tables_.reset();
      lazy_join_tables_.reset();
      expr_relation_ids_.reset();
      inner_is_not_null_exprs_.reset();
      all_lazy_join_is_unique_join_ = true;
    }
    int assign(const LimitPushDownHelper &other);
    bool is_table_lazy_join(TableItem* table);
    uint64_t get_max_table_id() const;
    static int alloc_helper(ObIAllocator &allocator, LimitPushDownHelper* &helper);

    TO_STRING_KV(K(view_table_),
                 K(pushdown_conds_),
                 K(pushdown_order_items_),
                 K(pushdown_semi_infos_),
                 K(pushdown_tables_),
                 K(lazy_join_tables_),
                 K(expr_relation_ids_),
                 K(inner_is_not_null_exprs_),
                 K(all_lazy_join_is_unique_join_));

    TableItem *view_table_;                       //created by pushdown tables,conds,semi infos, order by
    ObSEArray<ObRawExpr*, 8> pushdown_conds_;
    ObSEArray<OrderItem, 8> pushdown_order_items_;
    ObSEArray<SemiInfo *, 8> pushdown_semi_infos_;
    ObSEArray<TableItem *, 8> pushdown_tables_;
    ObSEArray<ObTransformUtils::LazyJoinInfo, 8> lazy_join_tables_;     //lazy left join`s right table item and on condition
    ObSqlBitSet<> expr_relation_ids_;                  //table ids ref by conditions, semi infos, order by items
    ObSEArray<ObRawExpr*, 8> inner_is_not_null_exprs_;   //inner join need construct is not null conditions
    bool all_lazy_join_is_unique_join_;                         //all lazy left join key is unique
  };

public:
  explicit ObTransformJoinLimitPushDown(ObTransformerCtx *ctx) :
    ObTransformRule(ctx, TransMethod::PRE_ORDER, T_PUSH_LIMIT) {}

  virtual ~ObTransformJoinLimitPushDown() {}
  
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;

private:
  int check_stmt_validity(ObDMLStmt *stmt,
                          ObIArray<LimitPushDownHelper*> &helpers,
                          bool &is_valid,
                          bool &is_only_inner_valid);

  int check_inner_join_rewrite_done(ObSelectStmt *stmt,
                                    TableItem *inner_join_table,
                                    LimitPushDownHelper *helper, 
                                    bool &is_valid);
                                    
  int check_inner_join_validity(ObDMLStmt *stmt, 
                                LimitPushDownHelper *inner_helper, 
                                TableItem *inner_join_table, 
                                bool &is_valid);

  int check_lazy_join_is_unique(ObIArray<ObTransformUtils::LazyJoinInfo> &lazy_join, 
                                ObDMLStmt *stmt,
                                bool &is_unique_join);

  int do_transform(ObSelectStmt *select_stmt,
                   LimitPushDownHelper &helper);

  int create_is_not_null_cond_expr(ObRawExpr *&expr,
                                   ObRawExpr *&cond);
  
  int create_not_null_condition_exprs(ObDMLStmt *select_stmt,
                                      ObIArray<TableItem *> &from_tables,
                                      ObIArray<ObRawExpr *> &is_not_null_exprs,
                                      ObIArray<ObRawExpr *> &is_not_null_conditions);

  /* 
   * add constraint while view_stmt limit > select_stmt offset+limit
   * for example:
   * select * from (select * from A limit 5) V left join B on V.id = B.id limit 3;
   * will add a constraint "5 > 3" 
  */
  int add_constraint_for_generated_table_with_limit(ObSelectStmt *select_stmt,
                                                    ObSelectStmt *view_stmt,
                                                    ObIArray<TableItem *> &from_tables);
  // do transform while containing inner join
  int do_transform_inner_join(ObSelectStmt *select_stmt,
                              LimitPushDownHelper &helper,
                              bool keep_limit);                

  int split_cartesian_tables(ObSelectStmt *select_stmt,
                             ObIArray<LimitPushDownHelper*> &helpers,
                             bool &is_valid,
                             bool &has_cartesian);

  int check_contain_correlated_function_table(ObDMLStmt *stmt, bool &is_contain);

  int check_contain_correlated_json_table(ObDMLStmt *stmt, bool &is_contain);

  int check_cartesian(ObSelectStmt *stmt, UnionFind &uf, bool &is_valid);

  int connect_tables(ObSelectStmt *stmt,
                     const ObIArray<uint64_t> &table_ids,
                     const ObIArray<TableItem *> &from_tables,
                     UnionFind &uf);

  int collect_cartesian_infos(ObSelectStmt *stmt,
                              UnionFind &uf,
                              ObIArray<LimitPushDownHelper*> &helpers);

  int collect_cartesian_exprs(ObSelectStmt *stmt, LimitPushDownHelper *helper);

  int get_idx_from_table_ids(ObSelectStmt *stmt,
                             const ObIArray<uint64_t> &src_table_ids,
                             const ObIArray<TableItem *> &target_tables,
                             ObIArray<int64_t> &indices);

  int get_limit_expr_value(const ObRawExpr *limit_expr, int64_t& limit_value_int);

  /* 
   * check if ref_query limit <= stmt offset+limit
   * if true, return ref_limit_lower_equal as true
   * then the rewrite will stops 
   */
  int check_ref_query_offset_limit_expr_lower_equal(ObSelectStmt *ref_query,
                                                    ObSelectStmt *stmt,
                                                    bool &ref_limit_lower_equal);

  int check_table_validity(ObSelectStmt *stmt, 
                          const ObIArray<TableItem *> &target_tables, 
                          bool &is_valid);

  int collect_pushdown_exprs(ObSelectStmt *stmt,
                               LimitPushDownHelper &helper);

  int check_offset_limit_expr(ObRawExpr *offset_limit_expr,
                              bool &is_valid);

  int check_limit(ObSelectStmt *select_stmt, 
                  bool &is_valid);

  int remove_lazy_left_join(ObDMLStmt *stmt, LimitPushDownHelper &helper);

  int inner_remove_lazy_left_join(TableItem* &table, LimitPushDownHelper &helper);

  int build_lazy_join(ObDMLStmt *stmt, LimitPushDownHelper &helper);

  int add_limit_for_view(ObSelectStmt *generated_view,
                         ObSelectStmt *upper_stmt,
                         bool pushdown_offset);

  int rename_pushdown_exprs(ObSelectStmt *select_stmt,
                            ObIArray<LimitPushDownHelper*> &helpers);

  int sort_pushdown_helpers(ObSEArray<LimitPushDownHelper*, 4> &helpers);

};

} //namespace sql
} //namespace oceanbase
#endif /* _OB_TRANSFORM_JOIN_LIMIT_PUSHDOWN_H */
