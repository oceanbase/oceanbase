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

#ifndef _OB_TRANSFORM_ELIMINATE_OUTER_JOIN_H
#define _OB_TRANSFORM_ELIMINATE_OUTER_JOIN_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObTransformEliminateOuterJoin : public ObTransformRule
{
public:
  explicit ObTransformEliminateOuterJoin(ObTransformerCtx *ctx)
  : ObTransformRule(ctx, TransMethod::POST_ORDER, T_OUTER_TO_INNER) {}

  virtual ~ObTransformEliminateOuterJoin() {}

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:

  /**
   * @brief eliminate_outer_join
   * 分别消除from list、semi from list里面的外连接
   * 对于inner join直接消除
   * 对于左外连接的消除条件：
   *  1、右表在where conditions中有空拒绝谓词；
   *  2、或者是根据主外键连接消除
   */
  int eliminate_outer_join(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                           ObDMLStmt *&stmt,
                           bool &trans_happened);

  /**
   * @brief recursive_eliminate_outer_join_in_joined_table
   * 如果table_item是joined_table，并且是left join、full join、inner join
   * 先消除顶层joined_table，然后递归消除左右表，否则不消除
   * 同时如果消除成功了，对应的join_table会拆分左右表
   * 到from_item_list中
   * @param select_stmt
   * @param cur_table_item 待处理的table item
   * @param from_item_list 最后一个顶层tbale_item放入from_item
   * @param joined_table_list 收集处理后得到的jioned_table
   * @param conditions 搜索空拒绝的表达式集
   * @param shou_move_to_from_list 如果消除成功了是否可以拆分左右表到from_item_list
   * 如果从顶层到当前层都消除成功了，才能拆分
   * @param trans_happened 是否发生改写
   */
  int recursive_eliminate_outer_join_in_table_item(ObDMLStmt *stmt,
                                                  TableItem *cur_table_item,
                                                  common::ObIArray<FromItem> &from_item_list,
                                                  common::ObIArray<JoinedTable*> &joined_table_list,
                                                  ObIArray<ObRawExpr*> &conditions,
                                                  bool should_move_to_from_list,
                                                  bool &trans_happened);

  /**
   * @brief is_outer_joined_table_type
   * 判定是否是joined table，并且是left join、full join、inner join
   * 如果不是，依据should_move_to_from_list判定
   * 是否要放置到from_item_list、joined_table_list
   */
  int is_outer_joined_table_type(ObDMLStmt *stmt,
                                  TableItem *cur_table_item,
                                  common::ObIArray<FromItem> &from_item_list,
                                  common::ObIArray<JoinedTable*> &joined_table_list,
                                  bool should_move_to_from_list,
                                  bool &is_my_joined_table_type);

  /**
   * @brief do_eliminate_outer_join
   * 执行外连接消除
   * inner join直接消除
   * full join先消除左连接，再把右连接转左连接消除
   * 最后如果外连接完全消除为inner join，处理from_lsit和conditions
   */
  int do_eliminate_outer_join(ObDMLStmt *stmt,
                              JoinedTable *cur_joined_table,
                              common::ObIArray<FromItem> &from_item_list,
                              common::ObIArray<JoinedTable*> &joined_table_list,
                              ObIArray<ObRawExpr*> &conditions,
                              bool should_move_to_from_list,
                              bool &trans_happened);

  /**
   * @brief can_be_eliminated
   * 拆分左外连接消除判定逻辑，分为两种情况
   * 1.能否利用where condition的null reject谓词消除外连接
   * 2.能够利用主外键连接消除外连接
  */
  int can_be_eliminated(ObDMLStmt *stmt,
                        JoinedTable *joined_table,
                        ObIArray<ObRawExpr*> &conditions,
                        bool &can_eliminate);

  /**
   * @brief can_be_eliminated_with_null_reject
   * 如果右表在conditions中有空拒绝谓词，可以消除左连接
   * @param stmt select stmt
   * @param joined_table 待处理的table items
   * @param conditions 待处理的表达式集
   * @param has_null_reject 右表在conditions中是否有空拒绝谓词
   */
  int can_be_eliminated_with_null_reject(ObDMLStmt *stmt,
                                        JoinedTable *joined_table,
                                        ObIArray<ObRawExpr*> &conditions,
                                        bool &has_null_reject);

  /**
   * @brief can_be_eliminated_with_foreign_primary_join
   * 根据主外键连接判断是否可能消除左外连接，消除要求：
   * 1、链接条件为主外键连接
   * 2、外键所在表为左表
   * 3、外键有非空约束或者有空拒绝谓词
   * 4、主外键为级联、检查状态
   * 5、如果左表或右表有part hint，禁掉改写
   * 6、on condition只有主外键等值连接条件（即保证右表的主键无损）
   * eg:
   * t1(c1 int, c2 int, primary key(c1))
   * t2(c1 int, c2 int not null,
   *          foreign key(c1) references t1(c1),
   *          foreign key(c2) references t1(c1))
   * 1.select * from t2 left join t1 on t1.c1=t2.c1;
   *  \->不能消除，t2(c1)没有非空约束
   * 2.select * from t2 left join t1 on t1.c1=t2.c2;
   *  \->可以改写为select * from t1,t2 where t1.c1=t2.c2;
   * @param  joined_table 需要判定是否能够消除左外连接的joined table
   * @param can_be_eliminated 返回是否能够消除左外连接
   */
  int can_be_eliminated_with_foreign_primary_join(ObDMLStmt *stmt,
                                                  JoinedTable *joined_table,
                                                  ObIArray<ObRawExpr*> &conditions,
                                                  bool &can_eliminate);

  /**
   * @brief can_be_eliminated_with_null_side_column_in_aggr
   * when the column on the null side of the outer join appears in the aggregate function of scalar group by, eliminate outer join
   * select count(B.c2) as xx from t1 A, t2 B where A.c1 = B.c2(+);
   *   =>
   * select count(B.c2) as xx from t1 A, t2 B where A.c1 = B.c2;
   */
  int can_be_eliminated_with_null_side_column_in_aggr(ObDMLStmt *stmt,
                                                      JoinedTable *joined_table,
                                                      bool &can_eliminate);

  /**
   * @brief all_columns_is_not_null
   * col_exprs中所有的列是否都非空
   */
  int is_all_columns_not_null(ObDMLStmt *stmt,
                              const ObIArray<const ObRawExpr *> &col_exprs,
                              ObIArray<ObRawExpr*> &conditions,
                              bool &is_not_null);

  /**
   * @brief is_simple_join_condition
   * 判断连接条件是否是简单的列相等的AND连接
   */
  int is_simple_join_condition(const ObIArray<ObRawExpr *> &join_condition,
                              const TableItem* left_table,
                              const TableItem* right_table,
                              bool &is_simple);

  /**
   * @brief extract_columns
   * 在 expr 中找出所有满足以下条件的列 col
   *    col 的 table_id \in rel_ids
   */
  int extract_columns(const ObRawExpr *expr,
                      const ObSqlBitSet<> &rel_ids,
                      common::ObIArray<const ObRawExpr *> &col_exprs);

  /**
   * @brief extract_columns
   * 在 exprs 中找出所有满足以下条件的列 col
   *    col 的 table_id \in rel_ids
   */
  int extract_columns_from_join_conditions(const ObIArray<ObRawExpr *> &exprs,
                                          const ObSqlBitSet<> &rel_ids,
                                          common::ObIArray<const ObRawExpr *> &col_exprs);

  int get_extra_condition_from_parent(ObIArray<ObParentDMLStmt> &parent_stmts,
                                      ObDMLStmt *&stmt,
                                      ObIArray<ObRawExpr *> &conditions);

  /**
   * @brief check_expr_ref_column_all_in_aggr
   * check column reference only in aggr
   * such as:
   *   count(a + b + 1) => true
   *   count(a + 1) + b => false
   */
  int check_expr_ref_column_all_in_aggr(const ObRawExpr *expr, bool &is_in);

};
}//namespace sql
}//namespace oceanbase

#endif
