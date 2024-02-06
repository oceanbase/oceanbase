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

#ifndef OB_TRANSFORM_JOIN_ELIMINATION_H
#define OB_TRANSFORM_JOIN_ELIMINATION_H

#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase
{
namespace share {
namespace schema {
class ObForeignKeyInfo;
}
}
namespace sql
{
struct ObStmtMapInfo;
class ObDelUpdStmt;

class ObTransformJoinElimination : public ObTransformRule
{
  struct EliminationHelper {
    EliminationHelper()
      : count_(0),
        remain_(0)
    {
    }
    int push_back(TableItem *child, TableItem *parent, share::schema::ObForeignKeyInfo *info);
    int get_eliminable_group(TableItem *&child,
                             TableItem *&parent,
                             share::schema::ObForeignKeyInfo *&info,
                             bool &find);
    int is_table_in_child_items(const TableItem *target,
                                bool &find);
    int64_t get_remain() {return remain_;}
    int64_t count_;
    int64_t remain_;
    ObSEArray<TableItem *, 16> child_table_items_;
    ObSEArray<TableItem *, 16> parent_table_items_;
    ObSEArray<share::schema::ObForeignKeyInfo *, 16> foreign_key_infos_;
    ObSEArray<bool, 16> bitmap_;
  };

public:
  explicit ObTransformJoinElimination(ObTransformerCtx *ctx)
      : ObTransformRule(ctx, TransMethod::POST_ORDER, T_ELIMINATE_JOIN) {}

  virtual ~ObTransformJoinElimination() {}
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
private:
  int construct_eliminated_tables(const ObDMLStmt *stmt,
                                  const ObIArray<TableItem *> &removed_tables,
                                  ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  int construct_eliminated_table(const ObDMLStmt *stmt,
                                 const TableItem *table,
                                 ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  /**
   * @brief 
   * check if a table can be eliminated
   *  update t1, t1 tt set t1.b = 1, tt.b = 2 where t1.a = tt.a;
   * update a column twice is illegal when the column (t1.b in this case) is a primary key/parition key/index key
   * for simplicity, we forbid the join eliminate when current sql is update
   * @param stmt 
   * @param table_id 
   * @param is_valid 
   * @return int 
   */
  int check_eliminate_delupd_table_valid(const ObDelUpdStmt *stmt, uint64_t table_id, bool &is_valid);
  int check_hint_valid(const ObDMLStmt &stmt,
                       const TableItem &table,
                       bool &is_valid);
  /**
   * @brief eliminate_join_self_key
   * self key join消除场景有：
   * stmt from items中的basic_table或generated_table
   * semi items中的basic_table或generated_table
   * stmt items中的joined_table中的inner join tables
   * semi items中的joined_table中的inner join tables
   */
  int eliminate_join_self_foreign_key(ObDMLStmt *stmt,
                                      bool &trans_happened,
                                      ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  int check_eliminate_join_self_key_valid(ObDMLStmt *stmt,
                                          TableItem *source_table,
                                          TableItem *target_table,
                                          ObStmtMapInfo &stmt_map_info,
                                          bool is_from_base_table,
                                          bool &is_valid,
                                          EqualSets *equal_sets);
  /**
   * @brief eliminate_SKJ_in_from_base_table
   * 消除stmt from items中的basic_table或generated_table
   * 存在的self key loseless join
   */
  int eliminate_join_in_from_base_table(ObDMLStmt *stmt,
                                        bool &trans_happened,
                                        ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  int eliminate_join_in_from_item(ObDMLStmt *stmt,
                                  const ObIArray<FromItem> &from_items,
                                  SemiInfo *semi_info,
                                  bool &trans_happened);

  int extract_candi_table(ObDMLStmt *stmt,
                          const ObIArray<FromItem> &from_items,
                          ObIArray<TableItem *> &candi_tables,
                          ObIArray<TableItem *> &candi_child_tables);

  int extract_candi_table(JoinedTable *table,
                          ObIArray<TableItem *> &candi_child_tables);

  /**
   * @brief eliminate_join_in_joined_table
   * 消除joined_table中的inner join tables
   * 存在的self key loseless join
   */
  int eliminate_join_in_joined_table(ObDMLStmt *stmt,
                                     bool &trans_happened,
                                     ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  /**
   * @brief eliminate_join_in_joined_table
   * 如果from_item是joined table，尝试消除，并更新joined_table结构
   */
  int eliminate_join_in_joined_table(ObDMLStmt *stmt,
                                     FromItem &from_item,
                                     ObIArray<JoinedTable*> &joined_tables,
                                     bool &trans_happened,
                                     ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  
  /**
   * @brief eliminate_join_in_joined_table
   * 尝试消除joined table,如果是inner join，并且是无损自连接
   */
  int eliminate_join_in_joined_table(ObDMLStmt *stmt,
                                     TableItem *&table_item,
                                     ObIArray<TableItem *> &child_candi_tables,
                                     ObIArray<ObRawExpr *> &trans_conditions,
                                     bool &trans_happened,
                                     ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  int eliminate_candi_tables(ObDMLStmt *stmt,
                             ObIArray<ObRawExpr*> &conds,
                             ObIArray<TableItem*> &candi_tables,
                             ObIArray<TableItem*> &child_candi_tables,
                             bool is_from_base_table,
                             bool &trans_happened,
                             ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  int check_on_null_side(ObDMLStmt *stmt,
                         uint64_t source_table_id,
                         uint64_t target_table_id,
                         bool is_from_base_table,
                         bool &is_on_null_side);

  int do_join_elimination_self_key(ObDMLStmt *stmt,
                                   TableItem *source_table,
                                   TableItem *target_table,
                                   bool is_from_base_table,
                                   bool &trans_happened,
                                   ObIArray<ObSEArray<TableItem *, 4>> &trans_tables,
                                   EqualSets *equal_sets = NULL);

  int do_join_elimination_foreign_key(ObDMLStmt *stmt,
                                      const TableItem *child_table,
                                      const TableItem *parent_table,
                                      const share::schema::ObForeignKeyInfo *foreign_key_info);

  int classify_joined_table(JoinedTable *joined_table,
                            ObIArray<JoinedTable *> &inner_join_tables,
                            ObIArray<TableItem *> &outer_join_tables,
                            ObIArray<TableItem *> &other_tables,
                            ObIArray<ObRawExpr *> &inner_join_conds);

  int rebuild_joined_tables(ObDMLStmt *stmt,
                            TableItem *&top_table,
                            ObIArray<JoinedTable*> &inner_join_tables,
                            ObIArray<TableItem*> &tables,
                            ObIArray<ObRawExpr*> &join_conds);

  int extract_lossless_join_columns(JoinedTable *joined_table,
                                    const ObIArray<int64_t> &output_map,
                                    ObIArray<ObRawExpr *> &source_exprs,
                                    ObIArray<ObRawExpr *> &target_exprs);
  int extract_child_conditions(ObDMLStmt *stmt,
                               TableItem *source_table,
                               ObIArray<ObRawExpr *> &join_conditions,
                               ObSqlBitSet<> &right_rel_ids);

  int adjust_relation_exprs(const ObSqlBitSet<8, int64_t> &right_rel_ids,
                            const ObIArray<ObRawExpr *> &join_conditions,
                            ObIArray<ObRawExpr *> &relation_exprs,
                            bool &is_valid);

  int compute_table_expr_ref_count(const ObSqlBitSet<> &rel_ids,
                                   const ObIArray<ObRawExpr *> &exprs,
                                   int64_t &total_ref_count);

  int compute_table_expr_ref_count(const ObSqlBitSet<> &rel_ids,
                                   const ObIArray<ObRawExprPointer> &exprs,
                                   int64_t &total_ref_count);

  int compute_table_expr_ref_count(const ObSqlBitSet<> &rel_ids,
                                   const ObRawExpr* exprs,
                                   int64_t &total_ref_count);

  int extract_equal_join_columns(const ObIArray<ObRawExpr *> &join_conds,
                                 const TableItem *source_table,
                                 const TableItem *target_table,
                                 ObIArray<ObRawExpr *> &source_exprs,
                                 ObIArray<ObRawExpr *> &target_exprs,
                                 ObIArray<int64_t> *unused_conds);

  int adjust_table_items(ObDMLStmt *stmt,
                         TableItem *source_table,
                         TableItem *target_table,
                         ObStmtMapInfo &info);

  int reverse_select_items_map(const ObSelectStmt *source_stmt,
                               const ObSelectStmt *target_stmt,
                               const ObIArray<int64_t> &column_map,
                               ObIArray<int64_t> &reverse_column_map);

  int create_missing_select_items(ObSelectStmt *source_stmt,
                                  ObSelectStmt *target_stmt,
                                  ObIArray<int64_t> &column_map,
                                  const ObIArray<int64_t> &table_map);
  /**
   * @brief trans_table_item
   * 将关联target_table的结构改写为关联source_table
   * 从from item中删除target table
   * 从partiton信息中删除target table
   * 重构 rel_idx
   * @param stmt
   * @param source_table
   * @param target_table
   * @return
   */
  int trans_table_item(ObDMLStmt *stmt,
                       const TableItem *source_table,
                       const TableItem *target_table);

  int eliminate_outer_join(ObIArray<ObParentDMLStmt> &parent_stmts,
                           ObDMLStmt *stmt,
                           bool &trans_happened,
                           ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  int eliminate_outer_join_in_joined_table(ObDMLStmt *stmt,
                                           TableItem *&table_item,
                                           const bool is_non_sens_dul_vals,
                                           ObIArray<uint64_t> &removed_ids,
                                           ObIArray<ObRawExprPointer> &relation_exprs,
                                           bool &trans_happen,
                                           ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  int check_vaild_non_sens_dul_vals(ObIArray<ObParentDMLStmt> &parent_stmts,
                                    ObDMLStmt *stmt,
                                    bool &is_valid,
                                    bool &need_add_limit_constraint);

  int get_eliminable_tables(const ObDMLStmt *stmt,
                            const ObIArray<ObRawExpr *> &conds,
                            const ObIArray<TableItem *> &candi_tables,
                            const ObIArray<TableItem *> &candi_child_tables,
                            EliminationHelper &helper);

  /**
   * @brief check_transform_validity_foreign_key
   * 检查source_table和target_table是否可以进行主外键连接消除。
   * 对于任意存在主外键约束的两表，主外键连接消除的条件:
   *    1. 只涉及到父表的主键列
   *    2. 连接条件中存在主外键一一对应的等值condition（即保证连接是unique的）
   *
   *  举例 t1(c1, c2, c3, primary key (c1, c2))
   *       t2(c1, c2, c3, c4,
   *          foreign key (c1, c2) references t1(c1, c2),
   *          foreign key (c3, c4) references t1(c1, c2))
   *  则 1. select * from t1, t2 where t1.c1 = t2.c1 and t1.c2 = t2.c2;
   *        \-> 不能消除，引用了父表的非主键列，需要读取父表才能获取该列的值。
   *
   *     2. select t1.c1, t2.* from t1, t2 where t1.c1 = t2.c1;
   *        |   不能消除，连接条件中不存在主外键一一对应的等值condition，
   *        \-> t2中的一列可能会连接到t1中的多行，需要读取t1进行验证。
   *
   *     3. select t1.c1, t2.* from t1, t2 where t1.c1 = t2.c1 and t1.c2 and t2.c2;
   *        |   可以消除，连接条件中存在主外键一一对应的等值condition，保证了
   *        |   t2的每一列最多只能连接到t1中的一行；且只引用父表的主键列，可以
   *        |   使用子表对应的外键列代替。
   *        |   本查询可改写为
   *        \-> select t2.c1, t2.* from t2 where t2.c1 is not null and t2. c2 is not null;
   */
  int check_transform_validity_foreign_key(const ObDMLStmt *stmt,
                                           const ObIArray<ObRawExpr *> &join_conds,
                                           const TableItem *source_table,
                                           const TableItem *target_table,
                                           bool &can_be_eliminated,
                                           bool &is_first_table_parent,
                                           share::schema::ObForeignKeyInfo *&foreign_key_info);

  int check_transform_validity_foreign_key(const ObDMLStmt *target_stmt,
                                           const TableItem *source_table,
                                           const TableItem *target_table,
                                           const ObIArray<ObRawExpr*> &source_exprs,
                                           const ObIArray<ObRawExpr*> &target_exprs,
                                           bool &can_be_eliminated,
                                           share::schema::ObForeignKeyInfo *&foreign_key_info);

  int extract_equal_join_columns(const ObIArray<ObRawExpr *> &join_conds,
                                 const uint64_t source_tid,
                                 const uint64_t upper_target_tid,
                                 const ObIArray<ObRawExpr*> &upper_column_exprs,
                                 const ObIArray<ObRawExpr*> &child_column_exprs,
                                 ObIArray<ObRawExpr*> &source_exprs,
                                 ObIArray<ObRawExpr*> &target_exprs,
                                 ObIArray<int64_t> *unused_conds);

  /**
   * @brief check_transform_validity_outer_join
   * 检查joined table是否可以做外连接消除
   * 对于外连接可消除的改写，条件如下：
   *    1. 连接条件中右表的连接列是unqiue的
   *    2. 需要为左外连接的类型
   *    3. 右表的列不存在于select item，groupby，orderby, having中。
   * 举例：t1(a int, b int, c int);
   *       t2(a int, b int, c int, primary key(a,b))
   *    1. select t1.* from t1 inner join on t2 on t1.a=t2.a and t1.b=t2,b;
   *       \->不能消除，不是left join的类型
   *    2. select t1.* from t1 left join on t2 on t1.a=t2.a
   *       \->不能消除，连接条件不是unique的
   *    3. select t1.*, t2.a from t1 left join t2 on t1.a=t2.a and t1.b=t2.b
   *       \->不能改写，因为select items中含有右表列。
   */
  int check_transform_validity_outer_join(ObDMLStmt *stmt,
                                          JoinedTable *joined_table,
                                          const bool is_non_sens_dul_vals,
                                          ObIArray<ObRawExprPointer> &relation_exprs,
                                          bool &is_valid);

  int check_has_semi_join_conditions(ObDMLStmt *stmt,
                                     const ObSqlBitSet<8, int64_t> &rel_ids,
                                     bool &is_valid);


  /**
   *  @brief check_all_column_primary_key
   *  检查stmt中属于table_id的列是否全部在foreign key info的父表列中
   */
  int check_all_column_primary_key(const ObDMLStmt *stmt,
                                   const uint64_t table_id,
                                   const share::schema::ObForeignKeyInfo *info,
                                   bool &all_primary_key);

  /**
   * @brief trans_column_items_foreign_key
   * 如果col0 \in child, col1 \in parent，且两者存在存在主外键关系，
   * 那么删除col1，并将所有表达式中指向col1的指针改为指向col0。
   *
   * 在主外键连接消除中，父表的列出现时，一定存在对应的子表列，
   * 因此不需要处理col单独出现在父表中的情况。
   */
  int trans_column_items_foreign_key(ObDMLStmt *stmt,
                                     const TableItem *child_table,
                                     const TableItem *parent_table,
                                     const share::schema::ObForeignKeyInfo *foreign_key_info);

  /**
   * @brief get_child_column_id_by_parent_column_id
   * 从foreign key info中获取父表列id对应的子表列id
   */
  int get_child_column_id_by_parent_column_id(const share::schema::ObForeignKeyInfo *info,
                                              const uint64_t parent_column_id,
                                              uint64_t &child_column_id);

  //zhenling.zzg增强semi anti 的self join消除
  /**
   * @brief eliminate_semi_join_self_key
   * 消除自连接的semi、anti join
   * 消除semi join的规则要求：
   * 1、condition至少有一个简单连接谓词（相同列相等的连接谓词）；
   * 2、join_expr的简单连接谓词含有唯一性约束；（或者join_expr都是简单连接的and连接，没有其他谓词，则不需要唯一性约束）
   * 3、如果有partition hint，inner table引用的分区包含outer table引用的分区；
   * 消除anti join的规则要求：
   * 1、join_expr的and连接树中，都是简单连接谓词（相同列相等的连接谓词）,inner table 的过滤谓词为简单and树；
   * 2、join_expr的简单连接谓词含有唯一性约束；（或者join_expr都是简单连接的and连接，并且inner table没有过滤谓词，则不需要唯一性约束）
   * 3、如果有partition hint，inner table引用的分区包含outer table引用的分区；
   */
  //int eliminate_semi_join_self_key(ObDMLStmt *stmt,
  //                                bool &trans_happened);

  int eliminate_semi_join_self_foreign_key(ObDMLStmt *stmt,
                                           bool &trans_happened,
                                           ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  /**
   * @brief 
   *  eliminate left outer join when the join condition is always false:
   *    select t1.a, t2.a from t1 left join t2 on false;
   *  can be transformed into:
   *    select t1.a, null from t1;
   * @param stmt 
   * @param trans_happened 
   * @return int 
   */
  int eliminate_left_outer_join(ObDMLStmt *stmt, bool &trans_happened,
                                ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  int do_eliminate_left_outer_join_rec(ObDMLStmt *stmt,
                                       TableItem *&table,
                                       bool &trans_happened,
                                       ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  int do_eliminate_left_outer_join(ObDMLStmt *stmt,
                                   TableItem *&table,
                                   ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);
  int get_table_items_and_ids(ObDMLStmt *stmt,
                              TableItem *table,
                              ObIArray<uint64_t> &table_ids,
                              ObIArray<TableItem *> &table_items);

  int left_join_can_be_eliminated(ObDMLStmt *stmt, TableItem *table, bool &can_be_eliminated);

  int eliminate_semi_join_self_key(ObDMLStmt *stmt,
                                   SemiInfo *semi_info,
                                   ObIArray<ObRawExpr*> &conds,
                                   bool &trans_happened,
                                   bool &has_removed_semi_info,
                                   ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  int eliminate_semi_join_foreign_key(ObDMLStmt *stmt,
                                      SemiInfo *semi_info,
                                      bool &trans_happened,
                                      bool &has_removed_semi_info,
                                      ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  int try_remove_semi_info(ObDMLStmt *stmt, SemiInfo *semi_info);

  int eliminate_semi_right_child_table(ObDMLStmt *stmt,
                                       SemiInfo *semi_info,
                                       ObIArray<TableItem*> &right_tables,
                                       ObIArray<ObRawExpr*> &source_col_exprs,
                                       ObIArray<ObRawExpr*> &target_col_exprs,
                                       ObIArray<ObSEArray<TableItem *, 4>> &trans_tables);

  int adjust_source_table(ObDMLStmt *source_stmt,
                          ObDMLStmt *target_stmt,
                          const TableItem *source_table,
                          const TableItem *target_table,
                          const ObIArray<int64_t> *output_map,
                          ObIArray<ObRawExpr*> &source_col_exprs,
                          ObIArray<ObRawExpr*> &target_col_exprs);

  int convert_target_table_column_exprs(ObDMLStmt *source_stmt,
                                        ObDMLStmt *target_stmt,
                                        const ObIArray<TableItem*> &source_tables,
                                        const ObIArray<TableItem*> &target_tables,
                                        ObIArray<ObStmtMapInfo> &stmt_map_infos,
                                        ObIArray<ObRawExpr*> &source_col_exprs,
                                        ObIArray<ObRawExpr*> &target_col_exprs);

  int convert_target_table_column_exprs(ObDMLStmt *source_stmt,
                              ObDMLStmt *target_stmt,
                              const ObIArray<TableItem*> &child_tables,
                              const ObIArray<TableItem*> &parent_tables,
                              const ObIArray<share::schema::ObForeignKeyInfo*> &foreign_key_infos,
                              ObIArray<ObRawExpr*> &source_col_exprs,
                              ObIArray<ObRawExpr*> &target_col_exprs);

  /**
   * @brief check_transform_validity_semi_self_key
   * 检查semi_info包含的semi join是否可以消除
   * @param stmt 
   * @param semi_join 待检查的semi join
   * @param stmt_map_infos semi_join相关的generated_table的映射关系
   * @param rel_map_info semi_join的左右表集的映射关系
   * @param can_be_eliminated 是否能消除
   */
  int check_transform_validity_semi_self_key(ObDMLStmt *stmt,
                                             SemiInfo *semi_info,
                                             ObIArray<ObRawExpr*> &candi_conds,
                                             TableItem *&source_table,
                                             TableItem *&right_table,
                                             ObStmtMapInfo &stmt_map_info);

  int check_transform_validity_semi_self_key(ObDMLStmt *stmt,
                                             SemiInfo *semi_info,
                                             ObIArray<ObRawExpr*> &candi_conds,
                                             ObIArray<TableItem*> &left_tables,
                                             ObIArray<TableItem*> &right_tables,
                                             ObIArray<ObStmtMapInfo> &stmt_map_infos,
                                             ObDMLStmt *&target_stmt);

  int is_table_column_used_in_subquery(const ObSelectStmt &stmt,
                                       const uint64_t table_id,
                                       bool &used);

  /**
   * @brief check_semi_join_condition
   * 检测semi join的condition
   * @param stmt
   * @param semi_info
   * @param source_tables semi_info->left_tables_
   * @param target_tables semi_info->right_tables_
   * @param stmt_map_info generate table的映射关系
   * @param rel_map_info source tables、target tables的映射关系
   * @param source_exprs 返回source tables参与semi join的列
   * @param target_exprs 返回target tables参与semi join的列
   * @param is_simple_join_condition semi join condition是否只有相同列等式
   * @param target_tables_have_filter target tables上是否有过滤谓词
   * 如果target tables有多个表，且内部有连接谓词，那连接谓词算笛卡尔积后的过滤谓词
   * @param is_simple_filter 如果target table上有filter，返回所有的filter
   * 是否是由简单谓词and构成
   */
  // source_table and target_table come from the same stmt
  int check_semi_join_condition(ObDMLStmt *stmt,
                                ObIArray<ObRawExpr*> &semi_conds,
                                const TableItem *source_table,
                                const TableItem *target_table,
                                const ObStmtMapInfo &stmt_map_info,
                                ObIArray<ObRawExpr*> &source_exprs,
                                ObIArray<ObRawExpr*> &target_exprs,
                                bool &is_simple_join_condition,
                                bool &target_tables_have_filter,
                                bool &is_simple_filter);

  // source_table comes from stmt, target_table comes from child select stmt target_stmt
  int check_semi_join_condition(ObDMLStmt *stmt,
                                ObSelectStmt *target_stmt,
                                ObIArray<ObRawExpr *> &semi_conds,
                                ObIArray<ObSqlBitSet<>> &select_relids,
                                const TableItem *source_table,
                                const TableItem *target_table,
                                const ObStmtMapInfo &stmt_map_info,
                                ObIArray<ObRawExpr *> &source_exprs,
                                ObIArray<ObRawExpr *> &target_exprs,
                                bool &is_simple_join_condition,
                                bool &target_tables_have_filter,
                                bool &is_simple_filter);

  int get_epxrs_rel_ids_in_child_stmt(ObDMLStmt *stmt,
                                      ObSelectStmt *child_stmt,
                                      ObIArray<ObRawExpr *> &cond_exprs,
                                      uint64_t table_id,
                                      ObIArray<ObSqlBitSet<>> &rel_ids);

  /**
   * @is_equal_column
   * source_col与target_col是否存在映射关系
   * @param is_equal 是否存在映射关系
   * @param is_reverse 是否是翻转后匹配映射关系
   */
  int is_equal_column(const ObIArray<TableItem*> &source_tables,
                      const ObIArray<TableItem*> &target_tables,
                      const ObIArray<ObStmtMapInfo> &stmt_map_infos,
                      const ObIArray<int64_t> &rel_map_info,
                      const ObColumnRefRawExpr *source_col,
                      const ObColumnRefRawExpr *target_col,
                      bool &is_equal,
                      bool &is_reverse);

  int is_equal_column(const TableItem *source_table,
                      const TableItem *target_table,
                      const ObIArray<int64_t> &output_map,
                      uint64_t source_col_id,
                      uint64_t target_col_id,
                      bool &is_equal);
  
  /**
   * @brief do_elimination_semi_join_self_key
   * 消除semi join后的谓词处理
   * anti join的处理比较特殊
   * 需要先改写谓词再删除inner table、更新table hash idx
   */
  int do_elimination_semi_join_self_key(ObDMLStmt *stmt,
                                        SemiInfo *semi_info,
                                        const ObIArray<TableItem*> &source_tables,
                                        const ObIArray<TableItem*> &target_tables,
                                        ObIArray<ObStmtMapInfo> &stmt_map_infos,
                                        const ObIArray<int64_t> &rel_map_info);
  
   /**
   * @brief trans_semi_table_item
   * 将关联target_table的结构改写为关联source_table
   * 从table items中删除target table
   * 从partiton信息中删除target table
   * 重构 rel_idx
   * @param stmt
   * @param source_table
   * @param target_table
   * @return
   */
  int trans_semi_table_item(ObDMLStmt *stmt,
                            const TableItem *target_table);

  /**
   * @brief trans_semi_condition_exprs
   * 消除anti join的谓词改写与消除semi join不同，需要在adjust table item
   * 之前做，因为对于target table上的过滤谓词表达式需要取LNNVL，source table
   * 上的过滤谓词不需要处理，所以如果adjust table item之后，就无法区分source table
   * 与target table
   */
  int trans_semi_condition_exprs(ObDMLStmt *stmt,
                                 SemiInfo *semi_info);

  /**
   * @brief eliminate_semi_join_foreign_key
   * 消除主外键连接的semi、anti join
   * 消除semi join的规则要求：
   * 1、主外键连接；
   * 2、outer table为外键所在表；
   * 3、inner table上没有partition hint
   * 4、inner table上没有非主键过滤谓词；
   * 5、当主外键索引时NOVALIDATE + norely 状态时，不应该用这个约束做主外键连接消除；
   * 消除semi join的规则要求：
   * 1、主外键连接；
   * 2、outer table为外键所在表；
   * 3、inner table上没有partition hint
   * 4、inner table上没有过滤谓词；
   * 5、当主外键索引时NOVALIDATE + norely 状态时，不应该用这个约束做主外键连接消除；
   */
  //int eliminate_semi_join_foreign_key(ObDMLStmt *stmt,
  //                                    bool &trans_happened);

  /**
   * @brief check_transform_validity_semi_foreign_key
   * 检查semi_info包含的semi join是否可以消除
   * @param stmt 
   * @param semi_join 待检查的semi join
   * @param can_be_eliminated 是否能消除
   */
  int check_transform_validity_semi_foreign_key(ObDMLStmt *stmt,
                                                SemiInfo *semi_info,
                                                TableItem *right_table,
                                                TableItem *&left_table,
                                                share::schema::ObForeignKeyInfo *&foreign_key_info);
                                                

  int check_transform_validity_semi_foreign_key(ObDMLStmt *stmt,
                                  SemiInfo *semi_info,
                                  TableItem *right_table,
                                  ObIArray<TableItem*> &left_tables,
                                  ObIArray<TableItem*> &right_tables,
                                  ObIArray<share::schema::ObForeignKeyInfo*> &foreign_key_infos);

  int get_column_exprs(ObDMLStmt &stmt,
                       ObSelectStmt &child_stmt,
                       const uint64_t upper_table_id,
                       const uint64_t child_table_id,
                       ObIArray<ObRawExpr*> &upper_columns,
                       ObIArray<ObRawExpr*> &child_columns);


  // functions below trans self equal conditions to not null
  /**
   * 重写 stmt 中同一列上的等值判断条件
   * 如果是not null col，直接删除; 如果是nullable col，替换为 IS_NOT_NULL
   */
  int trans_self_equal_conds(ObDMLStmt *stmt);
  int trans_self_equal_conds(ObDMLStmt *stmt, ObIArray<ObRawExpr*> &cond_exprs);
  // 检查col是否是nullable,如果是，那么添加is_not_null表达式
  int add_is_not_null_if_needed(ObDMLStmt *stmt,
                                ObIArray<ObColumnRefRawExpr *> &col_exprs,
                                ObIArray<ObRawExpr*> &cond_exprs);
  // 对于expr如果 column 没有非空约束，则替换为IS NOT NULL;
  // 否则替换为TRUE, 对于OR、AND表达式，会对子表达式递归改写
  int recursive_trans_equal_join_condition(ObDMLStmt *stmt, ObRawExpr *expr);
  int do_trans_equal_join_condition(ObDMLStmt *stmt,
                                    ObRawExpr *expr,
                                    bool &has_trans,
                                    ObRawExpr* &new_expr);

};

} //namespace sql
} //namespace oceanbase

#endif // OB_TRANSFORM_JOIN_ELIMINATION_H
