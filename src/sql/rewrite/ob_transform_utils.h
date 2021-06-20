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

#ifndef OCEANBASE_SQL_REWRITE_OB_TRANSFORM_UTILS_H_
#define OCEANBASE_SQL_REWRITE_OB_TRANSFORM_UTILS_H_ 1

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "sql/rewrite/ob_transform_rule.h"
#include "sql/optimizer/ob_fd_item.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObForeignKeyInfo;
class ObTableSchema;
}  // namespace schema
}  // namespace share

namespace sql {

class ObStmtHint;
struct ObTransformerCtx;
struct ObStmtMapInfo;
class ObUpdateStmt;
class ObSQLSessionInfo;

enum CheckStmtUniqueFlags {
  FLAGS_DEFAULT = 0,               // nothing
  FLAGS_IGNORE_DISTINCT = 1 << 0,  // for distinct
  FLAGS_IGNORE_GROUP = 1 << 1,     // for group by
};

class ObTransformUtils {
private:
  struct UniqueCheckInfo {
    UniqueCheckInfo()
    {}
    virtual ~UniqueCheckInfo()
    {}

    ObRelIds table_set_;
    ObSEArray<ObRawExpr*, 4> const_exprs_;
    EqualSets equal_sets_;
    ObFdItemSet fd_sets_;
    ObFdItemSet candi_fd_sets_;
    ObSEArray<ObRawExpr*, 4> not_null_;

    int assign(const UniqueCheckInfo& other);
    void reset();

  private:
    DISALLOW_COPY_AND_ASSIGN(UniqueCheckInfo);
  };
  struct UniqueCheckHelper {
    UniqueCheckHelper()
        : alloc_(NULL), fd_factory_(NULL), expr_factory_(NULL), schema_checker_(NULL), session_info_(NULL)
    {}
    virtual ~UniqueCheckHelper()
    {}

    ObIAllocator* alloc_;
    ObFdItemFactory* fd_factory_;
    ObRawExprFactory* expr_factory_;
    ObSchemaChecker* schema_checker_;
    ObSQLSessionInfo* session_info_;

  private:
    DISALLOW_COPY_AND_ASSIGN(UniqueCheckHelper);
  };

public:
  static int is_correlated_expr(const ObRawExpr* expr, int32_t correlated_level, bool& is_correlated);

  static int is_direct_correlated_expr(const ObRawExpr* expr, int32_t correlated_level, bool& is_direct_correlated);

  static int has_current_level_column(const ObRawExpr* expr, int32_t curlevel, bool& has);

  static int is_column_unique(
      const ObRawExpr* expr, uint64_t table_id, ObSchemaChecker* schema_checker, bool& is_unique);

  static int is_columns_unique(
      const ObIArray<ObRawExpr*>& exprs, uint64_t table_id, ObSchemaChecker* schema_checker, bool& is_unique);

  static int exprs_has_unique_subset(
      const common::ObIArray<ObRawExpr*>& full, const common::ObRowkeyInfo& sub, bool& is_subset);

  static int add_new_table_item(
      ObTransformerCtx* ctx, ObDMLStmt* stmt, ObSelectStmt* subquery, TableItem*& new_table_item);

  static int add_new_joined_table(ObTransformerCtx* ctx, ObDMLStmt& stmt, const ObJoinType join_type,
      TableItem* left_table, TableItem* right_table, const ObIArray<ObRawExpr*>& joined_conds, TableItem*& join_table,
      bool add_table = true);

  static int merge_from_items_as_inner_join(ObTransformerCtx* ctx, ObDMLStmt& stmt, TableItem*& ret_table);

  static int create_new_column_expr(ObTransformerCtx* ctx, const TableItem& table_item, const int64_t column_id,
      const SelectItem& select_item, ObDMLStmt* stmt, ObColumnRefRawExpr*& new_expr);

  static int create_columns_for_view(
      ObTransformerCtx* ctx, TableItem& view_table_item, ObDMLStmt* stmt, ObIArray<ObRawExpr*>& column_exprs);

  static int create_columns_for_view(ObTransformerCtx* ctx, TableItem& view_table_item, ObDMLStmt* stmt,
      ObIArray<ObRawExpr*>& new_select_list, ObIArray<ObRawExpr*>& new_column_list);

  static int find_base_info_for_stmt(
      ObSelectStmt* stmt, uint64_t sel_idx, ObSelectStmt*& target_stmt, ObRawExpr*& target_expr);

  static int create_select_item(ObIAllocator& allocator, ObRawExpr* select_expr, ObSelectStmt* select_stmt);

  static int create_select_item(ObIAllocator& allocator, ObIArray<ColumnItem>& column_items, ObSelectStmt* select_stmt);

  static int create_select_item(
      ObIAllocator& allocator, common::ObIArray<ObRawExpr*>& select_exprs, ObSelectStmt* select_stmt);

  static int copy_stmt(ObStmtFactory& stmt_factory, const ObDMLStmt* stmt, ObDMLStmt*& new_stmt);

  static int deep_copy_stmt(
      ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory, const ObDMLStmt* stmt, ObDMLStmt*& new_stmt);

  static int add_joined_table_single_table_ids(JoinedTable& joined_table, TableItem& child_table);

  static int deep_copy_order_items(ObRawExprFactory& expr_factory, const common::ObIArray<OrderItem>& input_items,
      common::ObIArray<OrderItem>& output_items, const uint64_t copy_types, bool use_new_allocator = false);
  static int deep_copy_semi_infos(ObRawExprFactory& expr_factory, const ObIArray<SemiInfo*>& input_semi_infos,
      ObIArray<SemiInfo*>& output_semi_infos, const uint64_t copy_types);
  static int deep_copy_column_items(ObRawExprFactory& expr_factory,
      const common::ObIArray<ColumnItem>& input_column_items, common::ObIArray<ColumnItem>& output_column_items);
  static int deep_copy_related_part_expr_arrays(ObRawExprFactory& expr_factory,
      const ObIArray<ObDMLStmt::PartExprArray>& input_related_part_expr_arrays,
      ObIArray<ObDMLStmt::PartExprArray>& output_related_part_expr_arrays, const uint64_t copy_types);
  static int deep_copy_part_expr_items(ObRawExprFactory& expr_factory,
      const common::ObIArray<ObDMLStmt::PartExprItem>& input_part_expr_items,
      common::ObIArray<ObDMLStmt::PartExprItem>& output_part_expr_items, const uint64_t copy_types);
  static int deep_copy_table_items(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory,
      const common::ObIArray<TableItem*>& input_table_items, common::ObIArray<TableItem*>& output_table_items);
  static int deep_copy_join_tables(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory,
      const common::ObIArray<JoinedTable*>& input_joined_tables, common::ObIArray<JoinedTable*>& output_joined_tables,
      const uint64_t copy_types);
  static int deep_copy_join_table(ObStmtFactory& stmt_factory, ObRawExprFactory& expr_factory,
      const JoinedTable& input_joined_table, JoinedTable*& output_joined_table, const uint64_t copy_types);
  static int deep_copy_select_items(ObRawExprFactory& expr_factory,
      const common::ObIArray<SelectItem>& input_select_items, common::ObIArray<SelectItem>& output_select_items,
      const uint64_t copy_types);

  static int replace_equal_expr(ObRawExpr* old_expr, ObRawExpr* new_expr, ObRawExpr*& expr);

  static int replace_equal_expr(const common::ObIArray<ObRawExpr*>& other_exprs,
      const common::ObIArray<ObRawExpr*>& current_exprs, ObRawExpr*& expr);

  static int replace_expr(ObRawExpr* old_expr, ObRawExpr* new_expr, ObRawExpr*& expr);

  static int replace_expr(ObRawExpr* old_expr, ObRawExpr* new_expr, ObDMLStmt* stmt);

  static int replace_expr(
      const common::ObIArray<ObRawExpr*>& other_exprs, const common::ObIArray<ObRawExpr*>& new_exprs, ObRawExpr*& expr);

  static int replace_expr_for_order_item(const common::ObIArray<ObRawExpr*>& other_exprs,
      const common::ObIArray<ObRawExpr*>& new_exprs, common::ObIArray<OrderItem>& order_items);
  template <typename T>
  static int replace_specific_expr(
      const common::ObIArray<T*>& other_subquery_exprs, const common::ObIArray<T*>& subquery_exprs, ObRawExpr*& expr);

  template <typename T>
  static int replace_exprs(const common::ObIArray<ObRawExpr*>& other_exprs,
      const common::ObIArray<ObRawExpr*>& new_exprs, common::ObIArray<T*>& exprs);

  static int update_table_id_for_from_item(const common::ObIArray<FromItem>& other_from_items,
      const uint64_t old_table_id, const uint64_t new_table_id, common::ObIArray<FromItem>& from_items);
  static int update_table_id_for_joined_tables(const common::ObIArray<JoinedTable*>& other_joined_tables,
      const uint64_t old_table_id, const uint64_t new_table_id, common::ObIArray<JoinedTable*>& joined_tables);
  static int update_table_id_for_joined_table(const JoinedTable& other_joined_table, const uint64_t old_table_id,
      const uint64_t new_table_id, JoinedTable& joined_table);
  static int update_table_id_for_part_item(const common::ObIArray<ObDMLStmt::PartExprItem>& other_part_items,
      const uint64_t old_table_id, const uint64_t new_table_id, common::ObIArray<ObDMLStmt::PartExprItem>& part_items);
  static int update_table_id_for_part_array(const common::ObIArray<ObDMLStmt::PartExprArray>& other_part_expr_arrays,
      const uint64_t old_table_id, const uint64_t new_table_id,
      common::ObIArray<ObDMLStmt::PartExprArray>& part_expr_arrays);
  static int update_table_id_for_semi_info(const ObIArray<SemiInfo*>& other_semi_infos, const uint64_t old_table_id,
      const uint64_t new_table_id, ObIArray<SemiInfo*>& semi_infos);
  static int update_table_id_for_view_table_id(const common::ObIArray<uint64_t>& other_view_table_id,
      const uint64_t old_table_id, const uint64_t new_table_id, common::ObIArray<uint64_t>& view_table_id);
  static int update_table_id_for_column_item(const common::ObIArray<ColumnItem>& other_column_items,
      const uint64_t old_table_id, const uint64_t new_table_id, const int32_t old_bit_id, const int32_t new_bit_id,
      common::ObIArray<ColumnItem>& column_items);
  static int update_table_id_for_stmt_hint(const ObStmtHint& other_stmt_hint, const uint64_t old_table_id,
      const uint64_t new_table_id, const int32_t old_bit_id, const int32_t new_bit_id, ObStmtHint& stmt_hint);
  static int update_table_id_index(const common::ObIArray<ObPQDistributeIndex>& old_ids, const int32_t old_bit_id,
      const int32_t new_bit_id, common::ObIArray<ObPQDistributeIndex>& new_ids);
  static int update_table_id_index(
      const ObRelIds& old_ids, const int32_t old_bit_id, const int32_t new_bit_id, ObRelIds& new_ids);
  static int update_table_id_index(const common::ObIArray<ObRelIds>& old_ids, const int32_t old_bit_id,
      const int32_t new_bit_id, common::ObIArray<ObRelIds>& new_ids);
  static int update_table_id(const common::ObIArray<uint64_t>& old_ids, const uint64_t old_table_id,
      const uint64_t new_table_id, common::ObIArray<uint64_t>& new_ids);
  static int update_table_id(const common::ObIArray<ObTablesIndex>& old_ids, const uint64_t old_table_id,
      const uint64_t new_table_id, common::ObIArray<ObTablesIndex>& new_ids);

  // for window function related transformatino
  static bool is_valid_type(ObItemType expr_type);

  static int is_expr_query(const ObSelectStmt* stmt, bool& is_expr_type);

  static int is_aggr_query(const ObSelectStmt* stmt, bool& is_aggr_type);

  static int is_ref_outer_block_relation(const ObSelectStmt* stmt, const int32_t level, bool& ref_outer_block_relation);

  static int add_is_not_null(ObTransformerCtx* ctx, ObDMLStmt* stmt, ObRawExpr* child_expr, ObOpRawExpr*& is_not_expr);

  static int is_column_nullable(
      const ObDMLStmt* stmt, ObSchemaChecker* schema_checker, const ObColumnRefRawExpr* col_expr, bool& is_nullable);

  static int flatten_expr(common::ObIArray<ObRawExpr*>& exprs);
  static int flatten_expr(ObRawExpr* expr, common::ObIArray<ObRawExpr*>& flattened_exprs);

  static int check_stmt_output_nullable(const ObSelectStmt* stmt, const ObRawExpr* expr, bool& is_nullable);

  static int find_not_null_expr(ObDMLStmt& stmt, ObRawExpr*& not_null_expr, bool& is_valid);

  static int check_expr_nullable(ObDMLStmt* stmt, ObRawExpr* expr, bool& is_nullable);

  static int check_is_not_null_column(const ObDMLStmt* stmt, const ObRawExpr* expr, bool& col_not_null);

  static int find_null_reject_exprs(const ObIArray<ObRawExpr*>& conditions, const ObIArray<ObRawExpr*>& exprs,
      ObIArray<ObRawExpr*>& null_reject_exprs);
  /**
   * @brief has_null_reject_condition
   * check is there null-reject conditions for the expr
   */
  static int has_null_reject_condition(
      const ObIArray<ObRawExpr*>& conditions, const ObRawExpr* expr, bool& has_null_reject);

  static int is_null_reject_conditions(
      const ObIArray<ObRawExpr*>& conditions, const ObRelIds& target_table, bool& is_null_reject);

  /**
   * @brief is_null_reject_condition
   * check the result of the condition
   * 1. when targets are null, condition return null;
   * 2. when targets are null, condition return false
   */
  static int is_null_reject_condition(
      const ObRawExpr* condition, const ObIArray<const ObRawExpr*>& targets, bool& is_null_reject);

  static int is_simple_null_reject(
      const ObRawExpr* condition, const ObIArray<const ObRawExpr*>& targets, bool& is_null_reject);

  static int is_null_propagate_expr(const ObRawExpr* expr, const ObIArray<ObRawExpr*>& targets, bool& bret);

  static int is_null_propagate_expr(const ObRawExpr* expr, const ObIArray<const ObRawExpr*>& targets, bool& bret);

  static int find_expr(const ObIArray<const ObRawExpr*>& source, const ObRawExpr* target, bool& bret);

  static int find_expr(const ObIArray<OrderItem>& source, const ObRawExpr* target, bool& bret);

  /**
   * @brief is_null_propagate_type
   * white list for null propagate expr
   */
  static bool is_null_propagate_type(const ObItemType type);

  static int get_simple_filter_column(
      ObDMLStmt* stmt, ObRawExpr* expr, int64_t table_id, ObIArray<ObColumnRefRawExpr*>& col_exprs);

  static int get_parent_stmt(
      ObDMLStmt* root_stmt, ObDMLStmt* stmt, ObDMLStmt*& parent_stmt, int64_t& table_id, bool& is_valid);

  static int get_simple_filter_column_in_parent_stmt(ObDMLStmt* root_stmt, ObDMLStmt* stmt, ObDMLStmt* view_stmt,
      int64_t table_id, ObIArray<ObColumnRefRawExpr*>& col_exprs);

  static int get_filter_columns(
      ObDMLStmt* root_stmt, ObDMLStmt* stmt, int64_t table_id, ObIArray<ObColumnRefRawExpr*>& col_exprs);

  static int check_column_match_index(ObDMLStmt* root_stmt, ObDMLStmt* stmt, ObSqlSchemaGuard* schema_guard,
      const ObColumnRefRawExpr* col_expr, bool& is_match);

  static int check_select_item_match_index(
      ObDMLStmt* root_stmt, ObSelectStmt* stmt, ObSqlSchemaGuard* schema_guard, int64_t sel_index, bool& is_match);

  static int is_match_index(ObSqlSchemaGuard* schema_guard, const ObDMLStmt* stmt, const ObColumnRefRawExpr* col_expr,
      bool& is_match, EqualSets* equal_sets = NULL, ObIArray<ObColumnRefRawExpr*>* col_exprs = NULL);

  static int is_match_index(const ObDMLStmt* stmt, const ObIArray<uint64_t>& index_cols,
      const ObColumnRefRawExpr* col_expr, bool& is_match, EqualSets* equal_sets = NULL,
      ObIArray<ObColumnRefRawExpr*>* col_exprs = NULL);

  static int extract_query_ref_expr(ObIArray<ObRawExpr*>& exprs, ObIArray<ObQueryRefRawExpr*>& subqueries);

  static int extract_query_ref_expr(ObRawExpr* expr, ObIArray<ObQueryRefRawExpr*>& subqueries);

  static int extract_aggr_expr(int32_t expr_level, ObIArray<ObRawExpr*>& exprs, ObIArray<ObAggFunRawExpr*>& aggrs);

  static int extract_aggr_expr(int32_t expr_level, ObRawExpr* expr, ObIArray<ObAggFunRawExpr*>& aggrs);

  static int extract_winfun_expr(ObIArray<ObRawExpr*>& exprs, ObIArray<ObWinFunRawExpr*>& win_exprs);

  static int extract_winfun_expr(ObRawExpr* expr, ObIArray<ObWinFunRawExpr*>& win_exprs);

  /**
   * @brief check_foreign_primary_join
   * check whether first-table join second-table is a primary-foreign join
   *
   * @param first_exprs             first table's join keys
   * @param second_exprs            second table's join keys
   * @param is_foreign_primary_join is a primary-foreign join
   * @param is_first_table_parent   first_table is the parent table or not
   */
  static int check_foreign_primary_join(const TableItem* first_table, const TableItem* second_table,
      const ObIArray<const ObRawExpr*>& first_exprs, const ObIArray<const ObRawExpr*>& second_exprs,
      ObSchemaChecker* schema_checker, bool& is_foreign_primary_join, bool& is_first_table_parent,
      share::schema::ObForeignKeyInfo*& foreign_key_info);
  static int check_foreign_primary_join(const TableItem* first_table, const TableItem* second_table,
      const ObIArray<ObRawExpr*>& first_exprs, const ObIArray<ObRawExpr*>& second_exprs,
      ObSchemaChecker* schema_checker, bool& is_foreign_primary_join, bool& is_first_table_parent,
      share::schema::ObForeignKeyInfo*& foreign_key_info);

  /**
   * @brief is_all_foreign_key_involved
   * check whether child_exprs and parents exprs covers all foreign-key columns
   *
   * e.g. t2 has two foreign key constraints:
   *                        foreign key (c1, c2) references t1(c1, c2)
   *                        foreign key (c3, c4) references t1(c1, c2)
   * the value of child_exprs and parent_exprs can be one of the followings:
   *     1. child_exprs = [c1, c2] and parent_exprs = [c1, c2]
   *     2. child_exprs = [c3, c4] and parent_exprs = [c1, c2]
   *
   */
  static int is_all_foreign_key_involved(const ObIArray<const ObRawExpr*>& child_exprs,
      const ObIArray<const ObRawExpr*>& parent_exprs, const share::schema::ObForeignKeyInfo& info,
      bool& is_all_involved);
  static int is_all_foreign_key_involved(const ObIArray<ObRawExpr*>& child_exprs,
      const ObIArray<ObRawExpr*>& parent_exprs, const share::schema::ObForeignKeyInfo& info, bool& is_all_involved);

  /**
   * @brief is_foreign_key_rely
   * check whether the foreign key constraint is reliable
   */
  static int is_foreign_key_rely(
      ObSQLSessionInfo* session_info, const share::schema::ObForeignKeyInfo* foreign_key_info, bool& is_rely);

  /**
   * @brief check_exprs_unique
   * given the output of a table, check whether exprs are unique
   *
   * @param conditions: potential null-reject conditions
   * @param is_unique: is exprs unique or not
   */
  static int check_exprs_unique(ObDMLStmt& stmt, TableItem* table, const ObIArray<ObRawExpr*>& exprs,
      const ObIArray<ObRawExpr*>& conditions, ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker,
      bool& is_unique);

  static int check_exprs_unique(ObDMLStmt& stmt, TableItem* table, const ObIArray<ObRawExpr*>& exprs,
      ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker, bool& is_unique);

  /**
   * @brief check_exprs_unique_on_table_items
   * given the join results of multi table_items, check whether exprs are unique or not
   * @param stmt
   * @param table_items: tables assoicated with join
   * @param exprs
   * @param conditions: potential join predicates
   * @param is_strict: null value is consider to be differnt or not
   * @param is_unique: unique or not
   */
  static int check_exprs_unique_on_table_items(ObDMLStmt* stmt, ObSQLSessionInfo* session_info,
      ObSchemaChecker* schema_checker, const ObIArray<TableItem*>& table_items, const ObIArray<ObRawExpr*>& exprs,
      const ObIArray<ObRawExpr*>& conditions, bool is_strict, bool& is_unique);

  /**
   * @brief check_stmt_unique
   * given a stmt, check whether its output is unique or not
   * @param stmt
   * @param is_strict: null is consider to be differnt or not
   * @param is_unique: unique or not
   */
  static int check_stmt_unique(ObSelectStmt* stmt, ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker,
      const bool is_strict, bool& is_unique);

  static int check_stmt_unique(ObSelectStmt* stmt, ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker,
      const ObIArray<ObRawExpr*>& exprs, const bool is_strict, bool& is_unique,
      const uint64_t extra_flag = FLAGS_DEFAULT);

  /**
   * @brief compute_stmt_property
   * compute stmt output properties, i.e. fd_sets, equal_sets, const_exprs
   * @param stmt
   * @param res_info results
   * @param extra_flag whether ignore distinct/groupby
   */
  static int compute_stmt_property(ObSelectStmt* stmt, UniqueCheckHelper& check_helper, UniqueCheckInfo& res_info,
      const uint64_t extra_flags = FLAGS_DEFAULT);

  static int compute_set_stmt_property(ObSelectStmt* stmt, UniqueCheckHelper& check_helper, UniqueCheckInfo& res_info,
      const uint64_t extra_flags = FLAGS_DEFAULT);

  static int compute_path_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, UniqueCheckInfo& res_info);

  static int compute_tables_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper,
      const ObIArray<TableItem*>& table_items, const ObIArray<ObRawExpr*>& conditions, UniqueCheckInfo& res_info);

  static int compute_table_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, TableItem* table,
      ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info);

  static int compute_basic_table_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, TableItem* table,
      ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info);

  static int compute_generate_table_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, TableItem* table,
      ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info);

  static int compute_inner_join_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, JoinedTable* table,
      ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info);

  static int compute_inner_join_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, UniqueCheckInfo& left_info,
      UniqueCheckInfo& right_info, ObIArray<ObRawExpr*>& inner_join_cond_exprs, ObIArray<ObRawExpr*>& cond_exprs,
      UniqueCheckInfo& res_info);

  static int compute_outer_join_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, JoinedTable* table,
      ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info);

  static int compute_connect_by_join_property(ObDMLStmt* stmt, UniqueCheckHelper& check_helper, JoinedTable* table,
      ObIArray<ObRawExpr*>& cond_exprs, UniqueCheckInfo& res_info);

  static int get_equal_set_conditions(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
      ObSelectStmt* stmt, ObIArray<ObRawExpr*>& set_exprs, ObIArray<ObRawExpr*>& equal_conds);

  static int get_expr_in_cast(ObIArray<ObRawExpr*>& input_exprs, ObIArray<ObRawExpr*>& output_exprs);
  static ObRawExpr* get_expr_in_cast(ObRawExpr* expr);

  static int extract_table_exprs(const ObDMLStmt& stmt, const ObIArray<ObRawExpr*>& source_exprs,
      const TableItem& target, ObIArray<ObRawExpr*>& exprs);

  static int extract_table_exprs(const ObDMLStmt& stmt, const ObIArray<ObRawExpr*>& source_exprs,
      const ObIArray<TableItem*>& tables, ObIArray<ObRawExpr*>& exprs);

  static int extract_table_exprs(const ObDMLStmt& stmt, const ObIArray<ObRawExpr*>& source_exprs,
      const ObSqlBitSet<>& table_set, ObIArray<ObRawExpr*>& table_exprs);
  static int get_table_joined_exprs(const ObDMLStmt& stmt, const TableItem& source, const TableItem& target,
      const ObIArray<ObRawExpr*>& conditions, ObIArray<ObRawExpr*>& target_exprs, ObSqlBitSet<>& join_source_ids,
      ObSqlBitSet<>& join_target_ids);

  static int get_table_joined_exprs(const ObDMLStmt& stmt, const ObIArray<TableItem*>& sources, const TableItem& target,
      const ObIArray<ObRawExpr*>& conditions, ObIArray<ObRawExpr*>& target_exprs);

  static int get_table_joined_exprs(const ObSqlBitSet<>& source_ids, const ObSqlBitSet<>& target_ids,
      const ObIArray<ObRawExpr*>& conditions, ObIArray<ObRawExpr*>& target_exprs, ObSqlBitSet<>& join_source_ids,
      ObSqlBitSet<>& join_target_ids);

  static int relids_to_table_items(ObDMLStmt* stmt, const ObRelIds& rel_ids, ObIArray<TableItem*>& rel_array);

  static int relids_to_table_ids(ObDMLStmt* stmt, const ObSqlBitSet<>& rel_ids, ObIArray<uint64_t>& table_ids);

  static int get_table_rel_ids(const ObDMLStmt& stmt, const TableItem& target, ObSqlBitSet<>& table_set);

  static int get_table_rel_ids(const ObDMLStmt& stmt, const ObIArray<uint64_t>& table_ids, ObSqlBitSet<>& table_set);

  static int get_table_rel_ids(const ObDMLStmt& stmt, const uint64_t table_id, ObSqlBitSet<>& table_set);

  static int get_table_rel_ids(const ObDMLStmt& stmt, const ObIArray<TableItem*>& tables, ObSqlBitSet<>& table_set);

  static int get_from_item(ObDMLStmt* stmt, TableItem* table, FromItem& from);

  static int is_equal_correlation(ObRawExpr* cond, const int64_t stmt_level, bool& is_valid,
      ObRawExpr** outer_param = NULL, ObRawExpr** inner_param = NULL);

  static int is_semi_join_right_table(const ObDMLStmt& stmt, const uint64_t table_id, bool& is_semi_table);

  /**
   * @brief is_common_comparsion_correlation
   * expr(outer.c) = | <=> | > | >= | < | <= | != expr(inner.c)
   * @return
   */
  static int is_common_comparsion_correlation(ObRawExpr* cond, const int64_t stmt_level, bool& is_valid,
      ObRawExpr** outer_param = NULL, ObRawExpr** inner_param = NULL);

  static int is_correlated_condition(ObRawExpr* left, ObRawExpr* right, const int64_t stmt_level, bool& is_valid);

  static int merge_table_items(ObDMLStmt* stmt, const TableItem* source_table, const TableItem* target_table,
      const ObIArray<int64_t>* output_map);

  static int merge_table_items(ObSelectStmt* source_stmt, ObSelectStmt* target_stmt, const TableItem* source_table,
      const TableItem* target_table, ObIArray<ObRawExpr*>& old_exprs, ObIArray<ObRawExpr*>& new_exprs);

  static int find_parent_expr(ObDMLStmt* stmt, ObRawExpr* target, ObRawExpr*& root, ObRawExpr*& parent);

  static int find_parent_expr(ObRawExpr* expr, ObRawExpr* target, ObRawExpr*& parent);

  static int find_relation_expr(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& targets, ObIArray<ObRawExprPointer>& parents);

  static int generate_not_null_column(ObTransformerCtx* ctx, ObDMLStmt* stmt, ObRawExpr*& not_null_column);

  static int generate_not_null_column(
      ObTransformerCtx* ctx, ObDMLStmt* stmt, TableItem* item, ObRawExpr*& not_null_column);

  static int generate_row_key_column(ObDMLStmt* stmt, ObRawExprFactory& expr_factory,
      const share::schema::ObTableSchema& table_schema, uint64_t table_id, uint64_t row_key_id,
      ObRawExpr*& row_key_expr);

  static int generate_unique_key(
      ObTransformerCtx* ctx, ObDMLStmt* stmt, TableItem* item, ObIArray<ObRawExpr*>& unique_keys);

  static int generate_unique_key(
      ObTransformerCtx* ctx, ObDMLStmt* stmt, ObSqlBitSet<>& ignore_tables, ObIArray<ObRawExpr*>& unique_keys);

  static int extract_column_exprs(ObDMLStmt* stmt, const int64_t stmt_level, const ObSqlBitSet<>& table_set,
      const ObIArray<ObDMLStmt*>& ignore_stmts, common::ObIArray<ObRawExpr*>& columns);

  static int extract_column_exprs(const ObIArray<ObRawExpr*>& exprs, const int64_t stmt_level,
      const ObSqlBitSet<>& table_set, const ObIArray<ObDMLStmt*>& ignore_stmts, common::ObIArray<ObRawExpr*>& columns);

  static int extract_column_exprs(ObRawExpr* expr, const int64_t stmt_level, const ObSqlBitSet<>& table_set,
      const ObIArray<ObDMLStmt*>& ignore_stmts, common::ObIArray<ObRawExpr*>& columns);

  static bool is_subarray(const ObRelIds& table_ids, const common::ObIArray<ObRelIds>& other);

  static int check_loseless_join(ObDMLStmt* stmt, ObTransformerCtx* ctx, TableItem* source_table,
      TableItem* target_table, ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker,
      ObStmtMapInfo& stmt_map_info, bool& is_loseless, EqualSets* input_equal_sets = NULL);

  static int check_relations_containment(ObDMLStmt* stmt, const common::ObIArray<TableItem*>& source_rels,
      const common::ObIArray<TableItem*>& target_rels, common::ObIArray<ObStmtMapInfo>& stmt_map_infos,
      common::ObIArray<int64_t>& rel_map_info, bool& is_contain);

  static int check_table_item_containment(ObDMLStmt* stmt, const TableItem* source_table, const TableItem* target_table,
      ObStmtMapInfo& stmt_map_info, bool& is_contain);

  static int extract_lossless_join_columns(ObDMLStmt* stmt, ObTransformerCtx* ctx, const TableItem* source_table,
      const TableItem* target_table, const ObIArray<int64_t>& output_map, ObIArray<ObRawExpr*>& source_exprs,
      ObIArray<ObRawExpr*>& target_exprs, EqualSets* input_equal_sets = NULL);

  static int extract_lossless_mapping_columns(ObDMLStmt* stmt, const TableItem* source_table,
      const TableItem* target_table, const ObIArray<int64_t>& output_map, ObIArray<ObRawExpr*>& candi_source_exprs,
      ObIArray<ObRawExpr*>& candi_target_exprs);
  static int adjust_agg_and_win_expr(ObSelectStmt* source_stmt, ObRawExpr*& source_expr);

  static int check_group_by_consistent(ObSelectStmt* sel_stmt, bool& is_consistent);

  static int contain_select_ref(ObRawExpr* expr, bool& has);

  static int remove_select_items(ObTransformerCtx* ctx, const uint64_t table_id, ObSelectStmt& child_stmt,
      ObDMLStmt& upper_stmt, ObIArray<ObRawExpr*>& removed_select_exprs);

  static int remove_select_items(ObTransformerCtx* ctx, const uint64_t table_id, ObSelectStmt& child_stmt,
      ObDMLStmt& upper_stmt, ObSqlBitSet<>& removed_idxs);

  static int remove_select_items(ObTransformerCtx* ctx, ObSelectStmt& union_stmt, ObSqlBitSet<>& removed_idxs);

  static int create_dummy_select_item(ObSelectStmt& stmt, ObTransformerCtx* ctx);

  static int remove_column_if_no_ref(ObSelectStmt& stmt, ObIArray<ObRawExpr*>& removed_exprs);

  static int create_union_stmt(
      ObTransformerCtx* ctx, const bool is_distinct, ObIArray<ObSelectStmt*>& child_stmts, ObSelectStmt*& union_stmt);

  static int create_union_stmt(ObTransformerCtx* ctx, const bool is_distinct, ObSelectStmt* left_stmt,
      ObSelectStmt* right_stmt, ObSelectStmt*& union_stmt);

  static int create_simple_view(
      ObTransformerCtx* ctx, ObDMLStmt* stmt, ObSelectStmt*& view_stmt, bool push_subquery = true);

  static int adjust_updatable_view(ObRawExprFactory& expr_factory, ObDelUpdStmt* stmt, TableItem& view_table_item);

  static int push_down_groupby(ObTransformerCtx* ctx, ObSelectStmt* stmt, TableItem* view_table);

  static int push_down_vector_assign(
      ObTransformerCtx* ctx, ObUpdateStmt* stmt, ObAliasRefRawExpr* root_expr, TableItem* view_table);

  static int create_stmt_with_generated_table(
      ObTransformerCtx* ctx, ObSelectStmt* child_stmt, ObSelectStmt*& parent_stmt);

  static int create_stmt_with_basic_table(
      ObTransformerCtx* ctx, ObDMLStmt* stmt, TableItem* table, ObSelectStmt*& simple_stmt);

  static int create_stmt_with_joined_table(
      ObTransformerCtx* ctx, ObDMLStmt* stmt, JoinedTable* joined_table, ObSelectStmt*& simple_stmt);

  static int create_view_with_table(ObDMLStmt* stmt, ObTransformerCtx* ctx, TableItem* table, TableItem*& view_table);

  static int pushdown_semi_info_right_filter(ObDMLStmt* stmt, ObTransformerCtx* ctx, SemiInfo* semi_info);

  static int pushdown_semi_info_right_filter(
      ObDMLStmt* stmt, ObTransformerCtx* ctx, SemiInfo* semi_info, ObIArray<ObRawExpr*>& right_filters);

  static int can_push_down_filter_to_table(TableItem& table, bool& can_push);

  static int replace_table_in_stmt(ObDMLStmt* stmt, TableItem* other_table, TableItem* current_table);

  static int remove_tables_from_stmt(ObDMLStmt* stmt, TableItem* table_item, ObIArray<uint64_t>& table_ids);

  static int replace_table_in_semi_infos(ObDMLStmt* stmt, const TableItem* other_table, const TableItem* current_table);

  static int replace_table_in_joined_tables(ObDMLStmt* stmt, TableItem* other_table, TableItem* current_table);
  static int replace_table_in_joined_tables(TableItem* table, TableItem* other_table, TableItem* current_table);

  static int get_from_tables(const ObDMLStmt& stmt, ObRelIds& output_rel_ids);

  static int classify_rownum_conds(
      ObDMLStmt& stmt, ObIArray<ObRawExpr*>& spj_conds, ObIArray<ObRawExpr*>& rownum_conds);

  static int rebuild_select_items(ObSelectStmt& stmt, ObRelIds& output_rel_ids);
  static int replace_columns_and_aggrs(ObRawExpr*& expr, ObTransformerCtx* ctx);

  static int build_const_expr_for_count(ObRawExprFactory& expr_factory, const int64_t value, ObConstRawExpr*& expr);

  static int build_case_when_expr(ObDMLStmt& stmt, ObRawExpr* expr, ObRawExpr* then_expr, ObRawExpr* default_expr,
      ObRawExpr*& out_expr, ObTransformerCtx* ctx);

  static int merge_limit_offset(ObTransformerCtx* ctx, ObRawExpr* view_limit, ObRawExpr* upper_limit,
      ObRawExpr* view_offset, ObRawExpr* upper_offset, ObRawExpr*& limit_expr, ObRawExpr*& offset_expr);

  static int create_dummy_add_zero(ObTransformerCtx* ctx, ObRawExpr*& expr);

  static int get_stmt_limit_value(const ObDMLStmt& stmt, int64_t& limit);

  static int check_limit_value(const ObDMLStmt& stmt, const ParamStore& param_store, ObSQLSessionInfo* session_info,
      ObIAllocator* allocator, int64_t limit, bool& is_equal, bool add_param_constraint = false,
      ObQueryCtx* query_ctx = NULL);

  static int convert_column_expr_to_select_expr(const common::ObIArray<ObRawExpr*>& column_exprs,
      const ObSelectStmt& inner_stmt, common::ObIArray<ObRawExpr*>& select_exprs);

  static int convert_select_expr_to_column_expr(const common::ObIArray<ObRawExpr*>& select_exprs,
      const ObSelectStmt& inner_stmt, ObDMLStmt& outer_stmt, uint64_t table_id,
      common::ObIArray<ObRawExpr*>& column_exprs);

  static int pull_up_subquery(ObDMLStmt* parent_stmt, ObSelectStmt* child_stmt);

  static int right_join_to_left(ObDMLStmt* stmt);

  static int change_join_type(TableItem* joined_table);

  static int get_subquery_expr_from_joined_table(ObDMLStmt* stmt, common::ObIArray<ObQueryRefRawExpr*>& subqueries);

  static int get_on_conditions(ObDMLStmt& stmt, common::ObIArray<ObRawExpr*>& conditions);

  static int get_on_condition(TableItem* table_item, common::ObIArray<ObRawExpr*>& conditions);

  static int get_semi_conditions(ObIArray<SemiInfo*>& semi_infos, ObIArray<ObRawExpr*>& conditions);

  static int set_limit_expr(ObDMLStmt* stmt, ObTransformerCtx* ctx);

  //
  // pushdown_limit_count = NULL == limit_offset
  //                        ? limit_count
  //                        : limit_count + limit_offset
  static int make_pushdown_limit_count(ObRawExprFactory& expr_factory, const ObSQLSessionInfo& session,
      ObRawExpr* limit_count, ObRawExpr* limit_offset, ObRawExpr*& pushdown_limit_count);

  static int recursive_set_stmt_unique(ObSelectStmt* select_stmt, ObTransformerCtx* ctx,
      bool ignore_check_unique = false, common::ObIArray<ObRawExpr*>* unique_keys = NULL);

  static int check_can_set_stmt_unique(ObDMLStmt* stmt, bool& can_set_unique);

  static int get_rel_ids_from_tables(ObDMLStmt* stmt, const ObIArray<TableItem*>& table_items, ObRelIds& rel_ids);

  static int get_rel_ids_from_join_table(ObDMLStmt* stmt, const JoinedTable* joined_table, ObRelIds& rel_ids);

  static int adjust_single_table_ids(JoinedTable* joined_table);

  static int adjust_single_table_ids(TableItem* table, common::ObIArray<uint64_t>& table_ids);

  static int extract_table_items(TableItem* table_item, ObIArray<TableItem*>& table_items);

  static int create_simplie_view(ObTransformerCtx* ctx, ObDMLStmt* stmt, ObSelectStmt*& view_stmt);

  static int set_view_base_item(
      ObDMLStmt* upper_stmt, TableItem* view_table, ObSelectStmt* view_stmt, TableItem* base_table);

  static int reset_stmt_column_item(
      ObDMLStmt* stmt, ObIArray<ColumnItem>& column_items, ObIArray<ObRawExpr*>& column_expr);

  static int get_base_column(ObDMLStmt* stmt, ObColumnRefRawExpr*& col);

  static int get_post_join_exprs(ObDMLStmt* stmt, ObIArray<ObRawExpr*>& exprs, bool with_vector_assign = false);

  static int free_stmt(ObStmtFactory& stmt_factory, ObDMLStmt* stmt);

  static int extract_shared_expr(ObDMLStmt* upper_stmt, ObDMLStmt* child_stmt, ObIArray<ObRawExpr*>& shared_exprs);

  static int extract_stmt_column_contained_expr(
      ObDMLStmt* stmt, int32_t stmt_level, ObIArray<ObRawExpr*>& contain_column_exprs);

  static int extract_column_contained_expr(
      ObRawExpr* expr, int32_t stmt_level, ObIArray<ObRawExpr*>& contain_column_exprs);

  static int check_for_update_validity(ObSelectStmt* stmt);

  static int is_question_mark_pre_param(const ObDMLStmt& stmt, const int64_t param_idx, bool& is_pre_param);

  static int extract_pseudo_column_like_expr(
      ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& pseudo_column_like_exprs);

  static int extract_pseudo_column_like_expr(ObRawExpr* expr, ObIArray<ObRawExpr*>& pseudo_column_like_exprs);

  static int adjust_pseudo_column_like_exprs(ObDMLStmt& stmt);

  static int check_has_rownum(const ObIArray<ObRawExpr*>& exprs, bool& has_rownum);

  static int create_view_with_from_items(ObDMLStmt* stmt, ObTransformerCtx* ctx, TableItem* table_item,
      const ObIArray<ObRawExpr*>& new_select_exprs, const ObIArray<ObRawExpr*>& new_conds, TableItem*& view_table);

  static int add_table_item(ObDMLStmt* stmt, TableItem* table_item);

  static int add_table_item(ObDMLStmt* stmt, ObIArray<TableItem*>& table_items);

  static int replace_having_expr(ObSelectStmt* stmt, ObRawExpr* from, ObRawExpr* to);
  static int check_subquery_match_index(ObTransformerCtx* ctx, ObSelectStmt* subquery, bool& is_match);

  static int get_limit_value(const ObRawExpr* limit_expr, const ObDMLStmt* stmt, const ParamStore* param_store,
      ObSQLSessionInfo* session_info, ObIAllocator* allocator, int64_t& limit_value, bool& is_null_value);

  static int add_const_param_constraints(ObRawExpr* expr, ObQueryCtx* query_ctx, ObTransformerCtx* ctx);

  static int deep_copy_grouping_sets_items(ObRawExprFactory& expr_factory,
      const common::ObIArray<ObGroupingSetsItem>& input_items, common::ObIArray<ObGroupingSetsItem>& output_items,
      const uint64_t copy_types);

  static int deep_copy_multi_rollup_items(ObRawExprFactory& expr_factory,
      const common::ObIArray<ObMultiRollupItem>& input_items, common::ObIArray<ObMultiRollupItem>& output_items,
      const uint64_t copy_types);

  static int replace_stmt_expr_with_groupby_exprs(ObSelectStmt* select_stmt);

  static int replace_with_groupby_exprs(ObSelectStmt* select_stmt, ObRawExpr*& expr);

private:
  static int create_select_item_for_subquery(
      ObSelectStmt& stmt, ObSelectStmt*& child_stmt, ObIAllocator& alloc, ObIArray<ObRawExpr*>& query_ref_exprs);

  static int add_non_duplicated_select_expr(
      ObIArray<ObRawExpr*>& add_select_exprs, ObIArray<ObRawExpr*>& org_select_exprs);
};

template <typename T>
int ObTransformUtils::replace_exprs(const common::ObIArray<ObRawExpr*>& other_exprs,
    const common::ObIArray<ObRawExpr*>& new_exprs, common::ObIArray<T*>& exprs)
{
  int ret = common::OB_SUCCESS;
  common::ObSEArray<T*, 4> temp_expr_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObRawExpr* temp_expr = exprs.at(i);
    if (OB_ISNULL(temp_expr)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs, new_exprs, temp_expr))) {
      SQL_LOG(WARN, "failed to replace expr", K(ret));
    } else if (OB_FAIL(temp_expr_array.push_back(static_cast<T*>(temp_expr)))) {
      SQL_LOG(WARN, "failed to push back expr", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(exprs.assign(temp_expr_array))) {
      SQL_LOG(WARN, "failed to assign expr", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

template <typename T>
int ObTransformUtils::replace_specific_expr(
    const common::ObIArray<T*>& other_exprs, const common::ObIArray<T*>& current_exprs, ObRawExpr*& expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_UNLIKELY(other_exprs.count() != current_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "should have equal column item count", K(other_exprs.count()), K(current_exprs.count()), K(ret));
  } else {
    bool is_find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_find && i < other_exprs.count(); i++) {
      if (OB_ISNULL(other_exprs.at(i)) || OB_ISNULL(current_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "null column expr", K(other_exprs.at(i)), K(current_exprs.at(i)), K(ret));
      } else if (expr == other_exprs.at(i)) {
        is_find = true;
        expr = current_exprs.at(i);
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

#endif /* OCEANBASE_SQL_REWRITE_OB_TRANSFORM_UTILS_H_ */
