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

#ifndef _OB_OPTIMIZER_UTIL_H
#define _OB_OPTIMIZER_UTIL_H
#include "lib/container/ob_array.h"
#include "lib/allocator/ob_allocator.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_opt_est_sel.h"
#include "sql/optimizer/ob_fd_item.h"

namespace oceanbase {
namespace common {
class ObExprCtx;
}
namespace sql {

enum PartitionRelation { NO_COMPATIBLE, COMPATIBLE_SMALL, COMPATIBLE_LARGE, COMPATIBLE_COMMON };

struct MergeKeyInfo {
  MergeKeyInfo(common::ObIAllocator& allocator, int64_t size)
      : need_sort_(false),
        map_array_(allocator, size),
        order_directions_(allocator, size),
        order_exprs_(allocator, size),
        order_items_(allocator, size)
  {}
  virtual ~MergeKeyInfo(){};
  TO_STRING_KV(K_(need_sort), K_(map_array), K_(order_directions), K_(order_exprs), K_(order_items));
  bool need_sort_;
  common::ObFixedArray<int64_t, common::ObIAllocator> map_array_;
  common::ObFixedArray<ObOrderDirection, common::ObIAllocator> order_directions_;
  common::ObFixedArray<ObRawExpr*, common::ObIAllocator> order_exprs_;
  common::ObFixedArray<OrderItem, common::ObIAllocator> order_items_;
};

struct ColumnItem;
struct OrderItem;
class ExprProducer;
class ObOptimizerContext;
class ObShardingInfo;
class ObTablePartitionInfo;
struct SubPlanInfo;
class ObOptimizerUtil {
public:
  static int is_prefix_ordering(const common::ObIArray<OrderItem>& pre, const common::ObIArray<OrderItem>& full,
      const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& const_exprs, bool& is_prefix);

  static int is_prefix_ordering(const common::ObIArray<ObRawExpr*>& pre, const common::ObIArray<OrderItem>& full,
      const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& const_exprs, bool& is_prefix,
      const common::ObIArray<ObOrderDirection>* pre_directions = NULL);

  static int is_prefix_ordering(const common::ObIArray<OrderItem>& pre, const common::ObIArray<ObRawExpr*>& full,
      const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& const_exprs, bool& is_prefix,
      const common::ObIArray<ObOrderDirection>* right_directions = NULL);

  // find common prefix ordering
  static int find_common_prefix_ordering(const common::ObIArray<OrderItem>& left_side,
      const common::ObIArray<ObRawExpr*>& right_side, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& const_exprs, int64_t& left_match_count, int64_t& right_match_count,
      const common::ObIArray<ObOrderDirection>* right_directions = NULL);

  // find common prefix ordering
  static int find_common_prefix_ordering(const common::ObIArray<OrderItem>& left_side,
      const common::ObIArray<OrderItem>& right_side, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& const_exprs, int64_t& left_match_count, int64_t& right_match_count);

  static int is_const_or_equivalent_expr(const common::ObIArray<ObRawExpr*>& exprs, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& const_exprs, const int64_t& pos, bool& is_const);

  static int is_const_or_equivalent_expr(const common::ObIArray<OrderItem>& order_items, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& const_exprs, const int64_t& pos, bool& is_const);

  static int prefix_subset_ids(
      const common::ObIArray<uint64_t>& pre, const common::ObIArray<uint64_t>& full, bool& is_prefix);

  static int prefix_subset_exprs(const common::ObIArray<ObRawExpr*>& exprs,
      const common::ObIArray<ObRawExpr*>& ordering, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& const_exprs, bool& is_covered, int64_t* match_count);
  static int adjust_exprs_by_ordering(common::ObIArray<ObRawExpr*>& exprs, const common::ObIArray<OrderItem>& ordering,
      const EqualSets& equal_sets, const common::ObIArray<ObRawExpr*>& const_exprs, bool& ordering_used,
      common::ObIArray<ObOrderDirection>& directions, common::ObIArray<int64_t>* match_map = NULL);

  static int adjust_exprs_by_mapping(const common::ObIArray<ObRawExpr*>& exprs,
      const common::ObIArray<int64_t>& match_map, common::ObIArray<ObRawExpr*>& adjusted_exprs);

  static bool is_same_ordering(const common::ObIArray<OrderItem>& ordering1,
      const common::ObIArray<OrderItem>& ordering2, const EqualSets& equal_sets);

  static bool is_expr_equivalent(const ObRawExpr* from, const ObRawExpr* to, const EqualSets& equal_sets);

  static int is_const_expr(const ObRawExpr* expr, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& const_exprs, bool& is_const);

  static int get_non_const_expr_size(const ObIArray<ObRawExpr*>& exprs, const EqualSets& equal_sets,
      const common::ObIArray<ObRawExpr*>& const_exprs, int64_t& number);
  static int extract_target_level_query_ref_expr(ObIArray<ObQueryRefRawExpr*>& exprs, const int64_t level,
      const ObIArray<ObQueryRefRawExpr*>& ignore_exprs, ObIArray<ObRawExpr*>& subqueries);
  static int extract_target_level_query_ref_expr(ObIArray<ObRawExpr*>& exprs, const int64_t level,
      const ObIArray<ObQueryRefRawExpr*>& ignore_exprs, ObIArray<ObRawExpr*>& subqueries);
  static int extract_target_level_query_ref_expr(ObRawExpr* expr, const int64_t level,
      const ObIArray<ObQueryRefRawExpr*>& ignore_exprs, ObIArray<ObRawExpr*>& subqueries);

  static bool is_sub_expr(const ObRawExpr* sub_expr, const ObRawExpr* expr);
  static bool is_sub_expr(const ObRawExpr* sub_expr, const ObIArray<ObRawExpr*>& exprs);
  static bool is_sub_expr(const ObRawExpr* sub_expr, ObRawExpr*& expr, ObRawExpr**& addr_matched_expr);
  static bool is_point_based_sub_expr(const ObRawExpr* sub_expr, const ObRawExpr* expr);
  static bool overlap_exprs(const common::ObIArray<ObRawExpr*>& exprs1, const common::ObIArray<ObRawExpr*>& exprs2);
  static bool subset_exprs(const common::ObIArray<OrderItem>& sub_exprs, const common::ObIArray<ObRawExpr*>& exprs);

  static bool subset_exprs(const common::ObIArray<ObRawExpr*>& sub_exprs, const common::ObIArray<ObRawExpr*>& exprs,
      const EqualSets& equal_sets);
  static bool subset_exprs(const common::ObIArray<ObRawExpr*>& sub_exprs, const common::ObIArray<ObRawExpr*>& exprs);
  static int prefix_subset_exprs(const common::ObIArray<ObRawExpr*>& sub_exprs, const uint64_t subexpr_prefix_count,
      const common::ObIArray<ObRawExpr*>& exprs, const uint64_t expr_prefix_count, bool& is_subset);
  static bool subset_exprs(const common::ObIArray<OrderItem>& sort_keys, const common::ObIArray<ObRawExpr*>& exprs2,
      const EqualSets& equal_sets);

  static int intersect_exprs(ObIArray<ObRawExpr*>& first, ObIArray<ObRawExpr*>& right, ObIArray<ObRawExpr*>& result);

  static int except_exprs(ObIArray<ObRawExpr*>& first, ObIArray<ObRawExpr*>& right, ObIArray<ObRawExpr*>& result);

  static int copy_exprs(
      ObRawExprFactory& expr_factory, const common::ObIArray<ObRawExpr*>& src, common::ObIArray<ObRawExpr*>& dst);

  // for topk, non terminal expr with agg params need to be deep copyed to prevent sum being replaced
  // with sum(sum)
  static int clone_expr_for_topk(ObRawExprFactory& expr_factory, ObRawExpr* src, ObRawExpr*& dst);
  static int copy_sort_keys(const common::ObIArray<OrderItem>& src, common::ObIArray<OrderItem>& dst);
  static int copy_sort_keys(const common::ObIArray<ObRawExpr*>& src, common::ObIArray<OrderItem>& dst);
  static int copy_sort_keys(const common::ObIArray<ObRawExpr*>& src,
      const common::ObIArray<ObOrderDirection>& directions, common::ObIArray<OrderItem>& dst);
  template <class T>
  static int remove_item(common::ObIArray<T>& items, const T& item, bool* removed = NULL);

  template <class T>
  static int remove_item(common::ObIArray<T>& items, const common::ObIArray<T>& rm_items, bool* removed = NULL);

  template <class T, class E>
  static bool find_item(const common::ObIArray<T>& items, E item, int64_t* idx = NULL)
  {
    bool bret = false;
    for (int64_t i = 0; !bret && i < items.count(); ++i) {
      bret = (items.at(i) == item);
      if (NULL != idx && bret) {
        *idx = i;
      }
    }
    return bret;
  }

  template <class T>
  static int intersect(const ObIArray<T>& first, const ObIArray<T>& second, ObIArray<T>& third);

  template <class T>
  static bool overlap(const ObIArray<T>& first, const ObIArray<T>& second);

  template <class T>
  static bool is_subset(const ObIArray<T>& first, const ObIArray<T>& second);

  static bool find_equal_expr(const common::ObIArray<ObRawExpr*>& exprs, const ObRawExpr* expr)
  {
    int64_t dummy_index = -1;
    EqualSets dummy_sets;
    return find_equal_expr(exprs, expr, dummy_sets, dummy_index);
  }

  static bool find_equal_expr(const common::ObIArray<ObRawExpr*>& exprs, const ObRawExpr* expr, int64_t& idx)
  {
    EqualSets dummy_sets;
    return find_equal_expr(exprs, expr, dummy_sets, idx);
  }

  static bool find_equal_expr(
      const common::ObIArray<ObRawExpr*>& exprs, const ObRawExpr* expr, const EqualSets& equal_sets)
  {
    int64_t dummy_index = -1;
    return find_equal_expr(exprs, expr, equal_sets, dummy_index);
  }

  static bool find_equal_expr(
      const common::ObIArray<ObRawExpr*>& exprs, const ObRawExpr* expr, const EqualSets& equal_sets, int64_t& idx);

  static bool find_equal_expr(
      const common::ObIArray<ObRawExpr*>& exprs, const ObRawExpr* expr, ObExprParamCheckContext& context)
  {
    int64_t dummy_index = -1;
    return find_equal_expr(exprs, expr, context, dummy_index);
  }

  static bool find_equal_expr(
      const common::ObIArray<ObRawExpr*>& exprs, const ObRawExpr* expr, ObExprParamCheckContext& context, int64_t& idx);

  static int find_stmt_expr_direction(const ObDMLStmt& stmt, const common::ObIArray<ObRawExpr*>& exprs,
      const EqualSets& equal_sets, common::ObIArray<ObOrderDirection>& directions);
  static int find_stmt_expr_direction(
      const ObDMLStmt& select_stmt, const ObRawExpr* expr, const EqualSets& equal_sets, ObOrderDirection& direction);
  static bool find_expr_direction(const common::ObIArray<OrderItem>& exprs, const ObRawExpr* expr,
      const EqualSets& equal_sets, ObOrderDirection& direction);
  static uint64_t hash_expr(ObRawExpr* expr, uint64_t seed)
  {
    return expr == NULL ? seed : common::do_hash(*expr, seed);
  }
  template <typename T>
  static uint64_t hash_exprs(uint64_t seed, const common::ObIArray<T>& expr_array);

  static uint64_t hash_array(uint64_t seed, const common::ObIArray<uint64_t>& data_array);

  /**
   *  Check if the ctx contains the specified expr
   */
  static bool find_expr(common::ObIArray<ExprProducer>* ctx, const ObRawExpr& expr);

  static bool find_expr(common::ObIArray<ExprProducer>* ctx, const ObRawExpr& expr, ExprProducer*& producer);

  static int classify_equal_conds(const common::ObIArray<ObRawExpr*>& conds, common::ObIArray<ObRawExpr*>& normal_conds,
      common::ObIArray<ObRawExpr*>& nullsafe_conds);

  static int get_equal_keys(const common::ObIArray<ObRawExpr*>& exprs, const ObRelIds& left_table_sets,
      common::ObIArray<ObRawExpr*>& left_keys, common::ObIArray<ObRawExpr*>& right_keys);
  static ObRawExpr* find_exec_param(
      const common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params_, const int64_t param_num);

  static int64_t find_exec_param(const common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params, const ObRawExpr* expr);

  static ObRawExpr* find_param_expr(const common::ObIArray<ObRawExpr*>& exprs, const int64_t param_num);

  static int extract_equal_exec_params(const common::ObIArray<ObRawExpr*>& exprs,
      const common::ObIArray<std::pair<int64_t, ObRawExpr*>>& params, common::ObIArray<ObRawExpr*>& left_key,
      common::ObIArray<ObRawExpr*>& right_key);
  /**
   * extract all params from the input expr
   * @param expr
   * @param params
   * @return
   */
  static int extract_params(ObRawExpr* expr, common::ObIArray<ObRawExpr*>& params);

  static int extract_params(common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<ObRawExpr*>& params);

  static int add_col_ids_to_set(const common::ObIArray<ObRawExpr*>& exprs, common::ObBitSet<>& bitset);

  static int add_col_ids_to_set(const ObRawExpr* expr, common::ObBitSet<>& bitset, bool check_single_col = false,
      bool restrict_table_id = false, uint64_t target_table_id = common::OB_INVALID_ID);

  static int check_if_column_ids_covered(const common::ObIArray<ObRawExpr*>& exprs, const common::ObBitSet<>& colset,
      bool restrict_table_id, uint64_t target_table_id, common::ObIArray<bool>& result);

  static int check_if_column_ids_covered(const ObRawExpr* expr, const common::ObBitSet<>& colset,
      bool restrict_table_id, uint64_t target_table_id, bool& result);

  static int generate_rowkey_column_items(ObDMLStmt* stmt, ObRawExprFactory& expr_factory, uint64_t table_id,
      const share::schema::ObTableSchema& index_table_schema, common::ObIArray<ColumnItem>& index_columns);
  static int generate_rowkey_exprs(ObDMLStmt* stmt, ObOptimizerContext& opt_ctx, const uint64_t table_id,
      const uint64_t ref_table_id, common::ObIArray<ObRawExpr*>& keys, common::ObIArray<ObRawExpr*>& ordering);
  static int generate_rowkey_exprs(ObDMLStmt* stmt, ObRawExprFactory& expr_factory, uint64_t table_id,
      const share::schema::ObTableSchema& index_table_schema, common::ObIArray<ObRawExpr*>& index_keys,
      common::ObIArray<ObRawExpr*>& index_ordering);

  static int generate_column_exprs(ObDMLStmt* stmt, ObOptimizerContext& opt_ctx, const uint64_t table_id,
      const uint64_t ref_table_id, const ObIArray<uint64_t>& column_ids, ObIArray<ObRawExpr*>& column_exprs);

  static int build_range_columns(
      const ObDMLStmt* stmt, common::ObIArray<ObRawExpr*>& rowkeys, common::ObIArray<ColumnItem>& range_columns);

  static int check_filter_before_indexback(uint64_t index_id, share::schema::ObSchemaGetterGuard* schema_guard,
      const common::ObIArray<ObRawExpr*>& filters, bool restrict_table_id, uint64_t target_table_id,
      common::ObIArray<bool>& filter_before_ib);

  // generate expr and column item for column.
  //
  static int generate_rowkey_expr(ObDMLStmt* stmt, ObRawExprFactory& expr_factory, const uint64_t& table_id,
      const share::schema::ObColumnSchemaV2& column_schema, ObColumnRefRawExpr*& rowkey,
      common::ObIArray<ColumnItem>* column_items = NULL);

  /// for diff prefix filters and postfix filters
  enum ColCntType {
    INITED_VALUE = -4,         // only const value
    TABLE_RELATED = -3,        // has column not index_column and primary key
    INDEX_STORE_RELATED = -2,  // has column not index column, but storing column
    MUL_INDEX_COL = -1,        // has more than one index columns in expr not row.some case present not prefix filter
  };
  static int get_subquery_id(const ObDMLStmt* upper_stmt, const ObSelectStmt* stmt, uint64_t& id);

  static int get_child_corresponding_exprs(const ObDMLStmt* upper_stmt, const ObSelectStmt* stmt,
      const common::ObIArray<ObRawExpr*>& exprs, common::ObIArray<ObRawExpr*>& corr_exprs);
  static int get_child_corresponding_exprs(
      const TableItem* table, const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& corr_exprs);

  static int is_table_on_null_side(const ObDMLStmt* stmt, uint64_t table_id, bool& is_on_null_side);

  static int is_table_on_null_side_recursively(
      const TableItem* table_item, uint64_t table_id, bool& found, bool& is_on_null_side);

  static int get_referenced_columns(const ObDMLStmt* stmt, const uint64_t table_id,
      const common::ObIArray<ObRawExpr*>& keys, common::ObIArray<ObRawExpr*>& columns);

  static int get_non_referenced_columns(const ObDMLStmt* stmt, const uint64_t table_id,
      const common::ObIArray<ObRawExpr*>& keys, common::ObIArray<ObRawExpr*>& columns);

  static int is_contain_nl_params(
      const common::ObIArray<ObRawExpr*>& filters, const int64_t max_param_num, bool& is_contain);

  static int extract_parameterized_correlated_filters(const common::ObIArray<ObRawExpr*>& filters,
      const int64_t max_param_num, common::ObIArray<ObRawExpr*>& correlated_filters,
      common::ObIArray<ObRawExpr*>& uncorrelated_filters);

  static int add_parameterized_expr(
      ObRawExpr*& target_expr, ObRawExpr* orig_expr, ObRawExpr* child_expr, int64_t child_idx);

  static int make_sort_keys(const ObIArray<OrderItem>& candi_sort_keys, const ObIArray<ObRawExpr*>& need_sort_exprs,
      ObIArray<OrderItem>& sort_keys);

  static int make_sort_keys(const ObIArray<ObRawExpr*>& candi_sort_exprs,
      const ObIArray<ObOrderDirection>& candi_directions, const ObIArray<ObRawExpr*>& need_sort_exprs,
      ObIArray<OrderItem>& sort_keys);

  static int make_sort_keys(const common::ObIArray<ObRawExpr*>& sort_exprs, const ObOrderDirection direction,
      common::ObIArray<OrderItem>& sort_keys);

  static int make_sort_keys(const common::ObIArray<ObRawExpr*>& sort_exprs,
      const common::ObIArray<ObOrderDirection>& sort_directions, common::ObIArray<OrderItem>& sort_keys);

  static int split_expr_direction(const common::ObIArray<OrderItem>& order_items,
      common::ObIArray<ObRawExpr*>& raw_exprs, common::ObIArray<ObOrderDirection>& directions);
  static int extract_column_ids(const ObRawExpr* expr, const uint64_t table_id, common::ObIArray<uint64_t>& column_ids);

  static int is_const_expr(const ObRawExpr* expr, const common::ObIArray<ObRawExpr*>& conditions, bool& is_const);

  static bool is_const_expr(const ObRawExpr* expr, const int64_t stmt_level = -1);

  static int is_same_table(const ObIArray<OrderItem>& exprs, uint64_t& table_id, bool& is_same);
  static int find_equal_set(
      const ObRawExpr* ordering, const EqualSets& equal_sets, common::ObIArray<const EqualSet*>& found_sets);

  static int extract_row_col_idx_for_in(const common::ObIArray<uint64_t>& column_ids, const int64_t index_col_pos,
      const uint64_t table_id, const ObRawExpr& l_expr, const ObRawExpr& r_expr, common::ObBitSet<>& col_idxs,
      int64_t& min_col_idx, bool& is_table_filter);

  static int extract_row_col_idx(const common::ObIArray<uint64_t>& column_ids, const int64_t index_col_pos,
      const uint64_t table_id, const ObRawExpr& l_expr, const ObRawExpr& r_expr, common::ObBitSet<>& col_idxs,
      int64_t& min_col_idx, bool& is_table_filter);

  static int extract_column_idx(const common::ObIArray<uint64_t>& column_ids, const int64_t index_col_pos,
      const uint64_t table_id, const ObRawExpr* raw_expr, int64_t& col_idx, common::ObBitSet<>& col_idxs,
      const bool is_org_filter = false);

  static bool is_query_range_op(const ObItemType type);

  static int find_table_item(const TableItem* table_item, uint64_t table_id, bool& found);
  static int extract_column_ids(const ObRawExpr* expr, const uint64_t table_id, common::ObBitSet<>& column_ids);

  static int extract_column_ids(
      const common::ObIArray<ObRawExpr*>& exprs, const uint64_t table_id, common::ObIArray<uint64_t>& column_ids);

  static int extract_column_ids(
      const common::ObIArray<ObRawExpr*>& exprs, const uint64_t table_id, common::ObBitSet<>& column_ids);

  static int check_equal_query_ranges(const common::ObIArray<common::ObNewRange*>& ranges, const int64_t prefix_len,
      bool& all_prefix_single, bool& all_full_single);

  static int check_prefix_ranges_count(const common::ObIArray<common::ObNewRange*>& ranges, int64_t& equal_prefix_count,
      int64_t& equal_prefix_null_count, int64_t& range_prefix_count, bool& contain_always_false);

  static int check_prefix_ranges_count(const common::ObIArray<common::ObNewRange>& ranges, int64_t& equal_prefix_count,
      int64_t& equal_prefix_null_count, int64_t& range_prefix_count);

  static int check_prefix_range_count(
      const common::ObNewRange* range, int64_t& equal_prefix_count, int64_t& range_prefix_count);
  static int check_equal_prefix_null_count(
      const common::ObNewRange* range, const int64_t equal_prefix_count, int64_t& equal_prefix_null_count);
  static bool same_partition_exprs(const common::ObIArray<ObRawExpr*>& l_exprs,
      const common::ObIArray<ObRawExpr*>& r_exprs, const EqualSets& equal_sets);
  static int classify_subquery_exprs(const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& subquery_exprs,
      ObIArray<ObRawExpr*>& non_subquery_exprs);
  static int get_subquery_exprs(const ObIArray<ObRawExpr*>& exprs, ObIArray<ObRawExpr*>& subquery_exprs);

  static int check_is_onetime_expr(const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, ObRawExpr* expr,
      ObDMLStmt* stmt, bool& is_onetime_expr);

  static int classify_get_scan_ranges(const common::ObIArray<ObNewRange>& input_ranges, common::ObIAllocator& allocator,
      common::ObIArray<ObNewRange>& get_ranges, common::ObIArray<ObNewRange>& scan_ranges,
      const share::schema::ObTableSchema* table_schema = NULL, const bool should_trans_rowid = false);

  static int is_exprs_unique(const ObIArray<OrderItem>& ordering, const ObIArray<ObFdItem*>& fd_item_set,
      const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& order_unique);

  static int is_exprs_unique(const ObIArray<OrderItem>& ordering, const ObRelIds& all_tables,
      const ObIArray<ObFdItem*>& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
      bool& order_unique);

  static int is_exprs_unique(const ObIArray<ObRawExpr*>& exprs, const ObRelIds& all_tables,
      const ObIArray<ObFdItem*>& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
      bool& is_unique);

  static int is_exprs_unique(const ObIArray<ObRawExpr*>& exprs, const ObIArray<ObFdItem*>& fd_item_set,
      const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_unique);

  static int is_exprs_unique(ObIArray<ObRawExpr*>& extend_exprs, ObRelIds& remain_tables,
      const ObIArray<ObFdItem*>& fd_item_set, ObIArray<ObRawExpr*>& fd_set_parent_exprs, ObSqlBitSet<>& skip_fd,
      const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_unique);

  static int get_fd_set_parent_exprs(const ObIArray<ObFdItem*>& fd_item_set, ObIArray<ObRawExpr*>& fd_set_parent_exprs);

  static int split_child_exprs(const ObFdItem* fd_item, const EqualSets& equal_sets, ObIArray<ObRawExpr*>& exprs,
      ObIArray<ObRawExpr*>& child_exprs);

  static int is_exprs_contain_fd_parent(const ObIArray<ObRawExpr*>& exprs, const ObFdItem& fd_item,
      const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, bool& is_unique);

  static int add_fd_item_set_for_n21_join(ObFdItemFactory& fd_factory, ObIArray<ObFdItem*>& target,
      const ObIArray<ObFdItem*>& source, const ObIArray<ObRawExpr*>& join_exprs, const EqualSets& equal_sets,
      const ObRelIds& right_table_set);

  static int add_fd_item_set_for_n2n_join(
      ObFdItemFactory& fd_factory, ObIArray<ObFdItem*>& target, const ObIArray<ObFdItem*>& source);

  static int enhance_fd_item_set(const ObIArray<ObRawExpr*>& quals, ObIArray<ObFdItem*>& candi_fd_item_set,
      ObIArray<ObFdItem*>& fd_item_set, ObIArray<ObRawExpr*>& not_null_columns);

  static int try_add_fd_item(ObDMLStmt* stmt, ObFdItemFactory& fd_factory, const uint64_t table_id, ObRelIds& tables,
      const share::schema::ObTableSchema* index_schema, const ObIArray<ObRawExpr*>& quals,
      ObIArray<ObRawExpr*>& not_null_columns, ObIArray<ObFdItem*>& fd_item_set, ObIArray<ObFdItem*>& candi_fd_item_set);

  static int convert_subplan_scan_equal_sets(ObIAllocator* allocator, ObRawExprFactory& expr_factory,
      const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt,
      const EqualSets& input_equal_sets, EqualSets& output_equal_sets);

  static int convert_subplan_scan_fd_item_sets(ObFdItemFactory& fd_factory, ObRawExprFactory& expr_factory,
      const EqualSets& equal_sets, const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt,
      const ObFdItemSet& input_fd_item_sets, ObFdItemSet& output_fd_item_sets);

  static int add_fd_item_set_for_left_join(ObFdItemFactory& fd_factory, const ObRelIds& right_tables,
      const ObIArray<ObRawExpr*>& right_join_exprs, const ObIArray<ObRawExpr*>& right_const_exprs,
      const EqualSets& right_equal_sets, const ObIArray<ObFdItem*>& right_fd_item_sets,
      const ObIArray<ObRawExpr*>& all_left_join_exprs, const EqualSets& left_equal_sets,
      const ObIArray<ObFdItem*>& left_fd_item_sets, const ObIArray<ObFdItem*>& left_candi_fd_item_sets,
      ObIArray<ObFdItem*>& fd_item_sets, ObIArray<ObFdItem*>& candi_fd_item_sets);

  static int check_need_sort(const ObIArray<OrderItem>& expected_order_items, const ObIArray<OrderItem>& input_ordering,
      const ObFdItemSet& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
      const bool is_at_most_one_row, bool& need_sort);

  static int check_need_sort(const ObIArray<ObRawExpr*>& expected_order_exprs,
      const ObIArray<ObOrderDirection>* expected_order_directions, const ObIArray<OrderItem>& input_ordering,
      const ObFdItemSet& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
      const bool is_at_most_one_row, bool& need_sort);

  static int decide_sort_keys_for_merge_style_op(const ObIArray<OrderItem>& input_ordering,
      const ObFdItemSet& fd_item_set, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
      const bool is_at_most_one_row, const ObIArray<ObRawExpr*>& merge_exprs,
      const ObIArray<ObOrderDirection>& merge_directions, MergeKeyInfo& merge_key);

  static int flip_op_type(const ObItemType expr_type, ObItemType& rotated_expr_type);

  static int get_rownum_filter_info(
      ObRawExpr* rownum_expr, ObItemType& expr_type, ObRawExpr*& const_expr, bool& is_const_filter);

  static int convert_rownum_filter_as_offset(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
      const ObItemType filter_type, ObRawExpr* const_expr, ObRawExpr*& offset_int_expr);

  static int convert_rownum_filter_as_limit(ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info,
      const ObItemType filter_type, ObRawExpr* const_expr, ObRawExpr*& limit_int_expr);

  // used by
  // 1. rownum <= num_expr => limit int_expr
  // int_expr = floor(num_expr)
  // rownum <= 2.4 => limit 2
  // rownum <= 2   => limit 2
  // 2. rownum > num_expr => offset int_expr
  // rownum > 2.4  => offset 2
  // rownum > 2    => offset 2
  static int floor_number_as_limit_offset_value(
      ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info, ObRawExpr* num_expr, ObRawExpr*& int_expr);

  // used by
  // 1. rownum < num_expr => limit int_expr
  // int_expr = ceill(num_expr) - 1
  // rownum < 2.4  => limit 2
  // rownum < 2    => limit 1
  // 2. rownum >= num_expr => offset int_expr
  // rownum >= 2.4 => offset 2
  // rownum >= 2   => offset 1
  static int ceil_number_as_limit_offset_value(
      ObRawExprFactory& expr_factory, ObSQLSessionInfo* session_info, ObRawExpr* num_expr, ObRawExpr*& int_expr);

  static int get_type_safe_join_exprs(const ObIArray<ObRawExpr*>& join_quals, const ObRelIds& left_tables,
      const ObRelIds& right_tables, ObIArray<ObRawExpr*>& left_exprs, ObIArray<ObRawExpr*>& right_exprs,
      ObIArray<ObRawExpr*>& all_left_exprs, ObIArray<ObRawExpr*>& all_right_exprs);

  static int split_or_qual_on_table(const ObDMLStmt* stmt, ObOptimizerContext& opt_ctx, const ObRelIds& table_ids,
      ObOpRawExpr& or_qual, ObOpRawExpr*& new_expr);

  static int check_push_down_expr(const ObRelIds& table_ids, ObOpRawExpr& or_qual,
      ObIArray<ObSEArray<ObRawExpr*, 16>>& sub_exprs, bool& all_contain);

  static int generate_push_down_expr(const ObDMLStmt* stmt, ObOptimizerContext& opt_ctx,
      ObIArray<ObSEArray<ObRawExpr*, 16>>& sub_exprs, ObOpRawExpr*& new_expr);

  static int simplify_exprs(const ObFdItemSet fd_item_set, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, ObIArray<ObRawExpr*>& root_exprs,
      const ObIArray<ObRawExpr*>& candi_exprs);

  static int simplify_ordered_exprs(const ObFdItemSet& fd_item_set, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, ObIArray<ObRawExpr*>& root_exprs,
      const ObIArray<ObRawExpr*>& candi_exprs);

  static int simplify_ordered_exprs(const ObFdItemSet& fd_item_set, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, ObIArray<OrderItem>& root_items, const ObIArray<OrderItem>& candi_items);

  static int remove_equal_exprs(
      const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& target_exprs, ObIArray<ObRawExpr*>& exprs);

  static int check_subquery_filter(const JoinedTable* table, bool& has);

  static bool has_equal_join_conditions(const ObIArray<ObRawExpr*>& join_conditions);

  static int convert_subplan_scan_expr(ObRawExprFactory& expr_factory, const EqualSets& equal_sets,
      const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt, bool skip_invalid,
      const ObIArray<ObRawExpr*>& input_exprs, common::ObIArray<ObRawExpr*>& output_exprs);

  static int convert_subplan_scan_expr(ObRawExprFactory& expr_factory, const EqualSets& equal_sets,
      const uint64_t table_id, ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt, const ObRawExpr* input_expr,
      ObRawExpr*& output_expr);

  static int replace_subplan_scan_expr(const EqualSets& equal_sets, const uint64_t table_id, ObDMLStmt& parent_stmt,
      const ObSelectStmt& child_stmt, ObRawExpr*& expr);

  static int check_subplan_scan_expr_validity(const EqualSets& equal_sets, const uint64_t table_id,
      ObDMLStmt& parent_stmt, const ObSelectStmt& child_stmt, const ObRawExpr* input_expr, bool& is_valid);

  static int get_parent_stmt_expr(const EqualSets& equal_sets, const uint64_t table_id, ObDMLStmt& parent_stmt,
      const ObSelectStmt& child_stmt, const ObRawExpr* child_expr, ObRawExpr*& parent_expr);

  static int compute_const_exprs(
      const ObIArray<ObRawExpr*>& condition_exprs, const int64_t stmt_level, ObIArray<ObRawExpr*>& const_exprs);

  static int compute_const_exprs(const ObIArray<ObRawExpr*>& condition_exprs, ObIArray<ObRawExpr*>& const_exprs)
  {
    return compute_const_exprs(condition_exprs, -1, const_exprs);
  }

  static int compute_stmt_interesting_order(const ObIArray<OrderItem>& ordering, const ObDMLStmt* stmt,
      const bool in_subplan_scan, EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
      const int64_t check_scope, int64_t& match_info, int64_t* max_prefix_count_ptr = NULL);

  static int is_group_by_match(const ObIArray<OrderItem>& ordering, const ObSelectStmt* select_stmt,
      const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count,
      bool& sort_match);

  static int is_winfunc_match(const ObIArray<OrderItem>& ordering, const ObSelectStmt* select_stmt,
      const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count,
      bool& sort_match, bool& sort_is_required);

  static int is_winfunc_match(const ObIArray<OrderItem>& ordering, const ObSelectStmt* select_stmt,
      const ObWinFunRawExpr* win_expr, const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs,
      int64_t& match_prefix_count, bool& sort_match, bool& sort_is_required);

  static int match_order_by_against_index(const ObIArray<OrderItem>& expect_ordering,
      const ObIArray<OrderItem>& input_ordering, const int64_t input_start_offset, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, bool& full_covered, int64_t& match_count, bool check_direction = true);

  static int is_set_or_distinct_match(const ObIArray<ObRawExpr*>& keys, const ObSelectStmt* select_stmt,
      const EqualSets& equal_sets, const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count,
      bool& sort_match);

  static int is_distinct_match(const ObIArray<ObRawExpr*>& keys, const ObSelectStmt* stmt, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count, bool& sort_match);

  static int is_set_match(const ObIArray<ObRawExpr*>& keys, const ObSelectStmt* stmt, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count, bool& sort_match);

  static int is_order_by_match(const ObIArray<OrderItem>& ordering, const ObDMLStmt* stmt, const EqualSets& equal_sets,
      const ObIArray<ObRawExpr*>& const_exprs, int64_t& match_prefix_count, bool& sort_match);

  static int is_lossless_column_cast(const ObRawExpr* expr, bool& is_lossless);

  static int gen_set_target_list(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
      ObRawExprFactory* expr_factory, ObSelectStmt& left_stmt, ObSelectStmt& right_stmt, ObSelectStmt* select_stmt);

  static int gen_set_target_list(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
      ObRawExprFactory* expr_factory, ObIArray<ObSelectStmt*>& left_stmts, ObIArray<ObSelectStmt*>& right_stmts,
      ObSelectStmt* select_stmt);

  static int gen_set_target_list(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
      ObRawExprFactory* expr_factory, ObSelectStmt* select_stmt);

  static int get_set_res_types(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
      ObIArray<ObSelectStmt*>& child_querys, ObIArray<ObExprResType>& res_types);

  static int try_add_cast_to_set_child_list(ObIAllocator* allocator, ObSQLSessionInfo* session_info,
      ObRawExprFactory* expr_factory, const bool is_distinct, ObIArray<ObSelectStmt*>& left_stmts,
      ObIArray<ObSelectStmt*>& right_stmts, ObIArray<ObExprResType>* res_types);

  static int add_cast_to_set_list(ObSQLSessionInfo* session_info, ObRawExprFactory* expr_factory,
      ObIArray<ObSelectStmt*>& stmts, const ObExprResType& res_type, const int64_t idx);

  static int check_subquery_has_ref_assign_user_var(ObRawExpr* expr, bool& is_has);

  static bool is_param_expr_correspond_subquey(
      const ObConstRawExpr& const_expr, ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs);

  static int pushdown_filter_into_subquery(ObDMLStmt& parent_stmt, ObSelectStmt& subquery, ObOptimizerContext& opt_ctx,
      ObIArray<ObRawExpr*>& pushdown_filters, ObIArray<ObRawExpr*>& candi_filters, ObIArray<ObRawExpr*>& remain_filters,
      bool& can_pushdown);

  static int check_pushdown_filter(ObDMLStmt& parent_stmt, ObSelectStmt& subquery, ObOptimizerContext& opt_ctx,
      ObIArray<ObRawExpr*>& pushdown_filters, ObIArray<ObRawExpr*>& candi_filters,
      ObIArray<ObRawExpr*>& remain_filters);

  static int check_pushdown_filter_overlap_index(ObDMLStmt& stmt, ObOptimizerContext& opt_ctx,
      ObIArray<ObRawExpr*>& pushdown_filters, ObIArray<ObRawExpr*>& candi_filters,
      ObIArray<ObRawExpr*>& remain_filters);

  static int check_pushdown_filter_for_set(ObSelectStmt& parent_stmt, ObSelectStmt& subquery,
      ObIArray<ObRawExpr*>& common_exprs, ObIArray<ObRawExpr*>& pushdown_filters, ObIArray<ObRawExpr*>& candi_filters,
      ObIArray<ObRawExpr*>& remain_filters);

  static int check_pushdown_filter_for_subquery(ObSelectStmt& parent_stmt, ObSelectStmt& subquery,
      ObOptimizerContext& opt_ctx, ObIArray<ObRawExpr*>& common_exprs, ObIArray<ObRawExpr*>& pushdown_filters,
      ObIArray<ObRawExpr*>& candi_filters, ObIArray<ObRawExpr*>& remain_filters);

  static int get_groupby_win_func_common_exprs(
      ObSelectStmt& subquery, ObIArray<ObRawExpr*>& common_exprs, bool& is_valid);

  static int rename_pushdown_filter(const ObDMLStmt& parent_stmt, const ObSelectStmt& subquery, int64_t table_id,
      ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory, ObIArray<ObRawExpr*>& candi_filters,
      ObIArray<ObRawExpr*>& rename_filters);

  static int rename_set_op_pushdown_filter(const ObSelectStmt& parent_stmt, const ObSelectStmt& subquery,
      ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory, ObIArray<ObRawExpr*>& candi_filters,
      ObIArray<ObRawExpr*>& rename_filters);

  static int rename_subquery_pushdown_filter(const ObDMLStmt& parent_stmt, const ObSelectStmt& subquery,
      int64_t table_id, ObSQLSessionInfo* session_info, ObRawExprFactory& expr_factory,
      ObIArray<ObRawExpr*>& candi_filters, ObIArray<ObRawExpr*>& rename_filters);

  static int get_set_op_remain_filter(const ObSelectStmt& stmt, const ObIArray<ObRawExpr*>& child_pushdown_preds,
      ObIArray<ObRawExpr*>& output_pushdown_preds, const bool first_child);

  static int check_is_null_qual(
      const ObEstSelInfo& est_sel_info, const ObRelIds& table_ids, const ObRawExpr* qual, bool& is_null_qual);

  static int check_expr_contain_subquery(const ObIArray<ObRawExpr*>& exprs,
      const ObIArray<std::pair<int64_t, ObRawExpr*>>* onetime_exprs, bool& has_subquery);

  static int check_expr_contain_subquery(
      const ObRawExpr* expr, const ObIArray<std::pair<int64_t, ObRawExpr*>>* onetime_exprs, bool& has_subquery);

  static int check_expr_contain_sharable_subquery(ObRawExpr* expr, int32_t stmt_level,
      const ObIArray<std::pair<int64_t, ObRawExpr*>>& onetime_exprs, const bool ignore_root, bool& contains);

  static bool has_psedu_column(const ObRawExpr& expr);

  static bool has_hierarchical_expr(const ObRawExpr& expr);

  static int check_pushdown_filter_to_base_table(ObLogPlan& plan, const uint64_t table_id,
      const ObIArray<ObRawExpr*>& pushdown_filters, const ObIArray<ObRawExpr*>& restrict_infos, bool& can_pushdown);

private:
  // disallow construct
  ObOptimizerUtil();
  ~ObOptimizerUtil();
};

template <class T>
int ObOptimizerUtil::remove_item(common::ObIArray<T>& items, const T& item,
    bool* removed)  // default null
{
  int ret = common::OB_SUCCESS;
  int64_t N = items.count();
  common::ObArray<T> new_arr;
  bool tmp_removed = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const T& v = items.at(i);
    if (v == item) {
      tmp_removed = true;
    } else if (OB_FAIL(new_arr.push_back(items.at(i)))) {
      SQL_OPT_LOG(WARN, "failed to push back expr to array", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(items.assign(new_arr))) {
      SQL_OPT_LOG(WARN, "failed to reset exprs array", K(ret));
    } else if (NULL != removed) {
      *removed = tmp_removed;
    }
  }
  return ret;
}

template <class T>
int ObOptimizerUtil::remove_item(common::ObIArray<T>& items, const common::ObIArray<T>& rm_items,
    bool* removed)  // default null)
{
  int ret = common::OB_SUCCESS;
  int64_t N = items.count();
  common::ObArray<T> new_arr;
  bool tmp_removed = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (!find_item(rm_items, items.at(i))) {
      if (OB_FAIL(new_arr.push_back(items.at(i)))) {
        SQL_OPT_LOG(WARN, "failed to push back expr to array", K(ret));
      }
    } else {
      tmp_removed = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(items.assign(new_arr))) {
      SQL_OPT_LOG(WARN, "failed to reset exprs array", K(ret));
    } else if (NULL != removed) {
      *removed = tmp_removed;
    }
  }
  return ret;
}

template <class T>
int ObOptimizerUtil::intersect(const ObIArray<T>& first, const ObIArray<T>& second, ObIArray<T>& third)
{
  int ret = common::OB_SUCCESS;
  int64_t N = first.count();
  common::ObSEArray<T, 8> new_arr;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (find_item(second, first.at(i))) {
      if (OB_FAIL(new_arr.push_back(first.at(i)))) {
        SQL_OPT_LOG(WARN, "failed to push back item to array", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(third.assign(new_arr))) {
      SQL_OPT_LOG(WARN, "failed to assign item array", K(ret));
    }
  }
  return ret;
}

template <class T>
bool ObOptimizerUtil::overlap(const ObIArray<T>& first, const ObIArray<T>& second)
{
  bool b_ret = false;
  int64_t N = first.count();
  for (int64_t i = 0; !b_ret && i < N; ++i) {
    if (find_item(second, first.at(i))) {
      b_ret = true;
    }
  }
  return b_ret;
}

template <class T>
bool ObOptimizerUtil::is_subset(const ObIArray<T>& first, const ObIArray<T>& second)
{
  bool b_ret = true;
  int64_t N = first.count();
  for (int64_t i = 0; b_ret && i < N; ++i) {
    if (!find_item(second, first.at(i))) {
      b_ret = false;
    }
  }
  return b_ret;
}

template <typename T>
uint64_t ObOptimizerUtil::hash_exprs(uint64_t seed, const ObIArray<T>& expr_array)
{
  uint64_t hash_value = seed;
  int64_t N = expr_array.count();
  for (int64_t i = 0; i < N; ++i) {
    hash_value = hash_expr(expr_array.at(i), hash_value);
  }
  return hash_value;
}

#define HASH_ARRAY(items, seed)                \
  do {                                         \
    int64_t N = (items).count();               \
    for (int64_t i = 0; i < N; i++) {          \
      (seed) = do_hash((items).at(i), (seed)); \
    }                                          \
  } while (0);

#define HASH_PTR_ARRAY(items, seed)                 \
  do {                                              \
    int64_t N = (items).count();                    \
    for (int64_t i = 0; i < N; i++) {               \
      if (NULL != (items).at(i)) {                  \
        (seed) = do_hash(*((items).at(i)), (seed)); \
      }                                             \
    }                                               \
  } while (0);

}  // namespace sql
}  // namespace oceanbase

#endif
