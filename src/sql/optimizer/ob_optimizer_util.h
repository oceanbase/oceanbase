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
#include "sql/optimizer/ob_fd_item.h"

namespace oceanbase
{
namespace sql
{

enum PartitionRelation
{
  NO_COMPATIBLE,
  COMPATIBLE_SMALL,
  COMPATIBLE_LARGE,
  COMPATIBLE_COMMON
};

struct MergeKeyInfo
{
  MergeKeyInfo(common::ObIAllocator &allocator, int64_t size)
    : need_sort_(false),
      order_needed_(true),
      prefix_pos_(0),
      map_array_(allocator, size),
      order_directions_(allocator, size),
      order_exprs_(allocator, size),
      order_items_(allocator, size) { }
  virtual ~MergeKeyInfo() {};
  int assign(MergeKeyInfo &other);
  TO_STRING_KV(K_(need_sort),
               K_(order_needed),
               K_(prefix_pos),
               K_(map_array),
               K_(order_directions),
               K_(order_exprs),
               K_(order_items));
  bool need_sort_;
  bool order_needed_;
  int64_t prefix_pos_;
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
class OptSelectivityCtx;
class Path;
class ObSharedExprResolver;
class ObOptimizerUtil
{
public:
  static int is_prefix_ordering(const ObIArray<OrderItem> &first,
                                const ObIArray<OrderItem> &second,
                                const EqualSets &equal_sets,
                                const ObIArray<ObRawExpr*> &const_exprs,
                                bool &first_is_prefix,
                                bool &second_is_prefix);

  static int find_common_prefix_ordering(const ObIArray<ObRawExpr *> &left_side,
                                         const ObIArray<ObRawExpr *> &right_side,
                                         const EqualSets &equal_sets,
                                         const ObIArray<ObRawExpr*> &const_exprs,
                                         bool &left_is_prefix,
                                         bool &right_is_prefix,
                                         const ObIArray<ObOrderDirection> *left_directions,
                                         const ObIArray<ObOrderDirection> *right_directions);

  static int is_const_or_equivalent_expr(const common::ObIArray<ObRawExpr *> &exprs,
                                         const EqualSets &equal_sets,
                                         const common::ObIArray<ObRawExpr*> &const_exprs,
                                         const common::ObIArray<ObRawExpr *> &exec_ref_exprs,
                                         const int64_t &pos,
                                         bool &is_const);

  static int is_const_or_equivalent_expr(const common::ObIArray<OrderItem> &order_items,
                                         const EqualSets &equal_sets,
                                         const common::ObIArray<ObRawExpr*> &const_exprs,
                                         const common::ObIArray<ObRawExpr *> &exec_ref_exprs,
                                         const int64_t &pos,
                                         bool &is_const);

  static int prefix_subset_ids(const common::ObIArray<uint64_t> &pre,
                               const common::ObIArray<uint64_t> &full,
                               bool &is_prefix);

  static int prefix_subset_exprs(const common::ObIArray<ObRawExpr *> &exprs,
                                 const common::ObIArray<ObRawExpr *> &ordering,
                                 const EqualSets &equal_sets,
                                 const common::ObIArray<ObRawExpr*> &const_exprs,
                                 bool &is_covered,
                                 int64_t *match_count);
  static int prefix_subset_exprs(const ObIArray<ObRawExpr *> &exprs,
                                 const ObIArray<ObRawExpr *> &ordering,
                                 const EqualSets &equal_sets,
                                 const ObIArray<ObRawExpr*> &const_exprs,
                                 bool &is_prefix);
  static int adjust_exprs_by_ordering(common::ObIArray<ObRawExpr *> &exprs,
                                      const common::ObIArray<OrderItem> &ordering,
                                      const EqualSets &equal_sets,
                                      const common::ObIArray<ObRawExpr*> &const_exprs,
                                      const common::ObIArray<ObRawExpr *> &exec_ref_exprs,
                                      int64_t &prefix_count,
                                      bool &ordering_all_used,
                                      common::ObIArray<ObOrderDirection> &directions,
                                      common::ObIArray<int64_t> *match_map = NULL);

  static int adjust_exprs_by_mapping(const common::ObIArray<ObRawExpr *> &exprs,
                                     const common::ObIArray<int64_t> &match_map,
                                     common::ObIArray<ObRawExpr *> &adjusted_exprs);

  static bool is_same_ordering(const common::ObIArray<OrderItem> &ordering1,
                               const common::ObIArray<OrderItem> &ordering2,
                               const EqualSets &equal_sets);

  static bool in_same_equalset(const ObRawExpr *from,
                               const ObRawExpr *to,
                               const EqualSets &equal_sets);

  static bool is_expr_equivalent(const ObRawExpr *from,
                                 const ObRawExpr *to,
                                 const EqualSets &equal_sets);

  static bool is_expr_equivalent(const ObRawExpr *from,
                                 const ObRawExpr *to);

  static int is_const_expr(const ObRawExpr* expr,
                           const EqualSets &equal_sets,
                           const common::ObIArray<ObRawExpr *> &const_exprs,
                           const common::ObIArray<ObRawExpr *> &exec_ref_exprs,
                           bool &is_const);
  static int is_const_expr_recursively(const ObRawExpr* expr,
                                       const common::ObIArray<ObRawExpr *> &exec_ref_exprs,
                                       bool &is_const);
  static int is_root_expr_const(const ObRawExpr* expr,
                                const EqualSets &equal_sets,
                                const common::ObIArray<ObRawExpr *> &const_exprs,
                                const common::ObIArray<ObRawExpr *> &exec_ref_exprs,
                                bool &is_const);
  static int is_const_expr(const ObRawExpr* expr,
                           const EqualSets &equal_sets,
                           const common::ObIArray<ObRawExpr *> &const_exprs,
                           bool &is_const);

  static int get_non_const_expr_size(const ObIArray<ObRawExpr *> &exprs,
                                     const EqualSets &equal_sets,
                                     const common::ObIArray<ObRawExpr *> &const_exprs,
                                     const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                     int64_t &number);

  static bool is_sub_expr(const ObRawExpr *sub_expr, const ObRawExpr *expr);
  static bool is_sub_expr(const ObRawExpr *sub_expr, const ObIArray<ObRawExpr*> &exprs);
  static bool is_sub_expr(const ObRawExpr *sub_expr, ObRawExpr *&expr, ObRawExpr **&addr_matched_expr);
  static bool is_point_based_sub_expr(const ObRawExpr *sub_expr, const ObRawExpr *expr);
  static bool overlap_exprs(const common::ObIArray<ObRawExpr*> &exprs1,
                            const common::ObIArray<ObRawExpr*> &exprs2);
  static bool subset_exprs(const common::ObIArray<ObRawExpr*> &sub_exprs,
                           const common::ObIArray<ObRawExpr*> &exprs,
                           const EqualSets &equal_sets);
  static bool subset_exprs(const common::ObIArray<ObRawExpr*> &sub_exprs,
                           const common::ObIArray<ObRawExpr*> &exprs);
  static int prefix_subset_exprs(const common::ObIArray<ObRawExpr*> &sub_exprs,
                                 const uint64_t subexpr_prefix_count,
                                 const common::ObIArray<ObRawExpr*> &exprs,
                                 const uint64_t expr_prefix_count,
                                 bool &is_subset);

  static int intersect_exprs(const ObIArray<ObRawExpr *> &first,
                             const ObIArray<ObRawExpr *> &right,
                             const EqualSets &equal_sets,
                             ObIArray<ObRawExpr *> &result);

  static int intersect_exprs(const ObIArray<ObRawExpr *> &first,
                             const ObIArray<ObRawExpr *> &right,
                             ObIArray<ObRawExpr *> &result);

  static int except_exprs(const ObIArray<ObRawExpr *> &first,
                          const ObIArray<ObRawExpr *> &right,
                          ObIArray<ObRawExpr *> &result);

  static bool same_exprs(const common::ObIArray<ObRawExpr*> &src_exprs,
                         const common::ObIArray<ObRawExpr*> &target_exprs,
                         const EqualSets &equal_sets);

  static bool same_exprs(const common::ObIArray<ObRawExpr*> &src_exprs,
                         const common::ObIArray<ObRawExpr*> &target_exprs);

  //for topk, non terminal expr with agg params need to be deep copied to prevent sum being replaced
  //with sum(sum)
  static int clone_expr_for_topk(ObRawExprFactory &expr_factory, ObRawExpr* src, ObRawExpr* &dst);

  template <class T>
  static void revert_items(common::ObIArray<T> &items, const int64_t N);

  template <class T>
  static int remove_item(common::ObIArray<T> &items, const T &item, bool *removed = NULL);

  template <class T>
  static int remove_item(common::ObIArray<T> &items,
                         const common::ObIArray<T> &rm_items,
                         bool *removed = NULL);

  /**
   * @brief find_item
   * 基于指针比较两个表达式是否相同。通常应用在以下两个场景中：
   * 1. 表达式生成过程。判断一个表达式是否在下层已经生成了
   * 2. 比较 column, aggregation, query ref, winfunc 等表达式
   * @param items
   * @param item
   * @param idx
   * @return
   */
  template <class T, class E>
  static bool find_item(const common::ObIArray<T> &items,
                        E item,
                        int64_t *idx = NULL)
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
  static int intersect(const ObIArray<T> &first,
                       const ObIArray<T> &second,
                       ObIArray<T> &third);

  template <class T>
  static int intersect(const ObIArray<ObSEArray<T,4>> &sets,
                       ObIArray<T> &result);

  template <class T>
  static bool overlap(const ObIArray<T> &first,
                      const ObIArray<T> &second);

  template <class T>
  static bool is_subset(const ObIArray<T> &first,
                        const ObIArray<T> &second);
  /**
   * @brief find_equal_expr
   * 基于语义比较两个表达式是否相同。
   * 先比较指针，后比较表达式结构，最后尝试利用 equal sets
   * 主要引用在改写或者计划生成中，判断一个谓词，一个排序/分组表达式是否重复
   * @param exprs
   * @param expr
   * @return
   */
  static bool find_equal_expr(const common::ObIArray<ObRawExpr*> &exprs,
                              const ObRawExpr *expr)
  {
    int64_t idx = -1;
    return find_equal_expr(exprs, expr, idx);
  }

  static bool find_equal_expr(const common::ObIArray<ObRawExpr*> &exprs,
                              const ObRawExpr *expr,
                              int64_t &idx);

  static bool find_equal_expr(const common::ObIArray<ObRawExpr*> &exprs,
                              const ObRawExpr *expr,
                              const EqualSets &equal_sets)
  {
    int64_t dummy_index = -1;
    return find_equal_expr(exprs, expr, equal_sets, dummy_index);
  }

  static bool find_equal_expr(const common::ObIArray<ObRawExpr*> &exprs,
                              const ObRawExpr *expr,
                              const EqualSets &equal_sets,
                              int64_t &idx);

  static int append_exprs_no_dup(ObIArray<ObRawExpr *> &dst, const ObIArray<ObRawExpr *> &src);

  static int find_stmt_expr_direction(const ObDMLStmt &stmt,
                                      const common::ObIArray<ObRawExpr*> &exprs,
                                      const EqualSets &equal_sets,
                                      common::ObIArray<ObOrderDirection> &directions);
  static int find_stmt_expr_direction(const ObDMLStmt &select_stmt,
                                      const ObRawExpr *expr,
                                      const EqualSets &equal_sets,
                                      ObOrderDirection &direction);
  static bool find_expr_direction(const common::ObIArray<OrderItem> &exprs,
                                  const ObRawExpr *expr,
                                  const EqualSets &equal_sets,
                                  ObOrderDirection &direction);
  static uint64_t hash_expr(ObRawExpr *expr, uint64_t seed)
  {
    return expr == NULL ? seed : common::do_hash(*expr, seed);
  }
  template<typename T>
  static uint64_t hash_exprs(uint64_t seed, const common::ObIArray<T> &expr_array);

  static uint64_t hash_array(uint64_t seed, const common::ObIArray<uint64_t> &data_array);

  /**
   *  Check if the ctx contains the specified expr
   */
  static bool find_expr(common::ObIArray<ExprProducer> *ctx, const ObRawExpr &expr);

  static bool find_expr(common::ObIArray<ExprProducer> *ctx, const ObRawExpr &expr, ExprProducer *&producer);

  static int classify_equal_conds(const common::ObIArray<ObRawExpr *> &conds,
                                  common::ObIArray<ObRawExpr *> &normal_conds,
                                  common::ObIArray<ObRawExpr *> &nullsafe_conds);

  static int get_equal_keys(const common::ObIArray<ObRawExpr*> &exprs,
                            const ObRelIds &left_table_sets,
                            common::ObIArray<ObRawExpr*> &left_keys,
                            common::ObIArray<ObRawExpr*> &right_keys,
                            common::ObIArray<bool> &null_safe_info);

  static bool find_exec_param(const common::ObIArray<ObExecParamRawExpr *> &params,
                              const ObExecParamRawExpr *ele);
  static ObRawExpr* find_exec_param(const common::ObIArray<std::pair<int64_t, ObRawExpr*> > &params_,
                                    const int64_t param_num);

  static int64_t find_exec_param(const common::ObIArray<std::pair<int64_t, ObRawExpr*> > &params,
                                 const ObRawExpr* expr);

  static int get_exec_ref_expr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                               ObIArray<ObRawExpr*> &ref_exprs);

  static int extract_equal_exec_params(const ObIArray<ObRawExpr *> &exprs,
                                       const ObIArray<ObExecParamRawExpr *> &my_params,
                                       ObIArray<ObRawExpr *> &left_keys,
                                       common::ObIArray<ObRawExpr*> &right_key,
                                       common::ObIArray<bool> &null_safe_info);

  static int generate_rowkey_exprs(const ObDMLStmt *stmt,
                                   ObOptimizerContext &opt_ctx,
                                   const uint64_t table_id,
                                   const uint64_t ref_table_id,
                                   common::ObIArray<ObRawExpr*> &keys,
                                   common::ObIArray<ObRawExpr*> &ordering);
  static int generate_rowkey_exprs(const ObDMLStmt* stmt,
                                   ObRawExprFactory &expr_factory,
                                   uint64_t table_id,
                                   const share::schema::ObTableSchema &index_table_schema,
                                   common::ObIArray<ObRawExpr*> &index_keys,
                                   common::ObIArray<ObRawExpr*> &index_ordering);

  static int build_range_columns(const ObDMLStmt *stmt,
                                 common::ObIArray<ObRawExpr*> &rowkeys,
                                 common::ObIArray<ColumnItem> &range_columns);

  ///for diff prefix filters and postfix filters
  enum ColCntType
  {
    INITED_VALUE = -4, //only const value
    TABLE_RELATED = -3, //has column not index_column and primary key
    INDEX_STORE_RELATED = -2, //has column not index column, but storing column
    MUL_INDEX_COL = -1, //has more than one index columns in expr not row.some case present not prefix filter
  };
  static int get_subquery_id(const ObDMLStmt *upper_stmt, const ObSelectStmt *stmt, uint64_t &id);

  static int get_child_corresponding_exprs(const ObDMLStmt *upper_stmt,
                                           const ObSelectStmt *stmt,
                                           const common::ObIArray<ObRawExpr*> &exprs,
                                           common::ObIArray<ObRawExpr*> &corr_exprs);
  static int get_child_corresponding_exprs(const TableItem *table,
                                           const ObIArray<ObRawExpr*> &exprs,
                                           ObIArray<ObRawExpr*> &corr_exprs);

  static int is_table_on_null_side(const ObDMLStmt *stmt,
                                   uint64_t table_id,
                                   bool &is_on_null_side);

  static int is_table_on_null_side_recursively(const TableItem *table_item,
                                               uint64_t table_id,
                                               bool &found,
                                               bool &is_on_null_side);

  static int is_table_on_null_side_of_parent(const ObDMLStmt *stmt,
                                             uint64_t source_table_id,
                                             uint64_t target_table_id,
                                             bool &is_on_null_side);
  static int find_common_joined_table(JoinedTable *joined_table,
                                      uint64_t source_table_id,
                                      uint64_t target_table_id,
                                      JoinedTable *&target_joined_table);

  static int get_referenced_columns(const ObDMLStmt *stmt,
                                    const uint64_t table_id,
                                    const common::ObIArray<ObRawExpr*> &keys,
                                    common::ObIArray<ObRawExpr*> &columns);

  static int get_non_referenced_columns(const ObDMLStmt *stmt,
                                        const uint64_t table_id,
                                        const common::ObIArray<ObRawExpr*> &keys,
                                        common::ObIArray<ObRawExpr*> &columns);

  static int extract_parameterized_correlated_filters(const common::ObIArray<ObRawExpr*> &filters,
                                                      common::ObIArray<ObRawExpr*> &correlated_filters,
                                                      common::ObIArray<ObRawExpr*> &uncorrelated_filters);

  static int add_parameterized_expr(ObRawExpr *&target_expr,
                                    ObRawExpr *orig_expr,
                                    ObRawExpr *child_expr,
                                    int64_t child_idx);

  static int get_default_directions(const int64_t direction_num,
                                    ObIArray<ObOrderDirection> &directions);

  static int make_sort_keys(const ObIArray<OrderItem> &candi_sort_keys,
                            const ObIArray<ObRawExpr *> &need_sort_exprs,
                            ObIArray<OrderItem> &sort_keys);

  static int make_sort_keys(const ObIArray<ObRawExpr *> &candi_sort_exprs,
                            const ObIArray<ObOrderDirection> &candi_directions,
                            const ObIArray<ObRawExpr *> &need_sort_exprs,
                            ObIArray<OrderItem> &sort_keys);

  static int make_sort_keys(const common::ObIArray<ObRawExpr*> &sort_exprs,
                            common::ObIArray<OrderItem> &sort_keys);

  static int make_sort_keys(const common::ObIArray<ObRawExpr*> &sort_exprs,
                            const ObOrderDirection direction,
                            common::ObIArray<OrderItem> &sort_keys);

  static int make_sort_keys(const common::ObIArray<ObRawExpr *> &sort_exprs,
                            const common::ObIArray<ObOrderDirection> &sort_directions,
                            common::ObIArray<OrderItem> &sort_keys);

  static int split_expr_direction(const common::ObIArray<OrderItem> &order_items,
                                  common::ObIArray<ObRawExpr*> &raw_exprs,
                                  common::ObIArray<ObOrderDirection> &directions);
  static int get_expr_and_types(const common::ObIArray<OrderItem> &order_items,
                                ObIArray<ObRawExpr*> &order_exprs,
                                ObIArray<ObExprResType> &order_types);
  static int extract_column_ids(const ObRawExpr *expr,
                                const uint64_t table_id,
                                common::ObIArray<uint64_t> &column_ids);

  static int is_const_expr(const ObRawExpr *expr,
                           const common::ObIArray<ObRawExpr *> &conditions,
                           bool &is_const);

  static int is_same_table(
    const ObIArray<OrderItem> &exprs,
    uint64_t &table_id,
    bool &is_same);
  static int find_equal_set(const ObRawExpr* ordering,
                            const EqualSets &equal_sets,
                            common::ObIArray<const EqualSet*> &found_sets);

  /** @brief 获取T_OP_ROW IN T_OP_ROW中对query_range范围产生影响的column
   *  最小的column_idx最为整体filter的column_idx，决定是否是前缀索引
   *  @param [in] column_ids 索引列 + primary key
   *  @param [in] index_col_pos 索引列在range_columns中最大的pos
   *  @param [in] table_id 访问表在stmt中的table_id
   *  @param [in] l_expr, r_expr T_OP_ROW
   *  @param [in/out] col_idxs记录影响query range的column
   *  @param [out] is_table_filter 是否包含不在range columns中的列
   */
  static int extract_row_col_idx_for_in(const common::ObIArray<uint64_t> &column_ids,
                                        const int64_t index_col_pos,
                                        const uint64_t table_id,
                                        const ObRawExpr &l_expr,
                                        const ObRawExpr &r_expr,
                                        common::ObBitSet<> &col_idxs,
                                        int64_t &min_col_idx,
                                        bool &is_table_filter);

  ///@brief 获取T_OP_ROW cmp T_OP_ROW中对query_range range范围产生影响的的column
  ///最小的column_idx最为整体filter的column_idx，决定是否是前缀索引
  ///@param [in] column_ids 索引列 + primary key
  ///@param [in] index_col_pos 索引列在range_columns中最大的pos
  ///@param [in] table_id 访问表在stmt中的table_id
  ///@param [in] l_expr, r_expr T_OP_ROW
  ///@param [in/out] col_idxs记录影响query range的column
  ///@param [out] is_table_filter 是否包含不在range columns中的列
  static int extract_row_col_idx(const common::ObIArray<uint64_t> &column_ids,
                                 const int64_t index_col_pos,
                                 const uint64_t table_id,
                                 const ObRawExpr &l_expr,
                                 const ObRawExpr &r_expr,
                                 common::ObBitSet<> &col_idxs,
                                 int64_t &min_col_idx,
                                 bool &is_table_filter);

  ///@brief 获取filter中包含的column在column_ids中的column_idx.
  ///负值的column_idx意义见类ColCntType
  //@is_org_filter 输入的expr是否是原始的filter,而不是递归的expr
  static int extract_column_idx(const common::ObIArray<uint64_t> &column_ids,
                                const int64_t index_col_pos,
                                const uint64_t table_id,
                                const ObRawExpr *raw_expr,
                                int64_t &col_idx,
                                common::ObBitSet<> &col_idxs,
                                const bool is_org_filter = false);

  static bool is_query_range_op(const ObItemType type);

  static int find_table_item(const TableItem *table_item, uint64_t table_id, bool &found);
  static int extract_column_ids(const ObRawExpr *expr,
                                const uint64_t table_id,
                                common::ObBitSet<> &column_ids);

  static int extract_column_ids(const common::ObIArray<ObRawExpr*> &exprs,
                                const uint64_t table_id,
                                common::ObIArray<uint64_t> &column_ids);

  static int extract_column_ids(const common::ObIArray<ObRawExpr*> &exprs,
                                const uint64_t table_id,
                                common::ObBitSet<> &column_ids);

  static int check_equal_query_ranges(const common::ObIArray<common::ObNewRange*> &ranges,
                                      const int64_t prefix_len,
                                      bool &all_prefix_single,
                                      bool &all_full_single);

  static int check_prefix_ranges_count(const common::ObIArray<common::ObNewRange*> &ranges,
                                       int64_t &equal_prefix_count,
                                       int64_t &equal_prefix_null_count,
                                       int64_t &range_prefix_count,
                                       bool &contain_always_false);

  static int check_prefix_ranges_count(const common::ObIArray<common::ObNewRange> &ranges,
                                       int64_t &equal_prefix_count,
                                       int64_t &equal_prefix_null_count,
                                       int64_t &range_prefix_count);

  static int check_prefix_range_count(const common::ObNewRange* range,
                                      int64_t &equal_prefix_count,
                                      int64_t &range_prefix_count);
  static int check_equal_prefix_null_count(const common::ObNewRange *range,
                                           const int64_t equal_prefix_count,
                                           int64_t &equal_prefix_null_count);
  static bool same_partition_exprs(const common::ObIArray<ObRawExpr *> &l_exprs,
                                   const common::ObIArray<ObRawExpr *> &r_exprs,
                                   const EqualSets &equal_sets);
  static int classify_subquery_exprs(const ObIArray<ObRawExpr*> &exprs,
                                     ObIArray<ObRawExpr*> &subquery_exprs,
                                     ObIArray<ObRawExpr*> &non_subquery_exprs,
                                     const bool with_onetime = true);

  static int get_subquery_exprs(const ObIArray<ObRawExpr*> &exprs,
                                ObIArray<ObRawExpr*> &subquery_exprs,
                                const bool with_onetime = true);

  static int get_onetime_exprs(ObRawExpr* expr,
                               ObIArray<ObExecParamRawExpr*> &onetime_exprs);

  static int get_query_ref_exprs(ObIArray<ObRawExpr *> &subquery_exprs,
                                 ObIArray<ObRawExpr *> &subqueries,
                                 ObIArray<ObRawExpr *> &nested_subqueries);

  static int get_nested_exprs(ObIArray<ObQueryRefRawExpr *> &exprs,
                              ObIArray<ObRawExpr *> &nested_exprs);

  static int classify_get_scan_ranges(const common::ObIArray<ObNewRange> &input_ranges,
                                      common::ObIArray<ObNewRange> &get_ranges,
                                      common::ObIArray<ObNewRange> &scan_ranges);

  static int is_exprs_unique(const ObIArray<OrderItem> &ordering,
                             const ObIArray<ObFdItem *> &fd_item_set,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             bool &order_unique);

  static int is_exprs_unique(const ObIArray<OrderItem> &ordering,
                             const ObRelIds &all_tables,
                             const ObIArray<ObFdItem *> &fd_item_set,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             bool &order_unique);

  static int is_exprs_unique(const ObIArray<ObRawExpr *> &exprs,
                             const ObRelIds &all_tables,
                             const ObIArray<ObFdItem *> &fd_item_set,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             bool &is_unique);

  static int is_exprs_unique(const ObIArray<ObRawExpr *> &exprs,
                             const ObIArray<ObFdItem *> &fd_item_set,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             bool &is_unique);

  static int is_exprs_unique(ObIArray<ObRawExpr *> &extend_exprs,
                             ObRelIds &remain_tables,
                             const ObIArray<ObFdItem *> &fd_item_set,
                             ObIArray<ObRawExpr *> &fd_set_parent_exprs,
                             ObSqlBitSet<> &skip_fd,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             bool &is_unique);

  static int get_fd_set_parent_exprs(const ObIArray<ObFdItem *> &fd_item_set,
                                     ObIArray<ObRawExpr *> &fd_set_parent_exprs);

  /*
   * @param [in] fd_item
   * @param [in] equal_sets
   * @param [in/out] exprs, 分离 exprs 中包含在 fd_item child exprs 中的 expr 到 child_exprs 中
   * @param [in/out] child_exprs
   * */
  static int split_child_exprs(const ObFdItem *fd_item,
                               const EqualSets &equal_sets,
                               ObIArray<ObRawExpr *> &exprs,
                               ObIArray<ObRawExpr *> &child_exprs);

  static int is_expr_is_determined(const ObIArray<ObRawExpr *> &exprs,
                                   const ObFdItemSet &fd_item_set,
                                   const EqualSets &equal_sets,
                                   const ObIArray<ObRawExpr *> &const_exprs,
                                   const ObRawExpr *expr,
                                   bool &is_determined);

  static int is_exprs_contain_fd_parent(const ObIArray<ObRawExpr *> &exprs,
                                        const ObFdItem &fd_item,
                                        const EqualSets &equal_sets,
                                        const ObIArray<ObRawExpr *> &const_exprs,
                                        bool &is_unique);

  static int add_fd_item_set_for_n21_join(ObFdItemFactory &fd_factory,
                                          ObIArray<ObFdItem *> &target,
                                          const ObIArray<ObFdItem *> &source,
                                          const ObIArray<ObRawExpr *> &join_exprs,
                                          const EqualSets &equal_sets,
                                          const ObRelIds &right_table_set);

  static int add_fd_item_set_for_n2n_join(ObFdItemFactory &fd_factory,
                                          ObIArray<ObFdItem *> &target,
                                          const ObIArray<ObFdItem *> &source);

  static int enhance_fd_item_set(const ObIArray<ObRawExpr *> &quals,
                                 ObIArray<ObFdItem *> &candi_fd_item_set,
                                 ObIArray<ObFdItem *> &fd_item_set,
                                 ObIArray<ObRawExpr *> &not_null_columns);

  static int try_add_fd_item(const ObDMLStmt *stmt,
                             ObFdItemFactory &fd_factory,
                             const uint64_t table_id,
                             ObRelIds &tables,
                             const share::schema::ObTableSchema *index_schema,
                             const ObIArray<ObRawExpr *> &quals,
                             ObIArray<ObRawExpr *> &not_null_columns,
                             ObIArray<ObFdItem *> &fd_item_set,
                             ObIArray<ObFdItem *> &candi_fd_item_set);

  static int convert_subplan_scan_equal_sets(ObIAllocator *allocator,
                                             ObRawExprFactory &expr_factory,
                                             const uint64_t table_id,
                                             const ObDMLStmt &parent_stmt,
                                             const ObSelectStmt &child_stmt,
                                             const EqualSets &input_equal_sets,
                                             EqualSets &output_equal_sets);

  static int convert_subplan_scan_fd_item_sets(ObFdItemFactory &fd_factory,
                                               ObRawExprFactory &expr_factory,
                                               const EqualSets &equal_sets,
                                               const ObIArray<ObRawExpr*> &const_exprs,
                                               const uint64_t table_id,
                                               const ObDMLStmt &parent_stmt,
                                               const ObSelectStmt &child_stmt,
                                               const ObFdItemSet &input_fd_item_sets,
                                               ObFdItemSet &output_fd_item_sets);

  static int convert_subplan_scan_fd_parent_exprs(ObRawExprFactory &expr_factory,
                                                  const EqualSets &equal_sets,
                                                  const ObIArray<ObRawExpr*> &const_exprs,
                                                  const uint64_t table_id,
                                                  const ObDMLStmt &parent_stmt,
                                                  const ObSelectStmt &child_stmt,
                                                  const ObIArray<ObRawExpr*> &input_exprs,
                                                  ObIArray<ObRawExpr*> &output_exprs);

  static int add_fd_item_set_for_left_join(ObFdItemFactory &fd_factory,
                                           const ObRelIds &right_tables,
                                           const ObIArray<ObRawExpr*> &right_join_exprs,
                                           const ObIArray<ObRawExpr*> &right_const_exprs,
                                           const EqualSets &right_equal_sets,
                                           const ObIArray<ObFdItem *> &right_fd_item_sets,
                                           const ObIArray<ObRawExpr*> &all_left_join_exprs,
                                           const EqualSets &left_equal_sets,
                                           const ObIArray<ObFdItem *> &left_fd_item_sets,
                                           const ObIArray<ObFdItem *> &left_candi_fd_item_sets,
                                           ObIArray<ObFdItem *> &fd_item_sets,
                                           ObIArray<ObFdItem *> &candi_fd_item_sets);

  static int check_need_sort(const ObIArray<OrderItem> &expected_order_items,
                             const ObIArray<OrderItem> &input_ordering,
                             const ObFdItemSet &fd_item_set,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             const ObIArray<ObRawExpr *> &exec_ref_exprs,
                             const bool is_at_most_one_row,
                             bool &need_sort,
                             int64_t &prefix_pos,
                             const int64_t part_cnt,
                             const bool check_part_only = false);

  static int check_need_sort(const ObIArray<OrderItem> &expected_order_items,
                             const ObIArray<OrderItem> &input_ordering,
                             const ObFdItemSet &fd_item_set,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             const ObIArray<ObRawExpr *> &exec_ref_exprs,
                             const bool is_at_most_one_row,
                             bool &need_sort,
                             int64_t &prefix_pos);

  static int check_need_sort(const ObIArray<ObRawExpr*> &expected_order_exprs,
                             const ObIArray<ObOrderDirection> *expected_order_directions,
                             const ObIArray<OrderItem> &input_ordering,
                             const ObFdItemSet &fd_item_set,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             const ObIArray<ObRawExpr *> &exec_ref_exprs,
                             const bool is_at_most_one_row,
                             bool &need_sort,
                             int64_t &prefix_pos);

  static int check_need_sort(const ObIArray<ObRawExpr*> &expected_order_exprs,
                             const ObIArray<ObOrderDirection> *expected_order_directions,
                             const ObIArray<OrderItem> &input_ordering,
                             const ObFdItemSet &fd_item_set,
                             const EqualSets &equal_sets,
                             const ObIArray<ObRawExpr *> &const_exprs,
                             const ObIArray<ObRawExpr *> &exec_ref_exprs,
                             const bool is_at_most_one_row,
                             bool &need_sort,
                             int64_t &prefix_pos,
                             const int64_t part_cnt,
                             const bool check_part_only = false);

  static int decide_sort_keys_for_merge_style_op(const ObDMLStmt *stmt,
                                                 const EqualSets &stmt_equal_sets,
                                                 const ObIArray<OrderItem> &input_ordering,
                                                 const ObFdItemSet &fd_item_set,
                                                 const EqualSets &equal_sets,
                                                 const ObIArray<ObRawExpr*> &const_exprs,
                                                 const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                                 const bool is_at_most_one_row,
                                                 const ObIArray<ObRawExpr*> &merge_exprs,
                                                 const ObIArray<ObOrderDirection> &default_directions,
                                                 MergeKeyInfo &merge_key,
                                                 MergeKeyInfo *&interesting_key);

  static int create_interesting_merge_key(const ObDMLStmt *stmt,
                                          const ObIArray<ObRawExpr*> &merge_exprs,
                                          const EqualSets &equal_sets,
                                          MergeKeyInfo &merge_key);

  static int create_interesting_merge_key(const ObIArray<ObRawExpr*> &merge_exprs,
                                          const ObIArray<OrderItem> &expect_key,
                                          const EqualSets &equal_sets,
                                          ObIArray<ObRawExpr*> &sort_exprs,
                                          ObIArray<ObOrderDirection> &directions,
                                          ObIArray<int64_t> &sort_map);

  static int flip_op_type(const ObItemType expr_type, ObItemType &rotated_expr_type);

  static int get_rownum_filter_info(ObRawExpr *rownum_expr,
                                    ObItemType &expr_type,
                                    ObRawExpr *&const_expr,
                                    bool &is_const_filter);

  static int convert_rownum_filter_as_offset(ObRawExprFactory &expr_factory,
                                             ObSQLSessionInfo *session_info,
                                             const ObItemType filter_type,
                                             ObRawExpr *const_expr,
                                             ObRawExpr *&offset_int_expr,
                                             ObRawExpr *zero_expr,
                                             bool &offset_is_not_neg,
                                             ObTransformerCtx *ctx);

  static int convert_rownum_filter_as_limit(ObRawExprFactory &expr_factory,
                                            ObSQLSessionInfo *session_info,
                                            const ObItemType filter_type,
                                            ObRawExpr *const_expr,
                                            ObRawExpr *&limit_int_expr);

  // used by
  //1. rownum <= num_expr => limit int_expr
  // int_expr = floor(num_expr)
  // rownum <= 2.4 => limit 2
  // rownum <= 2   => limit 2
  //2. rownum > num_expr => offset int_expr
  // rownum > 2.4  => offset 2
  // rownum > 2    => offset 2
  static int floor_number_as_limit_offset_value(ObRawExprFactory &expr_factory,
                                                ObSQLSessionInfo *session_info,
                                                ObRawExpr *num_expr,
                                                ObRawExpr *&int_expr);

  // used by
  //1. rownum < num_expr => limit int_expr
  // int_expr = ceill(num_expr) - 1
  // rownum < 2.4  => limit 2
  // rownum < 2    => limit 1
  //2. rownum >= num_expr => offset int_expr
  // rownum >= 2.4 => offset 2
  // rownum >= 2   => offset 1
  static int ceil_number_as_limit_offset_value(ObRawExprFactory &expr_factory,
                                               ObSQLSessionInfo *session_info,
                                               ObRawExpr *num_expr,
                                               ObRawExpr *&int_expr);

  /**
     * @brief get_type_safe_join_exprs
     * 解析 join_quals，将等值连接条件分解成 left exprs 和 right exprs
     * 并且保证这些 exprs 在判断连接条件的时候不会有类型转换
     * @return
     */
  static int get_type_safe_join_exprs(const ObIArray<ObRawExpr *> &join_quals,
                                      const ObRelIds &left_tables,
                                      const ObRelIds &right_tables,
                                      ObIArray<ObRawExpr *> &left_exprs,
                                      ObIArray<ObRawExpr *> &right_exprs,
                                      ObIArray<ObRawExpr *> &all_left_exprs,
                                      ObIArray<ObRawExpr *> &all_right_exprs);
  /**
   *  @brief split_or_qual_on_table
   *  从or谓词中分离出属于特定table的谓词并生成一个新的谓词
   *  要求or谓词的每一个子谓词中都有属于该table的谓词
   *  @param new_expr 新生成的谓词，值为NULL表示无法从or谓词中分离属于table的谓词
   */
  static int split_or_qual_on_table(const ObDMLStmt *stmt,
                                    ObOptimizerContext &opt_ctx,
                                    const ObRelIds &table_ids,
                                    ObOpRawExpr &or_qual,
                                    ObOpRawExpr *&new_expr);

  static int check_push_down_expr(const ObRelIds &table_ids,
                                  ObOpRawExpr &or_qual,
                                  ObIArray<ObSEArray<ObRawExpr *, 16> > &sub_exprs,
                                  bool &all_contain);

  static int generate_push_down_expr(const ObDMLStmt *stmt,
                                     ObOptimizerContext &opt_ctx,
                                     ObIArray<ObSEArray<ObRawExpr *, 16> > &sub_exprs,
                                     ObOpRawExpr *&new_expr);

  static int simplify_exprs(const ObFdItemSet &fd_item_set,
                            const EqualSets &equal_sets,
                            const ObIArray<ObRawExpr *> &const_exprs,
                            const ObIArray<ObRawExpr *> &candi_exprs,
                            ObIArray<ObRawExpr *> &root_exprs);

  static int simplify_ordered_exprs(const ObFdItemSet &fd_item_set,
                                    const EqualSets &equal_sets,
                                    const ObIArray<ObRawExpr *> &const_exprs,
                                    const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                    const ObIArray<ObRawExpr *> &candi_exprs,
                                    ObIArray<ObRawExpr *> &root_exprs);

  static int simplify_ordered_exprs(const ObFdItemSet &fd_item_set,
                                    const EqualSets &equal_sets,
                                    const ObIArray<ObRawExpr *> &const_exprs,
                                    const ObIArray<ObRawExpr *> &exec_ref_exprs,
                                    const ObIArray<OrderItem> &candi_items,
                                    ObIArray<OrderItem> &root_items);

  static int check_subquery_filter(const JoinedTable *table,
                                   bool &has);

  static bool has_equal_join_conditions(const ObIArray<ObRawExpr*> &join_conditions);

  static int get_subplan_const_column(const ObDMLStmt &parent_stmt,
                                      const uint64_t table_id,
                                      const ObSelectStmt &child_stmt,
                                      const ObIArray<ObRawExpr*> &exec_ref_exprs,
                                      ObIArray<ObRawExpr *> &output_exprs);

  /*
   * 以下函数主要用于把subplan scan中内层继承上来的属性(e.g.,ordering,unique set,partition key)转换成外层可以识别的属性
   * 例子: select * from (select * from t1 where t1.a = t1.b order by t1.a) t2 order by t2.a, t2.b;
   * 内层ordering t1.a转换成外层可以识别的t2.a
   * 内存的equal set t1.a = t1.b转换成外层可以识别的t2.a = t2.b
   *
   * skip_invalid: true represents ignoring these that can not be converted(i.e., equal sets),
   * otherwise should return empty output exprs if some input expr can not be converted (i.e., unique sets)
   */
  static int convert_subplan_scan_expr(ObRawExprFactory &expr_factory,
                                       const EqualSets &equal_sets,
                                       const uint64_t table_id,
                                       const ObDMLStmt &parent_stmt,
                                       const ObSelectStmt &child_stmt,
                                       bool skip_invalid,
                                       const ObIArray<ObRawExpr*> &input_exprs,
                                       common::ObIArray<ObRawExpr*> &output_exprs);

  static int convert_subplan_scan_expr(ObRawExprCopier &copier,
                                       const EqualSets &equal_sets,
                                       const uint64_t table_id,
                                       const ObDMLStmt &parent_stmt,
                                       const ObSelectStmt &child_stmt,
                                       ObRawExpr *input_expr,
                                       ObRawExpr *&output_expr);

  static int check_subplan_scan_expr_validity(const EqualSets &equal_sets,
                                              const uint64_t table_id,
                                              const ObDMLStmt &parent_stmt,
                                              const ObSelectStmt &child_stmt,
                                              const ObRawExpr *input_expr,
                                              bool &is_valid);

  static int get_parent_stmt_expr(const EqualSets &equal_sets,
                                  const uint64_t table_id,
                                  const ObDMLStmt &parent_stmt,
                                  const ObSelectStmt &child_stmt,
                                  const ObRawExpr *child_expr,
                                  ObRawExpr *&parent_expr);

  static int get_parent_stmt_exprs(const EqualSets &equal_sets,
                                  const uint64_t table_id,
                                  const ObDMLStmt &parent_stmt,
                                  const ObSelectStmt &child_stmt,
                                  const ObIArray<ObRawExpr *> &child_exprs,
                                  ObIArray<ObRawExpr *> &parent_exprs);

  static int compute_const_exprs(const ObIArray<ObRawExpr *> &condition_exprs,
                                 ObIArray<ObRawExpr *> &const_exprs);

  static int compute_const_exprs(ObRawExpr *cur_expr,
                                 ObRawExpr *&res_const_expr);

  static int compute_stmt_interesting_order(const ObIArray<OrderItem> &ordering,
                                            const ObDMLStmt *stmt,
                                            const bool in_subplan_scan,
                                            EqualSets &equal_sets,
                                            const ObIArray<ObRawExpr *> &const_exprs,
                                            const bool is_parent_set_disinct,
                                            const int64_t check_scope,
                                            int64_t &match_info,
                                            int64_t &max_prefix_count);

  static int compute_stmt_interesting_order(const ObIArray<OrderItem> &ordering,
                                            const ObDMLStmt *stmt,
                                            const bool in_subplan_scan,
                                            EqualSets &equal_sets,
                                            const ObIArray<ObRawExpr *> &const_exprs,
                                            const bool is_parent_set_distinct,
                                            const int64_t check_scope,
                                            int64_t &match_info);
  /**
   * 用来判断group by是否匹配序的前缀
   * @ordering 待匹配的序
   * @match_prefix_count 匹配序前缀长度
   * @sort_match 是否是有效的匹配
   */
  static int is_group_by_match(const ObIArray<OrderItem> &ordering,
                               const ObSelectStmt *select_stmt,
                               const EqualSets &equal_sets,
                               const ObIArray<ObRawExpr *> &const_exprs,
                               int64_t &match_prefix_count,
                               bool &sort_match);

  static int is_group_by_match(const ObIArray<OrderItem> &ordering,
                               const ObSelectStmt *select_stmt,
                               const EqualSets &equal_sets,
                               const ObIArray<ObRawExpr *> &const_exprs,
                               bool &sort_match);

  /**
   * 判断win func的partition以及orderby是否匹配序的前缀
   * @ordering 待匹配的序
   * @match_prefix_count 匹配序前缀长度
   * @sort_match 是否是有效的匹配
   */
  static int is_winfunc_match(const ObIArray<OrderItem> &ordering,
                              const ObSelectStmt *select_stmt,
                              const EqualSets &equal_sets,
                              const ObIArray<ObRawExpr *> &const_exprs,
                              int64_t &match_prefix_count,
                              bool &sort_match,
                              bool &sort_is_required);

  static int is_winfunc_match(const ObIArray<OrderItem> &ordering,
                              const ObSelectStmt *select_stmt,
                              const ObWinFunRawExpr *win_expr,
                              const EqualSets &equal_sets,
                              const ObIArray<ObRawExpr *> &const_exprs,
                              int64_t &match_prefix_count,
                              bool &sort_match,
                              bool &sort_is_required);

  // fast check, return a bool result
  static int is_winfunc_match(const ObIArray<OrderItem> &ordering,
                              const ObSelectStmt *select_stmt,
                              const EqualSets &equal_sets,
                              const ObIArray<ObRawExpr *> &const_exprs,
                              bool &is_match);

  static int match_order_by_against_index(const ObIArray<OrderItem> &expect_ordering,
                                          const ObIArray<OrderItem> &input_ordering,
                                          const int64_t input_start_offset,
                                          const EqualSets &equal_sets,
                                          const ObIArray<ObRawExpr *> &const_exprs,
                                          bool &full_covered,
                                          int64_t &match_count,
                                          bool check_direction = true);

  /**
   * 用来判断distinct的列或set的列是否匹配序前缀
   * @ordering 待匹配的序
   * @match_prefix_count 匹配序前缀长度
   * @sort_match 是否是有效的匹配
   */
  static int is_set_or_distinct_match(const ObIArray<ObRawExpr*> &keys,
                                      const ObSelectStmt *select_stmt,
                                      const EqualSets &equal_sets,
                                      const ObIArray<ObRawExpr *> &const_exprs,
                                      int64_t &match_prefix_count,
                                      bool &sort_match);

  static int is_distinct_match(const ObIArray<ObRawExpr*> &keys,
                               const ObSelectStmt *stmt,
                               const EqualSets &equal_sets,
                               const ObIArray<ObRawExpr *> &const_exprs,
                               int64_t &match_prefix_count,
                               bool &sort_match);

  static int is_set_match(const ObIArray<ObRawExpr*> &keys,
                          const ObSelectStmt *stmt,
                          const EqualSets &equal_sets,
                          const ObIArray<ObRawExpr *> &const_exprs,
                          int64_t &match_prefix_count,
                          bool &sort_match);

  /**
   * 用来判断order by的列能否匹配序的前缀
   * @ordering 待匹配的序
   * @match_prefix_count 匹配序前缀长度
   * @sort_match 是否是有效的匹配
   */
  static int is_order_by_match(const ObIArray<OrderItem> &ordering,
                               const ObDMLStmt *stmt,
                               const EqualSets &equal_sets,
                               const ObIArray<ObRawExpr *> &const_exprs,
                               int64_t &match_prefix_count,
                               bool &sort_match);

  // fast check, return bool result
  static int is_order_by_match(const ObIArray<OrderItem> &expect_ordering,
                               const ObIArray<OrderItem> &input_ordering,
                               const EqualSets &equal_sets,
                               const ObIArray<ObRawExpr *> &const_exprs,
                               bool &is_match);

  static int is_lossless_column_cast(const ObRawExpr *expr, bool &is_lossless);
  static bool is_lossless_type_conv(const ObExprResType &child_type, const ObExprResType &dst_type);
  static int is_lossless_column_conv(const ObRawExpr *expr, bool &is_lossless);
  static int get_expr_without_lossless_cast(const ObRawExpr* ori_expr, const ObRawExpr*& expr);
  static int get_expr_without_lossless_cast(ObRawExpr* ori_expr, ObRawExpr*& expr);

  static int gen_set_target_list(ObIAllocator *allocator,
                                 ObSQLSessionInfo *session_info,
                                 ObRawExprFactory *expr_factory,
                                 ObSelectStmt &left_stmt,
                                 ObSelectStmt &right_stmt,
                                 ObSelectStmt *select_stmt,
                                 const bool is_mysql_recursive_union = false,
                                 ObIArray<ObString> *rcte_col_name = NULL);

  static int gen_set_target_list(ObIAllocator *allocator,
                                 ObSQLSessionInfo *session_info,
                                 ObRawExprFactory *expr_factory,
                                 ObIArray<ObSelectStmt*> &left_stmts,
                                 ObIArray<ObSelectStmt*> &right_stmts,
                                 ObSelectStmt *select_stmt,
                                 const bool is_mysql_recursive_union = false,
                                 ObIArray<ObString> *rcte_col_name = NULL);

  static int gen_set_target_list(ObIAllocator *allocator,
                                 ObSQLSessionInfo *session_info,
                                 ObRawExprFactory *expr_factory,
                                 ObSelectStmt *select_stmt);

  static int get_set_res_types(ObIAllocator *allocator,
                               ObSQLSessionInfo *session_info,
                               ObIArray<ObSelectStmt*> &child_querys,
                               ObIArray<ObExprResType> &res_types);

  static int try_add_cast_to_set_child_list(ObIAllocator *allocator,
                                            ObSQLSessionInfo *session_info,
                                            ObRawExprFactory *expr_factory,
                                            const bool is_distinct,
                                            ObIArray<ObSelectStmt*> &left_stmts,
                                            ObIArray<ObSelectStmt*> &right_stmts,
                                            ObIArray<ObExprResType> *res_types,
                                            const bool is_mysql_recursive_union = false,
                                            ObIArray<ObString> *rcte_col_name = NULL);

  static int add_cast_to_set_list(ObSQLSessionInfo *session_info,
                                  ObRawExprFactory *expr_factory,
                                  ObIArray<ObSelectStmt*> &stmts,
                                  const ObExprResType &res_type,
                                  const int64_t idx);

  static int add_column_conv_to_set_list(ObSQLSessionInfo *session_info,
                                         ObRawExprFactory *expr_factory,
                                         ObIArray<ObSelectStmt*> &stmts,
                                         const ObExprResType &res_type,
                                         const int64_t idx,
                                         ObIArray<ObString> *rcte_col_name);

  static int check_subquery_has_ref_assign_user_var(ObRawExpr *expr, bool &is_has);

  /**
    * @brief pushdown_filter_into_subquery
    * 条件下推到subquery内部，如果不能下推到subquery的where
    * 返回不能下推
    * @param pushdown_quals 参数化后待下推的谓词
    * @param candi_filters 能够下推的谓词
    * @param remain_filters 未成功下推的谓词
    * @param can_pushdown 能够下推谓词
    */
  static int pushdown_filter_into_subquery(const ObDMLStmt &parent_stmt,
                                           const ObSelectStmt &subquery,
                                           ObOptimizerContext &opt_ctx,
                                           ObIArray<ObRawExpr*> &pushdown_filters,
                                           ObIArray<ObRawExpr*> &candi_filters,
                                           ObIArray<ObRawExpr*> &remain_filters,
                                           bool &can_pushdown,
                                           bool check_match_index = true);

  /**
    * @brief check_pushdown_filter
    * 检查哪些谓词可以下推到子查询的where
    * 谓词需要下推过win func和group by
    * @param pushdown_filters 待下推的谓词
    * @param candi_filters 能够下推的谓词
    * @param remain_filters 未成功下推的谓词
    */
  static int check_pushdown_filter(const ObDMLStmt &parent_stmt,
                                   const ObSelectStmt &subquery,
                                   ObOptimizerContext &opt_ctx,
                                   ObIArray<ObRawExpr*> &pushdown_filters,
                                   ObIArray<ObRawExpr*> &candi_filters,
                                   ObIArray<ObRawExpr*> &remain_filters,
                                   bool check_match_index = true);

  static int remove_special_exprs(ObIArray<ObRawExpr*> &pushdown_filters,
                                  ObIArray<ObRawExpr*> &remain_filters);

  static int check_pushdown_filter_overlap_index(const ObDMLStmt &stmt,
                                                 ObOptimizerContext &opt_ctx,
                                                 ObIArray<ObRawExpr*> &pushdown_filters,
                                                 ObIArray<ObRawExpr*> &candi_filters,
                                                 ObIArray<ObRawExpr*> &remain_filters);

  /**
    * @brief check_pushdown_filter_for_set
    * 检查哪些谓词可以下推到子查询的where
    * 谓词需要下推过win func和group by
    * 如果parent stmt是set stmt，需要调用此函数
    * @param pushdown_filters 待下推的谓词
    * @param candi_filters 能够下推的谓词
    * @param remain_filters 未成功下推的谓词
    */
  static int check_pushdown_filter_for_set(const ObSelectStmt &parent_stmt,
                                           const ObSelectStmt &subquery,
                                           ObIArray<ObRawExpr*> &common_exprs,
                                           ObIArray<ObRawExpr*> &pushdown_filters,
                                           ObIArray<ObRawExpr*> &candi_filters,
                                           ObIArray<ObRawExpr*> &remain_filters);

  static int check_pushdown_filter_for_subquery(const ObDMLStmt &parent_stmt,
                                                const ObSelectStmt &subquery,
                                                ObOptimizerContext &opt_ctx,
                                                ObIArray<ObRawExpr*> &common_exprs,
                                                ObIArray<ObRawExpr*> &pushdown_filters,
                                                ObIArray<ObRawExpr*> &candi_filters,
                                                ObIArray<ObRawExpr*> &remain_filters,
                                                bool check_match_index = true);

  /**
    * @brief get_groupby_win_func_common_exprs
    * 获取group exprs与win func partition by exprs的交集
    * @param common_exprs 交集表达式集合
    * @param is_valid 是否有group 或 win func
    */
  static int get_groupby_win_func_common_exprs(const ObSelectStmt &subquery,
                                              ObIArray<ObRawExpr*> &common_exprs,
                                              bool &is_valid);

  static int rename_pushdown_filter(const ObDMLStmt &parent_stmt,
                                    const ObSelectStmt &subquery,
                                    int64_t table_id,
                                    ObSQLSessionInfo *session_info,
                                    ObRawExprFactory &expr_factory,
                                    ObIArray<ObRawExpr*> &candi_filters,
                                    ObIArray<ObRawExpr*> &rename_filters);

  static int rename_set_op_pushdown_filter(const ObSelectStmt &parent_stmt,
                                          const ObSelectStmt &subquery,
                                          ObSQLSessionInfo *session_info,
                                          ObRawExprFactory &expr_factory,
                                          ObIArray<ObRawExpr*> &candi_filters,
                                          ObIArray<ObRawExpr*> &rename_filters);

  static int rename_subquery_pushdown_filter(const ObDMLStmt &parent_stmt,
                                            const ObSelectStmt &subquery,
                                            int64_t table_id,
                                            ObSQLSessionInfo *session_info,
                                            ObRawExprFactory &expr_factory,
                                            ObIArray<ObRawExpr*> &candi_filters,
                                            ObIArray<ObRawExpr*> &rename_filters);

  /**
    * @brief get_set_op_remain_filter
    * 收集set stmt左右子查询下推谓词后剩余在当前stmt的谓词
    * @param child_pushdown_preds 单个子查询未下推的谓词
    * @param output_pushdown_preds 剩余在set stmt中的谓词
    * @param first_child 第一个分支
    */
  static int get_set_op_remain_filter(const ObSelectStmt &stmt,
                                      const ObIArray<ObRawExpr *> &child_pushdown_preds,
                                      ObIArray<ObRawExpr *> &output_pushdown_preds,
                                      const bool first_child);

  static int check_is_null_qual(const ParamStore *params,
                                const ObDMLStmt *stmt,
                                ObExecContext *exec_ctx,
                                ObIAllocator &allocator,
                                const ObRelIds &table_ids,
                                const ObRawExpr* qual,
                                bool &is_null_qual);

  static int check_expr_contain_subquery(const ObIArray<ObRawExpr*> &exprs,
                                         bool &has_subquery);

  static bool has_psedu_column(const ObRawExpr &expr);

  static bool has_hierarchical_expr(const ObRawExpr &expr);


  static int check_pushdown_filter_to_base_table(ObLogPlan &plan,
                                                 const uint64_t table_id,
                                                 const ObIArray<ObRawExpr*> &pushdown_filters,
                                                 const ObIArray<ObRawExpr*> &restrict_infos,
                                                 bool &can_pushdown);

  static int compute_ordering_relationship(const bool left_is_interesting,
                                           const bool right_is_interesting,
                                           const common::ObIArray<OrderItem> &left_ordering,
                                           const common::ObIArray<OrderItem> &right_ordering,
                                           const EqualSets &equal_sets,
                                           const common::ObIArray<ObRawExpr*> &condition_exprs,
                                           DominateRelation &relation);

  static int compute_sharding_relationship(const ObShardingInfo *left_strong_sharding,
                                           const ObIArray<ObShardingInfo*> &left_weak_sharding,
                                           const ObShardingInfo *right_strong_sharding,
                                           const ObIArray<ObShardingInfo*> &right_weak_sharding,
                                           const EqualSets &equal_sets,
                                           DominateRelation &relation);

  static int compute_sharding_relationship(const ObIArray<ObShardingInfo*> &left_sharding,
                                           const ObIArray<ObShardingInfo*> &right_sharding,
                                           const EqualSets &equal_sets,
                                           DominateRelation &relation);

  static int compute_sharding_relationship(const ObShardingInfo *left_sharding,
                                           const ObShardingInfo *right_sharding,
                                           const EqualSets &equal_sets,
                                           DominateRelation &relation);

  static int check_sharding_set_left_dominate(const ObIArray<ObShardingInfo*> &left_sharding,
                                              const ObIArray<ObShardingInfo*> &right_sharding,
                                              const EqualSets &equal_sets,
                                              bool &is_left_dominate);

  static int get_range_params(ObLogicalOperator *root,
                              ObIArray<ObRawExpr*> &range_exprs,
                              ObIArray<ObRawExpr*> &all_table_filters);

  static int check_basic_sharding_info(const ObAddr &local_addr,
                                       const ObIArray<ObLogicalOperator *> &child_ops,
                                       bool &is_basic);

  static int check_basic_sharding_info(const ObAddr &local_addr,
                                       const ObIArray<ObLogicalOperator *> &child_ops,
                                       bool &is_basic,
                                       bool &is_remote);

  static int check_basic_sharding_info(const ObAddr &local_server,
                                       const ObIArray<ObShardingInfo*> &input_shardings,
                                       bool &is_basic);

  static int check_basic_sharding_info(const ObAddr &local_server,
                                       const ObIArray<ObShardingInfo*> &input_shardings,
                                       bool &is_basic,
                                       bool &is_remote);

  static int compute_basic_sharding_info(const ObAddr &local_addr,
                                         const ObIArray<ObLogicalOperator *> &child_ops,
                                         ObIAllocator &allocator,
                                         ObShardingInfo *&result_sharding,
                                         int64_t &inherit_sharding_index);

  static int compute_basic_sharding_info(const ObAddr &local_addr,
                                         const ObIArray<ObShardingInfo*> &input_shardings,
                                         ObIAllocator &allocator,
                                         ObShardingInfo *&result_sharding,
                                         int64_t &inherit_sharding_index);

  static int get_duplicate_table_replica(const ObCandiTableLoc &phy_table_loc,
                                         ObIArray<ObAddr> &valid_addrs);

  static int compute_duplicate_table_sharding(const ObAddr &local_addr,
                                              const ObAddr &selected_addr,
                                              ObIAllocator &allocator,
                                              ObShardingInfo &src_sharding,
                                              ObIArray<ObAddr> &valid_addrs,
                                              ObShardingInfo *&target_sharding);

  static int generate_duplicate_table_replicas(ObIAllocator &allocator,
                                               const ObCandiTableLoc *source_table_loc,
                                               ObIArray<ObAddr> &valid_addrs,
                                               ObCandiTableLoc *&target_table_loc);

  static int64_t get_join_style_parallel(const int64_t left_parallel,
                                         const int64_t right_parallel,
                                         const DistAlgo join_dist_algo,
                                         const bool use_left = false);

  static bool is_left_need_exchange(const ObShardingInfo &sharding, const DistAlgo dist_algo);
  static bool is_right_need_exchange(const ObShardingInfo &sharding, const DistAlgo dist_algo);

  static ObPQDistributeMethod::Type get_left_dist_method(const ObShardingInfo &sharding,
                                                         const DistAlgo dist_algo);

  static ObPQDistributeMethod::Type get_right_dist_method(const ObShardingInfo &sharding,
                                                          const DistAlgo dist_algo);

  // aggregate partial results
  static int generate_pullup_aggr_expr(ObRawExprFactory &expr_factory,
                                       ObSQLSessionInfo *session_info,
                                       ObAggFunRawExpr *origin_aggr,
                                       ObAggFunRawExpr *&replaced_aggr);

  static int check_filter_before_indexback(const ObIArray<ObRawExpr*> &filter_exprs,
                                           const ObIArray<uint64_t> &index_columns,
                                           ObIArray<bool> &filter_before_index_back);

  //generate expr and column item for column.
  //
  static int generate_rowkey_expr(ObDMLStmt *stmt,
                                  ObRawExprFactory &expr_factory,
                                  const uint64_t &table_id,
                                  const share::schema::ObColumnSchemaV2 &column_schema,
                                  ObColumnRefRawExpr *&rowkey,
                                  common::ObIArray<ColumnItem> *column_items = NULL);

  static int check_contain_ora_rowscn_expr(const ObRawExpr *expr, bool &contains);

  static int allocate_group_id_expr(ObLogPlan *log_plan, ObRawExpr *&group_id_expr);

  static int check_contribute_query_range(ObLogicalOperator *tsc,
                                          const ObIArray<ObExecParamRawExpr *> &params,
                                          bool &is_valid);

  static int check_pushdown_range_cond(ObLogicalOperator *root,
                                       bool &cnt_pd_range_cond);
 
  static int check_exec_param_filter_exprs(const ObIArray<ObRawExpr *> &input_filters,
                                           bool &has_exec_param_filters);


  static int check_contain_batch_stmt_parameter(ObRawExpr* expr, bool &contain);

  static int expr_calculable_by_exprs(const ObRawExpr *src_expr,
                                      const ObIArray<ObRawExpr*> &dst_exprs,
                                      bool &is_calculable);
  static int get_minset_of_exprs(const ObIArray<ObRawExpr *> &src_exprs,
                                 ObIArray<ObRawExpr *> &min_set);

  static int build_rel_ids_by_equal_set(const EqualSet &equal_set,
                                        ObRelIds &rel_ids);
  static int build_rel_ids_by_equal_sets(const EqualSets &equal_sets,
                                         ObIArray<ObRelIds> &rel_ids_array);
  static int find_expr_in_equal_sets(const EqualSets &equal_sets,
                                     const ObRawExpr *target_expr,
                                     int64_t &idx);
  static bool find_rel_ids(const ObIArray<ObRelIds> &rel_ids_array,
                           const ObRelIds &target_ids,
                           int64_t *idx = NULL);

  static int check_can_encode_sortkey(const common::ObIArray<OrderItem> &order_keys,
                                        bool &can_sort_opt,
                                        ObLogPlan& plan,
                                        double card);

  static int extract_equal_join_conditions(const ObIArray<ObRawExpr *> &equal_join_conditions,
                                           const ObRelIds &left_tables,
                                           ObIArray<ObRawExpr *> &left_exprs,
                                           ObIArray<ObRawExpr *> &right_exprs);

  static int extract_pushdown_join_filter_quals(const ObIArray<ObRawExpr *> &left_quals,
                                                const ObIArray<ObRawExpr *> &right_quals,
                                                const ObSqlBitSet<> &right_tables,
                                                ObIArray<ObRawExpr *> &pushdown_left_quals,
                                                ObIArray<ObRawExpr *> &pushdown_right_quals);

  static int pushdown_join_filter_into_subquery(const ObDMLStmt &parent_stmt,
                                                const ObSelectStmt &subquery,
                                                const ObIArray<ObRawExpr*> &pushdown_left_quals,
                                                const ObIArray<ObRawExpr*> &pushdown_right_quals,
                                                ObIArray<ObRawExpr*> &candi_left_quals,
                                                ObIArray<ObRawExpr*> &candi_right_quals,
                                                bool &can_pushdown);

  static int check_pushdown_join_filter_quals(const ObDMLStmt &parent_stmt,
                                              const ObSelectStmt &subquery,
                                              const ObIArray<ObRawExpr*> &pushdown_left_quals,
                                              const ObIArray<ObRawExpr*> &pushdown_right_quals,
                                              ObIArray<ObRawExpr*> &candi_left_quals,
                                              ObIArray<ObRawExpr*> &candi_right_quals);

  static int check_pushdown_join_filter_for_subquery(const ObDMLStmt &parent_stmt,
                                                     const ObSelectStmt &subquery,
                                                     ObIArray<ObRawExpr*> &common_exprs,
                                                     const ObIArray<ObRawExpr*> &pushdown_left_quals,
                                                     const ObIArray<ObRawExpr*> &pushdown_right_quals,
                                                     ObIArray<ObRawExpr*> &candi_left_quals,
                                                     ObIArray<ObRawExpr*> &candi_right_quals);

  static int check_pushdown_join_filter_for_set(const ObSelectStmt &parent_stmt,
                                                const ObSelectStmt &subquery,
                                                ObIArray<ObRawExpr*> &common_exprs,
                                                const ObIArray<ObRawExpr*> &pushdown_left_quals,
                                                const ObIArray<ObRawExpr*> &pushdown_right_quals,
                                                ObIArray<ObRawExpr*> &candi_left_quals,
                                                ObIArray<ObRawExpr*> &candi_right_quals);

  static int replace_gen_column(ObLogPlan *log_plan,
                                ObRawExpr *part_expr,
                                ObRawExpr *&new_part_expr);

  static int replace_column_with_select_for_partid(const ObInsertStmt *stmt,
                                                   ObOptimizerContext &opt_ctx,
                                                   ObRawExpr *&calc_part_id_expr);

  static int check_contain_my_exec_param(ObRawExpr* expr, const common::ObIArray<ObExecParamRawExpr*> & my_exec_params, bool &contain);

  static int generate_pseudo_trans_info_expr(ObOptimizerContext &opt_ctx,
                                             const common::ObString &table_name,
                                             ObOpPseudoColumnRawExpr *&expr);

  static int is_in_range_optimization_enabled(const ObGlobalHint &global_hint, ObSQLSessionInfo *session_info, bool &is_enabled);

  static int pushdown_and_rename_filter_into_subquery(const ObDMLStmt &parent_stmt,
                                                      const ObSelectStmt &subquery,
                                                      int64_t table_id,
                                                      ObOptimizerContext &opt_ctx,
                                                      ObIArray<ObRawExpr *> &input_filters,
                                                      ObIArray<ObRawExpr *> &push_filters,
                                                      ObIArray<ObRawExpr *> &remain_filters,
                                                      bool check_match_index = true);
  static int split_or_filter_into_subquery(const ObDMLStmt &parent_stmt,
                                           const ObSelectStmt &subquery,
                                           int64_t table_id,
                                           ObOptimizerContext &opt_ctx,
                                           ObRawExpr *filter,
                                           ObRawExpr *&push_filter,
                                           bool &can_pushdown_all,
                                           bool check_match_index = true);
  static int split_or_filter_into_subquery(ObIArray<const ObDMLStmt *> &parent_stmts,
                                           ObIArray<const ObSelectStmt *> &subqueries,
                                           ObIArray<int64_t> &table_ids,
                                           ObIArray<ObIArray<ObRawExpr *>*> &or_filter_params,
                                           ObOptimizerContext &opt_ctx,
                                           ObRawExpr *&push_filter,
                                           bool &can_pushdown_all,
                                           bool check_match_index = true);

  static bool find_superset(const ObRelIds &rel_ids,
                           const ObIArray<ObRelIds> &single_table_ids);

  static int try_push_down_temp_table_filter(ObOptimizerContext &opt_ctx,
                                             ObSqlTempTableInfo &temp_table_info,
                                             ObRawExpr *&temp_table_filter,
                                             ObRawExpr *&where_filter);
  static int push_down_temp_table_filter(ObRawExprFactory &expr_factory,
                                         ObSQLSessionInfo *session_info,
                                         ObSqlTempTableInfo &temp_table_info,
                                         ObRawExpr *&temp_table_filter,
                                         ObSelectStmt *temp_table_query = NULL);
private:
  //disallow construct
  ObOptimizerUtil();
  ~ObOptimizerUtil();
};

template <class T>
void ObOptimizerUtil::revert_items(common::ObIArray<T> &items,
                                   const int64_t N)
{
  for (int64_t i = items.count(); i > N && i > 0; --i) {
    items.pop_back();
  }
}

template <class T>
int ObOptimizerUtil::remove_item(common::ObIArray<T> &items,
                                 const T &item,
                                 bool *removed)// default null
{
  int ret = common::OB_SUCCESS;
  int64_t N = items.count();
  common::ObArray<T> new_arr;
  bool tmp_removed = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const T &v = items.at(i);
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
int ObOptimizerUtil::remove_item(common::ObIArray<T> &items,
                                 const common::ObIArray<T> &rm_items,
                                 bool *removed)// default null)
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
int ObOptimizerUtil::intersect(const ObIArray<T> &first,
                               const ObIArray<T> &second,
                               ObIArray<T> &third)
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

 /**
   * @brief intersect, to reduce the assign cost when computing many sets' overlap.
   * @param sets the element sets to calculate the overlap.
   * @param result the result of the overlap
   * @return
   */
template <class T>
int ObOptimizerUtil::intersect(const ObIArray<ObSEArray<T, 4>> &sets,
                               ObIArray<T> &result)
{
  int ret = common::OB_SUCCESS;
  int64_t N = sets.count();
  if (N == 0) {
    result.reset();
  } else {
    common::ObSEArray<common::ObSEArray<T, 8> , 2> intersection;
    if (OB_FAIL(intersection.prepare_allocate(2))) {
      SQL_OPT_LOG(WARN, "failed to pre allocate array", K(ret));
    } else {
      bool cur = 0;
      if (OB_FAIL(intersection.at(cur^1).assign(sets.at(0)))) {
        SQL_OPT_LOG(WARN, "failed to assign item array", K(ret));
      }
      for (int64_t i = 1; OB_SUCC(ret) && intersection.at(cur^1).count() > 0 && i < N; ++i, cur^=1) {
        for (int64_t j =0; OB_SUCC(ret) && j < sets.at(i).count(); j++) {
          if (find_item(intersection.at(cur^1), sets.at(i).at(j))) {
            if (OB_FAIL(intersection.at(cur).push_back(sets.at(i).at(j)))) {
              SQL_OPT_LOG(WARN, "failed to push back item to array", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)){
        if (OB_FAIL(result.assign(intersection.at(cur^1)))) {
          SQL_OPT_LOG(WARN, "failed to assign item array", K(ret));
        }
      }
    }
  }
  return ret;
}

template <class T>
bool ObOptimizerUtil::overlap(const ObIArray<T> &first,
                              const ObIArray<T> &second)
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
bool ObOptimizerUtil::is_subset(const ObIArray<T> &first,
                                const ObIArray<T> &second)
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
uint64_t ObOptimizerUtil::hash_exprs(uint64_t seed, const ObIArray<T> &expr_array)
{
  uint64_t hash_value = seed;
  int64_t N = expr_array.count();
  for (int64_t i = 0; i < N; ++i) {
    hash_value = hash_expr(expr_array.at(i), hash_value);
  }
  return hash_value;
}


#define  HASH_ARRAY(items, seed) \
do { \
  int64_t N = (items).count(); \
  for (int64_t i = 0; i < N; i++) { \
    (seed) = do_hash((items).at(i), (seed)); \
  } \
} while(0);

#define  HASH_PTR_ARRAY(items, seed) \
do { \
  int64_t N = (items).count(); \
  for (int64_t i = 0; i < N; i++) { \
    if (NULL != (items).at(i)) { \
      (seed) = do_hash(*((items).at(i)), (seed)); \
    } \
  } \
} while(0);

}
}

#endif
