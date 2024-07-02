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
#include "sql/rewrite/ob_union_find.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObForeignKeyInfo;
class ObTableSchema;
}
}

namespace sql {

struct ObStmtHint;
struct ObTransformerCtx;
struct ObStmtMapInfo;
class ObUpdateStmt;
class ObSQLSessionInfo;

enum CheckStmtUniqueFlags {
  FLAGS_DEFAULT          = 0,      //nothing
  FLAGS_IGNORE_DISTINCT  = 1 << 0, //for distinct
  FLAGS_IGNORE_GROUP     = 1 << 1, //for group by
};

enum NULLABLE_SCOPE {
  NS_FROM     = 1 << 0,
  NS_WHERE    = 1 << 1,
  NS_GROUPBY  = 1 << 2,
  NS_TOP      = 1 << 4
};

struct ObNotNullContext
{
  ObNotNullContext(ObTransformerCtx &ctx,
                   const ObDMLStmt *stmt = NULL) : 
    exec_ctx_(ctx.exec_ctx_), allocator_(ctx.allocator_),
    is_for_ctas_(false), stmt_(stmt)
  {}
  
  ObNotNullContext(const ObNotNullContext &other,
                   const ObDMLStmt *stmt) :
    exec_ctx_(other.exec_ctx_), allocator_(other.allocator_),
    is_for_ctas_(other.is_for_ctas_), stmt_(stmt)
  {}
  
  ObNotNullContext(ObExecContext *exec_ctx,
                   ObIAllocator *allocator,
                   const ObDMLStmt *stmt,
                   bool is_for_ctas = false) 
    : exec_ctx_(exec_ctx), allocator_(allocator), 
      is_for_ctas_(is_for_ctas), stmt_(stmt)
  {}
  
  ObNotNullContext() :
    exec_ctx_(NULL), allocator_(NULL), is_for_ctas_(false), stmt_(NULL)
  {}
      
  int generate_stmt_context(int64_t stmt_context = NULLABLE_SCOPE::NS_TOP);
  
  int add_joined_table(const JoinedTable *table);
  
  int add_filter(const ObIArray<ObRawExpr *> &filters);

  int add_filter(ObRawExpr *filter);
  
  int remove_filter(ObRawExpr *filter);

  int add_having_filter(const ObIArray<ObRawExpr *> &filters);

  int add_having_filter(ObRawExpr *filter);

  int remove_having_filter(ObRawExpr *filter);

  inline void reset() {
    group_clause_exprs_.reset();
    right_table_ids_.reset();
    filters_.reset();
    having_filters_.reset();
  }

public:
  // params
  ObExecContext *exec_ctx_;
  ObIAllocator *allocator_;
  
  // for CTAS in oracle mode
  bool is_for_ctas_;
  
  // relation context
  const ObDMLStmt *stmt_;
  
  ObArray<ObRawExpr *> group_clause_exprs_;
  ObArray<uint64_t> right_table_ids_;
  
  ObArray<ObRawExpr *> having_filters_;
  ObArray<ObRawExpr *> filters_;
};

class ObSelectStmtPointer {
public:
  ObSelectStmtPointer();

  virtual ~ObSelectStmtPointer();
  int get(ObSelectStmt *&stmt) const;
  int set(ObSelectStmt *stmt);
  int add_ref(ObSelectStmt **stmt);
  int64_t ref_count() const { return stmt_group_.count(); }
  TO_STRING_KV("", "");
private:
  common::ObSEArray<ObSelectStmt **, 1> stmt_group_;
};

class ObTransformUtils
{
  private:
  struct UniqueCheckInfo
  {
    UniqueCheckInfo() {}
    virtual ~UniqueCheckInfo() {}

    ObRelIds table_set_;
    ObSEArray<ObRawExpr *, 4> const_exprs_;
    EqualSets equal_sets_;
    ObFdItemSet fd_sets_;
    ObFdItemSet candi_fd_sets_;
    ObSEArray<ObRawExpr *, 4> not_null_;

    int assign(const UniqueCheckInfo &other);
    void reset();

    private:
      DISALLOW_COPY_AND_ASSIGN(UniqueCheckInfo);
  };
  struct UniqueCheckHelper
  {
    UniqueCheckHelper() :
      alloc_(NULL),
      fd_factory_(NULL),
      expr_factory_(NULL),
      schema_checker_(NULL),
      session_info_(NULL) {}
    virtual ~UniqueCheckHelper() {}

    ObIAllocator *alloc_;
    ObFdItemFactory *fd_factory_;
    ObRawExprFactory *expr_factory_;
    ObSchemaChecker *schema_checker_;
    ObSQLSessionInfo *session_info_;
  private:
    DISALLOW_COPY_AND_ASSIGN(UniqueCheckHelper);
  };
  static const uint64_t MAX_SET_STMT_SIZE_OF_COSTED_BASED_RELUES = 5;

public:
  struct LazyJoinInfo {
    LazyJoinInfo()
      :join_conditions_(),
      right_table_(NULL)
    {}
    
    void reset() {
      join_conditions_.reset();
      right_table_ = NULL;
    }
    int assign(const LazyJoinInfo &other);

    TO_STRING_KV(
      K(join_conditions_),
      K(right_table_)
    );

    ObSEArray<ObRawExpr*, 4> join_conditions_;
    TableItem *right_table_;
  };

  static int decorrelate(ObRawExpr *&expr, ObIArray<ObExecParamRawExpr *> &exec_params);

  static int decorrelate(ObIArray<ObRawExpr *> &exprs, ObIArray<ObExecParamRawExpr *> &exec_params);

  static int decorrelate(ObDMLStmt *stmt, ObIArray<ObExecParamRawExpr *> &exec_params);
  
  static int inherit_exec_params(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                 ObQueryRefRawExpr *query_ref);

  static int get_exec_params(const ObIArray<ObExecParamRawExpr *> &exec_params,
                             ObRawExpr *expr,
                             ObIArray<ObExecParamRawExpr *> &used_params);

  static int get_exec_params(const ObIArray<ObExecParamRawExpr *> &exec_params,
                             ObSelectStmt *stmt,
                             ObIArray<ObExecParamRawExpr *> &used_params);

  static int is_correlated_expr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                const ObRawExpr *expr,
                                bool &bret);

  static int is_correlated_subquery(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                    const ObSelectStmt *stmt,
                                    bool &bret);

  static int is_simple_correlated_pred(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                       ObRawExpr *cond,
                                       ObColumnRefRawExpr *&col_expr,
                                       ObRawExpr *&const_expr);

  static int mark_correlated_expr(ObSelectStmt *stmt,
                                  bool &is_stmt_marked,
                                  ObIArray<ObRawExpr *> &all_mark_exprs);

  static int mark_correlated_expr(ObRawExpr *expr,
                                  ObIArray<ObRawExpr *> &all_mark_exprs);

  static int unmark_correlated_expr(ObIArray<ObRawExpr *> &all_mark_exprs);

  static int add_correlated_flag(const ObIArray<ObExecParamRawExpr *> &param_exprs,
                                 ObIArray<ObRawExpr *> &all_mark_exprs);

  static int has_nested_subquery(const ObQueryRefRawExpr *query_ref,
                                 bool &has_nested);

  static int is_column_unique(const ObRawExpr *expr,
                              uint64_t table_id,
                              ObSchemaChecker *schema_checker,
                              ObSQLSessionInfo *session_info,
                              bool &is_unique);

  static int is_columns_unique(const ObIArray<ObRawExpr *> &exprs,
                               uint64_t table_id,
                               ObSchemaChecker *schema_checker,
                               ObSQLSessionInfo *session_info,
                               bool &is_unique);

  static int exprs_has_unique_subset(const common::ObIArray<ObRawExpr*> &full,
                                     const common::ObRowkeyInfo &sub,
                                     bool &is_subset);

  static int add_new_table_item(ObTransformerCtx *ctx,
                                ObDMLStmt *stmt,
                                ObSelectStmt *subquery,
                                TableItem *&new_table_item);

  static int add_new_joined_table(ObTransformerCtx *ctx,
                                  ObDMLStmt &stmt,
                                  const ObJoinType join_type,
                                  TableItem *left_table,
                                  TableItem *right_table,
                                  const ObIArray<ObRawExpr*> &joined_conds,
                                  TableItem *&join_table,
                                  bool add_table = true);

  static int merge_from_items_as_inner_join(ObTransformerCtx *ctx,
                                            ObDMLStmt &stmt,
                                            TableItem *&ret_table);

  static int merge_from_items_as_inner_join(ObTransformerCtx *ctx,
                                            ObDMLStmt &stmt,
                                            ObIArray<FromItem> &from_item_list,
                                            ObIArray<JoinedTable*> &joined_table_list,
                                            TableItem *&ret_table);

  static int create_new_column_expr(ObTransformerCtx *ctx,
                                    const TableItem &table_item,
                                    const int64_t column_id,
                                    const SelectItem &select_item,
                                    ObDMLStmt *stmt,
                                    ObColumnRefRawExpr *&new_expr);

  static int create_columns_for_view(ObTransformerCtx *ctx,
                                     TableItem &view_table_item,
                                     ObDMLStmt *stmt,
                                     ObIArray<ObRawExpr*> &column_exprs);

  static int create_columns_for_view(ObTransformerCtx *ctx,
                                     TableItem &view_table_item,
                                     ObDMLStmt *stmt,
                                     ObIArray<ObRawExpr *> &new_select_list,
                                     ObIArray<ObRawExpr *> &new_column_list,
                                     bool ignore_dup_select_expr = true,
                                     bool repeated_select = false);

  static int create_select_item(ObIAllocator &allocator,
                                ObRawExpr *select_expr,
                                ObSelectStmt *select_stmt);


  static int create_select_item(ObIAllocator &allocator,
                                ObIArray<ColumnItem> &column_items,
                                ObSelectStmt *select_stmt);

  static int create_select_item(ObIAllocator &allocator,
                                const common::ObIArray<ObRawExpr*> &select_exprs,
                                ObSelectStmt *select_stmt);

  static int copy_stmt(ObStmtFactory &stmt_factory,
                       const ObDMLStmt *stmt,
                       ObDMLStmt *&new_stmt);

  static int deep_copy_stmt(ObStmtFactory &stmt_factory,
                            ObRawExprFactory &expr_factory,
                            const ObDMLStmt *stmt,
                            ObDMLStmt *&new_stmt);

  /**
   * @brief joined_table需要维护一个基表的table id列表
   * 对于它的左右子节点，如果是基表 或者generated table，直接使用其table id；
   * 如果是joined_table,需要将它的single_table_ids全部搬过来
   *
   * todo(@ banliu.zyd) 这部分逻辑本来在resolver层，逻辑差别太大不好复用，
   * 而这部分逻辑还是有必要单独提出来以增强代码可读性和代码复用，加个todo在这，
   * 是因为这个函数目前作用单一，从功能上来看更应该是JoinedTable的一个成员方法
   */
  static int add_joined_table_single_table_ids(JoinedTable &joined_table, TableItem &child_table);

  static int replace_expr(ObRawExpr *old_expr, ObRawExpr *new_expr, ObRawExpr *&expr);

  static int replace_expr(const common::ObIArray<ObRawExpr *> &other_exprs,
                          const common::ObIArray<ObRawExpr *> &new_exprs,
                          ObRawExpr *&expr);

  static int replace_expr_for_order_item(const common::ObIArray<ObRawExpr *> &other_exprs,
                                         const common::ObIArray<ObRawExpr *> &new_exprs,
                                         common::ObIArray<OrderItem> &order_items);
  template <typename T>
  static int replace_specific_expr(const common::ObIArray<T*> &other_subquery_exprs,
                                   const common::ObIArray<T*> &subquery_exprs,
                                   ObRawExpr *&expr);

  template <typename T>
  static int replace_exprs(const common::ObIArray<ObRawExpr *> &other_exprs,
                           const common::ObIArray<ObRawExpr *> &new_exprs,
                           common::ObIArray<T*> &exprs);

  static int update_table_id_for_from_item(const common::ObIArray<FromItem> &other_from_items,
                                           const uint64_t old_table_id,
                                           const uint64_t new_table_id,
                                           common::ObIArray<FromItem> &from_items);
  static int update_table_id_for_joined_tables(const common::ObIArray<JoinedTable*> &other_joined_tables,
                                               const uint64_t old_table_id,
                                               const uint64_t new_table_id,
                                               common::ObIArray<JoinedTable*> &joined_tables);
  static int update_table_id_for_joined_table(const JoinedTable &other_joined_table,
                                              const uint64_t old_table_id,
                                              const uint64_t new_table_id,
                                              JoinedTable &joined_table);
  static int update_table_id_for_part_item(const common::ObIArray<ObDMLStmt::PartExprItem> &other_part_items,
                                           const uint64_t old_table_id,
                                           const uint64_t new_table_id,
                                           common::ObIArray<ObDMLStmt::PartExprItem> &part_items);
  static int update_table_id_for_check_constraint_items(
             const common::ObIArray<ObDMLStmt::CheckConstraintItem> &other_check_constraint_items,
             const uint64_t old_table_id,
             const uint64_t new_table_id,
             common::ObIArray<ObDMLStmt::CheckConstraintItem> &check_constraint_items);
  static int update_table_id_for_semi_info(const ObIArray<SemiInfo*> &other_semi_infos,
                                           const uint64_t old_table_id,
                                           const uint64_t new_table_id,
                                           ObIArray<SemiInfo*> &semi_infos);
  static int update_table_id_for_column_item(const common::ObIArray<ColumnItem> &other_column_items,
                                             const uint64_t old_table_id,
                                             const uint64_t new_table_id,
                                             const int32_t old_bit_id,
                                             const int32_t new_bit_id,
                                             common::ObIArray<ColumnItem> &column_items);
  static int update_table_id_for_pseudo_columns(const ObIArray<ObRawExpr*> &other_pseudo_columns,
                                                const uint64_t old_table_id,
                                                const uint64_t new_table_id,
                                                const int32_t old_bit_id,
                                                const int32_t new_bit_id,
                                                ObIArray<ObRawExpr*> &pseudo_columns);
  static int update_table_id_index(const ObRelIds &old_ids,
                                   const int32_t old_bit_id,
                                   const int32_t new_bit_id,
                                   ObRelIds &new_ids);
  static int update_table_id(const common::ObIArray<uint64_t> &old_ids,
                             const uint64_t old_table_id,
                             const uint64_t new_table_id,
                             common::ObIArray<uint64_t> &new_ids);

  //仅供window function相关改写使用
  static bool is_valid_type(ObItemType expr_type);

  /**
   * @brief is_expr_query
   * 如果一个子查询不引入新的relation，并且select item只有一项
   * 那么它至多返回一个值，行为类似于一个表达式
   */
  static int is_expr_query(const ObSelectStmt *stmt, bool &is_expr_type);
  
  /**
   * @brief is_aggr_query
   * 如果一个查询的select item只有一项聚合函数，并且没有 group 表达式，那么该查询至多返回一个值
   * 如果该查询不引用上层block的表，那么该查询可以独立于上层查询执行
   */
  static int is_aggr_query(const ObSelectStmt *stmt, bool &is_aggr_type);

  /**
   * @brief add_is_not_null
   * 增加对 child_expr 结果的 not null 判断
   * @param stmt
   * @param child_expr
   * @return
   */
  static int add_is_not_null(ObTransformerCtx *ctx, const ObDMLStmt *stmt,
                             ObRawExpr *child_expr, ObOpRawExpr *&is_not_expr);

  static int is_column_nullable(const ObDMLStmt *stmt,
                                ObSchemaChecker *schema_checker,
                                const ObColumnRefRawExpr *col_expr,
                                const ObSQLSessionInfo *session_info,
                                bool &is_nullable);

  static int flatten_joined_table(ObDMLStmt *stmt);

  static int flatten_expr(ObRawExpr *expr,
                          common::ObIArray<ObRawExpr*> &flattened_exprs);
  static int flatten_and_or_xor(ObTransformerCtx *ctx, ObIArray<ObRawExpr*> &conditions, bool *trans_happened = NULL);
  static int flatten_and_or_xor(ObRawExpr* expr, bool *trans_happened = NULL);
  static int find_not_null_expr(const ObDMLStmt &stmt,
                                ObRawExpr *&not_null_expr,
                                bool &is_valid,
                                ObTransformerCtx *ctx);
  
  static int is_expr_not_null(ObTransformerCtx *ctx,
                              const ObDMLStmt *stmt,
                              const ObRawExpr *expr,
                              int context_scope,
                              bool &is_not_null,
                              ObIArray<ObRawExpr *> *constraints = NULL);

  static int is_expr_not_null(ObNotNullContext &ctx,
                              const ObRawExpr *expr, 
                              bool &is_not_null,
                              ObIArray<ObRawExpr *> *constraints);

  static int is_column_expr_not_null(ObNotNullContext &ctx,
                                     const ObColumnRefRawExpr *expr,
                                     bool &is_not_null,
                                     ObIArray<ObRawExpr *> *constraints);

  static int is_set_expr_not_null(ObNotNullContext &ctx,
                                  const ObSetOpRawExpr *expr,
                                  bool &is_not_null,
                                  ObIArray<ObRawExpr *> *constraints);
  
  static int is_const_expr_not_null(ObNotNullContext &ctx,
                                    const ObRawExpr *expr,
                                    bool &is_not_null,
                                    bool &is_null);

  static int is_general_expr_not_null(ObNotNullContext &ctx,
                                      const ObRawExpr *expr,
                                      bool &is_not_null,
                                      ObIArray<ObRawExpr *> *constraints);

  /**
   * @brief has_null_reject_condition
   * 判断 conditions 中是否存在空值拒绝条件
   */
  static int has_null_reject_condition(const ObIArray<ObRawExpr *> &conditions,
                                       const ObRawExpr *expr,
                                       bool &has_null_reject);

  static int has_null_reject_condition(const ObIArray<ObRawExpr *> &conditions,
                                       const ObIArray<ObRawExpr *> &targets,
                                       bool &has_null_reject);

  /**
   * @brief is_null_reject_conditions
   * 检查conditions是否有拒绝指定表集上空值的谓词
   */
  static int is_null_reject_conditions(const ObIArray<ObRawExpr *> &conditions,
                                       const ObRelIds &target_table,
                                       bool &is_null_reject);
  
  
  /**
   * @brief is_null_reject_condition
   * 判断当前条件是否构成一个的空值拒绝条件，满足以下条件之一
   * 1. targets 均为 null 时， condition = null;
   * 2. targets 均为 null 时， condition = false
   */
  static int is_null_reject_condition(const ObRawExpr *condition,
                                      const ObIArray<const ObRawExpr *> &targets,
                                      bool &is_null_reject);

  /**
   * @brief is_simple_null_reject
   * 可能返回 false 的 null reject 条件
   */
  static int is_simple_null_reject(const ObRawExpr *condition,
                                   const ObIArray<const ObRawExpr *> &targets,
                                   bool &is_null_reject);

  static int is_null_propagate_expr(const ObRawExpr *expr,
                                    const ObIArray<ObRawExpr *> &targets,
                                    bool &bret);

  /**
   * @brief is_null_propagate_expr
   * 判断 expr 是否能够传递 target 产生的空值。
   * 当 targets 均为 NULL 时，expr 输出必然为 NULL，检查以下条件是否成立：
   * 1. targets 中的某个表达式 x_expr 存在于 expr 中
   * 2. x_expr 所在的计算路径上，涉及的表达式如果输入为NULL，那么输出必然为NULL
   */
  static int is_null_propagate_expr(const ObRawExpr *expr,
                                    const ObIArray<const ObRawExpr *> &targets,
                                    bool &bret);

  /**
   * @brief find_expr
   * 检查 target 是否存在于 source 中
   */
  static int find_expr(const ObIArray<const ObRawExpr *> &source,
                       const ObRawExpr *target,
                       bool &bret,
                       ObExprEqualCheckContext *check_context = NULL);

  static int find_expr(ObIArray<ObRawExpr *> &source,
                       ObRawExpr *target,
                       bool &bret,
                       ObExprEqualCheckContext *check_context = NULL);

  static int find_expr(const ObIArray<OrderItem> &source,
                       const ObRawExpr *target,
                       bool &bret);

  template <typename T>
  static int get_expr_idx(const ObIArray<T *> &source,
                          const T *target,
                          int64_t &idx);
  /**
   * @brief is_null_propagate_type
   * 简单空值传递表达式类型的列表
   */
  static bool is_null_propagate_type(const ObItemType type);

  static bool is_not_null_deduce_type(const ObItemType type);

  static int get_simple_filter_column(const ObDMLStmt *stmt,
                                      ObRawExpr *expr,
                                      int64_t table_id,
                                      ObIArray<ObColumnRefRawExpr*> &col_exprs);

  static int get_parent_stmt(const ObDMLStmt *root_stmt,
                             const ObDMLStmt *stmt,
                             const ObDMLStmt *&parent_stmt,
                             int64_t &table_id,
                             bool &is_valid);

  static int get_simple_filter_column_in_parent_stmt(const ObDMLStmt *root_stmt,
                                                     const ObDMLStmt *stmt,
                                                     const ObDMLStmt *view_stmt,
                                                     int64_t table_id,
                                                     ObIArray<ObColumnRefRawExpr*> &col_exprs);

  static int get_filter_columns(const ObDMLStmt *root_stmt,
                                const ObDMLStmt *stmt,
                                int64_t table_id,
                                ObIArray<ObColumnRefRawExpr*> &col_exprs);

  static int check_column_match_index(const ObDMLStmt *root_stmt,
                                      const ObDMLStmt *stmt,
                                      ObSqlSchemaGuard *schema_guard,
                                      const ObColumnRefRawExpr *col_expr,
                                      bool &is_match);

  static int check_select_item_match_index(const ObDMLStmt *root_stmt,
                                           const ObSelectStmt *stmt,
                                           ObSqlSchemaGuard *schema_guard,
                                           int64_t sel_index,
                                           bool &is_match);

  static int get_vaild_index_id(ObSqlSchemaGuard *schema_guard,
                                const ObDMLStmt *stmt,
                                const TableItem *table_item,
                                ObIArray<uint64_t> &index_ids);

  static int get_range_column_items_by_ids(const ObDMLStmt *stmt,
                                           uint64_t table_id,
                                           const ObIArray<uint64_t> &column_ids,
                                           ObIArray<ColumnItem> &column_items);

  static int check_index_extract_query_range(const ObDMLStmt *stmt,
                                             uint64_t table_id,
                                             const ObIArray<uint64_t> &index_cols,
                                             const ObIArray<ObRawExpr *> &predicate_exprs,
                                             ObTransformerCtx *ctx,
                                             bool &is_match);

  static int is_match_index(ObSqlSchemaGuard *schema_guard,
                            const ObDMLStmt *stmt,
                            const ObColumnRefRawExpr *col_expr,
                            bool &is_match,
                            EqualSets *equal_sets = NULL,
                            ObIArray<ObRawExpr*> *const_exprs = NULL,
                            ObIArray<ObColumnRefRawExpr*> *col_exprs = NULL,
                            const bool need_match_col_exprs = false,
                            const bool need_check_query_range = false,
                            ObTransformerCtx *ctx = NULL);

  static int is_match_index(const ObDMLStmt *stmt,
                            const ObIArray<uint64_t> &index_cols,
                            const ObColumnRefRawExpr *col_expr,
                            bool &is_match,
                            EqualSets *equal_sets = NULL,
                            ObIArray<ObRawExpr*> *const_exprs = NULL,
                            ObIArray<ObColumnRefRawExpr*> *col_exprs = NULL,
                            const bool need_match_col_exprs = false);

  static int classify_scalar_query_ref(ObIArray<ObRawExpr*> &exprs,
                                       ObIArray<ObRawExpr*> &scalar_query_refs,
                                       ObIArray<ObRawExpr*> &non_scalar_query_refs);

  static int classify_scalar_query_ref(ObRawExpr *expr,
                                       ObIArray<ObRawExpr*> &scalar_query_refs,
                                       ObIArray<ObRawExpr*> &non_scalar_query_refs);

  static int extract_query_ref_expr(const ObIArray<ObRawExpr*> &exprs,
                                    ObIArray<ObQueryRefRawExpr *> &subqueries,
                                    const bool with_nested = true);

  static int extract_query_ref_expr(ObRawExpr *expr,
                                    ObIArray<ObQueryRefRawExpr *> &subqueries,
                                    const bool with_nested = true);

  static int extract_aggr_expr(ObIArray<ObRawExpr*> &exprs,
                               ObIArray<ObAggFunRawExpr*> &aggrs);

  static int extract_aggr_expr(ObRawExpr *expr,
                               ObIArray<ObAggFunRawExpr*> &aggrs);

  static int extract_winfun_expr(ObIArray<ObRawExpr*> &exprs,
                                 ObIArray<ObWinFunRawExpr*> &win_exprs);

  static int extract_winfun_expr(ObRawExpr *expr,
                                 ObIArray<ObWinFunRawExpr*> &win_exprs);

  static int extract_alias_expr(ObRawExpr *expr,
                                ObIArray<ObAliasRefRawExpr *> &alias_exprs);

  static int extract_alias_expr(ObIArray<ObRawExpr*> &exprs,
                                ObIArray<ObAliasRefRawExpr *> &alias_exprs);

  /**
   * @brief check_foreign_primary_join
   * 检查first_table和second_table之间的连接是否为主外键连接
   *
   * @param first_exprs             第一个表的连接列
   * @param second_exprs            第二个表的连接列
   * @param is_foreign_primary_join 是否为主外键连接
   * @param is_first_table_parent   first_table是否为父表
   */
  static int check_foreign_primary_join(const TableItem *first_table,
                                        const TableItem * second_table,
                                        const ObIArray<const ObRawExpr *> &first_exprs,
                                        const ObIArray<const ObRawExpr *> &second_exprs,
                                        ObSchemaChecker *schema_checker,
                                        ObSQLSessionInfo *session_info,
                                        bool &is_foreign_primary_join,
                                        bool &is_first_table_parent,
                                        share::schema::ObForeignKeyInfo *&foreign_key_info);
  static int check_foreign_primary_join(const TableItem *first_table,
                                        const TableItem * second_table,
                                        const ObIArray< ObRawExpr *> &first_exprs,
                                        const ObIArray< ObRawExpr *> &second_exprs,
                                        ObSchemaChecker *schema_checker,
                                        ObSQLSessionInfo *session_info,
                                        bool &is_foreign_primary_join,
                                        bool &is_first_table_parent,
                                        share::schema::ObForeignKeyInfo *&foreign_key_info);

  /**
   * @brief is_all_foreign_key_involved
   * 检查child_exprs和parent_exprs是否包含了子表和父表主外键约束中一一对应的所有的键
   *
   * e.g. t2上存在两个外键约束foreign key (c1, c2) references t1(c1, c2)
   *                        和foreign key (c3, c4) references t1(c1, c2)
   *      则要求child_exprs = [c1, c2] 且 parent_exprs = [c1, c2]
   *          或child_exprs = [c3, c4] 且 parent_exprs = [c1, c2]
   *
   * @param is_all_involved       是否包含了主外键约束中一一对应的所有的键
   */
  static int is_all_foreign_key_involved(const ObIArray<const ObRawExpr *> &child_exprs,
                                         const ObIArray<const ObRawExpr *> &parent_exprs,
                                         const share::schema::ObForeignKeyInfo &info,
                                         bool &is_all_involved);
  static int is_all_foreign_key_involved(const ObIArray< ObRawExpr *> &child_exprs,
                                         const ObIArray< ObRawExpr *> &parent_exprs,
                                         const share::schema::ObForeignKeyInfo &info,
                                         bool &is_all_involved);

  /**
   * @brief is_foreign_key_rely
   * 判定主外键是否可靠，在MYSQL模式下检查全局变量foreign_key_check
   * 在ORACLE模式下检查foreign key info里面的enable_flag
   */
  static int is_foreign_key_rely (ObSQLSessionInfo* session_info,
                                  const share::schema::ObForeignKeyInfo *foreign_key_info,
                                  bool &is_rely);


  static int check_stmt_limit_validity(ObTransformerCtx *ctx,
                                       const ObSelectStmt *select_stmt,
                                       bool &is_valid,
                                       bool &need_add_const_constraint);

  static int check_stmt_is_non_sens_dul_vals(ObTransformerCtx *ctx,
                                             const ObDMLStmt *upper_stmt,
                                             const ObDMLStmt *stmt,
                                             bool &is_match,
                                             bool &need_add_limit_constraint);

  
  /**
   * @brief 
   * to check if semi join can be transformed
   *    select * from t1 where c1 = 3 or exists (select 1 from t1 left join t2 on t1.c1 = t2.c1);
   * ==>
   *    select * from t1 where c1 = 3 or exists (select 1 from t1);
   * @param ctx 
   * @param stmt 
   * @param is_match 
   * @param need_add_limit_constraint 
   * @return int 
   */
  static int check_stmt_is_non_sens_dul_vals_rec(ObTransformerCtx *ctx,
                                              const ObDMLStmt *stmt,
                                              const ObRawExpr *expr,
                                              bool &is_match,
                                              bool &need_add_limit_constraint);
  /**
   * @brief check_exprs_unique
   * 检查 exprs 在 table 上是否有唯一性
   * @param stmt
   * @param table
   * @param exprs 需要检查唯一性的表达式集
   * @param conditions 提供 null reject 检测谓词
   * @param is_unique 是否有唯一性
   */
  static int check_exprs_unique(const ObDMLStmt &stmt,
                                TableItem *table,
                                const ObIArray<ObRawExpr*> &exprs,
                                const ObIArray<ObRawExpr*> &conditions,
                                ObSQLSessionInfo *session_info,
                                ObSchemaChecker *schema_checker,
                                bool &is_unique);

  /**
   * @brief check_exprs_unique
   * 检查 exprs 在 table 上是否有唯一性
   * @param stmt
   * @param table
   * @param exprs 需要检查唯一性的表达式集, 不考虑空值
   * @param is_unique 是否有唯一性
   */
  static int check_exprs_unique(const ObDMLStmt &stmt,
                                TableItem *table,
                                const ObIArray<ObRawExpr*> &exprs,
                                ObSQLSessionInfo *session_info,
                                ObSchemaChecker *schema_checker,
                                bool &is_unique);

  /**
   * @brief check_exprs_unique_on_table_items
   * 检查 table_items 经过 conditions 连接后，exprs 是否有唯一性
   * @param stmt
   * @param table_items 表集
   * @param exprs 需要检查唯一性的表达式集
   * @param conditions from items的连接条件
   * @param is_strict 是否考虑空值
   * @param is_unique 是否有唯一性
   */
  static int check_exprs_unique_on_table_items(const ObDMLStmt *stmt,
                                               ObSQLSessionInfo *session_info,
                                               ObSchemaChecker *schema_checker,
                                               const ObIArray<TableItem*> &table_items,
                                               const ObIArray<ObRawExpr*> &exprs,
                                               const ObIArray<ObRawExpr*> &conditions,
                                               bool is_strict,
                                               bool &is_unique);

  static int check_exprs_unique_on_table_items(const ObDMLStmt *stmt,
                                               ObSQLSessionInfo *session_info,
                                               ObSchemaChecker *schema_checker,
                                               TableItem *table,
                                               const ObIArray<ObRawExpr*> &exprs,
                                               const ObIArray<ObRawExpr*> &conditions,
                                               bool is_strict,
                                               bool &is_unique);

  /**
   * @brief check_stmt_unique
   * 检查 stmt 的 select 输出是否有唯一性
   * @param stmt
   * @param is_strict 是否考虑空值
   * @param is_unique 是否有唯一性
   */
  static int check_stmt_unique(const ObSelectStmt *stmt,
                               ObSQLSessionInfo *session_info,
                               ObSchemaChecker *schema_checker,
                               const bool is_strict,
                               bool &is_unique);

  /**
   * @brief check_stmt_unique
   * 检查 exprs 在 stmt 中是否有唯一性
   * @param stmt
   * @param exprs 需要检查唯一性的表达式集
   * @param is_strict 是否考虑空值
   * @param is_unique 是否有唯一性
   * @param extra_flag 是否忽略 distinct/group 等对唯一性影响
   */
  static int check_stmt_unique(const ObSelectStmt *stmt,
                               ObSQLSessionInfo *session_info,
                               ObSchemaChecker *schema_checker,
                               const ObIArray<ObRawExpr *> &exprs,
                               const bool is_strict,
                               bool &is_unique,
                               const uint64_t extra_flag = FLAGS_DEFAULT);

  /**
   * @brief compute_stmt_property
   * 计算 stmt 输出的 fd_sets/equal_sets/const_expr 等信息
   * @param stmt
   * @param res_info 计算结果
   * @param extra_flag 是否忽略 distinct/group 等对信息影响
   */
  static int compute_stmt_property(const ObSelectStmt *stmt,
                                   UniqueCheckHelper &check_helper,
                                   UniqueCheckInfo &res_info,
                                   const uint64_t extra_flags = FLAGS_DEFAULT);

  static int compute_set_stmt_property(const ObSelectStmt *stmt,
                                       UniqueCheckHelper &check_helper,
                                       UniqueCheckInfo &res_info,
                                       const uint64_t extra_flags = FLAGS_DEFAULT);

  static int compute_path_property(const ObDMLStmt *stmt,
                                   UniqueCheckHelper &check_helper,
                                   UniqueCheckInfo &res_info);

  static int compute_tables_property(const ObDMLStmt *stmt,
                                     UniqueCheckHelper &check_helper,
                                     const ObIArray<TableItem*> &table_items,
                                     const ObIArray<ObRawExpr*> &conditions,
                                     UniqueCheckInfo &res_info);

  static int compute_table_property(const ObDMLStmt *stmt,
                                    UniqueCheckHelper &check_helper,
                                    const TableItem *table,
                                    ObIArray<ObRawExpr*> &cond_exprs,
                                    UniqueCheckInfo &res_info);

  static int compute_basic_table_property(const ObDMLStmt *stmt,
                                          UniqueCheckHelper &check_helper,
                                          const TableItem *table,
                                          ObIArray<ObRawExpr*> &cond_exprs,
                                          UniqueCheckInfo &res_info);
  static int need_compute_fd_item_set(ObIArray<ObRawExpr*> &exprs);
  static int try_add_table_fd_for_rowid(const ObSelectStmt *stmt,
                                        ObFdItemFactory &fd_factory,
                                        ObIArray<ObFdItem *> &fd_item_set,
                                        const ObSqlBitSet<> &tables);

  static int compute_generate_table_property(const ObDMLStmt *stmt,
                                             UniqueCheckHelper &check_helper,
                                             const TableItem *table,
                                             ObIArray<ObRawExpr*> &cond_exprs,
                                             UniqueCheckInfo &res_info);

  static int compute_inner_join_property(const ObDMLStmt *stmt,
                                         UniqueCheckHelper &check_helper,
                                         const JoinedTable *table,
                                         ObIArray<ObRawExpr*> &cond_exprs,
                                         UniqueCheckInfo &res_info);

  static int compute_inner_join_property(const ObDMLStmt *stmt,
                                         UniqueCheckHelper &check_helper,
                                         UniqueCheckInfo &left_info,
                                         UniqueCheckInfo &right_info,
                                         const ObIArray<ObRawExpr*> &inner_join_cond_exprs,
                                         ObIArray<ObRawExpr*> &cond_exprs,
                                         UniqueCheckInfo &res_info);

  static int compute_outer_join_property(const ObDMLStmt *stmt,
                                         UniqueCheckHelper &check_helper,
                                         const JoinedTable *table,
                                         ObIArray<ObRawExpr*> &cond_exprs,
                                         UniqueCheckInfo &res_info);

  static int get_equal_set_conditions(ObRawExprFactory &expr_factory,
                                      ObSQLSessionInfo *session_info,
                                      const ObSelectStmt *stmt,
                                      ObIArray<ObRawExpr*> &set_exprs,
                                      ObIArray<ObRawExpr*> &equal_conds);

  static int extract_udt_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &udt_exprs);

  static int extract_udf_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &udf_exprs);

  // json object with star : json_object(*)
  static int check_is_json_constraint(ObTransformerCtx *ctx,
                                      ObDMLStmt *stmt,
                                      ColumnItem& col_item,
                                      bool &is_json);
  static int extract_json_object_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &json_exprs);
  static int expand_wild_star_to_columns(ObTransformerCtx *ctx,
                                         ObDMLStmt *stmt,
                                         ObSysFunRawExpr *json_object_expr);
  static int get_columnitem_from_json_table(ObDMLStmt *stmt,
                                            const TableItem *tmp_table_item,
                                            ObSEArray<ColumnItem, 4>& column_list);
  static int get_column_node_from_table(ObTransformerCtx *ctx,
                                        ObDMLStmt *stmt,
                                        ObString& tab_name,
                                        ObSEArray<ColumnItem, 4>& column_list,
                                        bool all_tab,
                                        bool &tab_has_alias,
                                        TableItem *&tab_item,
                                        bool &is_empty_table);
  static int add_column_expr_for_json_object_node(ObTransformerCtx *ctx,
                                                  ObDMLStmt *stmt,
                                                  ColumnItem& col_item,
                                                  ObSEArray<ObRawExpr *, 1>& param_array);
  static int add_dummy_expr_for_json_object_node(ObTransformerCtx *ctx,
                                                 ObSEArray<ObRawExpr *, 1>& param_array);
  static int get_expand_node_from_star(ObTransformerCtx *ctx,
                                       ObDMLStmt *stmt,
                                       ObRawExpr *param_expr,
                                       ObSEArray<ObRawExpr *, 1>& param_array);
  // end json object with star


  static int add_cast_for_replace(ObRawExprFactory &expr_factory,
                                  const ObRawExpr *from_expr,
                                  ObRawExpr *&to_expr,
                                  ObSQLSessionInfo *session_info);

  static int add_cast_for_replace_if_need(ObRawExprFactory &expr_factory,
                                          const ObRawExpr *from_expr,
                                          ObRawExpr *&to_expr,
                                          ObSQLSessionInfo *session_info);

  static int extract_table_exprs(const ObDMLStmt &stmt,
                                 const ObIArray<ObRawExpr *> &source_exprs,
                                 const TableItem &target,
                                 ObIArray<ObRawExpr *> &exprs);

  static int extract_table_exprs(const ObDMLStmt &stmt,
                                 const ObIArray<ObRawExpr *> &source_exprs,
                                 const ObIArray<TableItem*> &tables,
                                 ObIArray<ObRawExpr *> &exprs);

  static int extract_table_exprs(const ObDMLStmt &stmt,
                                 const ObIArray<ObRawExpr *> &source_exprs,
                                 const ObSqlBitSet<> &table_set,
                                 ObIArray<ObRawExpr *> &table_exprs);
  static int extract_table_rel_ids(const ObIArray<ObRawExpr*> &exprs,
                                   ObRelIds& table_ids);
  static int get_table_joined_exprs(const ObDMLStmt &stmt,
                                    const TableItem &source,
                                    const TableItem &target,
                                    const ObIArray<ObRawExpr *> &conditions,
                                    ObIArray<ObRawExpr *> &target_exprs,
                                    ObSqlBitSet<> &join_source_ids,
                                    ObSqlBitSet<> &join_target_ids);

  static int get_table_joined_exprs(const ObDMLStmt &stmt,
                                    const ObIArray<TableItem *> &sources,
                                    const TableItem &target,
                                    const ObIArray<ObRawExpr *> &conditions,
                                    ObIArray<ObRawExpr *> &target_exprs);

  static int get_table_joined_exprs(const ObSqlBitSet<> &source_ids,
                                    const ObSqlBitSet<> &target_ids,
                                    const ObIArray<ObRawExpr *> &conditions,
                                    ObIArray<ObRawExpr *> &target_exprs,
                                    ObSqlBitSet<> &join_source_ids,
                                    ObSqlBitSet<> &join_target_ids);

  static int get_from_item(ObDMLStmt *stmt, TableItem *table, FromItem &from);
  
  static int get_outer_join_right_tables(const JoinedTable &joined_table,
                                         ObIArray<uint64_t> &table_ids);

  /**
   * @brief is_equal_correlation
   * expr(outer.c) = expr(inner.c)
   * 1. 等值过滤条件
   * 2. 一侧有且仅有上层的列
   * 3. 一侧有且仅有本层的列
   * @return
   */
  static int is_equal_correlation(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                  ObRawExpr *cond,
                                  bool &is_valid,
                                  ObRawExpr **outer_param = NULL,
                                  ObRawExpr **inner_param = NULL);

  static int is_semi_join_right_table(const ObDMLStmt &stmt,
                                      const uint64_t table_id,
                                      bool &is_semi_table);

  /**
   * @brief trans_column_items
   * 如果col0 \in source, col1 \in target，且两者指向相同的列；
   * 那么删除col1，并将所有表达式中指向col1的指针改为指向col0。
   *
   * 如果col仅在target中使用，那么将col中的表信息改为source的信息
   * @param stmt
   * @param table_id
   * @return
   */

  static int merge_table_items(ObDMLStmt *stmt,
                               const TableItem *source_table,
                               const TableItem *target_table,
                               const ObIArray<int64_t> *output_map,
                               ObIArray<ObRawExpr *> *old_target_col_expr = NULL,
                               ObIArray<ObRawExpr *> *new_target_col_expr = NULL,
                               ObIArray<ObRawExpr *> *pushed_pseudo_col_exprs = NULL,
                               ObIArray<ObRawExpr *> *merged_pseudo_col_exprs = NULL);

  static int merge_table_items(ObSelectStmt *source_stmt,
                               ObSelectStmt *target_stmt,
                               const TableItem *source_table,
                               const TableItem *target_table,
                               ObIArray<ObRawExpr*> &old_exprs,
                               ObIArray<ObRawExpr*> &new_exprs);

  static int find_parent_expr(ObDMLStmt *stmt,
                              ObRawExpr *target,
                              ObRawExpr *&root,
                              ObRawExpr *&parent);

  static int find_parent_expr(ObRawExpr *expr,
                              ObRawExpr *target,
                              ObRawExpr *&parent);

  static int find_relation_expr(ObDMLStmt *stmt,
                                ObIArray<ObRawExpr *> &targets,
                                ObIArray<ObRawExprPointer> &parents);

  static int generate_unique_key_for_basic_table(ObTransformerCtx *ctx,
                                                 ObDMLStmt *stmt,
                                                 TableItem *item,
                                                 ObIArray<ObRawExpr *> &unique_keys,
                                                 int64_t *rowkey_count = NULL);

  static int check_loseless_join(ObDMLStmt *stmt,
                                 ObTransformerCtx *ctx,
                                 TableItem *source_table,
                                 TableItem *target_table,
                                 ObSQLSessionInfo *session_info,
                                 ObSchemaChecker *schema_checker,
                                 ObStmtMapInfo &stmt_map_info,
                                 bool is_on_null_side,
                                 bool &is_loseless,
                                 EqualSets *input_equal_sets = NULL);

  /**
   * @brief check_relations_containment
   * 检查两个关系集合的包含关系
   * @param stmt
   * @param source_rels 第一个关系集合
   * @param target_rels 第二个关系集合
   * @param stmt_map_infos 如果关系中有generated_table，stmt_map_info记录generated_table stmt各个stmt的映射关系
   * @param rel_map_info 关系集合的映射
   * @param is_contain 返回第二个关系集合是否包含第一个关系集合
   */
  static int check_relations_containment(ObDMLStmt *stmt,
                                        const common::ObIArray<TableItem*> &source_rels,
                                        const common::ObIArray<TableItem*> &target_rels,
                                        common::ObIArray<ObStmtMapInfo> &stmt_map_infos,
                                        common::ObIArray<int64_t> &rel_map_info,
                                        bool &is_contain);

  static int check_table_item_containment(ObDMLStmt *stmt,
                                          const TableItem *source_table,
                                          ObDMLStmt *target_stmt,
                                          const TableItem *target_table,
                                          ObStmtMapInfo &stmt_map_info,
                                          bool &is_contain);

  static int extract_lossless_join_columns(ObDMLStmt *stmt,
                                           ObTransformerCtx *ctx,
                                           const TableItem *source_table,
                                           const TableItem *target_table,
                                           const ObIArray<int64_t> &output_map,
                                           ObIArray<ObRawExpr*> &source_exprs,
                                           ObIArray<ObRawExpr*> &target_exprs,
                                           EqualSets *input_equal_sets = NULL);

  static int extract_lossless_mapping_columns(ObDMLStmt *stmt,
                                              const TableItem *source_table,
                                              const TableItem *target_table,
                                              const ObIArray<int64_t> &output_map,
                                              ObIArray<ObRawExpr*> &candi_source_exprs,
                                              ObIArray<ObRawExpr*> &candi_target_exprs);
  static int check_at_least_one_row(TableItem *table_item, bool &at_least_one_row);
  static int adjust_agg_and_win_expr(ObSelectStmt *source_stmt,
                                     ObRawExpr *&source_expr);

  static int check_group_by_consistent(ObSelectStmt *sel_stmt,
                                       bool &is_consistent);

  static int contain_select_ref(ObRawExpr *expr, bool &has);


  static int remove_select_items(ObTransformerCtx *ctx,
                                 const uint64_t table_id,
                                 ObSelectStmt &child_stmt,
                                 ObDMLStmt &upper_stmt,
                                 ObIArray<ObRawExpr*> &removed_select_exprs);

  static int remove_select_items(ObTransformerCtx *ctx,
                                 const uint64_t table_id,
                                 ObSelectStmt &child_stmt,
                                 ObDMLStmt &upper_stmt,
                                 ObSqlBitSet<> &removed_idxs);

  static int remove_select_items(ObTransformerCtx *ctx,
                                 ObSelectStmt &union_stmt,
                                 ObSqlBitSet<> &removed_idxs);

  static int create_dummy_select_item(ObSelectStmt &stmt, ObTransformerCtx *ctx);

  static int remove_column_if_no_ref(ObSelectStmt &stmt,
                                     ObIArray<ObRawExpr*> &removed_exprs);

  static int create_set_stmt(ObTransformerCtx *ctx,
                             const ObSelectStmt::SetOperator set_type,
                             const bool is_distinct,
                             ObIArray<ObSelectStmt*> &child_stmts,
                             ObSelectStmt *&union_stmt);

  static int create_set_stmt(ObTransformerCtx *ctx,
                             const ObSelectStmt::SetOperator set_type,
                             const bool is_distinct,
                             ObSelectStmt *left_stmt,
                             ObSelectStmt *right_stmt,
                             ObSelectStmt *&union_stmt);

  static int pushdown_group_by(ObSelectStmt *parent_stmt,
                               ObIArray<ObRawExpr *> &pushdown_groupby,
                               ObIArray<ObRawExpr *> &pushdown_rollup,
                               ObIArray<ObRawExpr *> &pushdown_aggr);

  static int create_simple_view(ObTransformerCtx *ctx,
                                ObDMLStmt *stmt,
                                ObSelectStmt *&view_stmt,
                                bool push_subquery = true,
                                bool push_conditions = true,
                                bool push_group_by = false,
                                ObAliasRefRawExpr *alias_expr = NULL);

  static int pushdown_pseudo_column_like_exprs(ObDMLStmt &upper_stmt,
                                               bool push_group_by,
                                               ObIArray<ObRawExpr*> &pushdown_exprs);
  static int check_need_pushdown_pseudo_column(const ObRawExpr &expr,
                                               const bool push_group_by,
                                               bool &need_pushdown);

  static int create_view_with_groupby_items(ObSelectStmt *stmt,
                                            TableItem *&view_stmt_item,
                                            ObTransformerCtx *ctx);

  static int transform_aggregation_exprs(ObTransformerCtx *ctx,
                                         ObSelectStmt *select_stmt,
                                         TableItem &view_item);
  static int transform_aggregation_expr(ObTransformerCtx *ctx,
                                        ObSelectStmt *select_stmt,
                                        TableItem &view_item,
                                        ObAggFunRawExpr &aggr_expr,
                                        ObRawExpr *&new_aggr_expr);
  static int create_view_with_pre_aggregate(ObSelectStmt *stmt,
                                            ObSelectStmt *&view_stmt,
                                            ObTransformerCtx *ctx);

  static int adjust_updatable_view(ObRawExprFactory &expr_factory,
                                   ObDelUpdStmt *stmt,
                                   TableItem &view_table_item,
                                   ObIArray<uint64_t>* origin_table_ids = NULL);

  static int create_stmt_with_generated_table(ObTransformerCtx *ctx,
                                              ObSelectStmt *child_stmt,
                                              ObSelectStmt *&parent_stmt);

  static int create_stmt_with_basic_table(ObTransformerCtx *ctx,
                                          ObDMLStmt *stmt,
                                          TableItem *table,
                                          ObSelectStmt *&simple_stmt);

  static int create_stmt_with_joined_table(ObTransformerCtx *ctx,
                                           ObDMLStmt *stmt,
                                           JoinedTable *joined_table,
                                           ObSelectStmt *&simple_stmt);

  static int copy_joined_table_expr(ObRawExprFactory &expr_factory,
                                    ObDMLStmt *stmt,
                                    JoinedTable *table);

  static int inner_copy_joined_table_expr(ObRawExprCopier &copier,
                                          JoinedTable *table);

  static int extract_right_tables_from_semi_infos(ObDMLStmt *stmt,
                                                  const ObIArray<SemiInfo *> &semi_infos, 
                                                  ObIArray<TableItem *> &tables);

  static int can_push_down_filter_to_table(TableItem &table, bool &can_push);

  static int add_limit_to_semi_right_table(ObDMLStmt *stmt,
                                           ObTransformerCtx *ctx,
                                           SemiInfo *semi_info);

  static int replace_table_in_stmt(ObDMLStmt *stmt,
                                   TableItem *other_table,
                                   TableItem *current_table);

  static int remove_tables_from_stmt(ObDMLStmt *stmt,
                                     TableItem *table_item,
                                     ObIArray<uint64_t> &table_ids);

  static int replace_table_in_semi_infos(ObDMLStmt *stmt,
                                         const TableItem *other_table,
                                         const TableItem *current_table);

  static int replace_table_in_joined_tables(ObDMLStmt *stmt,
                                            TableItem *other_table,
                                            TableItem *current_table);
  static int replace_table_in_joined_tables(TableItem *table,
                                            TableItem *other_table,
                                            TableItem *current_table);

  static int classify_rownum_conds(ObDMLStmt &stmt,
                                   ObIArray<ObRawExpr *> &spj_conds,
                                   ObIArray<ObRawExpr *> &rownum_conds);

  static int rebuild_select_items(ObSelectStmt &stmt,
                                  ObRelIds &output_rel_ids);
  static int replace_columns_and_aggrs(ObRawExpr *&expr, ObTransformerCtx *ctx);

  static int build_const_expr_for_count(ObRawExprFactory &expr_factory,
                                        const int64_t value,
                                        ObConstRawExpr *&expr);

  static int build_case_when_expr(ObDMLStmt &stmt,
                                  ObRawExpr *expr,
                                  ObRawExpr *then_expr,
                                  ObRawExpr *default_expr,
                                  ObRawExpr *&out_expr,
                                  ObTransformerCtx *ctx);
  static int build_case_when_expr(ObTransformerCtx *ctx,
                                  ObIArray<ObRawExpr*> &when_exprs,
                                  ObIArray<ObRawExpr*> &then_exprs,
                                  ObRawExpr *default_expr,
                                  ObCaseOpRawExpr *&case_expr);
  /**
   * @brief check_error_free_expr
   * Judging whether an expression has a high risk of reporting errors during execution.
   * @note The rules are mainly based on historical experience, results are not guaranteed to be accurate.
   *       Please use with care.
   */
  static int check_error_free_expr(ObRawExpr *expr, bool &is_error_free);
  static int check_error_free_exprs(ObIArray<ObRawExpr*> &exprs, bool &is_error_free);
  static int build_row_expr(ObRawExprFactory& expr_factory,
                            common::ObIArray<ObRawExpr*>& param_exprs,
                            ObOpRawExpr*& row_expr);

  static int query_cmp_to_value_cmp(const ObItemType cmp_type, ObItemType& new_type);

  static int merge_limit_as_zero(ObTransformerCtx &ctx,
                                  ObRawExpr *view_limit,
                                  ObRawExpr *upper_limit,
                                  ObRawExpr *view_offset,
                                  ObRawExpr *upper_offset,
                                  ObRawExpr *&limit_expr,
                                  ObRawExpr *&offset_expr,
                                  bool &is_valid);

  static int merge_limit_offset(ObTransformerCtx *ctx,
                                ObRawExpr *view_limit,
                                ObRawExpr *upper_limit,
                                ObRawExpr *view_offset,
                                ObRawExpr *upper_offset,
                                ObRawExpr *&limit_expr,
                                ObRawExpr *&offset_expr);
  static int compare_const_expr_result(ObTransformerCtx *ctx,
                                      ObRawExpr *expr,
                                      ObItemType op_type,
                                      int64_t value,
                                      bool &is_true);

  static int compare_const_expr_result(ObTransformerCtx *ctx,
                                        ObRawExpr &left_expr,
                                        ObItemType op_type,
                                        ObRawExpr &right_expr,
                                        bool &is_true);

  static int create_dummy_add_zero(ObTransformerCtx *ctx, ObRawExpr *&expr);

  static int get_stmt_limit_value(const ObDMLStmt &stmt, int64_t &limit);

  static int check_limit_value(const ObDMLStmt &stmt,
                               ObExecContext *exec_ctx,
                               ObIAllocator *allocator,
                               int64_t limit,
                               bool &is_equal,
                               ObPCConstParamInfo &const_param_info);

  static int convert_column_expr_to_select_expr(const common::ObIArray<ObRawExpr*> &column_exprs,
                                                const ObSelectStmt &inner_stmt,
                                                common::ObIArray<ObRawExpr*> &select_exprs);

  static int convert_set_op_expr_to_select_expr(const common::ObIArray<ObRawExpr*> &set_op_exprs,
                                                const ObSelectStmt &inner_stmt,
                                                common::ObIArray<ObRawExpr*> &select_exprs);

  /**
   * @brief convert_select_expr_to_column_expr
   * 将视图的select expr转换为outer stmt对应的column expr
   * @param select_exprs 视图的select exprs
   * @param table_id 视图在outer stmt的table id
   * @param column_exprs outer stmt对应的column exprs
   */
  static int convert_select_expr_to_column_expr(const common::ObIArray<ObRawExpr*> &select_exprs,
                                                const ObSelectStmt &inner_stmt,
                                                ObDMLStmt &outer_stmt,
                                                uint64_t table_id,
                                                common::ObIArray<ObRawExpr*> &column_exprs);

  static int pull_up_subquery(ObDMLStmt *parent_stmt,
                              ObSelectStmt *child_stmt);

  static int right_join_to_left(ObDMLStmt *stmt);

  static int change_join_type(TableItem *joined_table);

  static int get_subquery_expr_from_joined_table(ObDMLStmt *stmt,
                                                 common::ObIArray<ObQueryRefRawExpr *> &subqueries);

  static int get_on_conditions(ObDMLStmt &stmt,
                              common::ObIArray<ObRawExpr *> &conditions);

  static int get_on_condition(TableItem *table_item,
                              common::ObIArray<ObRawExpr *> &conditions);

  static int get_semi_conditions(ObIArray<SemiInfo *> &semi_infos,
                                 ObIArray<ObRawExpr *> &conditions);

  static int set_limit_expr(ObDMLStmt *stmt, ObTransformerCtx *ctx);

  //
  // pushdown_limit_count = NULL == limit_offset
  //                        ? limit_count
  //                        : limit_count + limit_offset
  static int make_pushdown_limit_count(ObRawExprFactory &expr_factory,
                                       const ObSQLSessionInfo &session,
                                       ObRawExpr *limit_count,
                                       ObRawExpr *limit_offset,
                                       ObRawExpr *&pushdown_limit_count);

  static int get_rel_ids_from_tables(const ObDMLStmt *stmt,
                                     const ObIArray<TableItem*> &table_items,
                                     ObRelIds &rel_ids);

  static int get_rel_ids_from_tables(const ObDMLStmt *stmt,
                                     const ObIArray<uint64_t> &table_ids,
                                     ObRelIds &rel_ids);

  static int get_left_rel_ids_from_semi_info(const ObDMLStmt *stmt,
                                             SemiInfo *info,
                                             ObSqlBitSet<> &rel_ids);

  static int get_rel_ids_from_table(const ObDMLStmt *stmt,
                                    const TableItem *table,
                                    ObRelIds &rel_ids);

  static int adjust_single_table_ids(JoinedTable *joined_table);

  static int adjust_single_table_ids(TableItem *table,
                                    common::ObIArray<uint64_t> &table_ids);

  static int extract_table_items(TableItem *table_item,
                                 ObIArray<TableItem *> &table_items);

  static int set_view_base_item(ObDMLStmt *upper_stmt,
                                TableItem *view_table,
                                ObSelectStmt *view_stmt,
                                TableItem *base_table);

  static int reset_stmt_column_item(ObDMLStmt *stmt,
                                    ObIArray<ColumnItem> &column_items,
                                    ObIArray<ObRawExpr*> &column_expr);

  static int get_base_column(const ObDMLStmt *stmt,
                             ObColumnRefRawExpr *&col);

  static int get_post_join_exprs(ObDMLStmt *stmt,
                                 ObIArray<ObRawExpr *> &exprs,
                                 bool with_vector_assign = false);

  static int free_stmt(ObStmtFactory &stmt_factory, ObDMLStmt *stmt);

  static int extract_shared_expr(ObDMLStmt *upper_stmt,
                                 ObDMLStmt *child_stmt,
                                 ObIArray<ObRawExpr*> &shared_exprs,
                                 ObIArray<DmlStmtScope> *upper_scopes = NULL,
                                 ObIArray<DmlStmtScope> *child_scopes = NULL);

  static int check_for_update_validity(ObSelectStmt *stmt);

  static int is_question_mark_pre_param(const ObDMLStmt &stmt,
                                        const int64_t param_idx,
                                        bool &is_pre_param,
                                        int64_t &pre_param_count);

  static int extract_pseudo_column_like_expr(ObIArray<ObRawExpr*> &exprs,
                                             ObIArray<ObRawExpr *> &pseudo_column_like_exprs);

  static int extract_pseudo_column_like_expr(ObRawExpr *expr,
                                             ObIArray<ObRawExpr *> &pseudo_column_like_exprs);

  static int adjust_pseudo_column_like_exprs(ObDMLStmt &stmt);

  static int check_has_rownum(const ObIArray<ObRawExpr *> &exprs, bool &has_rownum);

  static int check_subquery_match_index(ObTransformerCtx *ctx,
                                        ObQueryRefRawExpr *query_ref,
                                        ObSelectStmt *subquery,
                                        bool &is_match);

  static int add_table_item(ObDMLStmt *stmt, TableItem *table_item);

  static int add_table_item(ObDMLStmt *stmt, ObIArray<TableItem *> &table_items);

  static int get_limit_value(ObRawExpr *limit_expr,
                                      const ParamStore *param_store,
                                      ObExecContext *exec_ctx,
                                      ObIAllocator *allocator,
                                      int64_t &limit_value,
                                      bool &is_null_value);

  static int get_expr_int_value(ObRawExpr *expr,
                             const ParamStore *param_store,
                             ObExecContext *exec_ctx,
                             ObIAllocator *allocator,
                             int64_t &value,
                             bool &is_null_value);

  static int get_percentage_value(ObRawExpr *percent_expr,
                                  const ObDMLStmt *stmt,
                                  const ParamStore *param_store,
                                  ObExecContext *exec_ctx,
                                  ObIAllocator *allocator,
                                  double &percent_value,
                                  bool &is_null_value);

  static int add_const_param_constraints(ObRawExpr *expr,
                                         ObTransformerCtx *ctx);

  static int replace_stmt_expr_with_groupby_exprs(ObSelectStmt *select_stmt,
                                                  ObTransformerCtx *trans_ctx);

  static int replace_add_exprs_with_groupby_exprs(ObRawExpr *&expr_l,
                                                  ObRawExpr *expr_r,
                                                  ObTransformerCtx *trans_ctx,
                                                  bool &is_existed);

  static bool check_objparam_abs_equal(const ObObjParam &obj1, const ObObjParam &obj2);
  static int add_neg_or_pos_constraint(ObTransformerCtx *trans_ctx,
                                       ObRawExpr *expr,
                                       bool is_negative = false);
  static int add_equal_expr_value_constraint(ObTransformerCtx *trans_ctx,
                                             ObRawExpr *left,
                                             ObRawExpr *right);
  static bool check_objparam_negative(const ObObjParam &obj1);
  static int add_param_bool_constraint(ObTransformerCtx *ctx,
                                       ObRawExpr *bool_expr,
                                       const bool is_true,
                                       const bool ignore_const_check = false);
  static int replace_with_groupby_exprs(ObSelectStmt *select_stmt,
                                        ObRawExpr *&expr,
                                        bool need_query_compare,
                                        ObTransformerCtx *tran_ctx,
                                        bool in_add_expr);

  static int add_param_not_null_constraint(ObIArray<ObExprConstraint> &constraints,
                                           ObIArray<ObRawExpr *> &not_null_exprs);

  static int add_param_not_null_constraint(ObIArray<ObExprConstraint> &constraints,
                                           ObRawExpr *not_null_expr,
                                           bool is_true = true);

  static int add_param_not_null_constraint(ObTransformerCtx &ctx, 
                                           ObIArray<ObRawExpr *> &not_null_exprs);
  
  static int add_param_not_null_constraint(ObTransformerCtx &ctx,
                                           ObRawExpr *not_null_expr,
                                           bool is_true = true);
  static int add_param_null_constraint(ObTransformerCtx &ctx,
                                      ObRawExpr *not_null_expr);

  static int add_param_lossless_cast_constraint(ObTransformerCtx &ctx,
                                                ObRawExpr *expr,
                                                const ObRawExpr *dst_expr);

  static int calc_expr_value(ObTransformerCtx &ctx, ObRawExpr *expr, bool &is_not_null);

  static int get_all_child_stmts(ObDMLStmt *stmt,
                                 ObIArray<ObSelectStmt*> &child_stmts,
                                 hash::ObHashMap<uint64_t, ObParentDMLStmt> *parent_map = NULL,
                                 const ObIArray<ObSelectStmt*> *ignore_stmts = NULL);

  static int check_select_expr_is_const(ObSelectStmt *stmt, ObRawExpr *expr, bool &is_const);

  static int check_project_pruning_validity(ObSelectStmt &stmt, bool &is_valid);

  static int check_select_item_need_remove(const ObSelectStmt *stmt,
                                           const int64_t idx,
                                           bool &need_remove);

  static int check_correlated_exprs_can_pullup(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                               const ObSelectStmt &subquery,
                                               bool &can_pullup);

  static int check_correlated_exprs_can_pullup_for_set(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                       const ObSelectStmt &subquery,
                                                       bool &can_pullup);

  static int check_correlated_condition_isomorphic(ObSelectStmt *left_query,
                                                   ObSelectStmt *right_query,
                                                   const ObIArray<ObExecParamRawExpr *> &left_exec_params,
                                                   const ObIArray<ObExecParamRawExpr *> &right_exec_params,
                                                   bool &is_valid,
                                                   ObIArray<ObRawExpr*> &left_new_select_exprs,
                                                   ObIArray<ObRawExpr*> &right_new_select_exprs);

  static int check_result_type_same(ObIArray<ObRawExpr*> &left_exprs, 
                                    ObIArray<ObRawExpr*> &right_exprs,
                                    bool &is_same);

  static int check_result_type_same(ObRawExpr* left_expr, 
                                    ObRawExpr* right_expr,
                                    bool &is_same);                                  

  static int get_correlated_conditions(const ObIArray<ObExecParamRawExpr *> & exec_params,
                                       const ObIArray<ObRawExpr*> &conds,
                                       ObIArray<ObRawExpr*> &correlated_conds);

  static int is_correlated_exprs_isomorphic(ObIArray<ObRawExpr *> &left_exprs,
                                            ObIArray<ObRawExpr *> &right_exprs,
                                            bool force_order,
                                            bool &is_valid);

  static int is_correlated_expr_isomorphic(ObRawExpr *left_expr,
                                           ObRawExpr* right_expr,
                                           bool &is_isomorphic);

  static int check_fixed_expr_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                         const ObSelectStmt &subquery,
                                         bool &is_valid);

  static int check_can_pullup_conds(const ObSelectStmt &subquery, bool &has_special_expr);

  static int is_table_item_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                      const ObSelectStmt &subquery,
                                      bool &contains);

  static int is_join_conditions_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                           const ObSelectStmt *subquery,
                                           bool &is_correlated);

  static int check_semi_conditions_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                              const SemiInfo *semi_info,
                                              bool &is_correlated);     

  static int check_joined_conditions_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                const JoinedTable *joined_table,
                                                bool &is_correlated);

  static int check_correlated_having_expr_can_pullup(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                     const ObSelectStmt &subquery,
                                                     bool has_special_expr,
                                                     bool &can_pullup);

  static int check_correlated_where_expr_can_pullup(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                    const ObSelectStmt &subquery,
                                                    bool has_special_expr,
                                                    bool &can_pullup);

  static int is_select_item_contain_subquery(const ObSelectStmt *subquery,
                                             bool &contain);

  static int create_spj_and_pullup_correlated_exprs(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                    ObSelectStmt *&subquery,
                                                    ObTransformerCtx *ctx);

  static int create_spj_and_pullup_correlated_exprs_for_set(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                                            ObSelectStmt *&stmt,
                                                            ObTransformerCtx *ctx);

  static int adjust_select_item_pos(ObIArray<ObRawExpr*> &right_select_exprs,
                                    ObSelectStmt *right_query);

  static int replace_none_correlated_exprs(ObIArray<ObRawExpr*> &exprs,
                                          const ObIArray<ObExecParamRawExpr *> &exec_params,
                                          int &pos,
                                          ObIArray<ObRawExpr*> &new_column_list);

  static int replace_none_correlated_expr(ObRawExpr *&expr,
                                          const ObIArray<ObExecParamRawExpr *> &exec_params,
                                          int &pos,
                                          ObIArray<ObRawExpr*> &new_column_list);

  static int pullup_correlated_exprs(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                     ObIArray<ObRawExpr*> &exprs,
                                     ObIArray<ObRawExpr*> &new_select_list);

  static int pullup_correlated_expr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                    ObRawExpr *expr,
                                    ObIArray<ObRawExpr*> &new_select_list,
                                    bool &is_correlated);

  static int pullup_correlated_select_expr(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                           ObSelectStmt &stmt,
                                           ObSelectStmt &view,
                                           ObIArray<ObRawExpr*> &new_select_list);

  static int pullup_correlated_conditions(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                          ObIArray<ObRawExpr *> &exprs,
                                          ObIArray<ObRawExpr *> &pullup_exprs,
                                          ObIArray<ObRawExpr *> &new_select_list);

  static int extract_rowid_exprs(ObIArray<ObRawExpr *> &exprs, ObIArray<ObRawExpr *> &rowid_exprs);

  static int extract_rowid_exprs(ObRawExpr *expr, ObIArray<ObRawExpr *> &rowid_exprs);

  static int add_part_column_exprs_for_heap_table(const ObDMLStmt *stmt,
                                                  const ObTableSchema *table_schema,
                                                  const uint64_t table_id,
                                                  ObIArray<ObRawExpr *> &unique_keys);

  //whether it is the target table for batch stmt
  static int is_batch_stmt_write_table(uint64_t table_id,
                                       const ObDMLStmt &stmt,
                                       bool &is_target_table);

  static int get_generated_table_item(ObDMLStmt &parent_stmt,
                                      ObDMLStmt *child_stmt,
                                      TableItem *&table_item);

  static int get_table_related_condition(ObDMLStmt &stmt,
                                         const TableItem *table,
                                         ObIArray<ObRawExpr *> &conditions);

  static int get_condition_from_joined_table(ObDMLStmt &stmt,
                                             const TableItem *target_table,
                                             ObIArray<ObRawExpr *> &conditions);

  static int extract_joined_table_condition(TableItem *table_item,
                                            const TableItem *target_table,
                                            ObIArray<ObRawExpr *> &conditions,
                                            bool &add_on_condition);
  /* extract all constant false exprs and constant true exprs in exprs*/
  static int extract_const_bool_expr_info(ObTransformerCtx *ctx,
                                          const common::ObIArray<ObRawExpr*> &exprs,
                                          common::ObIArray<int64_t> &true_exprs,
                                          common::ObIArray<int64_t> &false_exprs);
  /* extract exprs in all_exprs whoes indexs are in target_idx to target_exprs */
  static int extract_target_exprs_by_idx(const ObIArray<ObRawExpr*> &all_exprs,
                                         const ObIArray<int64_t> &target_idx,
                                         ObIArray<ObRawExpr*> &target_exprs);
  static int calc_const_expr_result(ObRawExpr * expr,
                                    ObTransformerCtx *ctx,
                                    ObObj &result,
                                    bool &calc_happend);
  static int check_integer_result_type(common::ObIArray<ObRawExpr*> &exprs,
                                       bool &is_valid_type);

  static int construct_trans_table(const ObDMLStmt *stmt,
                            const TableItem *table,
                            ObIArray<TableItem *> &trans_tables);

  static int construct_trans_tables(const ObDMLStmt *stmt,
                                    const ObIArray<TableItem *> &tables,
                                    ObIArray<TableItem *> &trans_tables);

  static int get_exprs_relation_ids(ObIArray<ObRawExpr*> &exprs, ObSqlBitSet<> &exprs_relation_ids);

  static int get_lazy_left_join(ObDMLStmt *stmt,
                                const ObIArray<TableItem *> &tables,
                                ObSqlBitSet<> &expr_relation_ids,
                                ObIArray<LazyJoinInfo> &lazy_join_infos);
  
  static int is_null_propagate_expr(const ObRawExpr *expr,
                                    const ObRawExpr *target,
                                    bool &bret);

  static int get_join_keys(ObIArray<ObRawExpr*> &conditions,
                           ObSqlBitSet<> &table_ids,
                           ObIArray<ObRawExpr*> &join_keys,
                           bool &is_simply_join);

  static int check_joined_table_combinable(ObDMLStmt *stmt,
                                           JoinedTable *joined_table,
                                           TableItem *target_table,
                                           bool is_right_child,
                                           bool &combinable);

  static int rebuild_win_compare_range_expr(ObRawExprFactory* expr_factory,
                                            ObWinFunRawExpr &win_expr,
                                            ObRawExpr* order_expr);

  static int check_expr_valid_for_stmt_merge(ObIArray<ObRawExpr*> &select_exprs,
                                             bool &is_valid);

  static int replace_with_empty_view(ObTransformerCtx *ctx,
                                     ObDMLStmt *stmt,
                                     TableItem *&view_table,
                                     TableItem *from_table,
                                     ObIArray<SemiInfo *> *semi_infos = NULL);

  static int replace_with_empty_view(ObTransformerCtx *ctx,
                                     ObDMLStmt *stmt,
                                     TableItem *&view_table,
                                     ObIArray<TableItem *> &from_tables,
                                     ObIArray<SemiInfo *> *semi_infos = NULL);

  static int create_inline_view(ObTransformerCtx *ctx,
                                ObDMLStmt *stmt,
                                TableItem *&view_table,
                                TableItem * push_table,
                                ObIArray<ObRawExpr *> *conditions = NULL,
                                ObIArray<SemiInfo *> *semi_infos = NULL,
                                ObIArray<ObRawExpr *> *select_exprs = NULL,
                                ObIArray<ObRawExpr *> *group_exprs = NULL,
                                ObIArray<ObRawExpr *> *rollup_exprs = NULL,
                                ObIArray<ObRawExpr *> *having_exprs = NULL,
                                ObIArray<OrderItem> *order_items = NULL);

  static int create_inline_view(ObTransformerCtx *ctx,
                                ObDMLStmt *stmt,
                                TableItem *&view_table,
                                ObIArray<TableItem *> &from_tables,
                                ObIArray<ObRawExpr *> *conditions = NULL,
                                ObIArray<SemiInfo *> *semi_infos = NULL,
                                ObIArray<ObRawExpr *> *select_exprs = NULL,
                                ObIArray<ObRawExpr *> *group_exprs = NULL,
                                ObIArray<ObRawExpr *> *rollup_exprs = NULL,
                                ObIArray<ObRawExpr *> *having_exprs = NULL,
                                ObIArray<OrderItem> *order_items = NULL);

  /* Push all content of the parent stmt into an inline view,
     and keep the ptr of the parent stmt  */
  static int pack_stmt(ObTransformerCtx *ctx,
                       ObSelectStmt *parent_stmt,
                       ObSelectStmt **child_stmt_ptr = NULL);

  static int generate_select_list(ObTransformerCtx *ctx,
                                  ObDMLStmt *stmt,
                                  TableItem *table,
                                  ObIArray<ObRawExpr *> *basic_select_exprs = NULL);

  static int remove_const_exprs(ObIArray<ObRawExpr *> &input_exprs,
                                ObIArray<ObRawExpr *> &output_exprs);

  static int check_table_contain_in_semi(const ObDMLStmt *stmt,
                                         const TableItem *table,
                                         bool &is_contain);

  static int check_has_assignment(const ObDMLStmt &stmt, bool &has_assignment);

  static int check_exprs_contain_lob_type(ObIArray<ObRawExpr *> &exprs, bool &has_lob);

  static int check_expr_contain_lob_type(ObRawExpr *expr, bool &has_lob);

  static int extract_copier_exprs(ObRawExprCopier &copier,
                                  ObIArray<ObRawExpr *> &old_exprs,
                                  ObIArray<ObRawExpr *> &new_exprs);

  static int transform_bit_aggr_to_common_expr(ObDMLStmt &stmt,
                                               ObRawExpr *aggr,
                                               ObTransformerCtx *ctx,
                                               ObRawExpr *&out_expr);

  static int get_view_column(ObTransformerCtx *ctx,
                             ObDMLStmt &stmt,
                             TableItem *table_item,
                             bool is_outer_join_table,
                             ObRawExpr *aggr_expr,
                             ObRawExpr *&aggr_column);
  static int convert_aggr_expr(ObTransformerCtx *ctx_,
                               ObDMLStmt *stmt,
                               ObAggFunRawExpr *aggr_expr,
                               ObRawExpr *&output_expr);
  static int wrap_case_when_for_count(ObTransformerCtx *ctx,
                                      ObDMLStmt *stmt,
                                      ObColumnRefRawExpr *view_count,
                                      ObRawExpr *&output,
                                      bool is_count_star = false);
  static int refresh_select_items_name(ObIAllocator &allocator, ObSelectStmt *select_stmt);
  static int refresh_column_items_name(ObDMLStmt *stmt, int64_t table_id);

  static int get_real_alias_name(ObSelectStmt *stmt, int64_t sel_idx, ObString& alias_name);

  template <typename T>
  static int remove_dup_expr(ObIArray<T *> &check,
                             ObIArray<T *> &base);

  static int append_hashset(ObRawExpr *expr,
                            hash::ObHashSet<uint64_t> &expr_set);

  static int find_hashset(ObRawExpr *expr,
                          hash::ObHashSet<uint64_t> &expr_set,
                          ObIArray<ObRawExpr *> &common_exprs);

  static int extract_shared_exprs(ObDMLStmt *parent,
                                  ObIArray<ObRawExpr *> &relation_exprs,
                                  ObIArray<ObRawExpr *> &common_exprs);
  static int check_is_index_part_key(ObTransformerCtx &ctx, ObDMLStmt &stmt, ObRawExpr *check_expr, bool &is_valid);

  static int check_stmt_is_only_full_group_by(const ObSelectStmt *stmt,
                                              bool &is_only_full_group_by);

  static int check_group_by_subset(ObRawExpr *expr,
                                   const ObIArray<ObRawExpr *> &group_exprs,
                                   bool &bret);

  static int check_expand_temp_table_valid(ObSelectStmt *stmt, bool &is_valid);

  static int expand_temp_table(ObTransformerCtx *ctx, ObDMLStmt::TempTableInfo& table_info);

  static int get_stmt_map_after_copy(ObDMLStmt *origin_stmt,
                                     ObDMLStmt *new_stmt,
                                     hash::ObHashMap<uint64_t, ObDMLStmt *> &stmt_map);

  static int check_stmt_contain_oversize_set_stmt(ObDMLStmt *stmt, bool &is_contain);

  static int convert_preds_vector_to_scalar(ObTransformerCtx &ctx,
                                            ObRawExpr *expr,
                                            ObIArray<ObRawExpr*> &exprs,
                                            bool &trans_happened);
    // used to stable outline
  static int get_sorted_table_hint(ObSEArray<TableItem *, 4> &tables, ObIArray<ObTableInHint> &table_hints);

  /**
   * @brief check whether can convert f(A) to f(B) for any B that satisfied A = B
   * @param expr target expr A
   * @param parent_exprs the parent exprs of A in f(A)
   * @param used_in_compare whether f(A) is used in compare, such as order by, group by
   */
  static int check_can_replace(ObRawExpr *expr,
                               ObIArray<ObRawExpr *> &parent_exprs,
                               bool used_in_compare,
                               bool &can_replace);

  static int check_pushdown_into_set_valid(const ObSelectStmt* child_stmt,
                                           ObRawExpr *expr,
                                           const ObIArray<ObRawExpr *> &set_op_exprs,
                                           bool &is_valid);

  static int recursive_check_pushdown_into_set_valid(const ObSelectStmt* child_stmt,
                                                     ObRawExpr *expr,
                                                     const ObIArray<ObRawExpr *> &set_op_exprs,
                                                     ObIArray<ObRawExpr *> &parent_exprs,
                                                     bool &is_valid);
  static int get_explicated_ref_columns(const uint64_t table_id,
                                        ObDMLStmt *stmt,
                                        ObIArray<ObRawExpr*> &table_cols);
  static int check_child_projection_validity(const ObSelectStmt *child_stmt,
                                             ObRawExpr *expr,
                                             bool &is_valid);
  static int check_fulltext_index_match_column(const ColumnReferenceSet &match_column_set,
                                               const ObTableSchema *table_schema,
                                               const ObTableSchema *inv_idx_schema,
                                               bool &found_matched_index);
  static int is_winfunc_topn_filter(const ObIArray<ObWinFunRawExpr *> &winfunc_exprs,
                                    ObRawExpr *filter,
                                    bool &is_topn_filter,
                                    ObRawExpr * &topn_const_expr,
                                    bool &is_fetch_with_ties,
                                    ObWinFunRawExpr *&win_expr);
  static int pushdown_qualify_filters(ObSelectStmt *stmt);
  // check if a constant or parameterized constant is NULL.
  static bool is_const_null(ObRawExpr &expr);
  static bool is_full_group_by(ObSelectStmt& stmt, ObSQLMode mode);

  static int check_table_with_fts_or_multivalue_recursively(TableItem *table,
                                                   ObSchemaChecker *schema_checker,
                                                   ObSQLSessionInfo *session_info,
                                                   bool &has_fts_or_multivalue_index);
  static int add_aggr_winfun_expr(ObSelectStmt *stmt,
                                  ObRawExpr *expr);
  static int expand_mview_table(ObTransformerCtx *ctx, ObDMLStmt *upper_stmt, TableItem *rt_mv_table);
  static int adjust_col_and_sel_for_expand_mview(ObTransformerCtx *ctx,
                                                 ObIArray<ColumnItem> &uppper_col_items,
                                                 ObIArray<SelectItem> &view_sel_items,
                                                 uint64_t mv_table_id);

  static int generate_view_stmt_from_query_string(const ObString &expand_view,
                                                  ObTransformerCtx *ctx,
                                                  ObSelectStmt *&view_stmt);
  static int set_expand_mview_flag(ObSelectStmt *view_stmt);

  static int is_where_subquery_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                          const ObSelectStmt &subquery,
                                          bool &is_correlated);

  static int is_select_item_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                       const ObSelectStmt &subquery,
                                       bool &is_correlated);

  static int is_correlated_exprs(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                 ObIArray<ObRawExpr *> &exprs,
                                 bool &bret);

  static int is_orderby_correlated(const ObIArray<ObExecParamRawExpr *> &exec_params,
                                   const ObSelectStmt &subquery,
                                   bool &is_correlated);
  static int check_is_basic_aggr_item(const ObSelectStmt &subquery,
                                      bool &is_valid);

  static int deduce_query_values(ObTransformerCtx &ctx,
                                 ObDMLStmt &stmt,
                                 ObIArray<bool> &is_null_prop,
                                 ObRawExpr *not_null_expr,
                                 bool is_outer_join,
                                 ObIArray<ObRawExpr *> &select_exprs,
                                 ObIArray<ObRawExpr *> &view_columns,
                                 ObIArray<ObRawExpr *> &real_values);

  static int extract_nullable_exprs(const ObRawExpr *expr, ObIArray<const ObRawExpr *> &vars);

  static int check_contain_correlated_lateral_table(const TableItem *table_item, bool &is_contain);

  static int check_lateral_ref_outer_table(const ObDMLStmt *stmt,
                                           const TableItem *parent_table_item,
                                           const TableItem *table_item,
                                           bool &is_ref);

  static int check_contain_correlated_lateral_table(ObDMLStmt *stmt, bool &is_contain);

  static int cartesian_tables_pre_split(ObSelectStmt *subquery,
                                        ObIArray<ObRawExpr*> &outer_conditions,
                                        ObIArray<ObSEArray<TableItem*, 4>> &all_connected_tables);
  static int do_split_cartesian_tables(ObTransformerCtx *ctx,
                                       ObDMLStmt *stmt,
                                       ObSelectStmt *subquery,
                                       ObSEArray<ObRawExpr*, 4> &outer_conditions,
                                       ObIArray<ObSEArray<TableItem*, 4>> &all_connected_tables,
                                       ObIArray<TableItem*> &right_tables,
                                       ObIArray<ObSEArray<ObRawExpr*, 4>> &new_outer_conds);
  static int create_columns_for_view_tables(ObTransformerCtx *ctx,
                                            ObDMLStmt *stmt,
                                            ObIArray<TableItem*> &right_tables,
                                            ObIArray<ObSEArray<ObRawExpr*, 4>> &outer_conds);
  static int connect_tables(const ObIArray<uint64_t> &table_ids,
                            const ObIArray<TableItem *> &from_tables,
                            UnionFind &uf);
  static int check_contain_correlated_function_table(const ObDMLStmt *stmt, bool &is_contain);
  static int check_contain_correlated_json_table(const ObDMLStmt *stmt, bool &is_contain);
  static int check_contain_cannot_duplicate_expr(const ObIArray<ObRawExpr*> &exprs, bool &is_contain);
  static bool is_enable_values_table_rewrite(const uint64_t compat_version);
  // check whether the score calculated by match expr is actually utilized
  static int check_need_calc_match_score(ObExecContext *exec_ctx,
                                        const ObDMLStmt* stmt,
                                        ObRawExpr* match_expr,
                                        bool &need_calc,
                                        ObIArray<ObExprConstraint> &constraints);
  static int check_expr_eq_zero(ObExecContext *ctx,
                                ObRawExpr *expr,
                                bool &eq_zero,
                                ObIArray<ObExprConstraint> &constraints);
  static int get_having_filters_for_deduce(const ObSelectStmt* sel_stmt,
                                           const ObIArray<ObRawExpr*> &raw_having_exprs,
                                           const ObIArray<ObRawExpr*> &group_clause_exprs,
                                           ObIArray<ObRawExpr*> &having_exprs_for_deduce);
  static int check_expr_used_as_condition(ObDMLStmt *stmt,
                                          ObRawExpr *root_expr,
                                          ObRawExpr *expr,
                                          bool &used_as_condition);
  static int inner_check_expr_used_as_condition(ObRawExpr *cur_expr,
                                                ObRawExpr *expr,
                                                bool parent_as_condition,
                                                bool &used_as_condition);
  static int check_can_trans_any_all_as_exists(ObTransformerCtx *ctx,
                                               ObRawExpr* expr,
                                               bool used_as_condition,
                                               bool need_match_index,
                                               bool& is_valid);
  static int do_trans_any_all_as_exists(ObTransformerCtx *ctx,
                                        ObRawExpr *&expr,
                                        ObNotNullContext *not_null_ctx,
                                        bool &trans_happened);
  static ObItemType get_opposite_sq_cmp_type(ObItemType item_type);
  static int check_enable_global_parallel_execution(ObDMLStmt *stmt,
                                                    ObSQLSessionInfo *session,
                                                    ObQueryCtx *query_ctx,
                                                    bool &enable_parallel);
private:
  static int inner_get_lazy_left_join(ObDMLStmt *stmt,
                                      TableItem *table,
                                      ObSqlBitSet<> &expr_relation_ids,
                                      ObIArray<LazyJoinInfo> &lazy_join_infos,
                                      bool in_full_join);

  static int check_lazy_left_join_valid(ObDMLStmt *stmt,
                                        JoinedTable *table,
                                        ObSqlBitSet<> &expr_relation_ids,
                                        bool in_full_join,
                                        bool &is_valid);

  static int check_left_join_right_view_combinable(ObDMLStmt *parent_stmt,
                                                  TableItem *view_table,
                                                  ObIArray<ObRawExpr*> &outer_join_conditions,
                                                  bool &combinable);

  static int inner_check_left_join_right_table_combinable(ObSelectStmt *child_stmt, 
                                                          TableItem *table, 
                                                          ObSqlBitSet<> &outer_expr_relation_ids, 
                                                          bool &combinable);

  static int get_view_exprs(ObDMLStmt *parent_stmt,
                            TableItem *view_table,
                            ObIArray<ObRawExpr*> &from_exprs, 
                            ObIArray<ObRawExpr*> &view_exprs);

  static int extract_shared_exprs(ObDMLStmt *parent,
                                  ObSelectStmt *view_stmt,
                                  ObIArray<ObRawExpr *> &common_exprs,
                                  const ObIArray<ObRawExpr *> *extra_view_exprs = NULL);
  static int is_scalar_expr(ObRawExpr* expr, bool &is_scalar);

  static int check_is_bypass_string_expr(const ObRawExpr *expr,
                                         const ObRawExpr *src_expr,
                                         bool &is_bypass);

  static int check_convert_string_safely(const ObRawExpr *expr,
                                         const ObRawExpr *src_expr,
                                         bool &is_safe);

  static int get_idx_from_table_ids(const ObIArray<uint64_t> &src_table_ids,
                                    const ObIArray<TableItem *> &target_tables,
                                    ObIArray<int64_t> &indices);

  static int collect_cartesian_tables(ObTransformerCtx *ctx,
                                      ObDMLStmt *stmt,
                                      ObSelectStmt *subquery,
                                      ObIArray<ObRawExpr*> &outer_conditions,
                                      ObIArray<ObSEArray<TableItem*, 4>> &all_connected_tables,
                                      ObIArray<TableItem*> &right_tables,
                                      ObIArray<ObSEArray<ObRawExpr*, 4>> &new_outer_conds);
  static int collect_split_exprs_for_view(ObTransformerCtx *ctx,
                                          ObDMLStmt *stmt,
                                          ObSelectStmt *origin_subquery,
                                          TableItem *&view_table,
                                          ObIArray<TableItem*> &connected_tables);
  static int collect_split_outer_conds(ObSelectStmt *origin_subquery,
                                       ObIArray<ObRawExpr*> &outer_conditions,
                                       ObIArray<TableItem*> &connected_tables,
                                       ObIArray<ObRawExpr*> &split_outer_conds);
  static int collect_common_conditions(ObTransformerCtx *ctx,
                                       ObSelectStmt *origin_subquery,
                                       ObIArray<ObRawExpr*> &outer_conditions,
                                       ObIArray<TableItem*> &right_tables,
                                       ObIArray<ObSEArray<ObRawExpr*, 4>> &new_outer_conds);
  static int recursive_check_cannot_duplicate_expr(const ObRawExpr *expr, bool &is_contain);
  static int inner_check_need_calc_match_score(ObExecContext *exec_ctx,
                                              ObRawExpr* expr,
                                              ObRawExpr* match_expr,
                                              bool &need_calc,
                                              ObIArray<ObExprConstraint> &constraints);
  static int check_stmt_can_trans_as_exists(ObSelectStmt *stmt,
                                            ObTransformerCtx *ctx,
                                            bool is_correlated,
                                            bool need_match_index,
                                            bool &match_index,
                                            bool &is_valid);
  static int prepare_trans_any_all_as_exists(ObTransformerCtx *ctx,
                                             ObQueryRefRawExpr* right_hand,
                                             ObSelectStmt *&trans_stmt);
  static int query_cmp_to_exists_value_cmp(ObItemType type, bool is_with_all, ObItemType& new_type);
};

class StmtUniqueKeyProvider
{
public:
  StmtUniqueKeyProvider(bool for_costed_trans = true) :
    for_costed_trans_(for_costed_trans),
    in_temp_table_(false)
  {}
  virtual ~StmtUniqueKeyProvider() {}

  static int check_can_set_stmt_unique(ObDMLStmt *stmt,
                                       bool &can_set_unique);
  int recursive_set_stmt_unique(ObSelectStmt *select_stmt,
                                ObTransformerCtx *ctx,
                                bool ignore_check_unique = false,
                                common::ObIArray<ObRawExpr *> *unique_keys = NULL);
  /**
   * @brief generate_unique_key
   * generate unique key for stmt, need call check_can_set_stmt_unique before to ensure unique key can be generated
   */
  int generate_unique_key(ObTransformerCtx *ctx,
                          ObDMLStmt *stmt,
                          ObSqlBitSet<> &ignore_tables,
                          ObIArray<ObRawExpr *> &unique_keys);
  int recover_useless_unique_for_temp_table();
private:
  int get_unique_keys_from_unique_stmt(const ObSelectStmt *select_stmt,
                                       ObRawExprFactory *expr_factory,
                                       ObIArray<ObRawExpr*> &unique_keys,
                                       ObIArray<ObRawExpr*> &added_unique_keys);
  int try_push_back_modified_info(ObSelectStmt *select_stmt,
                                  int64_t sel_item_count,
                                  int64_t col_item_count);
  int add_non_duplicated_select_expr(ObIArray<ObRawExpr*> &add_select_exprs,
                                     ObIArray<ObRawExpr*> &org_select_exprs);

private:
    DISALLOW_COPY_AND_ASSIGN(StmtUniqueKeyProvider);
    const bool for_costed_trans_;
    bool in_temp_table_;
    // select items in temp tables may be appended by recursive_set_stmt_unique, store some information to recover them.
    // ordering in array below must be maintained by this class
    common::ObSEArray<ObSelectStmt*, 4> sel_stmts_;
    common::ObSEArray<int64_t, 4> sel_item_counts_;
    common::ObSEArray<int64_t, 4> col_item_counts_;
};

template <typename T>
int ObTransformUtils::replace_exprs(const common::ObIArray<ObRawExpr *> &other_exprs,
                                    const common::ObIArray<ObRawExpr *> &new_exprs,
                                    common::ObIArray<T*> &exprs)
{
  int ret = common::OB_SUCCESS;
  common::ObSEArray<T*, 4> temp_expr_array;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    ObRawExpr *temp_expr = exprs.at(i);
    if (OB_ISNULL(temp_expr)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "expr is null", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_expr(other_exprs,
                                                      new_exprs,
                                                      temp_expr))) {
      SQL_LOG(WARN, "failed to replace expr", K(ret));
    } else if (OB_FAIL(temp_expr_array.push_back(static_cast<T*>(temp_expr)))) {
      SQL_LOG(WARN, "failed to push back expr", K(ret));
    } else { /*do nothing*/}
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(exprs.assign(temp_expr_array))) {
      SQL_LOG(WARN, "failed to assign expr", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

template <typename T>
int ObTransformUtils::replace_specific_expr(const common::ObIArray<T*> &other_exprs,
                                            const common::ObIArray<T*> &current_exprs,
                                            ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_UNLIKELY(other_exprs.count() != current_exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "should have equal column item count", K(other_exprs.count()),
        K(current_exprs.count()), K(ret));
  } else {
    bool is_find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_find && i < other_exprs.count(); i++) {
      if (OB_ISNULL(other_exprs.at(i)) ||
          OB_ISNULL(current_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "null column expr", K(other_exprs.at(i)),
            K(current_exprs.at(i)), K(ret));
      } else if (expr == other_exprs.at(i)) {
        is_find = true;
        expr = current_exprs.at(i);
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

template <typename T>
int ObTransformUtils::get_expr_idx(const ObIArray<T *> &source,
                                const T *target,
                                int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = -1;
  if (OB_ISNULL(target)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest expr is null", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && idx == -1 && i < source.count(); ++i) {
    if (OB_ISNULL(source.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr in source is null", K(ret));
    } else if (source.at(i) == target) {
      idx = i;
    }
  }
  return ret;
}


template <typename T>
int ObTransformUtils::remove_dup_expr(common::ObIArray<T *> &check,
                                      common::ObIArray<T *> &base)
{
  int ret = OB_SUCCESS;
  ObSEArray<T *, 4> no_dup;
  for (int64_t i = 0; i < base.count(); i++) {
    int64_t idx = -1;
    if (OB_FAIL(ObTransformUtils::get_expr_idx(check, base.at(i), idx))) {
      LOG_WARN("find expr failed", K(ret));
    } else if (idx != -1) {
    } else if (OB_FAIL(no_dup.push_back(base.at(i)))) {
      LOG_WARN("push back failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(base.assign(no_dup))) {
      LOG_WARN("assign failed", K(ret));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase



#endif /* OCEANBASE_SQL_REWRITE_OB_TRANSFORM_UTILS_H_ */
