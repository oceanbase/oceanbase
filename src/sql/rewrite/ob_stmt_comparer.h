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

#ifndef OB_STMT_COMPARER_H
#define OB_STMT_COMPARER_H

#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{

// NOTE (link.zt) remember to de-construct the struct
/**
 * @brief The ObStmtMapInfo struct
 * 记录两个 stmt 中语义相等项的映射关系
 */
enum QueryRelation
{
  QUERY_LEFT_SUBSET,
  QUERY_RIGHT_SUBSET,
  QUERY_EQUAL,
  QUERY_UNCOMPARABLE
};
 struct ObStmtMapInfo {
  common::ObSEArray<common::ObSEArray<int64_t, 4>, 4> view_select_item_map_;
  common::ObSEArray<ObExprConstraint, 4> expr_cons_map_;
  common::ObSEArray<ObPCConstParamInfo, 4> const_param_map_;
  common::ObSEArray<ObPCParamEqualInfo, 4> equal_param_map_;
  common::ObSEArray<int64_t, 4> table_map_;
  common::ObSEArray<int64_t, 4> from_map_;
  common::ObSEArray<int64_t, 4> semi_info_map_;
  common::ObSEArray<int64_t, 4> cond_map_;
  common::ObSEArray<int64_t, 4> group_map_;
  common::ObSEArray<int64_t, 4> having_map_;
  common::ObSEArray<int64_t, 4> select_item_map_;
  bool is_table_equal_;
  bool is_from_equal_;
  bool is_semi_info_equal_;
  bool is_cond_equal_;
  bool is_group_equal_;
  bool is_having_equal_;
  bool is_order_equal_;
  bool is_select_item_equal_;
  bool is_distinct_equal_;
  bool is_qualify_filter_equal_;
  bool left_can_be_replaced_; // used for mv rewrite

  //如果from item是generated table，需要记录ref query的select item map关系
  //如果是set stmt，每个set query对应的映射关系也记录在view_select_item_map_
  ObStmtMapInfo()
    :is_table_equal_(false),
    is_from_equal_(false),
    is_semi_info_equal_(false),
    is_cond_equal_(false),
    is_group_equal_(false),
    is_having_equal_(false),
    is_order_equal_(false),
    is_select_item_equal_(false),
    is_distinct_equal_(false),
    is_qualify_filter_equal_(false),
    left_can_be_replaced_(true)
    {}

  void reset();
  int assign(const ObStmtMapInfo& other);

  TO_STRING_KV(K_(table_map),
               K_(from_map),
               K_(semi_info_map),
               K_(cond_map),
               K_(group_map),
               K_(having_map),
               K_(select_item_map),
               K_(equal_param_map),
               K_(view_select_item_map),
               K_(is_order_equal),
               K_(is_distinct_equal),
               K_(left_can_be_replaced));
};

struct StmtCompareHelper {
  StmtCompareHelper()
  :stmt_map_infos_(),
  similar_stmts_(),
  hint_force_stmt_set_(),
  stmt_(NULL)
  {}

  virtual ~StmtCompareHelper(){}
  static int alloc_compare_helper(ObIAllocator &allocator, StmtCompareHelper* &helper);

  TO_STRING_KV(
    K_(stmt_map_infos),
    K_(similar_stmts),
    K_(hint_force_stmt_set),
    K_(stmt)
  );

  ObSEArray<ObStmtMapInfo, 8> stmt_map_infos_;
  ObSEArray<ObSelectStmt*, 8> similar_stmts_;
  QbNameList hint_force_stmt_set_;
  ObSelectStmt *stmt_;
};

// NOTE (link.zt) remember to de-construct the struct
struct ObStmtCompareContext : ObExprEqualCheckContext
{
  ObStmtCompareContext() :
    ObExprEqualCheckContext(),
    calculable_items_(NULL),
    inner_(NULL),
    outer_(NULL),
    map_info_(),
    equal_param_info_(),
    is_in_same_stmt_(true)
  {
    init_override_params();
  }
  ObStmtCompareContext(bool need_check_deterministic) :
    ObExprEqualCheckContext(need_check_deterministic),
    calculable_items_(NULL),
    inner_(NULL),
    outer_(NULL),
    map_info_(),
    equal_param_info_(),
    is_in_same_stmt_(true)
  {
    init_override_params();
  }
  // for common expression extraction
  ObStmtCompareContext(const ObIArray<ObHiddenColumnItem> *calculable_items,
                       bool need_check_deterministic = false,
                       bool is_in_same_stmt = true) :
    ObExprEqualCheckContext(need_check_deterministic),
    calculable_items_(calculable_items),
    inner_(NULL),
    outer_(NULL),
    map_info_(),
    equal_param_info_(),
    is_in_same_stmt_(is_in_same_stmt)
  {
    init_override_params();
  }
  ObStmtCompareContext(const ObDMLStmt *inner,
                       const ObDMLStmt *outer,
                       const ObStmtMapInfo &map_info,
                       const ObIArray<ObHiddenColumnItem> *calculable_items,
                       bool need_check_deterministic = false,
                       bool is_in_same_stmt = true) :
    ObExprEqualCheckContext(need_check_deterministic),
    calculable_items_(calculable_items),
    inner_(inner),
    outer_(outer),
    map_info_(map_info),
    equal_param_info_(),
    is_in_same_stmt_(is_in_same_stmt)
  {
    init_override_params();
  }
  inline void init_override_params()
  {
    override_column_compare_ = true;
    override_const_compare_ = true;
    override_query_compare_ = true;
    override_set_op_compare_ = true;
  }
  virtual ~ObStmtCompareContext() {}

  // since the init() func only initialize the class members,
  // it is better to use constructor
  // for common expression extraction
  void init(const ObIArray<ObHiddenColumnItem> *calculable_items);

  // for win_magic rewrite
  void init(const ObDMLStmt *inner,
            const ObDMLStmt *outer,
            const ObStmtMapInfo &map_info,
            const ObIArray<ObHiddenColumnItem> *calculable_items);
  
  int get_table_map_idx(uint64_t l_table_id, uint64_t r_table_id);

  // 用于比较两个 expr 是否结构对称
  // 区别仅在于部分 column 的 table id 不同
  bool compare_column(const ObColumnRefRawExpr &inner, const ObColumnRefRawExpr &outer) override;

  bool compare_const(const ObConstRawExpr &inner, const ObConstRawExpr &outer) override;

  bool compare_query(const ObQueryRefRawExpr &first, const ObQueryRefRawExpr &second) override;

  int get_calc_expr(const int64_t param_idx, const ObRawExpr *&expr);

  int is_pre_calc_item(const ObConstRawExpr &const_expr, bool &is_calc);

  bool compare_set_op_expr(const ObSetOpRawExpr& left, const ObSetOpRawExpr& right) override;

  const ObIArray<ObHiddenColumnItem> *calculable_items_; // from query context
  // first is the table id from the inner stmt
  // second is the table id from the outer stmt
  const ObDMLStmt *inner_;
  const ObDMLStmt *outer_;
  ObStmtMapInfo map_info_;
  common::ObSEArray<ObPCParamEqualInfo, 4> equal_param_info_;
  common::ObSEArray<ObExprConstraint, 4> expr_cons_info_;
  common::ObSEArray<ObPCConstParamInfo, 4> const_param_info_;
  bool is_in_same_stmt_; // only if the two stmts are in the same parent stmt, can we compare table id and column id directly
};

class ObStmtComparer
{
public:

   /**
   * @brief compute_overlap_between_stmts
   * 仅考虑 from, where 部分的重叠
   * from_map[i]: first stmt 的第 i 个 from item 对应 second stmt 的第 from_map[i] 个 from item
   *              如果没有对应，那么 from_map[i] = OB_INVALID_ID
   * cond_map[i]: first stmt 的第 i get condition 对应 second stmt 的第 cond_map[i] 个 condition
   *              如果没有对应，那么 cond_map[i] = OB_INVALID_ID
   * @return
   */
  static int compute_stmt_overlap(const ObDMLStmt *first,
                                  const ObDMLStmt *second,
                                  ObStmtMapInfo &map_info);

  /* is_strict_select_list = true, it requerys same order select list between two stmts. */
  static int check_stmt_containment(const ObDMLStmt *first,
                                    const ObDMLStmt *second,
                                    ObStmtMapInfo &map_info,
                                    QueryRelation &relation,
                                    bool is_strict_select_list = false,
                                    bool need_check_select_items = true,
                                    bool is_in_same_stmt = true);

  static int compute_conditions_map(const ObDMLStmt *first,
                                    const ObDMLStmt *second,
                                    const ObIArray<ObRawExpr*> &first_exprs,
                                    const ObIArray<ObRawExpr*> &second_exprs,
                                    ObStmtMapInfo &map_info,
                                    ObIArray<int64_t> &condition_map,
                                    QueryRelation &relation,
                                    bool is_in_same_cond = true,
                                    bool is_same_by_order = false,
                                    bool need_check_second_range = false);

  static int compute_orderby_map(const ObDMLStmt *first,
                                 const ObDMLStmt *second,
                                 const ObIArray<OrderItem> &first_orders,
                                 const ObIArray<OrderItem> &second_orders,
                                 ObStmtMapInfo &map_info,
                                 int64_t &match_count);

  static int compute_from_items_map(const ObDMLStmt *first,
                                    const ObDMLStmt *second,
                                    bool is_in_same_stmt,
                                    ObStmtMapInfo &map_info,
                                    QueryRelation &relation);

  static int is_same_from(const ObDMLStmt *first,
                          const FromItem &first_from,
                          const ObDMLStmt *second,
                          const FromItem &second_from,
                          bool is_in_same_stmt,
                          ObStmtMapInfo &map_info,
                          bool &is_same);

  static int is_same_condition(const ObRawExpr *left,
                               const ObRawExpr *right,
                               ObStmtCompareContext &context,
                               bool &is_same);

  static int compute_semi_infos_map(const ObDMLStmt *first,
                                    const ObDMLStmt *second,
                                    bool is_in_same_stmt,
                                    ObStmtMapInfo &map_info,
                                    int64_t &match_count);

  static int is_same_semi_info(const ObDMLStmt *first,
                              const SemiInfo *first_semi_info,
                              const ObDMLStmt *second,
                              const SemiInfo *second_semi_info,
                              bool is_in_same_stmt,
                              ObStmtMapInfo &map_info,
                              bool &is_same);

  static int compute_tables_map(const ObDMLStmt *first,
                                const ObDMLStmt *second,
                                const ObIArray<uint64_t> &first_table_ids,
                                const ObIArray<uint64_t> &second_table_ids,
                                ObStmtMapInfo &map_info,
                                ObIArray<int64_t> &table_map,
                                int64_t &match_count);

  static int compute_unmatched_item(const ObIArray<int64_t> &item_map,
                                    int first_size,
                                    int second_size,
                                    ObIArray<int64_t> &first_unmatched_items,
                                    ObIArray<int64_t> &second_unmatched_items);

  static int compute_new_expr(const ObIArray<ObRawExpr*> &target_exprs,
                              const ObDMLStmt *target_stmt,
                              const ObIArray<ObRawExpr*> &source_exprs,
                              const ObDMLStmt *source_stmt,
                              ObStmtMapInfo &map_info,
                              ObRawExprCopier &expr_copier,
                              ObIArray<ObRawExpr*> &compute_exprs,
                              bool &is_all_computable);

  static int inner_compute_expr(const ObRawExpr *target_expr,
                                const ObIArray<ObRawExpr*> &source_exprs,
                                ObStmtCompareContext &context,
                                ObRawExprCopier &expr_copier,
                                bool &is_match);

  /**
   * @brief compare_basic_table_item
   * 如果两张表partition hint分区包含的关系，
   * 如果两张表不同，则不可比较
   * 如果两张表相同且没有partition hint，则相等
   * 如果两张表都是generated_table只比较引用的子查询是否相同
   */
  static int compare_basic_table_item (const ObDMLStmt *first,
                                      const TableItem *first_table,
                                      const ObDMLStmt *second,
                                      const TableItem *second_table,
                                      QueryRelation &relation);

  /**
   * @brief compare_joined_table_item
   * 比较两个joined table是否同构
   * 要求每一层的左右table item相同，并且on condition相同
   */
  static int compare_joined_table_item (const ObDMLStmt *first,
                                        const TableItem *first_table,
                                        const ObDMLStmt *second,
                                        const TableItem *second_table,
                                        bool is_in_same_stmt,
                                        ObStmtMapInfo &map_info,
                                        QueryRelation &relation);

  /**
   * @brief compare_table_item
   * 比较两个table item是否同构
   */
  static int compare_table_item (const ObDMLStmt *first,
                                const TableItem *first_table,
                                const ObDMLStmt *second,
                                const TableItem *second_table,
                                bool is_in_same_stmt,
                                ObStmtMapInfo &map_info,
                                QueryRelation &relation);

  static int compare_set_stmt(const ObSelectStmt *first,
                              const ObSelectStmt *second,
                              ObStmtMapInfo &map_info,
                              QueryRelation &relation,
                              bool is_in_same_stmt = true);

  static int compare_values_table_item(const ObDMLStmt *first,
                                       const TableItem *first_table,
                                       const ObDMLStmt *second,
                                       const TableItem *second_table,
                                       ObStmtMapInfo &map_info,
                                       QueryRelation &relation);

};

}
}


#endif // OB_STMT_COMPARER_H
