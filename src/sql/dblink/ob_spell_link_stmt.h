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

#ifndef OCEANBASE_SQL_OB_SPELL_LINK_STMT_H
#define OCEANBASE_SQL_OB_SPELL_LINK_STMT_H

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_array.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/dblink/ob_dblink_utils.h"

namespace oceanbase
{
namespace sql
{
typedef common::ObIArray<common::ObString> ObStringIArray;
typedef common::ObIArray<ObRawExpr *> ObRawExprIArray;
typedef common::ObIArray<OrderItem> ObOrderItemIArray;
class GenLinkStmtPostContext
{
private:
  /**
   * Description of struct LinkSpellNodeStatus.
   * 
   * 1.Summary
   * In the post-order traversing the logical plan tree, 
   * LinkSpellNodeStatus is used to save the relevant reverse spelling 
   * information of the current node and its child nodes
   * 
   * 2.Details_1
   * The following members are used to save the relevant 
   * information of the current node and its child nodes. 
   * From the name, you can see what information is saved.
   * "filter_exprs_, range_conds_, limit_count_exprs_, 
   * groupby_exprs_, rollup_exprs_, having_filter_exprs_,
   * pushdown_filter_exprs_"
   * 
   * 3.Dtails_2
   * partial_sql_  is used to temporarily save part of 
   * the SQL that the current node has reverse spelled.
   * 
   * 4.Detail_3
   * is_distinct_ and is_set_ mark that whether the current 
   * node or child node contains count op or set op, 
   * this attribute will be used in the reverse spelling 
   * subplanscan or link operator.
   */
  struct LinkSpellNodeStatus 
  {
    typedef common::ObSEArray<ObString, 4, common::ModulePageAllocator, true> StringArray;
    typedef common::ObSEArray<OrderItem, 4, common::ModulePageAllocator, true> OrderItemArray;
    typedef common::ObSEArray<ObRawExpr *, 2, common::ModulePageAllocator, true> RawExprArray;

    LinkSpellNodeStatus() : is_distinct_(false), is_set_(false) {}

    RawExprArray filter_exprs_;
    RawExprArray range_conds_;
    RawExprArray limit_count_exprs_;
    RawExprArray groupby_exprs_;
    RawExprArray rollup_exprs_;
    RawExprArray having_filter_exprs_;
    RawExprArray pushdown_filter_exprs_; // for process nest loop join
    ObString partial_sql_;
    bool is_distinct_;
    bool is_set_;

    bool is_filters_empty() const
    {
      return filter_exprs_.empty() &&
             range_conds_.empty()  &&
             limit_count_exprs_.empty();
    }

    TO_STRING_KV(
                 K_(filter_exprs), 
                 K_(range_conds), 
                 K_(limit_count_exprs),
                 K_(groupby_exprs),
                 K_(rollup_exprs),
                 K_(having_filter_exprs),
                 K_(pushdown_filter_exprs),
                 K_(partial_sql),
                 K_(is_distinct),
                 K_(is_set));
  };
  typedef common::ObSEArray<LinkSpellNodeStatus, 4, common::ModulePageAllocator, true> StatusArray;
public:
  explicit GenLinkStmtPostContext(common::ObIAllocator &alloc, ObSchemaGetterGuard *schema_guard)
  : dblink_id_(OB_INVALID_ID),
    alloc_(alloc),
    tmp_buf_(NULL),
    tmp_buf_len_(0),
    tmp_buf_pos_(0),
    is_inited_(false),
    gen_unique_alias_(),
    schema_guard_(schema_guard)
  {}
private:
ObJoinType reverse_join_type(ObJoinType join_type);
const ObString &join_type_str(ObJoinType join_type) const;
const ObString &set_type_str(ObSelectStmt::SetOperator set_type) const;
int64_t get_order_item_index(const ObRawExpr *order_item_expr,
                             const ObRawExprIArray &output_exprs);
int fill_string(const ObString &str, bool with_double_quotation = false);
int fill_strings(const ObStringIArray &strs, const ObString &sep, bool skip_first_sep = false);
int fill_expr(const ObRawExpr *expr,
              const ObString &sep,
              bool is_bool_expr = false,
              bool fill_column_alias = false,
              const ObRawExpr *subplanscan_expr = NULL);
int fill_exprs(const ObRawExprIArray &exprs,
               const ObString &sep,
               bool is_bool_expr = false,
               bool skip_first_sep = false,
               bool skip_nl_param = false,
               bool fill_column_alias = false,
               const ObRawExprIArray *subplanscan_outputs =NULL);
int fill_limit_expr(const ObRawExpr *expr,
                    const ObString &sep);
int fill_limit_exprs(const ObRawExprIArray &exprs,
                     const ObString &sep);
int fill_all_filters(const LinkSpellNodeStatus &status);
int is_filters_empty(const LinkSpellNodeStatus &status, bool &is_empty, bool skip_nl_param = false);
int fill_groupby(LinkSpellNodeStatus &status, const ObRawExprIArray &output_exprs, const ObSelectStmt *ref_stmt);

int fill_orderby_strs(const ObOrderItemIArray &order_items,
                      const ObRawExprIArray &output_exprs);
int fill_join_on(const LinkSpellNodeStatus &left_child_status,
                 const LinkSpellNodeStatus &right_child_status,
                 const ObRawExprIArray &join_conditions, 
                 const ObRawExprIArray &join_filters,
                 ObJoinType join_type,
                 bool right_child_is_join);
int fill_semi_exists(LinkSpellNodeStatus &left_child_status,
                     LinkSpellNodeStatus &right_child_status,
                     const ObRawExprIArray &join_conditions, 
                     const ObRawExprIArray &join_filters,
                     ObJoinType join_type);
ObString semi_join_name(ObString left_child_sql);
int add_status();
int save_status_filter(const LinkSpellNodeStatus &status);
int save_filter_exprs(const ObRawExprIArray &filter_exprs);
int save_range_conds(const ObRawExprIArray &range_conds);
int save_pushdown_filter_exprs(const ObRawExprIArray &pushdown_filter_exprs);
int save_limit_count_expr(ObRawExpr *limit_count_expr);
int save_limit_count_expr(const ObRawExprIArray &limit_count_exprs);
int save_group_by_info(const ObRawExprIArray &groupby_exprs,
                       const ObRawExprIArray &rollup_exprs,
                       const ObRawExprIArray &having_filter_exprs);
int save_distinct();
int save_partial_sql();

int expr_in_nl_param(ObRawExpr *expr, bool &in_nl_param);
int do_expr_in_nl_param(ObRawExpr *expr, bool &in_nl_param);

int extend_tmp_buf(int64_t need_length = 0);
public:
  int init();
  void reset(uint64_t dblink_id = OB_INVALID_ID);
  void check_dblink_id(uint64_t dblink_id);
  int append_nl_param_idx(int64_t param_idx);
  int spell_table_scan(TableItem *table_item, 
                       const ObRawExprIArray &filter_exprs, 
                       const ObRawExprIArray &startup_exprs, 
                       const ObRawExprIArray &range_conds,
                       const ObRawExprIArray &pushdown_filter_exprs,
                       ObRawExpr *limit_count_expr);
  int spell_join(ObJoinType join_type, 
                 bool right_child_is_join,
                 const ObRawExprIArray &filter_exprs,
                 const ObRawExprIArray &startup_exprs,
                 const ObRawExprIArray &join_conditions, 
                 const ObRawExprIArray &join_filters);
  int spell_group_by(const ObRawExprIArray &startup_exprs,
                     const ObRawExprIArray &groupby_exprs,
                     const ObRawExprIArray &rollup_exprs,
                     const ObRawExprIArray &filter_exprs);
  int spell_count(const ObRawExprIArray &startup_exprs,
                  const ObRawExprIArray &filter_exprs,
                  ObRawExpr *limit_count_expr);
  int spell_distinct(const ObRawExprIArray &startup_exprs,
                     const ObRawExprIArray &filter_exprs);
  int spell_set(const ObSelectStmt *ref_stmt,
                ObLogSet *set_op_ptr,
                const ObRawExprIArray &startup_exprs,
                const ObRawExprIArray &filter_exprs,
                ObSelectStmt::SetOperator set_op,
                bool is_distinct,
                bool is_parent_distinct);
  int spell_subplan_scan(const ObSelectStmt *ref_stmt,
                         const ObRawExprIArray &output_exprs,
                         const ObRawExprIArray &child_output_exprs,
                         const ObRawExprIArray &startup_exprs,
                         const ObRawExprIArray &filter_exprs,
                         ObString &subquery_name);
  int spell_link(const ObSelectStmt *ref_stmt,
                 char **stmt_fmt_buf,
                 int32_t &stmt_fmt_len,
                 const ObOrderItemIArray &op_ordering,
                 const ObRawExprIArray &output_exprs,
                 const ObRawExprIArray &startup_exprs,
                 const ObRawExprIArray &filter_exprs);
private:
  uint64_t dblink_id_;
  common::ObIAllocator &alloc_;
  char *tmp_buf_; //Each node will reverse its partial_sql_ on this memory
  int64_t tmp_buf_len_; //Size of tmp_buf_
  int64_t tmp_buf_pos_; //Record the location of partial_sql_ during the reverse spelling process
  bool is_inited_;
  StatusArray status_array_; //As a stack to save the status of each node in the traversal process
  ObArray<int64_t> nl_param_idxs_; // param_store index for nl_params
  GenUniqueAliasName gen_unique_alias_; // generate unique alias name
  ObSchemaGetterGuard *schema_guard_;
private:
  static const common::ObString JOIN_ON_;
  static const common::ObString LEFT_BRACKET_;
  static const common::ObString RIGHT_BRACKET_;
  static const common::ObString SEP_DOT_;
  static const common::ObString SEP_COMMA_;
  static const common::ObString SEP_AND_;
  static const common::ObString SEP_SPACE_;
  static const common::ObString UNION_ALL_;
  static const common::ObString ORDER_ASC_;
  static const common::ObString ORDER_DESC_;
  static const common::ObString ROWNUM_;
  static const common::ObString LESS_EQUAL_;
  static const common::ObString SELECT_CLAUSE_;
  static const common::ObString SELECT_DIS_CLAUSE_;
  static const common::ObString FROM_CLAUSE_;
  static const common::ObString WHERE_CLAUSE_;
  static const common::ObString GROUPBY_CLAUSE_;
  static const common::ObString GROUPBY_ROLLUP_;
  static const common::ObString COMMA_ROLLUP_;
  static const common::ObString HAVING_CLAUSE_;
  static const common::ObString ORDERBY_CLAUSE_;
  static const common::ObString WHERE_EXISTS_;
  static const common::ObString WHERE_NOT_EXISTS_;
  static const common::ObString DOUBLE_QUOTATION_;
  static const common::ObString ASTERISK_;
  static const common::ObString NULL_STR_;
};


} // end of namespace sql
} // end of namespace oceanbase

#endif // OCEANBASE_SQL_OB_SPELL_LINK_STMT_H