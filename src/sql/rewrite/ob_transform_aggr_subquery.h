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

#ifndef _OB_TRANSFORM_AGGR_SUBQUERY_H_
#define _OB_TRANSFORM_AGGR_SUBQUERY_H_ 1

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObUpdateStmt;
/**
 * ObTransformAggrSubquery实现JA类型的子查询的改写
 * 改写的实现方式主要参考了如下一些论文：
 * [Kim82]          W.Kim, "On Optimizing an SQL-like Nested Query"
 * [Ganski87]       Richard A. Ganski and Harry K. T. Long, "Optimization of
 *                  nested SQL queries Revisited"
 * [Murlikrishna92] M.Murlikrishna, "Improved Unnesting Algorithms for Join
 *                  Aggregate SQL queries"
 * 改写的示例可以参看论文和mysqltest测试用例
 */
class ObTransformAggrSubquery : public ObTransformRule
{
public:
  ObTransformAggrSubquery(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_UNNEST),
      join_first_happened_(false)
  {}
  virtual ~ObTransformAggrSubquery() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
protected:
  virtual int check_hint_status(const ObDMLStmt &stmt, bool &need_trans) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;
private:

  enum PullupFlag {
    USE_OUTER_JOIN = 1,
    ADD_CASE_WHEN_EXPR = 2,
    JOIN_FIRST = 4,
    AGGR_FIRST = 8
  };

  /**
   * @brief The TransformParam struct
   * a candidate ja query for transformation
   */
  struct TransformParam {
    TransformParam() : ja_query_ref_(NULL), nested_conditions_(), pullup_flag_(0),
        not_null_expr_(NULL), parent_expr_of_query_ref(NULL), limit_for_exists_(false),
        limit_value_(0), limit_to_aggr_(false), any_all_to_aggr_(false), exists_to_aggr_(false)
    {}

    TransformParam(int64_t trans_strategy) 
      : ja_query_ref_(NULL), nested_conditions_(), pullup_flag_(trans_strategy),
        not_null_expr_(NULL), parent_expr_of_query_ref(NULL), limit_for_exists_(false),
        limit_value_(0), limit_to_aggr_(false), any_all_to_aggr_(false), exists_to_aggr_(false)
    {}

    ObQueryRefRawExpr *ja_query_ref_;  // the ja query ref expr
    ObSEArray<ObRawExpr *, 4> query_refs_;
    ObSEArray<ObRawExpr *, 4> nested_conditions_; // nested conditions in the ja query
    ObSEArray<bool, 4> is_null_prop_; // need case when for a expr
    ObArray<ObRawExpr *> not_null_const_;
    int64_t pullup_flag_; // the methods uesd when pulling up the ja query
    ObRawExpr *not_null_expr_;  //  not null expr of subquery if correlated join happened
    ObRawExpr *parent_expr_of_query_ref;  // parent expr need to be modified for vector subquery comparison
    bool limit_for_exists_;
    int64_t limit_value_;
    bool limit_to_aggr_;  // convert limit 1 single set as scala groupby
    bool any_all_to_aggr_;  // convert any/all subquery as scala groupby
    bool exists_to_aggr_; // convert exists subquery as scala groupby
    ObSEArray<ObPCParamEqualInfo, 1, common::ModulePageAllocator, true> equal_param_info_;
    ObSEArray<ObRawExpr *, 4> upper_filters_; // filters can be pulled up to upper stmt (aggr first)
    TO_STRING_KV(K_(ja_query_ref),
                 K_(query_refs),
                 K_(nested_conditions),
                 K_(is_null_prop),
                 K_(pullup_flag),
                 K_(not_null_expr),
                 K_(limit_for_exists),
                 K_(limit_value),
                 K_(limit_to_aggr),
                 K_(any_all_to_aggr),
                 K_(exists_to_aggr),
                 K_(equal_param_info),
                 K_(upper_filters));
  };

  struct TransStmtInfo {
    TransStmtInfo()
    : qb_name_(), unnest_(nullptr), pullup_strategy_(AGGR_FIRST) {}
    int assign(const TransStmtInfo& other)
    {
      qb_name_ = other.qb_name_;
      unnest_ = other.unnest_;
      pullup_strategy_ = other.pullup_strategy_;
      return common::OB_SUCCESS;
    }
    TO_STRING_KV(K_(qb_name), KPC_(unnest), K_(pullup_strategy));
    common::ObString qb_name_;
    const ObHint *unnest_;
    int64_t pullup_strategy_;
  };

  int do_transform(ObDMLStmt *&stmt, bool &trans_happened);
  int transform_with_aggregation_first(ObDMLStmt *&stmt,
                                       bool &trans_happened);

  int do_aggr_first_transform(ObDMLStmt *&stmt,
                              ObRawExpr *expr,
                              bool &trans_happened);

  int gather_transform_params(ObDMLStmt &stmt,
                              ObRawExpr *root_expr,
                              ObRawExpr *child_expr,
                              int64_t pullup_strategy,
                              const bool is_select_item_expr,
                              ObIArray<TransformParam> &params);

  int check_aggr_first_validity(ObDMLStmt &stmt,
                                ObQueryRefRawExpr &query_ref,
                                const bool is_vector_assign,
                                ObRawExpr *root_expr,
                                ObRawExpr &parent_expr,
                                ObIArray<ObRawExpr*> &nested_conditions,
                                ObIArray<ObRawExpr*> &upper_filters,
                                const bool is_select_item_expr,
                                bool &is_valid,
                                int64_t &limit_value,
                                bool &limit_to_aggr,
                                bool &any_all_to_aggr,
                                bool &exists_to_aggr,
                                ObIArray<ObPCParamEqualInfo> &equal_param_info);

  int choose_pullup_method(ObIArray<ObRawExpr *> &conditions,
                           TransformParam &param);

  int fill_query_refs(ObDMLStmt *stmt, bool cnt_alias, TransformParam &param);

  int transform_child_stmt(ObDMLStmt *stmt,
                           ObSelectStmt &subquery,
                           TransformParam &param);

  int transform_upper_stmt(ObDMLStmt &stmt, TransformParam &param);

  int transform_from_list(ObDMLStmt &stmt,
                          TableItem *view_table_item,
                          const ObIArray<ObRawExpr *> &joined_conds,
                          const ObIArray<ObRawExpr *> &upper_filters,
                          const int64_t pullup_flag);

  inline bool use_outer_join(int64_t flag)
  { return 0 != (flag & USE_OUTER_JOIN); }

  inline bool add_case_when_expr(int64_t flag)
  { return 0 != (flag & ADD_CASE_WHEN_EXPR); }

  inline bool join_first(int64_t flag)
  { return 0 != (flag & JOIN_FIRST); }

  inline bool aggr_first(int64_t flag)
  { return 0 != (flag & AGGR_FIRST); }

  int transform_with_join_first(ObDMLStmt *&stmt,
                                bool &trans_happened);

  int check_stmt_valid(ObDMLStmt &stmt, bool &is_valid);

  int check_join_first_validity(ObQueryRefRawExpr &query_ref,
                                const bool is_vector_assign,
                                const bool in_exists,
                                const ObRawExpr &parent_expr,
                                const bool is_vector_cmp,
                                ObIArray<ObRawExpr *> &constraints,
                                bool &add_limit_constraints,
                                int64_t &limit_value,
                                bool &is_valid,
                                ObIArray<ObPCParamEqualInfo> &equal_param_info);

  int choose_pullup_method_for_exists(ObQueryRefRawExpr *query_ref,
                                      int64_t &pullup_flag,
                                      const bool with_not,
                                      bool &is_valid);

  int get_trans_param(ObDMLStmt &stmt,
                      TransformParam &trans_param,
                      ObRawExpr *&root,
                      bool &post_group_by);

  int get_trans_view(ObDMLStmt &stmt,
                     ObSelectStmt *&view_stmt,
                     ObRawExpr *root_expr,
                     bool post_group_by);

  int do_join_first_transform(ObSelectStmt &select_stmt,
                             TransformParam &trans_param,
                             ObRawExpr *root_expr,
                             const bool is_first_trans);

  int get_unique_keys(ObDMLStmt &stmt, ObIArray<ObRawExpr *> &pkeys, const bool is_first_trans);

  int transform_from_list(ObDMLStmt &stmt,
                          ObSelectStmt &subquery,
                          const int64_t pullup_flag);

  int rebuild_conditon(ObSelectStmt &stmt, ObSelectStmt &subquery);

  int check_single_set_subquery(const ObSelectStmt &subquery,
                                bool &group_single_set,
                                bool &limit_single_set,
                                int64_t &limit_value,
                                bool check_limit = true);

  int check_subquery_aggr_item(const ObSelectStmt &subquery,
                               bool &is_valid);

  int check_count_const_validity(const ObSelectStmt &subquery,
                                 ObIArray<ObRawExpr *> &constraints,
                                 bool &is_valid);

  int is_count_const_expr(const ObRawExpr *expr, bool &is_count_const);

  int replace_count_const(ObAggFunRawExpr *agg_expr, ObRawExpr *not_null_expr);

  int check_can_use_outer_join(TransformParam &param, bool &is_valid);
  int check_subquery_select(const ObQueryRefRawExpr &query_ref, bool &is_valid);
  int check_subquery_orderby(const ObQueryRefRawExpr &query_ref, bool &is_valid);

  int check_limit_single_set_validity(const ObSelectStmt &subquery,
                                      const ObRawExpr &parent_expr,
                                      bool &is_valid,
                                      ObIArray<ObPCParamEqualInfo>& equal_param_info);

  int check_join_first_condition_for_limit_1(ObQueryRefRawExpr &query_ref,
                                             ObSelectStmt &subquery,
                                             bool &is_valid);

  int check_subquery_having(const ObQueryRefRawExpr &query_ref,
                            const ObSelectStmt &subquery,
                            bool &is_valid);

  int check_subquery_conditions(ObQueryRefRawExpr &query_ref,
                                ObSelectStmt &subquery,
                                ObIArray<ObRawExpr *> &nested_conds,
                                ObIArray<ObRawExpr *> &upper_filters,
                                const bool check_idx,
                                bool &is_valid,
                                bool &has_equal_correlation);

  int replace_columns_and_aggrs(ObRawExpr *&expr, ObTransformerCtx *ctx);

  int modify_aggr_param_expr_for_outer_join(TransformParam& trans_param);

  int modify_vector_comparison_expr_if_necessary(ObSelectStmt &select_stmt, ObRawExprFactory *expr_factory, ObSEArray<ObRawExpr*, 4> &select_exprs, ObRawExpr *parent_expr_of_query_ref);

  inline bool is_exists_op(const ObItemType type)
  { return type == T_OP_EXISTS || type == T_OP_NOT_EXISTS; }

  bool check_is_filter(const ObDMLStmt &stmt, const ObRawExpr *expr);

  int get_filters(ObDMLStmt &stmt, ObRawExpr *expr, ObIArray<ObRawExpr *> &filters);

  int is_valid_group_by(const ObSelectStmt &subquery, bool &is_valid);
  int extract_no_rewrite_select_exprs(ObDMLStmt *&stmt);
  int extract_no_rewrite_expr(ObRawExpr *expr);
  int check_need_spj(ObDMLStmt *stmt, bool &is_valid);
  int transform_with_aggr_first_for_having(ObDMLStmt *&stmt, bool &trans_happened);
  int check_hint_allowed_unnest(ObDMLStmt &stmt,
                                ObSelectStmt &subquery,
                                const int64_t hint_loc,
                                const int64_t pullup_strategy,
                                bool &allowed);
  int add_trans_stmt_info(ObSelectStmt &subquery, int64_t flag);
  int check_limit_validity(ObSelectStmt &subquery, 
                           bool &add_limit_constraints,
                           int64_t &limit_value, 
                           bool &is_valid);
  int add_constraints_for_limit(TransformParam &param);

  int convert_limit_as_aggr(ObSelectStmt *subquery, TransformParam &trans_param);
  ObHint* get_sub_unnest_hint(ObSelectStmt &subquery, int64_t pullup_strategy);
  ObItemType get_unnest_strategy(int64_t pullup_strategy);
  int check_nested_subquery(ObQueryRefRawExpr &query_ref, bool &is_valid);
  int check_can_trans_any_all_as_scalar_subquery(ObDMLStmt &stmt,
                                                ObSelectStmt *subquery,
                                                ObRawExpr *parent_expr,
                                                ObRawExpr *root_expr,
                                                bool &is_valid);
  int check_can_trans_exists_as_scalar_subquery(ObQueryRefRawExpr &query_ref,
                                               ObRawExpr &parent_expr,
                                               int64_t limit_value,
                                               bool &is_valid);
  int convert_exists_as_scalar_subquery(ObDMLStmt *stmt,
                                        ObQueryRefRawExpr *query_ref,
                                        ObSelectStmt *subquery,
                                        TransformParam &trans_param);
  int eliminate_limit_if_need(ObSelectStmt &subquery,
                              TransformParam &trans_param,
                              bool in_exist);
  int convert_any_all_as_scalar_subquery(ObDMLStmt *stmt,
                                        ObQueryRefRawExpr *query_ref,
                                        ObSelectStmt *&subquery,
                                        TransformParam &trans_param);
  int deduce_query_values_for_exists(ObTransformerCtx &ctx,
                                     ObDMLStmt &stmt,
                                     ObRawExpr *not_null_expr,
                                     ObRawExpr *parent_expr_of_query_ref,
                                     ObRawExpr *&real_parent_value);
  int eliminate_redundant_aggregation_if_need(ObSelectStmt &stmt, TransformParam &trans_param);

private:
  common::ObSEArray<ObRawExpr *, 8, common::ModulePageAllocator, true> no_rewrite_exprs_;
  common::ObSEArray<TransStmtInfo, 4, common::ModulePageAllocator, true> trans_stmt_infos_;
  bool join_first_happened_;
};

}
}

#endif /* _OB_TRANSFORM_AGGREGATION_SUBQUERY_H_ */

