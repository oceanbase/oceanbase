/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_TRANSFORM_SUBQUERY_UNNEST_H_
#define _OB_TRANSFORM_SUBQUERY_UNNEST_H_

#include "sql/rewrite/ob_transform_rule.h"

namespace oceanbase
{
namespace sql
{
class ObTransformSubqueryUnnest : public ObTransformRule {
// same as ObTransformOrExpansion::MAX_STMT_NUM_FOR_OR_EXPANSION
static const int64_t MAX_STMT_NUM_FOR_OR_EXPANSION = 10;

public:
  ObTransformSubqueryUnnest(ObTransformerCtx *ctx)
      : ObTransformRule(ctx, TransMethod::PRE_ORDER, T_UNNEST), stmt_(NULL)
  {}

  virtual ~ObTransformSubqueryUnnest() {}

  virtual int transform(ObDMLStmt *&stmt, uint64_t &transform_types) override;
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int transform_one_stmt_with_outline(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened) override;

private:  // utility functions
  int check_hint_allowed_unnest(const ObDMLStmt &stmt,
                                const ObSelectStmt &subquery,
                                const int64_t outline_loc,
                                bool &allowed);
  int check_basic_validity_for_stmt(const ObDMLStmt *stmt,
                                    bool &is_valid) const;
  int check_subquery_unique(ObSelectStmt *subquery,
                            bool &is_unique);
  int check_subquery_unique(ObSelectStmt *subquery,
                            ObIArray<ObRawExpr *> &const_exprs,
                            bool &is_unique);
  int pullup_subquery_to_parent_stmt(ObSelectStmt *subquery, bool use_outer_join);

private:  // inherited virtual functions
  virtual int check_hint_status(const ObDMLStmt &stmt,
                                bool &need_trans) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt,
                                       void *trans_params) override;

private:  // for anyall subquery
  struct AnyAllSQParam {
    AnyAllSQParam()
        : expr_(NULL),
          subquery_expr_(NULL),
          can_transform_(true),
          need_create_spj_(false),
          need_add_limit_constraint_(false),
          need_split_semi_info_(false),
          has_nested_correlated_subquery_(false),
          outline_loc_(0)
    {}

    ObOpRawExpr *expr_;                 // the condition expr containing anyall/exists subquery
    ObQueryRefRawExpr *subquery_expr_;  // the subquery expr
    bool can_transform_;                // whether can be unnested
    bool need_create_spj_;              // whether need to create spj before unnest
    bool need_add_limit_constraint_;    // whether need to add limit constraint before unnest
    bool need_split_semi_info_;         // whether need to split semi info
    bool has_nested_correlated_subquery_; // whether subquery has nested subquery in conditions that is correlated to upper stmt
    int64_t outline_loc_;                // absolute position in outline trans_list for hint matching
    ObSEArray<ObSEArray<TableItem *, 4>, 4> connected_tables_;  // for split semi info
    ObSEArray<ObSelectStmt *, 4> or_subquery_stmts_;  // for or-subquery unnest only: store the ANY subqueries in or condition

    OB_INLINE ObSelectStmt *get_subquery() { return NULL == subquery_expr_ ? NULL : subquery_expr_->get_ref_stmt(); }
    OB_INLINE bool is_correlated() { return NULL == subquery_expr_ ? false : subquery_expr_->has_exec_param(); }

    TO_STRING_KV(K_(expr),
                 K_(subquery_expr),
                 K_(can_transform),
                 K_(need_create_spj),
                 K_(need_add_limit_constraint),
                 K_(need_split_semi_info),
                 K_(has_nested_correlated_subquery),
                 K_(or_subquery_stmts));
  };
  // entry
  int anyall_transform_subquery(ObIArray<ObSelectStmt *> &unnest_stmts, bool &trans_happened);
  // validity check functions
  int anyall_gather_transform_params(ObRawExpr *expr, AnyAllSQParam &trans_param);
  int anyall_check_subquery_validity(AnyAllSQParam &trans_param);
  int anyall_check_subquery_validity_inner(AnyAllSQParam &trans_param,
                                           ObQueryRefRawExpr *query_ref_expr,
                                           ObSelectStmt *subquery);
  int anyall_check_limit(const ObItemType op_type,
                         const ObSelectStmt *subquery,
                         bool &has_unremovable_limit,
                         bool &need_add_limit_constraint) const;
  // execute transformation functions
  int anyall_do_unnest_subquery(ObRawExpr *expr, AnyAllSQParam &trans_param);
  int anyall_prepare_for_unnest(AnyAllSQParam &trans_param, ObJoinType &join_type);
  int anyall_build_join_conds(AnyAllSQParam &trans_param,
                              ObJoinType join_type,
                              ObIArray<ObRawExpr *> &right_exprs,
                              ObIArray<ObRawExpr *> &join_conds);
  int anyall_build_semi_conds(AnyAllSQParam &trans_param,
                              ObJoinType join_type,
                              ObIArray<TableItem *> &right_tables,
                              ObIArray<ObSEArray<ObRawExpr *, 4>> &final_semi_conds);
  int anyall_convert_anyall_to_cmp_cond(AnyAllSQParam &trans_param,
                                        bool force_join_cond,
                                        ObIArray<ObRawExpr *> &right_exprs,
                                        ObIArray<ObRawExpr *> &join_conds);
  int anyall_convert_anyall_to_scalar_cmp_conds(AnyAllSQParam &trans_param,
                                                bool force_join_cond,
                                                ObItemType cmp_op_type,
                                                ObRawExpr *left_expr,
                                                ObIArray<ObRawExpr *> &right_exprs,
                                                ObIArray<ObRawExpr *> &join_conds);
  int anyall_convert_anyall_to_vector_cmp_cond(AnyAllSQParam &trans_param,
                                               bool force_join_cond,
                                               ObRawExpr *left_expr,
                                               ObIArray<ObRawExpr *> &right_exprs,
                                               ObIArray<ObRawExpr *> &join_conds);
  ObItemType anyall_get_cmp_op_type(ObRawExpr &expr);
  int anyall_build_anti_conds(const ObSelectStmt *subquery,
                              ObRawExpr *cond_expr,
                              ObRawExpr *left_arg,
                              ObRawExpr *right_arg,
                              ObRawExpr *&anti_expr);
  int anyall_check_can_split_semi_join(ObSelectStmt *subquery,
                                       ObIArray<ObRawExpr *> &semi_conditions,
                                       ObJoinType join_type,
                                       AnyAllSQParam &trans_param);
  int anyall_build_semi_infos(AnyAllSQParam &trans_param,
                              ObJoinType join_type,
                              ObIArray<TableItem *> &right_tables,
                              ObIArray<ObSEArray<ObRawExpr *, 4>> &final_semi_conds);

private:
  // anyall OR branch expansion: expand homogeneous OR conditions into a single =ANY(UNION ALL ...)
  struct OrBranchInfo {
    enum Type
    {
      SCALAR_CMP,
      INLIST,
      ANY_SUBQUERY,
      INVALID
    };

    OrBranchInfo()
        : type_(INVALID),
          left_expr_(NULL),
          right_expr_(),
          scalar_cmp_type_(T_INVALID),
          output_column_cnt_(0)
    {}

    Type type_;
    ObRawExpr *left_expr_;
    ObRawExpr *right_expr_;
    ObItemType scalar_cmp_type_;  // normalized: T_OP_EQ/NE/GT/GE/LT/LE
    int64_t output_column_cnt_;   // number of output columns

    TO_STRING_KV(K_(type), K_(left_expr), K_(right_expr), K_(scalar_cmp_type), K_(output_column_cnt));
  };

  int anyall_collect_or_branch_infos(ObRawExpr *expr,
                                     AnyAllSQParam &trans_param,
                                     ObIArray<OrBranchInfo> &branches);
  int anyall_check_validity_for_or_branches(ObRawExpr *expr,
                                            AnyAllSQParam &trans_param,
                                            ObIArray<OrBranchInfo> &branches);
  int anyall_check_single_or_branch_validity(ObRawExpr *expr,
                                             OrBranchInfo &info,
                                             AnyAllSQParam &trans_param);
  int anyall_check_or_branches_isomorphic(const OrBranchInfo &anchor,
                                          const OrBranchInfo &branch,
                                          AnyAllSQParam &trans_param);
  int anyall_expand_or_cond(ObIArray<OrBranchInfo> &branches, ObRawExpr *&expanded_expr);
  int anyall_expand_or_create_stmt_for_scalar_cmp(const OrBranchInfo &branch,
                                                  ObSelectStmt *&child_stmt);
  int anyall_expand_or_create_stmt_for_inlist(const OrBranchInfo &branch,
                                              int64_t select_item_count,
                                              ObSelectStmt *&child_stmt);
  int anyall_expand_or_create_values_table_for_inlist(ObIArray<ObRawExpr *> &value_exprs,
                                                      int64_t column_cnt,
                                                      ObSelectStmt *&values_stmt);
  int anyall_expand_or_create_union_query_ref(ObIArray<ObSelectStmt *> &child_stmts,
                                              ObIArray<ObExecParamRawExpr *> &exec_params,
                                              int64_t select_item_count,
                                              ObQueryRefRawExpr *&union_query_ref);

private:  // for single row subquery
  struct SingleRowSQParam {
    SingleRowSQParam()
        : query_ref_expr_(NULL),
          not_null_column_(NULL),
          null_reject_select_idx_(),
          use_outer_join_(false)
    {}

    ObQueryRefRawExpr *query_ref_expr_;
    ObRawExpr *not_null_column_;
    ObSqlBitSet<> null_reject_select_idx_;
    // true if the subquery is unnested using outer join, otherwise it's unnested using inner join
    bool use_outer_join_;

    TO_STRING_KV(K_(query_ref_expr),
                 K_(not_null_column),
                 K_(null_reject_select_idx),
                 K_(use_outer_join));
  };

  // entry
  int singlerow_transform_subquery(ObIArray<ObSelectStmt *> &unnest_stmts, bool &trans_happened);
  int singlerow_transform_subquery_inner(ObRawExpr *expr,
                                         const bool in_where_cond,
                                         ObIArray<SingleRowSQParam> &trans_params,
                                         ObIArray<ObSelectStmt *> &unnest_stmts,
                                         bool &trans_happened);
  int singlerow_gather_transform_params(ObRawExpr *root_expr,
                                        ObRawExpr *expr,
                                        bool in_where_cond,
                                        ObIArray<SingleRowSQParam> &trans_params,
                                        const int64_t outline_loc);
  int singlerow_check_transform_validity_for_expr(ObRawExpr *root_expr,
                                                  ObRawExpr *expr,
                                                  bool in_where_cond,
                                                  ObIArray<SingleRowSQParam> &trans_params,
                                                  const int64_t outline_loc,
                                                  bool &is_valid);
  int singlerow_gather_one_transform_param(ObRawExpr *root_expr,
                                           ObQueryRefRawExpr *subquery_expr,
                                           bool in_where_cond,
                                           SingleRowSQParam &param,
                                           bool &is_valid);
  int singlerow_check_transform_validity_for_param(SingleRowSQParam &param,
                                                   bool in_where_cond,
                                                   bool &is_valid);
  int singlerow_do_unnest_subquery(SingleRowSQParam &param, const bool is_vector_assign);
  int singlerow_wrap_case_when_for_select_expr(SingleRowSQParam &param,
                                               ObIArray<ObRawExpr *> &select_exprs);
  int singlerow_wrap_case_when(ObSelectStmt &child_stmt,
                               ObRawExpr *not_null_column,
                               ObRawExpr *&expr);
  int singlerow_adjust_subquery_comparison_expr(ObRawExpr *expr);

private:  // opt version checks for enabling rewrite
  OB_INLINE bool is_enable_anyall_split_semi_join(const ObDMLStmt *stmt) const
  {
    return OB_NOT_NULL(stmt) && OB_NOT_NULL(stmt->get_query_ctx())
           && stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_2_3,
                                                              COMPAT_VERSION_4_3_0,
                                                              COMPAT_VERSION_4_3_2);
  }

  OB_INLINE bool is_opt_version_ge_461(const ObDMLStmt *stmt) const
  {
    return OB_NOT_NULL(stmt) && OB_NOT_NULL(stmt->get_query_ctx())
           && stmt->get_query_ctx()->check_opt_compat_version(COMPAT_VERSION_4_6_1);
  }

  OB_INLINE bool is_enable_pre_order_unnest(const ObDMLStmt *stmt) const
  { return is_opt_version_ge_461(stmt); }

  OB_INLINE bool is_enable_anyall_unnest_nested_subquery(const ObDMLStmt *stmt) const
  { return is_opt_version_ge_461(stmt); }

  OB_INLINE bool is_enable_anyall_unnest_homogeneous_or_cond(const ObDMLStmt *stmt) const
  { return is_opt_version_ge_461(stmt); }

  OB_INLINE bool is_enable_singlerow_unnest_multi_col_subquery(const ObDMLStmt *stmt) const
  { return is_opt_version_ge_461(stmt); }

  OB_INLINE bool is_enable_singlerow_unnest_nested_subquery(const ObDMLStmt *stmt) const
  { return is_opt_version_ge_461(stmt); }

private:
  ObDMLStmt *stmt_;
  DISALLOW_COPY_AND_ASSIGN(ObTransformSubqueryUnnest);
};
} /* namespace sql */
} /* namespace oceanbase */

#endif /* _OB_TRANSFORM_SUBQUERY_UNNEST_H_ */
