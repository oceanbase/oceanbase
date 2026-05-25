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

#ifndef OCEANBASE_SQL_RESOLVER_EXPR_OB_FTS_ORACLE_RESOLVER_MATCH_UTIL_H_
#define OCEANBASE_SQL_RESOLVER_EXPR_OB_FTS_ORACLE_RESOLVER_MATCH_UTIL_H_

#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{

class ObRawExprFactory;
class ObSQLSessionInfo;

// Resolver-layer helpers: Oracle CONTAINS / SCORE / MATCH_AGAINST (see ob_fts_oracle_resolver_match_util.cpp).
class ObFtsOracleMatchExprUtil
{
public:
  static int convert_contains_to_match_against(
      const ParseNode &node,
      void *malloc_pool,
      const common::ParamStore &param_list,
      ParseNode *&match_against_node);
  static int convert_score_to_es_score(
      const ParseNode &node,
      void *malloc_pool,
      const common::ParamStore &param_list,
      ParseNode *&score_node);
  static int validate_and_set_match_against_mode(
      const ParseNode &node,
      const int64_t data_version,
      const common::ObIArray<ObMatchFunRawExpr *> *match_exprs,
      ObMatchFunRawExpr &match_against);
  static int find_and_validate_score_match(
      const int64_t score_label,
      const common::ObIArray<ObMatchFunRawExpr *> &match_exprs,
      ObMatchFunRawExpr *&matched_match_expr);
  static int check_duplicate_contains_labels(
      const common::ObIArray<ObMatchFunRawExpr *> &match_exprs);
  static int serialize_contains_expr(
      const ObMatchFunRawExpr &match_expr,
      const ExplainType type,
      char *buf,
      const int64_t buf_len,
      int64_t &pos);
  static int check_expr_after_resolve(ObRawExpr *conjunct_root);
  /** Record phrase LIKE for Oracle CONTAINS rewrite; used to gate SCORE(label) like MySQL MATCH phrase projection. */
  static int attach_oracle_phrase_like_for_score(ObMatchFunRawExpr *match_expr, ObRawExpr *like_expr);
  /** If phrase LIKE was attached, wrap score as CASE WHEN like THEN match ELSE 0; else out_expr stays match. */
  static int wrap_oracle_phrase_score_expr_if_applicable(
      ObRawExprFactory &expr_factory,
      const ObSQLSessionInfo *session_info,
      ObMatchFunRawExpr *matched_match_expr,
      ObRawExpr *&out_expr);
private:
  static int parse_contains_params_(
      const ParseNode &node,
      ParseNode *&column_node,
      ParseNode *&search_string_node,
      ParseNode *&label_node);
  static int extract_contains_label_(
      const ParseNode *label_node,
      const common::ParamStore &param_list,
      int64_t &contains_label);
  static int convert_column_to_ref_(
      const ParseNode &column_node,
      void *malloc_pool,
      ParseNode *&column_ref_node);
  static int create_match_against_node_(
      void *malloc_pool,
      ParseNode &column_ref_node,
      ParseNode &search_string_node,
      const int64_t contains_label,
      ParseNode *&match_against_node);
  static int find_matching_contains_for_score_(
      const int64_t score_label,
      const common::ObIArray<ObMatchFunRawExpr *> &match_exprs,
      ObMatchFunRawExpr *&matched_match_expr);
  static int rhs_is_literal_numeric_zero_(
      ObRawExpr *right_expr, bool &is_zero);
  static int match_relop_vs_zero_policy_(
      ObRawExpr *cmp_expr);
  static int walk_conjunct_contains_policy_(
      ObRawExpr *cur_expr, const int32_t depth);
  static int build_oracle_phrase_score_case_when_expr_(
      ObRawExprFactory &expr_factory,
      const ObSQLSessionInfo *session_info,
      ObRawExpr *phrase_like_expr,
      ObMatchFunRawExpr *match_expr,
      ObRawExpr *&case_when_expr);
};

} // namespace sql
} // namespace oceanbase

#endif /* OCEANBASE_SQL_RESOLVER_EXPR_OB_FTS_ORACLE_RESOLVER_MATCH_UTIL_H_ */
