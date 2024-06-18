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

#ifndef _OB_TRANSFORM_CONDITIONAL_AGGR_COALESCE_H_
#define _OB_TRANSFORM_CONDITIONAL_AGGR_COALESCE_H_ 1

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"

namespace oceanbase
{
namespace sql
{
/** @brief ObTransformConditionalAggrCoalesce attempts to coalesce conditional aggregation functions
* "aggr_func(case_expr)" with similar structure, in order to reduce the number of aggregation
* functions and the calculation times of case expr.
*
* There are two rewrite paths: [trans_without_pullup] and [trans_with_pullup].
* e.g. [trans_without_pullup]
* select count(distinct case when c1 = 0 then 0 else c2 end),
*        count(distinct case when c1 = 1 then 0 else c2 end),
*        count(distinct case when c1 = 2 then 0 else c2 end)
* from t1 group by c1;
*
* ==> coalesce
*
* select case when c1 = 0 then count(distinct 0) else count(distinct c2) end,
*        case when c1 = 1 then count(distinct 0) else count(distinct c2) end,
*        case when c1 = 2 then count(distinct 0) else count(distinct c2) end
* from t1 group by c1;
*
* e.g. [trans_with_pullup]
* select count(case when c1 = 0 then 0 else c2 end),
*        count(case when c1 = 1 then 0 else c2 end),
*        count(case when c1 = 2 then 0 else c2 end)
* from t1;
*
* ==> coalesce + pullup
*
* select count_sum(v.s1), count_sum(v.s2), count_sum(v.s3)
* from (
* select case when c1 = 0 then count(0) else count(c2) end as s1,
*        case when c1 = 1 then count(0) else count(c2) end as s2,
*        case when c1 = 2 then count(0) else count(c2) end as s3
* from t1
* group by c1) v;
*/
class ObTransformConditionalAggrCoalesce : public ObTransformRule
{
public:
  static const int64_t MIN_COND_AGGR_CNT_FOR_COALESCE;
  static const int64_t MAX_GBY_NDV_PRODUCT_FOR_COALESCE;
  static const double MIN_CUT_RATIO_FOR_COALESCE;

  explicit ObTransformConditionalAggrCoalesce(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_COALESCE_AGGR) {}
  virtual ~ObTransformConditionalAggrCoalesce() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;

private:
  typedef std::pair<bool, bool> TransFlagPair;
  struct TransformParam {
    TransformParam() : cond_aggrs_wo_extra_dep_(), cond_aggrs_with_extra_dep_(),
                       extra_dep_cols_() { }
    ~TransformParam() {}
    TO_STRING_KV(K_(cond_aggrs_wo_extra_dep), K_(cond_aggrs_with_extra_dep), K_(extra_dep_cols));

    // cond aggrs whose when exprs do not rely on columns beyond group by clause
    ObSEArray<ObAggFunRawExpr*, 4> cond_aggrs_wo_extra_dep_;
    // cond aggrs whose when exprs rely on columns beyond group by clause
    ObSEArray<ObAggFunRawExpr*, 4> cond_aggrs_with_extra_dep_;
    // extra dependent columns of cond_aggrs_with_extra_dep_
    ObSEArray<ObRawExpr*, 4> extra_dep_cols_;
  };
  int check_hint_valid(ObDMLStmt &stmt,
                       bool &force_trans_wo_pullup,
                       bool &force_no_trans_wo_pullup,
                       bool &force_trans_with_pullup,
                       bool &force_no_trans_with_pullup);
  int check_basic_validity(ObDMLStmt *stmt,
                           TransformParam &trans_param,
                           ObSelectStmt *&select_stmt,
                           bool &valid_wo_pullup,
                           bool &valid_with_pullup);
  int collect_cond_aggrs_info(ObSelectStmt *select_stmt,
                              TransformParam &trans_param,
                              bool &cnt_unpullupable_aggr);
  bool check_aggr_type(ObItemType aggr_type);
  int check_cond_aggr_form(ObAggFunRawExpr *aggr_expr,
                           ObCaseOpRawExpr *&case_expr,
                           bool &is_cond_aggr);
  int check_case_when_validity(ObSelectStmt *select_stmt,
                                 ObAggFunRawExpr *cond_aggr,
                                 ObCaseOpRawExpr *case_expr,
                                 bool &is_case_when_valid);
  int extract_extra_dep_cols(ObIArray<ObRawExpr*> &target_exprs,
                             ObIArray<ObRawExpr*> &exclude_exprs,
                             ObIArray<ObRawExpr*> &extra_dep_cols);
  int inner_extract_extra_dep_cols(ObRawExpr *target_expr,
                                   ObIArray<ObRawExpr*> &exclude_exprs,
                                   ObIArray<ObRawExpr*> &extra_dep_cols);
  int try_transform_wo_pullup(ObSelectStmt *select_stmt,
                              ObDMLStmt *parent_stmt,
                              bool force_trans,
                              TransformParam &trans_param,
                              bool &trans_happened);
  int try_transform_with_pullup(ObSelectStmt *select_stmt,
                                ObDMLStmt *parent_stmt,
                                bool force_trans,
                                TransformParam &trans_param,
                                bool &trans_happened);
  int coalesce_cond_aggrs(ObIArray<ObAggFunRawExpr*> &base_aggrs,
                     ObIArray<ObAggFunRawExpr*> &cond_aggrs,
                     ObIArray<ObRawExpr*> &case_exprs,
                     ObIArray<ObAggFunRawExpr*> &new_aggrs,
                     ObIArray<ObPCConstParamInfo> &constraints);
  int build_aggr_expr(ObItemType expr_type,
                      bool is_param_distinct,
                      ObRawExpr *param_expr,
                      ObAggFunRawExpr *&aggr_expr);
  int try_share_aggr(ObIArray<ObAggFunRawExpr*> &base_aggrs,
                     ObAggFunRawExpr *&target_aggr,
                     bool &is_shareable,
                     ObIArray<ObPCConstParamInfo> &constraints);
  int check_statistics_threshold(ObSelectStmt *select_stmt,
                                 TransformParam &trans_param,
                                 bool &hit_threshold);
  int collect_pushdown_select(ObSelectStmt *select_stmt,
                              ObIArray<ObRawExpr*> &extra_cols,
                              ObIArray<ObRawExpr*> &coalesced_case_exprs,
                              ObIArray<ObRawExpr*> &pushdown_select);
  int create_and_replace_aggrs_for_merge(ObSelectStmt *select_stmt, ObSelectStmt *view_stmt);
  int create_aggr_for_merge(ObItemType aggr_type,
                            ObRawExpr *param_expr,
                            ObAggFunRawExpr *&aggr_expr);
  int check_aggrs_count_decrease(ObIArray<ObAggFunRawExpr*> &old_aggrs,
                                 ObIArray<ObAggFunRawExpr*> &new_aggrs,
                                 bool &is_cnt_decrease);
  int check_rewrite_gain(TableItem* base_table,
                         ObIArray<ObRawExpr*> &cols_in_groupby,
                         int64_t cond_aggr_cnt,
                         bool ignore_threshold,
                         double &gain);
  int do_transform_wo_pullup(ObSelectStmt *select_stmt,
                             ObIArray<ObAggFunRawExpr*> &cond_aggrs,
                             ObIArray<ObRawExpr*> &coalesced_case_exprs,
                             ObIArray<ObAggFunRawExpr*> &new_aggr_items,
                             ObIArray<ObPCConstParamInfo> &constraints);
  int do_transform_with_pullup(ObSelectStmt *select_stmt,
                               ObIArray<ObAggFunRawExpr*> &cond_aggrs,
                               ObIArray<ObRawExpr*> &extra_dep_cols,
                               ObIArray<ObRawExpr*> &coalesced_case_exprs,
                               ObIArray<ObAggFunRawExpr*> &new_aggr_items,
                               ObIArray<ObPCConstParamInfo> &constraints);
  int refresh_project_name(ObDMLStmt *parent_stmt, ObSelectStmt *select_stmt);
  int get_aggr_type(ObRawExpr* expr, ObItemType &aggr_type);
  DISALLOW_COPY_AND_ASSIGN(ObTransformConditionalAggrCoalesce);

};

} //namespace sql
} //namespace oceanbase
#endif /* _OB_TRANSFORM_CONDITIONAL_AGGR_COALESCE_H_ */