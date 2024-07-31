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

#ifndef OCEANBASE_SQL_REWRITE_OB_TRANSFORM_CONST_PROPAGATE_
#define OCEANBASE_SQL_REWRITE_OB_TRANSFORM_CONST_PROPAGATE_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/rewrite/ob_transform_utils.h"

namespace oceanbase
{
namespace sql
{
class ObTransformConstPropagate : public ObTransformRule
{
public:
  explicit ObTransformConstPropagate(ObTransformerCtx *ctx) : 
    ObTransformRule(ctx, TransMethod::POST_ORDER, T_REPLACE_CONST),
    allocator_("ConstPropagate") {}

  virtual ~ObTransformConstPropagate();

  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int check_hint_status(const ObDMLStmt &stmt, bool &need_trans) override;

  int do_transform(ObDMLStmt *stmt,
                   bool ignore_all_select_exprs,
                   bool &trans_happened);

private:

  struct ExprConstInfo {
    ExprConstInfo() :
      column_expr_(NULL),
      const_expr_(NULL),
      exclude_expr_(NULL),
      new_expr_(NULL),
      equal_infos_(),
      need_add_constraint_(PRE_CALC_RESULT_NONE),
      mem_equal_(false),
      is_used_(false),
      is_complex_const_info_(false),
      multi_const_exprs_(),
      multi_need_add_constraints_()
      { }

    virtual ~ExprConstInfo() { }

    int merge_complex(ExprConstInfo &other);

    ObRawExpr* column_expr_;
    ObRawExpr* const_expr_;
    ObRawExpr* exclude_expr_;
    ObRawExpr* new_expr_;
    common::ObSEArray<ObPCParamEqualInfo, 2> equal_infos_;
    PreCalcExprExpectResult need_add_constraint_;
    bool mem_equal_; //param expr mem is const expr.
    bool is_used_;
    //record or/in predicate const exprs
    bool is_complex_const_info_;
    common::ObSEArray<ObRawExpr*, 2> multi_const_exprs_;
    common::ObSEArray<PreCalcExprExpectResult, 2> multi_need_add_constraints_;

    TO_STRING_KV(KPC_(column_expr),
                 KPC_(const_expr),
                 K_(exclude_expr),
                 K_(new_expr),
                 K_(equal_infos),
                 K_(need_add_constraint),
                 K_(mem_equal),
                 K_(is_used),
                 K_(is_complex_const_info),
                 K_(multi_const_exprs),
                 K_(multi_need_add_constraints));
  };

  struct ConstInfoContext {
    ConstInfoContext(const ObSharedExprChecker &shared_expr_checker,
                     bool allow_trans) : active_const_infos_(),
                         expired_const_infos_(),
                         extra_excluded_exprs_(),
                         allow_trans_(allow_trans),
                         shared_expr_checker_(shared_expr_checker)
    {
    }
    ~ConstInfoContext() {}
    void reset() {
      active_const_infos_.reset();
      expired_const_infos_.reset();
      extra_excluded_exprs_.reset();
    }

    int add_const_infos(ObIArray<ExprConstInfo> &const_infos);
    int add_const_info(ExprConstInfo &const_info);
    int merge_expired_const_infos(ConstInfoContext &other, bool can_pull_up);
    int find_exclude_expr(const ObRawExpr *expr, bool &found);
    int expire_const_infos();

    common::ObSEArray<ExprConstInfo, 2> active_const_infos_;
    common::ObSEArray<ExprConstInfo, 2> expired_const_infos_;
    common::ObSEArray<ObRawExpr *, 2> extra_excluded_exprs_;
    bool allow_trans_;
    const ObSharedExprChecker &shared_expr_checker_;

    TO_STRING_KV(K_(active_const_infos),
                 K_(expired_const_infos),
                 K_(extra_excluded_exprs),
                 K_(allow_trans));
  };

  struct PullupConstInfo {
    PullupConstInfo() :
      column_expr_(NULL),
      const_expr_(NULL),
      equal_infos_(),
      need_add_constraint_(PRE_CALC_RESULT_NONE)
      { }

    virtual ~PullupConstInfo() { }

    ObRawExpr* column_expr_;
    ObRawExpr* const_expr_;
    common::ObSEArray<ObPCParamEqualInfo, 2> equal_infos_;
    PreCalcExprExpectResult need_add_constraint_;

    TO_STRING_KV(K_(column_expr),
                 K_(const_expr),
                 K_(equal_infos),
                 K_(need_add_constraint));
  };

  int check_allow_trans(ObDMLStmt *stmt, bool &allow_trans);

  int recursive_collect_const_info_from_table(ObDMLStmt *stmt,
                                              TableItem *table_item,
                                              ConstInfoContext &const_ctx,
                                              bool is_null_side,
                                              bool &trans_happened);

  int replace_common_exprs(ObIArray<ObRawExpr *> &exprs,
                           ConstInfoContext &const_ctx,
                           bool &trans_happened,
                           bool used_in_compare = true);

  int replace_group_exprs(ObSelectStmt *stmt,
                          ConstInfoContext &const_ctx,
                          bool ignore_all_select_exprs,
                          bool &trans_happened);

  int replace_orderby_exprs(ObIArray<OrderItem> &order_items,
                            ConstInfoContext &const_ctx,
                            bool &trans_happened);

  int replace_select_exprs(ObSelectStmt *stmt,
                           ConstInfoContext &const_ctx,
                           bool &trans_happened);
  int replace_select_exprs_skip_agg(ObSelectStmt *stmt,
                                  ConstInfoContext &const_ctx,
                                  bool &trans_happened);
  int replace_select_exprs_skip_agg_internal(ObRawExpr *&cur_expr,
                                          ConstInfoContext &const_ctx,
                                          ObIArray<ObRawExpr *> &parent_exprs,
                                          bool &trans_happened,
                                          bool used_in_compare);

  int replace_assignment_exprs(ObIArray<ObAssignment> &assignments,
                               ConstInfoContext &const_ctx,
                               bool &trans_happened);

  int replace_join_conditions(ObDMLStmt *stmt,
                              ConstInfoContext &const_ctx,
                              bool &trans_happened);

  int replace_semi_conditions(ObDMLStmt *stmt,
                              ConstInfoContext &const_ctx,
                              bool &trans_happened);

  int exclude_redundancy_join_cond(ObIArray<ObRawExpr*> &condition_exprs,
                                  ObIArray<ExprConstInfo> &expr_const_infos,
                                  ObIArray<ObRawExpr*> &excluded_exprs);

  bool find_const_expr(ObIArray<ExprConstInfo> &expr_const_infos, 
                      ObRawExpr *expr, 
                      ObRawExpr* &const_expr);

  int collect_equal_pair_from_condition(ObDMLStmt *stmt,
                                        ObIArray<ObRawExpr*> &condition_exprs,
                                        ConstInfoContext &const_ctx,
                                        bool &trans_happened);

  int collect_equal_pair_from_tables(ObDMLStmt *stmt,
                                     ConstInfoContext &const_ctx,
                                     bool &trans_happened);

  int collect_equal_pair_from_pullup(ObDMLStmt *stmt,
                                     TableItem *table,
                                     ConstInfoContext &const_ctx,
                                     bool is_null_side);

  int collect_equal_pair_from_semi_infos(ObDMLStmt *stmt,
                                         ConstInfoContext &const_ctx,
                                         bool &trans_happened);

  int check_const_expr_validity(const ObDMLStmt &stmt,
                                ExprConstInfo &const_info,
                                bool &is_valid);

  int check_const_expr_not_null(const ObDMLStmt &stmt,
                                ExprConstInfo &const_info,
                                bool &is_valid);

  int check_cast_const_expr(ExprConstInfo &const_info,
                            bool &is_valid);

  int check_can_replace_in_select(ObSelectStmt *stmt,
                                  ObRawExpr *target_expr,
                                  bool ignore_all_select_exprs,
                                  bool &can_replace);

  int recursive_check_can_replace_in_select(ObRawExpr *expr,
                                            ObRawExpr *target_expr,
                                            ObIArray<ObRawExpr *> &parent_exprs,
                                            bool ignore_all_select_exprs,
                                            bool &can_replace);

  int recursive_replace_join_conditions(TableItem *table_item,
                                        ConstInfoContext &const_ctx,
                                        bool &trans_happened);

  int replace_expr_internal(ObRawExpr *&cur_expr,
                            ConstInfoContext &const_ctx,
                            bool &trans_happened,
                            bool used_in_compare = true);

  int recursive_replace_expr(ObRawExpr *&cur_expr,
                             ObIArray<ObRawExpr *> &parent_exprs,
                             ConstInfoContext &const_ctx,
                             bool used_in_compare,
                             bool &trans_happened);

  int replace_internal(ObRawExpr *&cur_expr,
                       ObIArray<ObRawExpr *> &parent_exprs,
                       ObIArray<ExprConstInfo> &expr_const_infos,
                       bool used_in_compare,
                       bool &trans_happened);

  int prepare_new_expr(ExprConstInfo &const_info);

  int check_set_op_expr_const(ObSelectStmt *stmt,
                              const ObSetOpRawExpr* set_op_expr,
                              ObRawExpr *&const_expr,
                              ObIArray<ObPCParamEqualInfo> &equal_infos);

  int is_parent_null_side(ObDMLStmt *&parent_stmt,
                          ObDMLStmt *&stmt,
                          bool &is_on_null_side);

  int remove_const_exec_param(ObDMLStmt *stmt);

  int remove_const_exec_param_exprs(ObDMLStmt *stmt,
                                    bool &trans_happened);

  int do_remove_const_exec_param(ObRawExpr *&expr,
                                 ObIArray<ObRawExpr *> &parent_exprs,
                                 bool &trans_happened);

  int check_need_cast_when_replace(ObRawExpr *expr,
                                   ObRawExpr *const_expr,
                                   ObIArray<ObRawExpr *> &parent_exprs,
                                   bool &need_cast);

  int check_is_in_or_notin_param(ObIArray<ObRawExpr *> &parent_exprs, bool &is_right_param);

  int collect_equal_param_constraints(ObIArray<ExprConstInfo> &expr_const_infos);

  int add_equal_param_constraint(ObRawExpr *column_expr,
                                 ObRawExpr *const_expr,
                                 PreCalcExprExpectResult expect_result);
  int recursive_collect_equal_pair_from_condition(ObDMLStmt *stmt,
                                                  ObRawExpr *expr,
                                                  ConstInfoContext &const_ctx,
                                                  bool &trans_happened);

  int merge_complex_const_infos(ObIArray<ExprConstInfo> &cur_const_infos,
                                ObIArray<ExprConstInfo> &new_const_infos);

  int merge_const_info(ExprConstInfo &const_info_l,
                       ExprConstInfo &const_info_r,
                       ExprConstInfo &new_const_info);

  int replace_check_constraint_exprs(ObDMLStmt *stmt,
                                     ConstInfoContext &const_ctx,
                                     bool &trans_happened);

  int check_constraint_expr_validity(ObRawExpr *check_constraint_expr,
                                     const ObIArray<ObDMLStmt::PartExprItem> &part_items,
                                     ObIArray<ExprConstInfo> &expr_const_infos,
                                     ObRawExpr *&part_column_expr,
                                     ObIArray<ObRawExpr*> &old_column_exprs,
                                     ObIArray<ObRawExpr*> &new_const_exprs,
                                     int64_t &complex_cst_info_idx,
                                     bool &is_valid);

  int do_check_constraint_param_expr_vaildity(ObRawExpr *column_param_expr,
                                              ObRawExpr *non_column_param_expr,
                                              const ObIArray<ObDMLStmt::PartExprItem> &part_items,
                                              ObIArray<ExprConstInfo> &expr_const_infos,
                                              ObIArray<ObRawExpr*> &old_column_exprs,
                                              ObIArray<ObRawExpr*> &new_const_exprs,
                                              int64_t &complex_cst_info_idx,
                                              bool &is_valid);

  int check_all_const_propagate_column(ObIArray<ObRawExpr*> &column_exprs,
                                       ObIArray<ExprConstInfo> &expr_const_infos,
                                       ObIArray<ObRawExpr*> &new_const_exprs,
                                       int64_t &const_info_idx,
                                       bool &is_all);

  int recursive_check_non_column_param_expr_validity(ObRawExpr *expr,
                                                     ObIArray<ObRawExpr *> &parent_exprs,
                                                     bool &is_valid);

  int do_replace_check_constraint_expr(ObDMLStmt *stmt,
                                       ObRawExpr *check_constraint_expr,
                                       ObIArray<ExprConstInfo> &expr_const_infos,
                                       ObRawExpr *part_column_expr,
                                       ObIArray<ObRawExpr*> &old_column_exprs,
                                       ObIArray<ObRawExpr*> &new_const_exprs,
                                       int64_t &complex_cst_info_idx,
                                       bool &trans_happened);

  int build_new_in_condition_expr(ObRawExpr *check_constraint_expr,
                                  ExprConstInfo &expr_const_info,
                                  ObRawExpr *part_column_expr,
                                  ObIArray<ObRawExpr*> &old_column_exprs,
                                  ObIArray<ObRawExpr*> &new_const_exprs,
                                  ObRawExpr *&new_condititon_expr,
                                  ObIArray<ObRawExpr*> &not_null_values,
                                  bool &reject);

  int batch_mark_expr_const_infos_used(ObIArray<ObRawExpr*> &column_exprs,
                                       ObIArray<ExprConstInfo> &expr_const_infos);

  int generate_pullup_const_info(ObSelectStmt *stmt,
                                 ObRawExpr *expr,
                                 ConstInfoContext &const_ctx);

  int acquire_pullup_infos(ObDMLStmt *stmt,
                           ObIArray<PullupConstInfo> *&pullup_infos);

  int collect_from_pullup_const_infos(ObDMLStmt *stmt,
                                      ObRawExpr *expr,
                                      ExprConstInfo &equal_info);

  int check_constraint_value_validity(ObRawExpr *value_expr, bool &reject);

  int check_can_replace_child_of_row(ConstInfoContext &const_ctx,
                                     ObRawExpr *&cur_expr,
                                     bool &can_replace_child);

private:
  typedef ObSEArray<PullupConstInfo, 2> PullupConstInfos;
  ObArenaAllocator allocator_;
  hash::ObHashMap<uint64_t, int64_t> stmt_map_;
  ObSEArray<PullupConstInfos *, 4> stmt_pullup_const_infos_;
};

} //namespace sql
} //namespace oceanbase
#endif //OCEANBASE_SQL_REWRITE_OB_TRANSFORM_CONST_PROPAGATE_
