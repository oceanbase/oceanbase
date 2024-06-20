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

#ifndef OB_TRANSFORM_OR_EXPANSION_H_
#define OB_TRANSFORM_OR_EXPANSION_H_

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
namespace oceanbase
{
namespace sql
{

class ObTransformOrExpansion: public ObTransformRule
{
  static const int64_t MAX_STMT_NUM_FOR_OR_EXPANSION;
  static const int64_t MAX_TIMES_FOR_OR_EXPANSION;
  typedef ObBitSet<8> ColumnBitSet;
  struct TableColBitSet{
  public:
    TableColBitSet()
      : table_id_(OB_INVALID_ID),
        column_bit_set_() {}
    TableColBitSet(int64_t table_id, const ColumnBitSet &column_bit_set)
      : table_id_(table_id),
        column_bit_set_(column_bit_set) {}
    int assign(const TableColBitSet& other)
    {
      int ret = OB_SUCCESS;
      table_id_ = other.table_id_;
      ret = column_bit_set_.assign(other.column_bit_set_);
      return ret;
    }
    bool operator==(const TableColBitSet& other) const
    {
      return table_id_ == other.table_id_ &&
             column_bit_set_ == other.column_bit_set_;
    }
    int64_t table_id_;
    ColumnBitSet column_bit_set_;
    TO_STRING_KV(K_(table_id),
                 K_(column_bit_set));
  };
  enum OR_EXPAND_TYPE {
    INVALID_OR_EXPAND_TYPE        = 1 << 0,
    OR_EXPAND_HINT                = 1 << 1,
    OR_EXPAND_TOP_K               = 1 << 2,
    OR_EXPAND_SUB_QUERY           = 1 << 3,
    OR_EXPAND_MULTI_INDEX         = 1 << 4,
    OR_EXPAND_JOIN                = 1 << 5
  };

  struct ObCostBasedRewriteCtx {
    ObCostBasedRewriteCtx()
      : trans_id_(OB_INVALID_ID),
        or_expand_type_(INVALID_OR_EXPAND_TYPE),
        is_set_distinct_(false),
        is_valid_topk_(false),
        is_unique_(false),
        orig_expr_(NULL),
        hint_(NULL) {}
    void reset()
    {
      expand_exprs_.reuse();
      trans_id_ = OB_INVALID_ID;
      or_expand_type_ = INVALID_OR_EXPAND_TYPE;
      is_set_distinct_ = false;
      is_valid_topk_ = false;
      is_unique_ = false;
      orig_expr_ = NULL;
      hint_ = NULL;
    }
    uint64_t trans_id_;
    uint64_t or_expand_type_;
    bool is_set_distinct_; // trans or expansion use distinct set
    bool is_valid_topk_; // determine whether to do or classify when is_topk
    bool is_unique_; // spj stmt before trans is unique
    const ObRawExpr *orig_expr_;
    const ObOrExpandHint *hint_;
    ObSEArray<ObRawExpr*, 2> expand_exprs_;
  };

  struct OrExpandInfo {
    OrExpandInfo()
      : pos_(-1),
        or_expand_type_(INVALID_OR_EXPAND_TYPE),
        is_set_distinct_(false),
        is_valid_topk_(false) {}
    int64_t pos_;
    uint64_t or_expand_type_;
    bool is_set_distinct_;
    bool is_valid_topk_;
    TO_STRING_KV(K_(pos),
                 K_(or_expand_type),
                 K_(is_set_distinct),
                 K_(is_valid_topk));
  };

public:
  ObTransformOrExpansion(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::PRE_ORDER, T_USE_CONCAT),
      try_times_(0) {}
  virtual ~ObTransformOrExpansion() {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
protected:
  virtual int adjust_transform_types(uint64_t &transform_types) override;
  virtual int is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid) override;
  virtual int transform_one_stmt_with_outline(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                              ObDMLStmt *&stmt,
                                              bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;

  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;
private:
  int transform_in_where_conditon(ObIArray<ObParentDMLStmt> &parent_stmts,
                                  ObDMLStmt *&stmt,
                                  bool &trans_happened);

  int transform_in_semi_info(ObIArray<ObParentDMLStmt> &parent_stmts,
                             ObDMLStmt *&stmt,
                             bool &trans_happened);

  int transform_in_joined_table(ObIArray<ObParentDMLStmt> &parent_stmts,
                                ObDMLStmt *&stmt,
                                bool &trans_happened);

  int transform_in_joined_table(ObIArray<ObParentDMLStmt> &parent_stmts,
                                ObDMLStmt *&stmt,
                                TableItem *table,
                                bool &trans_happened);

  int try_do_transform_inner_join(ObIArray<ObParentDMLStmt> &parent_stmts,
                                  ObDMLStmt *&stmt,
                                  JoinedTable *joined_table,
                                  bool &trans_happened);

  int try_do_transform_left_join(ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 JoinedTable *joined_table,
                                 bool &trans_happened);

  int do_transform_for_left_join(ObSelectStmt *&stmt,
                                 ObSqlBitSet<> &left_unique_pos,
                                 ObSqlBitSet<> &right_flag_pos);

  int create_single_joined_table_stmt(ObDMLStmt *trans_stmt,
                                      uint64_t joined_table_id,
                                      TableItem *&view_table,
                                      ObSelectStmt *&ref_query);
  int get_joined_table_pushdown_conditions(const TableItem *cur_table,
                                           const ObDMLStmt *trans_stmt,
                                           ObIArray<ObRawExpr *> &pushdown_conds);
  int add_select_item_to_ref_query(ObSelectStmt *stmt,
                                   const uint64_t flag_table_id,
                                   StmtUniqueKeyProvider &unique_key_provider,
                                   ObSqlBitSet<> &left_unique_pos,
                                   ObSqlBitSet<> &right_flag_pos,
                                   int64_t &flag_view_sel_count,
                                   ObSelectStmt *&orig_flag_stmt);
  int recover_flag_temp_table(ObSelectStmt *stmt,
                              const uint64_t flag_table_id,
                              const int64_t orig_sel_count,
                              ObSelectStmt *orig_flag_stmt);

  int create_row_number_window_function(ObIArray<ObRawExpr *> &partition_exprs,
                                        ObIArray<ObRawExpr *> &order_exprs,
                                        ObWinFunRawExpr *&win_expr);

  int add_filter_to_stmt(ObSelectStmt *stmt,
                         ObRawExpr *rn_expr,
                         ObIArray<ObRawExpr*> &flag_exprs);

  int check_child_table_valid(JoinedTable *cur_table,
                              TableItem *&not_null_side_table);

  int get_table_not_on_null_side(TableItem *cur_table, TableItem *&target_table);

  int convert_exprs_to_filter_level(ObSelectStmt *stmt,
                                    ObRawExpr *rn_expr,
                                    ObIArray<ObRawExpr*> &flag_exprs,
                                    ObRawExpr *&upper_rn_expr,
                                    ObIArray<ObRawExpr*> &upper_flag_exprs);

  int convert_exprs_to_win_func_level(ObSelectStmt *stmt,
                                      ObSqlBitSet<> &left_unique_pos,
                                      ObSqlBitSet<> &right_flag_pos,
                                      ObIArray<ObRawExpr*> &partition_exprs,
                                      ObIArray<ObRawExpr*> &order_exprs);

  int check_basic_validity(const ObDMLStmt &stmt, bool &is_valid);
  
  int has_valid_condition(ObDMLStmt &stmt,
                          ObCostBasedRewriteCtx &ctx,
                          const ObIArray<ObRawExpr*> &conds,
                          bool &has_valid,
                          ObIArray<ObRawExpr*> *expect_ordering);

  bool reached_max_times_for_or_expansion() { return try_times_ >= MAX_TIMES_FOR_OR_EXPANSION; }

  int check_upd_del_stmt_validity(const ObDelUpdStmt &stmt, bool &is_valid);
  int disable_pdml_for_upd_del_stmt(ObDMLStmt &stmt);

  int has_odd_function(const ObDMLStmt &stmt, bool &has);

  int get_trans_view(ObDMLStmt *stmt,
                      ObDMLStmt *&upper_stmt,
                      ObSelectStmt *&child_stmt);

  int merge_stmt(ObDMLStmt *&upper_stmt,
                 ObSelectStmt *input_stmt,
                 ObSelectStmt *union_stmt);

  int remove_stmt_select_item(ObSelectStmt *select_stmt,
                              int64_t select_item_count);

  int check_condition_on_same_columns(const ObDMLStmt &stmt,
                                      const ObRawExpr &expr,
                                      bool &using_same_cols);

  int is_match_index(const ObDMLStmt *stmt,
                     const ObRawExpr *expr,
                     EqualSets &equal_sets,
                     ObIArray<ObRawExpr*> &const_exprs,
                     bool &is_match);

  int is_valid_subquery_cond(const ObDMLStmt &stmt,
                             const ObRawExpr &expr,
                             bool &is_valid);

  int is_valid_topk_cond(const ObDMLStmt &stmt,
                         const ObIArray<ObRawExpr*> &expect_ordering,
                         const ObIArray<ObRawExpr*> &same_cols,
                         EqualSets &equal_sets,
                         ObIArray<ObRawExpr*> &const_exprs,
                         OrExpandInfo &trans_info);


  int is_valid_left_join_cond(const ObDMLStmt *stmt,
                              const ObRawExpr *expr,
                              ObSqlBitSet<> &right_table_set,
                              bool &is_valid);
  int is_valid_inner_join_cond(const ObDMLStmt *stmt,
                               const ObRawExpr *expr,
                               bool &is_valid);
  int is_contain_join_cond(const ObDMLStmt *stmt,
                           const ObRawExpr *expr,
                           bool &is_contain);
  int is_simple_cond(const ObDMLStmt *stmt,
                     const ObRawExpr *expr,
                     bool &is_simple);
  int is_valid_table_filter(const ObRawExpr *expr,
                            ObSqlBitSet<> &right_table_set,
                            bool &is_vaild);

  int check_condition_valid_basic(const ObDMLStmt *stmt,
                                  ObCostBasedRewriteCtx &ctx,
                                  const ObRawExpr *expr,
                                  const bool is_topk,
                                  bool &is_valid,
                                  ObIArray<ObRawExpr*> &same_cols,
                                  bool &using_same_cols);

  int get_use_hint_expand_type(const ObRawExpr &expr,
                               const bool can_set_distinct,
                               OrExpandInfo &trans_info);

  int is_condition_valid(const ObDMLStmt *stmt,
                         ObCostBasedRewriteCtx &ctx,
                         const ObRawExpr *expr,
                         const ObIArray<ObRawExpr*> &expect_ordering,
                         const JoinedTable *joined_table,
                         EqualSets &equal_sets,
                         ObIArray<ObRawExpr*> &const_exprs,
                         OrExpandInfo &trans_info);

  int get_expect_ordering(const ObDMLStmt &stmt,
                          ObIArray<ObRawExpr*> &expect_ordering);
  int convert_expect_ordering(ObDMLStmt *orig_stmt,
                              ObDMLStmt *spj_stmt,
                              ObIArray<ObRawExpr*> &expect_ordering);

  int get_common_columns_in_condition(const ObDMLStmt *stmt,
                                      const ObRawExpr *expr,
                                      ObIArray<ObRawExpr*> &common_cols);

  int inner_get_common_columns_in_condition(const ObDMLStmt *stmt,
                                            const ObRawExpr *expr,
                                            ObIArray<ObRawExpr*> &cols);

  int gather_transform_infos(ObSelectStmt *stmt,
                             ObCostBasedRewriteCtx &ctx,
                             const common::ObIArray<ObRawExpr*> &candi_conds,
                             const ObIArray<ObRawExpr*> &expect_ordering,
                             const JoinedTable *joined_table,
                             ObIArray<OrExpandInfo> &trans_infos);
  int transform_or_expansion(ObSelectStmt *stmt,
                             const uint64_t trans_id,
                             const int64_t expr_pos,
                             ObCostBasedRewriteCtx &ctx,
                             ObSelectStmt *&trans_stmt,
                             StmtUniqueKeyProvider &unique_key_provider);
  int adjust_or_expansion_stmt(ObIArray<ObRawExpr*> *conds_exprs,
                               const int64_t expr_pos,
                               const int64_t param_pos,
                               ObCostBasedRewriteCtx &ctx,
                               ObSelectStmt *&or_expansion_stmt);
  int create_expr_for_in_expr(const ObRawExpr &transformed_expr,
                              const int64_t param_pos,
                              ObCostBasedRewriteCtx &ctx,
                              common::ObIArray<ObRawExpr*> &generated_exprs);
  int create_expr_for_or_expr(ObRawExpr &transformed_expr,
                              const int64_t param_pos,
                              ObCostBasedRewriteCtx &ctx,
                              common::ObIArray<ObRawExpr*> &generated_exprs);

  int get_expand_conds(ObSelectStmt &stmt,
                       uint64_t trans_id,
                       ObIArray<ObRawExpr*> *&conds_exprs);
  int preprocess_or_condition(ObSelectStmt &stmt,
                              const uint64_t trans_id,
                              const int64_t expr_pos);
  int64_t get_or_expr_count(const ObRawExpr &expr);

  int check_select_expr_has_lob(ObDMLStmt &stmt, bool &has_lob);

  static int extract_columns(const ObRawExpr *expr,
                             int64_t &table_id, bool &from_same_table,
                             ColumnBitSet &col_bit_set);

  int is_contain_table_filter(const ObRawExpr *expr,
                              ObSqlBitSet<> &left_table_set,
                              ObSqlBitSet<> &right_table_set,
                              bool &is_contain);

  // check for semi anti join
  int is_expand_anti_or_cond(const ObRawExpr &expr, bool &is_anti_or_cond);
  int has_valid_semi_anti_cond(ObDMLStmt &stmt, ObCostBasedRewriteCtx &ctx, int64_t &begin_idx);
  int is_valid_semi_anti_cond(const ObDMLStmt *stmt,
                              ObCostBasedRewriteCtx &ctx,
                              const ObRawExpr *expr,
                              const SemiInfo *semi_info,
                              uint64_t &trans_type);

  // check log plan after transform
  int find_trans_log_set(ObLogicalOperator* op,
                         const uint64_t trans_id,
                         ObLogicalOperator *&log_set);
  int check_is_expected_plan(ObLogicalOperator* op,
                             ObCostBasedRewriteCtx &ctx,
                             bool &is_valid);
  int is_expected_topk_plan(ObLogicalOperator* op,
                            bool &is_valid);
  int is_expected_multi_index_plan(ObLogicalOperator* op,
                                   ObCostBasedRewriteCtx &ctx,
                                   bool &is_valid);
  int remove_filter_exprs(ObLogicalOperator* op,
                          ObIArray<ObRawExpr*> &candi_exprs);
  int is_candi_match_index_exprs(ObRawExpr *expr, bool &result);
  int get_candi_match_index_exprs(ObRawExpr *expr,
                                  ObIArray<ObRawExpr*> &candi_exprs);

  int pre_classify_or_expr(const ObRawExpr *expr, int &count);
  int classify_or_expr(const ObDMLStmt &stmt, ObRawExpr *&expr);
  int merge_expr_class(ObRawExpr *&expr_class, ObRawExpr *expr);
  int get_condition_related_tables(ObSelectStmt &stmt,
                                   int64_t expr_pos,
                                   const ObIArray<ObRawExpr*> &conds_exprs,
                                   bool &create_view,
                                   ObIArray<TableItem *> &or_expr_tables,
                                   ObIArray<SemiInfo *> &or_semi_infos);
  int get_condition_related_view(ObSelectStmt *stmt,
                                 ObSelectStmt *&view_stmt,
                                 TableItem *&view_table,
                                 int64_t &expr_pos,
                                 ObIArray<ObRawExpr*> *&conds_exprs,
                                 bool &is_set_distinct);
  int check_delay_expr(ObRawExpr* expr, bool &delay);
  int check_valid_rel_table(ObSelectStmt &stmt,
                            ObRelIds &rel_ids,
                            TableItem *rel_table,
                            bool &is_valid);
  int check_left_bottom_table(ObSelectStmt &stmt,
                              TableItem *rel_table,
                              TableItem *table,
                              bool &left_bottom);
  int check_stmt_valid_for_expansion(ObDMLStmt *stmt, bool &is_stmt_valid);
  DISALLOW_COPY_AND_ASSIGN(ObTransformOrExpansion);
private:
  int64_t try_times_;
};

} /* namespace sql */
} /* namespace oceanbase */

#endif /* OB_TRANSFORM_OR_EXPANSION_H_ */
