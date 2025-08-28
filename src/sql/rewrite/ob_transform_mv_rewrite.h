/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TRANSFORM_MV_REWRITE_H
#define _OB_TRANSFORM_MV_REWRITE_H

#include "sql/rewrite/ob_transform_rule.h"
#include "sql/rewrite/ob_stmt_comparer.h"
#include "sql/optimizer/ob_conflict_detector.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "objit/common/ob_item_type.h"


namespace oceanbase
{

namespace common
{
class ObIAllocator;
template <typename T>
class ObIArray;
}//common

namespace sql
{

struct PtrKey
{
  PtrKey () : ptr_(NULL) {}
  PtrKey (const void *ptr) : ptr_(ptr) {}
  const void *ptr_;
  uint64_t hash() const
  {
    return murmurhash(&ptr_, sizeof(ptr_), 0);
  }
  int hash(uint64_t &res) const
  {
    res = hash();
    return OB_SUCCESS;
  }
  bool operator==(const PtrKey &other) const
  {
    return (other.ptr_ == this->ptr_);
  }
};

/**
 * @brief ObTransformMVRewrite
 * 
 * Rewrite query with materialized view(s)
 * 
 * e.g.
 * create materialized view mv as
 * select * from t_info join t_item on t_info.item_id = t_item.item_id;
 *
 * select * from t_info join t_item on t_info.item_id = t_item.item_id where user_id = 'xxx';
 * rewrite ==>
 * select * from mv where user_id = 'xxx';
 */
class ObTransformMVRewrite : public ObTransformRule
{
public:
  ObTransformMVRewrite(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::PRE_ORDER, T_MV_REWRITE),
      parent_stmts_(NULL) {}
  virtual int transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                 ObDMLStmt *&stmt,
                                 bool &trans_happened) override;
  virtual int construct_transform_hint(ObDMLStmt &stmt, void *trans_params) override;

protected:

private:
  struct JoinTreeNode {
    JoinTreeNode() : table_set_(),
                     table_id_(common::OB_INVALID_ID),
                     ori_table_(NULL),
                     left_child_(NULL),
                     right_child_(NULL),
                     join_info_() {}
    TO_STRING_KV(
      K_(table_set),
      K_(table_id),
      KPC_(ori_table),
      KPC_(left_child),
      KPC_(right_child),
      K_(join_info)
    );
    ObRelIds table_set_;         // all tables included in this tree
    uint64_t table_id_;          // only for leaf node, base table id
    const TableItem* ori_table_; // the original table item, only for mv join tree
    JoinTreeNode *left_child_;   // left child tree
    JoinTreeNode *right_child_;  // right child tree
    JoinInfo join_info_;         // if tree node is leaf, join_info_.join_type_ is UNKNOWN_JOIN, and filters are stored in join_info_.where_conditions_
  };

  struct MvRewriteHelper {
    MvRewriteHelper (ObSelectStmt &ori_stmt,
                     MvInfo &mv_info,
                     ObSelectStmt *query_stmt,
                     ObIArray<int64_t> &base_table_map)
      : ori_stmt_(ori_stmt),
        mv_info_(mv_info),
        query_stmt_(query_stmt),
        base_table_map_(base_table_map) {}
    ~MvRewriteHelper();

    ObSelectStmt &ori_stmt_;
    MvInfo &mv_info_;
    // the following is used to do rewrite checks
    ObSelectStmt *query_stmt_;                          // the deep copy of ori_stmt_, and the rewrite will be done in this stmt
    ObIArray<int64_t> &base_table_map_;                 // ori_stmt table -> mv table  e.g. base_table_map_[0] = 1 means ori_stmt table 0 is map to mv table 1
    ObRelIds mv_delta_table_;                           // rel_ids of tables that only appear in mv
    ObRelIds query_delta_table_;                        // rel_ids of tables that only appear in query
    JoinTreeNode  *mv_tree_;                            // mv join tree
    JoinTreeNode  *query_tree_;                         // origin query join tree
    JoinTreeNode  *query_tree_mv_part_;                 // mv part of origin query join tree
    ObSEArray<ObRawExpr*, 16> mv_conds_;                // where/on conditions pulled up from mv join tree
    ObSEArray<ObRawExpr*, 16> query_conds_;             // where/on conditions pulled up from query join tree (only mv part)
    ObSEArray<ObRawExpr*, 16> query_other_conds_;       // where conditions which are not participating in join tree build/compare, will be added into rewrite stmt directly
    EqualSets mv_equal_sets_;                           // mv equal sets
    EqualSets query_equal_sets_;                        // origin query equal sets
    hash::ObHashMap<PtrKey, int64_t> mv_es_map_;        // mv equal sets map, expr -> equal set idx
    hash::ObHashMap<PtrKey, int64_t> query_es_map_;     // origin query equal sets map, expr -> equal set idx
    ObSEArray<ObSqlBitSet<>, 16> equal_sets_map_;       // map origin query equal set to mv equal sets
    // the following is used to generate rewrite stmt
    ObSEArray<ObRawExpr*, 4> mv_compensation_preds_;    // mv compensation predicates
    ObSEArray<ObRawExpr*, 4> query_compensation_preds_; // query compensation predicates, used to generate union rewrite
    bool need_group_by_roll_up_;                        // need group by roll up in rewrite stmt
    ObStmtExprReplacer query_expr_replacer_;            // replace query expr into mv select expr
    ObStmtExprReplacer mv_select_replacer_;             // replace mv select expr into mv table item column expr
    ObSelectStmt *mv_upper_stmt_;                       // the upper stmt which contain mv_item
    TableItem *mv_item_;                                // mv table item in rewrite stmt
    // the following is plan cache constraint info
    ObSEArray<ObPCParamEqualInfo, 4> equal_param_info_;
    ObSEArray<ObPCConstParamInfo, 4> const_param_info_;
    ObSEArray<ObExprConstraint, 4> expr_cons_info_;
  };

  struct MvRewriteCheckCtx : ObExprEqualCheckContext {
    MvRewriteCheckCtx(MvRewriteHelper &helper)
      : ObExprEqualCheckContext(),
        helper_(helper)
    {
      init_override_params();
    }

    MvRewriteCheckCtx(MvRewriteHelper &helper, bool can_use_equal_sets)
      : ObExprEqualCheckContext(),
        helper_(helper) 
    {
      init_override_params();
    }

    inline void init_override_params()
    {
      override_column_compare_ = true;
      override_const_compare_ = true;
      override_query_compare_ = true;
    }
    
    bool compare_column(const ObColumnRefRawExpr &inner, const ObColumnRefRawExpr &outer) override;
    bool compare_const(const ObConstRawExpr &left, const ObConstRawExpr &right) override;
    bool compare_query(const ObQueryRefRawExpr &first, const ObQueryRefRawExpr &second) override;
    
    int append_constraint_info(const MvRewriteCheckCtx &other);

    MvRewriteHelper &helper_;
    ObSEArray<ObPCParamEqualInfo, 4> equal_param_info_;
    ObSEArray<ObPCConstParamInfo, 4> const_param_info_;
    ObSEArray<ObExprConstraint, 4> expr_cons_info_;
  };

private:
  virtual int need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                             const int64_t current_level,
                             const ObDMLStmt &stmt,
                             bool &need_trans) override;
  int check_hint_valid(const ObDMLStmt &stmt,
                       bool &force_rewrite,
                       bool &force_no_rewrite);
  int check_basic_validity(const ObDMLStmt &stmt,
                           bool &is_valid);
  int try_transform_with_one_mv(ObSelectStmt *origin_stmt,
                                MvInfo &mv_info,
                                ObSelectStmt *&new_stmt,
                                bool &transform_happened);
  int gen_base_table_map(const ObIArray<TableItem*> &from_tables,
                         const ObIArray<TableItem*> &to_tables,
                         int64_t max_map_num,
                         ObIArray<ObSEArray<int64_t,4>> &table_maps);
  int inner_gen_base_table_map(int64_t from_rel_id,
                               const ObIArray<TableItem*> &from_tables,
                               const ObIArray<TableItem*> &to_tables,
                               hash::ObHashMap<uint64_t, int64_t> &from_table_num,
                               const ObIArray<ObSEArray<int64_t,4>> &to_table_ids,
                               const hash::ObHashMap<uint64_t, int64_t> &to_table_map,
                               ObSqlBitSet<> &used_to_table,
                               int64_t max_map_num,
                               ObIArray<int64_t> &current_map,
                               ObIArray<ObSEArray<int64_t,4>> &table_maps);
  int try_transform_contain_mode(ObSelectStmt *origin_stmt,
                                 MvInfo &mv_info,
                                 ObSelectStmt *&new_stmt,
                                 bool &transform_happened);
  int check_mv_rewrite_validity(MvRewriteHelper &helper,
                                bool &is_valid);
  int check_delta_table(MvRewriteHelper &helper,
                        bool &is_valid);
  int check_join_compatibility(MvRewriteHelper &helper,
                               bool &is_valid);
  int pre_process_quals(const ObIArray<ObRawExpr*> &input_conds,
                        ObIArray<ObRawExpr*> &normal_conds,
                        ObIArray<ObRawExpr*> &other_conds);
  int build_mv_join_tree(MvRewriteHelper &helper,
                         ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                         ObIArray<ObConflictDetector*> &conflict_detectors);
  int inner_build_mv_join_tree(MvRewriteHelper &helper,
                               const TableItem *table,
                               ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                               ObIArray<ObConflictDetector*> &conflict_detectors,
                               ObIArray<ObConflictDetector*> &used_detectors,
                               JoinTreeNode *&node);
  int build_query_join_tree(MvRewriteHelper &helper,
                            ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                            ObIArray<ObConflictDetector*> &conflict_detectors,
                            bool &is_valid);
  int inner_build_query_tree_mv_part(MvRewriteHelper &helper,
                                     JoinTreeNode *mv_node,
                                     ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                                     ObIArray<ObConflictDetector*> &conflict_detectors,
                                     ObIArray<ObConflictDetector*> &used_detectors,
                                     JoinTreeNode *&node,
                                     bool &is_valid);
  int inner_build_query_tree_delta_part(MvRewriteHelper &helper,
                                        const TableItem *table,
                                        ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                                        ObIArray<ObConflictDetector*> &conflict_detectors,
                                        ObIArray<ObConflictDetector*> &used_detectors,
                                        JoinTreeNode *&node,
                                        bool &is_delta_table,
                                        bool &is_valid);
  int build_join_tree_node(JoinTreeNode *left_node,
                           JoinTreeNode *right_node,
                           ObIArray<ObConflictDetector*> &conflict_detectors,
                           ObIArray<ObConflictDetector*> &used_detectors,
                           JoinTreeNode *&new_node,
                           bool &is_valid);
  int build_leaf_tree_node(int64_t rel_id,
                           uint64_t table_id,
                           ObIArray<ObSEArray<ObRawExpr*,4>> &baserel_filters,
                           JoinTreeNode *&leaf_node);
  int compare_join_tree(MvRewriteHelper &helper,
                        JoinTreeNode *mv_node,
                        JoinTreeNode *query_node,
                        bool can_compensate,
                        bool &is_valid);
  int compare_join_conds(MvRewriteHelper &helper,
                         const ObIArray<ObRawExpr*> &mv_conds,
                         const ObIArray<ObRawExpr*> &query_conds,
                         bool &is_valid);
  int compare_join_type(MvRewriteHelper &helper,
                        JoinTreeNode *mv_node,
                        JoinTreeNode *query_node,
                        bool &is_valid);
  int add_not_null_compensate(MvRewriteHelper &helper,
                              JoinTreeNode *mv_upper_node,
                              bool comp_left_child,
                              bool &is_valid);
  int find_mv_not_null_expr(MvRewriteHelper &helper,
                            JoinTreeNode *mv_upper_node,
                            bool comp_left_child,
                            ObRawExpr *&not_null_expr);
  int check_predicate_compatibility(MvRewriteHelper &helper,
                                    bool &is_valid);
  int check_equal_predicate(MvRewriteHelper &helper,
                            const ObIArray<ObRawExpr*> &mv_conds,
                            const ObIArray<ObRawExpr*> &query_conds,
                            bool &is_valid);
  int build_equal_sets(const ObIArray<ObRawExpr*> &input_conds,
                       const ObIArray<ObRawExpr*> &all_columns,
                       EqualSets &equal_sets,
                       hash::ObHashMap<PtrKey, int64_t> &equal_set_map);
  int generate_equal_compensation_preds(MvRewriteHelper &helper,
                                        bool &is_valid);
  int map_to_mv_column(MvRewriteHelper &helper,
                       const ObColumnRefRawExpr *query_expr,
                       ObColumnRefRawExpr *&mv_expr);
  int check_mv_equal_set_used(MvRewriteHelper &helper,
                              ObIArray<int64_t> &mv_map_query_ids,
                              int64_t current_query_es_id);
  int check_other_predicate(MvRewriteHelper &helper,
                            const ObIArray<ObRawExpr*> &mv_conds,
                            const ObIArray<ObRawExpr*> &query_conds);
  int check_compensation_preds_validity(MvRewriteHelper &helper,
                                        bool &is_valid);
  int is_same_condition(MvRewriteHelper &helper,
                        const ObRawExpr *left,
                        const ObRawExpr *right,
                        bool &is_same);
  int append_constraint_info(MvRewriteHelper &helper,
                             const MvRewriteCheckCtx &context);
  int compare_expr(MvRewriteCheckCtx &context,
                   const ObRawExpr *left,
                   const ObRawExpr *right,
                   bool &is_same);
  int check_group_by_col(MvRewriteHelper &helper,
                         bool &is_valid);
  int check_opt_feat_ctrl(MvRewriteHelper &helper,
                          bool &is_valid);
  int compute_stmt_expr_map(MvRewriteHelper &helper,
                            bool &is_valid);
  int compute_join_expr_map(MvRewriteHelper &helper,
                            bool &is_valid);
  int compute_select_expr_map(MvRewriteHelper &helper,
                              bool &is_valid);
  int compute_group_by_expr_map(MvRewriteHelper &helper,
                                bool &is_valid);
  int compute_order_by_expr_map(MvRewriteHelper &helper,
                                bool &is_valid);
  int inner_compute_exprs_map(MvRewriteHelper &helper,
                              ObIArray<ObRawExpr*> &exprs,
                              bool &is_valid);
  int inner_compute_expr_map(MvRewriteHelper &helper,
                             ObRawExpr *query_expr,
                             MvRewriteCheckCtx &context,
                             bool &is_valid);
  int compute_agg_expr_map(MvRewriteHelper &helper,
                           ObAggFunRawExpr *query_expr,
                           MvRewriteCheckCtx &context,
                           bool &is_valid);
  int find_mv_select_expr(MvRewriteHelper &helper,
                          const ObRawExpr *query_expr,
                          MvRewriteCheckCtx &context,
                          ObRawExpr *&select_expr);
  int trans_split_sum_expr(MvRewriteHelper &helper,
                           ObAggFunRawExpr *query_expr,
                           MvRewriteCheckCtx &context,
                           ObRawExpr *&output_expr);
  int check_agg_roll_up_expr(ObRawExpr *agg_expr,
                             bool &is_valid);
  int wrap_agg_roll_up_expr(ObRawExpr *ori_expr,
                            ObRawExpr *&output_expr);
  int generate_rewrite_stmt_contain_mode(MvRewriteHelper &helper);
  int clear_mv_common_tables(MvRewriteHelper &helper);
  int create_mv_table_item(MvRewriteHelper &helper);
  int create_mv_column_item(MvRewriteHelper &helper);
  int adjust_rewrite_stmt(MvRewriteHelper &helper);
  int recursive_build_from_table(MvRewriteHelper &helper,
                                 JoinTreeNode *node,
                                 TableItem *&output_table,
                                 ObIArray<ObRawExpr*> &filters);
  int append_on_replace(MvRewriteHelper &helper,
                        ObIArray<ObRawExpr*> &dst,
                        const ObIArray<ObRawExpr*> &src);
  int adjust_aggr_winfun_expr(ObSelectStmt *rewrite_stmt);
  int adjust_mv_item(MvRewriteHelper &helper);
  int check_rewrite_expected(MvRewriteHelper &helper,
                             bool &is_expected);
  int check_condition_match_index(MvRewriteHelper &helper,
                                  bool &is_match_index);
  int add_param_constraint(MvRewriteHelper &helper);

  static int find_query_rel_id(MvRewriteHelper &helper, int64_t mv_relid);
  static int classify_predicates(const ObIArray<ObRawExpr*> &input_conds,
                                 ObIArray<ObRawExpr*> &equal_conds,
                                 ObIArray<ObRawExpr*> &other_conds);
  static bool is_equal_cond(ObRawExpr *expr);
  static bool is_range_cond(ObRawExpr *expr);

private:
  ObIArray<ObParentDMLStmt> *parent_stmts_;
  ObQueryCtx mv_temp_query_ctx_; // used for generating mv stmt
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransformMVRewrite);

};

} //namespace sql
} //namespace oceanbase
#endif