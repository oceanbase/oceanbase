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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/rewrite/ob_transform_or_expansion.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_log_set.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_log_join.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
const int64_t ObTransformOrExpansion::MAX_STMT_NUM_FOR_OR_EXPANSION = 10;
const int64_t ObTransformOrExpansion::MAX_TIMES_FOR_OR_EXPANSION = 5;

int ObTransformOrExpansion::transform_one_stmt(ObIArray<ObParentDMLStmt> &parent_stmts,
                                               ObDMLStmt *&stmt,
                                               bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_stmt_valid = false;
  if (OB_FAIL(check_stmt_valid_for_expansion(stmt, is_stmt_valid))) {
    LOG_WARN("failed to check stmt valid for expansion", K(ret));
  } else if (!is_stmt_valid) {
    /* do nothing */
  } else if (OB_FAIL(transform_in_joined_table(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to do or expansion in joined condition", K(ret));
  } else if (trans_happened) {
    /* do nothing */
  } else if (OB_FAIL(transform_in_semi_info(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to do or expansion in semi condition", K(ret));
  } else if (trans_happened) {
    /* do nothing */
  } else if (OB_FAIL(transform_in_where_conditon(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to do or expansion in where condition", K(ret));
  }

  if (OB_SUCC(ret) && trans_happened) {
    add_trans_type(ctx_->happened_cost_based_trans_,
                   OR_EXPANSION);
  }
  return ret;
}

int ObTransformOrExpansion::transform_one_stmt_with_outline(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                            ObDMLStmt *&stmt,
                                                            bool &trans_happened)
{
  int ret = OB_SUCCESS;
  const ObHint *hint = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt)|| OB_ISNULL(hint = get_hint(stmt->get_stmt_hint()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(stmt), K(hint));
  } else if (!static_cast<const ObOrExpandHint*>(hint)->is_explicit()) {
    LOG_TRACE("use_concat hint has no explicit condition in outline", K(ctx_->src_qb_name_),
                                                                      K(*hint));
  } else if (OB_FAIL(transform_one_stmt(parent_stmts, stmt, trans_happened))) {
    LOG_WARN("failed to transform one stmt for or expansion", K(ret));
  } else if (trans_happened) {
    ++ctx_->trans_list_loc_;
    LOG_TRACE("succeed to do or expansion with outline", K(ctx_->src_qb_name_));
  } else {
    LOG_TRACE("can not do or expansion with outline", K(ctx_->src_qb_name_));
  }
  return ret;
}

int ObTransformOrExpansion::need_transform(const common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                           const int64_t current_level,
                                           const ObDMLStmt &stmt,
                                           bool &need_trans)
{
  int ret = OB_SUCCESS;
  bool is_valid = true;
  bool contain_inner_table = false;
  const ObHint *my_hint = get_hint(stmt.get_stmt_hint());
  need_trans = false;
  if (OB_FAIL(ObTransformRule::need_transform(parent_stmts,
                                              current_level,
                                              stmt,
                                              need_trans))) {
    LOG_WARN("failed to check need transformation", K(ret));
  } else if (!need_trans) {
    // do nothing
  } else if (OB_FAIL(check_basic_validity(stmt, is_valid))) {
    LOG_WARN("failed to check basic validity", K(ret));
  } else if (!is_valid) {
    need_trans = false;
  } else if (my_hint != NULL && my_hint->is_enable_hint()) {
    need_trans = true;
  } else if (OB_FAIL(stmt.check_if_contain_inner_table(contain_inner_table))) {
    LOG_WARN("failed to check contain inner table", K(ret));
  } else if (contain_inner_table) {
    need_trans = false;
    OPT_TRACE("stmt contain inner table, will not expand or expr");
  }
  return ret;
}

int ObTransformOrExpansion::transform_in_where_conditon(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                        ObDMLStmt *&stmt,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  bool is_valid = false;
  ObSEArray<OrExpandInfo, 2> trans_infos;
  ObSEArray<ObRawExpr*, 4> expect_ordering;
  ObCostBasedRewriteCtx ctx;
  ObDMLStmt *upper_stmt = NULL;
  ObSelectStmt *spj_stmt = NULL;
  ObTryTransHelper try_trans_helper1;
  ObTryTransHelper try_trans_helper2;
  OPT_TRACE("try to expand where condition");
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (reached_max_times_for_or_expansion()) {
    /*do nothing*/
    OPT_TRACE("retry count reached max times:", try_times_);
  } else if (OB_FAIL(has_valid_condition(*stmt, ctx, stmt->get_condition_exprs(),
                                         is_valid, &expect_ordering))) {
    LOG_WARN("failed to check where condition", K(ret));
  } else if (!is_valid) {
    /* do nothing */
    OPT_TRACE("can not expand where condition");
  } else if (OB_FAIL(ctx_->add_src_hash_val(ObTransformerCtx::SRC_STR_OR_EXPANSION_WHERE))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else if (OB_FAIL(try_trans_helper1.fill_helper(stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else if (OB_FAIL(get_trans_view(stmt, upper_stmt, spj_stmt))) {
    LOG_WARN("failed to get spj stmt", K(ret));
  } else if (OB_ISNULL(upper_stmt) || OB_ISNULL(spj_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(upper_stmt), K(spj_stmt));
  } else if (OB_FAIL(convert_expect_ordering(stmt, spj_stmt, expect_ordering))) {
    LOG_WARN("failed to convert expect ordering", K(ret));
  } else if (OB_FAIL(gather_transform_infos(spj_stmt, ctx, spj_stmt->get_condition_exprs(),
                                            expect_ordering, NULL, trans_infos))) {
    LOG_WARN("failed to get conds trans infos", K(ret));
  } else if (OB_FAIL(try_trans_helper2.fill_helper(stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper after pre operate", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < trans_infos.count(); ++i) {
      ctx.or_expand_type_ = trans_infos.at(i).or_expand_type_;
      ctx.is_set_distinct_ = trans_infos.at(i).is_set_distinct_;
      ctx.is_valid_topk_ = trans_infos.at(i).is_valid_topk_;
      ctx.expand_exprs_.reuse();
      ObDMLStmt *trans_stmt = upper_stmt;
      ObSelectStmt *transformed_union_stmt = NULL;
      StmtUniqueKeyProvider unique_key_provider;
      try_trans_helper2.unique_key_provider_ = &unique_key_provider;
      if (reached_max_times_for_or_expansion()) {
        /*do nothing*/
        OPT_TRACE("retry count reached max times:", try_times_);
      } else if (OB_FAIL(transform_or_expansion(spj_stmt,
                                                OB_INVALID_ID,
                                                trans_infos.at(i).pos_,
                                                ctx,
                                                transformed_union_stmt,
                                                unique_key_provider))) {
        LOG_WARN("failed to do transformation", K(ret));
      } else if (OB_FAIL(merge_stmt(trans_stmt, spj_stmt, transformed_union_stmt))) {
        LOG_WARN("failed to merge stmt", K(ret));
      } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt,
                                          NULL != ctx.hint_, false,
                                          trans_happened, &ctx))) {
        LOG_WARN("failed to accept transform", K(ret));
      } else if (trans_happened && OB_FAIL(add_transform_hint(*trans_stmt, &ctx))) {
        LOG_WARN("failed to add transform hint", K(ret));
      } else if (!trans_happened && OB_FAIL(try_trans_helper2.recover(stmt->get_query_ctx()))) {
        LOG_WARN("failed to recover params", K(ret));
      } else {
        ++try_times_;
        LOG_TRACE("transform or expansion in where conds", K(trans_happened));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!trans_happened && OB_FAIL(try_trans_helper1.recover(stmt->get_query_ctx()))) {
      LOG_WARN("failed to recover params", K(ret));
    } else {
      ctx_->src_hash_val_.pop_back();
    }
  }
  return ret;
}

// 1. semi join semi condition try expand to union distinct
//    cannot expand to union all, for A = (1), B = (1, 1), (null, 1),
//      A semi join B on (A.c1 = B.c1 or A.c1 = B.c2) result is (1),
//      A semi join B on (A.c1 = B.c1) union all
//      A semi join B on (lnnvl(A.c1 = B.c1) and A.c1 = B.c2) result is (1), (1)
// 2. anti join semi condition try expand to intersect
int ObTransformOrExpansion::transform_in_semi_info(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                   ObDMLStmt *&stmt,
                                                   bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  int64_t begin_idx = OB_INVALID_INDEX;
  ObCostBasedRewriteCtx ctx;
  ObDMLStmt *upper_stmt = NULL;
  ObSelectStmt *spj_stmt = NULL;
  ObTryTransHelper try_trans_helper1;
  ObTryTransHelper try_trans_helper2;
  OPT_TRACE("try to expand semi condition");
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(ctx_));
  } else if (reached_max_times_for_or_expansion()) {
    /*do nothing*/
    OPT_TRACE("retry count reached max times:", try_times_);
  } else if (OB_FAIL(has_valid_semi_anti_cond(*stmt, ctx, begin_idx))) {
    LOG_WARN("failed to check has valid semi anti condition", K(ret));
  } else if (OB_INVALID_INDEX == begin_idx) {
    /*do nothing*/
    OPT_TRACE("no valid semi condition");
  } else if (OB_FAIL(ctx_->add_src_hash_val(ObTransformerCtx::SRC_STR_OR_EXPANSION_SEMI))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else if (OB_FAIL(try_trans_helper1.fill_helper(stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else if (OB_FAIL(get_trans_view(stmt, upper_stmt, spj_stmt))) {
    LOG_WARN("failed to get spj stmt", K(ret));
  } else if (OB_ISNULL(upper_stmt) || OB_ISNULL(spj_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(upper_stmt), K(spj_stmt));
  } else if (OB_FAIL(ObTransformUtils::check_stmt_unique(spj_stmt, ctx_->session_info_,
                                                         ctx_->schema_checker_, true /* strict */,
                                                         ctx.is_unique_))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  } else if (OB_FAIL(try_trans_helper2.fill_helper(stmt->get_query_ctx()))) {
    // after create_single_joined_table_stmt, may generate new stmt
    LOG_WARN("failed to fill try trans helper after pre operate", K(ret));
  } else {
    ctx.is_set_distinct_ = true;
    const int64_t N = spj_stmt->get_semi_info_size();
    SemiInfo *semi_info = NULL;
    for (int64_t idx = begin_idx; OB_SUCC(ret) && !trans_happened && idx < N; ++idx) {
      if (OB_ISNULL(semi_info = spj_stmt->get_semi_infos().at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(semi_info));
      } else {
        ObIArray<ObRawExpr*> &conds = semi_info->semi_conditions_;
        ObDMLStmt *trans_stmt = NULL;
        for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < conds.count(); ++i) {
          ctx.expand_exprs_.reuse();
          ObSelectStmt *transformed_union_stmt = NULL;
          trans_stmt = upper_stmt;
          StmtUniqueKeyProvider unique_key_provider;
          try_trans_helper2.unique_key_provider_ = &unique_key_provider;
          if (reached_max_times_for_or_expansion()) {
            /*do nothing*/
            OPT_TRACE("retry count reached max times:", try_times_);
          } else if (OB_FAIL(is_valid_semi_anti_cond(spj_stmt, ctx, conds.at(i), semi_info,
                                                     ctx.or_expand_type_))) {
            LOG_WARN("failed to check is valid semi anti cond", K(ret), K(*semi_info));
          } else if (INVALID_OR_EXPAND_TYPE == ctx.or_expand_type_) {
            /*do nothing*/
          } else if (OB_FAIL(transform_or_expansion(spj_stmt,
                                                    semi_info->semi_id_,
                                                    i,
                                                    ctx,
                                                    transformed_union_stmt,
                                                    unique_key_provider))) {
            LOG_WARN("failed to do transformation", K(ret));
          } else if (OB_FAIL(merge_stmt(trans_stmt, spj_stmt, transformed_union_stmt))) {
            LOG_WARN("failed to merge stmt", K(ret));
          } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt,
                                              NULL != ctx.hint_, false,
                                              trans_happened, &ctx))) {
            LOG_WARN("failed to accept transform", K(ret));
          } else if (trans_happened && OB_FAIL(add_transform_hint(*trans_stmt, &ctx))) {
            LOG_WARN("failed to add transform hint", K(ret));
          } else if (!trans_happened && OB_FAIL(try_trans_helper2.recover(stmt->get_query_ctx()))) {
            LOG_WARN("failed to recover params", K(ret));
          } else {
            ++try_times_;
            LOG_TRACE("transform or expansion in semi info", K(trans_happened), K(i), K(*semi_info));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!trans_happened && OB_FAIL(try_trans_helper1.recover(stmt->get_query_ctx()))) {
      LOG_WARN("failed to recover params", K(ret));
    } else {
      ctx_->src_hash_val_.pop_back();
    }
  }
  return ret;
}

int ObTransformOrExpansion::transform_in_joined_table(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                      ObDMLStmt *&stmt,
                                                      bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
   if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else { 
    ObIArray<JoinedTable*> &joined_table = stmt->get_joined_tables();
    for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < joined_table.count(); ++i) {
      ret = transform_in_joined_table(parent_stmts, stmt, joined_table.at(i), trans_happened);
    }
  }
  return ret;
}

int ObTransformOrExpansion::transform_in_joined_table(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                      ObDMLStmt *&stmt,
                                                      TableItem *table,
                                                      bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  JoinedTable *joined_table = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(table));
  } else if (!table->is_joined_table()) {
    /* do nothing */
  } else if (OB_FALSE_IT(joined_table = static_cast<JoinedTable*>(table))) {
  } else if (OB_FAIL(SMART_CALL(transform_in_joined_table(parent_stmts, stmt,
                                                          joined_table->left_table_,
                                                          trans_happened)))) {
    LOG_WARN("failed to transform left join for left table", K(ret));
  } else if (trans_happened) {
    /* do nothing */
  } else if (OB_FAIL(SMART_CALL(transform_in_joined_table(parent_stmts, stmt,
                                                          joined_table->right_table_,
                                                          trans_happened)))) {
    LOG_WARN("failed to transform left join for right table", K(ret));
  } else if (trans_happened) {
    /* do nothing */
  } else if (reached_max_times_for_or_expansion()) {
    /* do nothing */
    OPT_TRACE("retry count reached max times:", try_times_);
  } else if (joined_table->is_inner_join()) {
    OPT_TRACE("try", joined_table);
    ret = try_do_transform_inner_join(parent_stmts, stmt, joined_table, trans_happened);
  } else if (joined_table->is_left_join()) {
    ret = try_do_transform_left_join(parent_stmts, stmt, joined_table, trans_happened);
  } else if (joined_table->is_right_join()) {
    OPT_TRACE("try", joined_table);
    TableItem *l_child = joined_table->left_table_;
    joined_table->left_table_ = joined_table->right_table_;
    joined_table->right_table_ = l_child;
    joined_table->joined_type_ = LEFT_OUTER_JOIN;
    if (OB_FAIL(try_do_transform_left_join(parent_stmts, stmt, joined_table, trans_happened))) {
      LOG_WARN("failed to transform joined table", K(ret), K(*joined_table));
    } else if (!trans_happened) {
      l_child = joined_table->left_table_;
      joined_table->left_table_ = joined_table->right_table_;
      joined_table->right_table_ = l_child;
      joined_table->joined_type_ = RIGHT_OUTER_JOIN;
    }
  }
  return ret;
}

// try do or expansion for inner join:
// 1. check is valid then deep copy stmt
// 2. create view from inner join table
// 3. do or expansion in view
int ObTransformOrExpansion::try_do_transform_inner_join(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                        ObDMLStmt *&stmt,
                                                        JoinedTable *joined_table,
                                                        bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObDMLStmt *origin_trans_stmt = NULL;
  TableItem *view_table = NULL;
  ObSelectStmt *ref_query = NULL;
  bool is_valid = false;
  ObCostBasedRewriteCtx ctx;
  ObSEArray<OrExpandInfo, 2> trans_infos;
  ObSEArray<ObRawExpr*, 1> dummy_exprs;
  ObTryTransHelper try_trans_helper1;
  ObTryTransHelper try_trans_helper2;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(ctx_)
      || OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(joined_table), K(ctx_));
  } else if (OB_FAIL(has_valid_condition(*stmt, ctx, joined_table->get_join_conditions(),
                                         is_valid, NULL))) {
    LOG_WARN("failed to check has valid condition", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
    OPT_TRACE("can not expand join condition");
  } else if (OB_FAIL(ctx_->add_src_hash_val(ObTransformerCtx::SRC_STR_OR_EXPANSION_INNER_JOIN))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else if (OB_FAIL(try_trans_helper1.fill_helper(stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                      stmt, origin_trans_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(disable_pdml_for_upd_del_stmt(*origin_trans_stmt))) {
    LOG_WARN("failed to disable pdml for upd_del_stmt", K(ret));
  } else if (OB_FAIL(create_single_joined_table_stmt(origin_trans_stmt, joined_table->table_id_,
                                                     view_table, ref_query))) {
    LOG_WARN("failed to create view with table", K(ret));
  } else if (OB_ISNULL(ref_query)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected view table", K(ret), K(ref_query));
  } else if (OB_FAIL(ObTransformUtils::flatten_joined_table(ref_query))) {
    LOG_WARN("failed to flatten joined tbale", K(ret), K(ref_query));
  } else if (OB_FAIL(gather_transform_infos(ref_query, ctx, ref_query->get_condition_exprs(),
                                            dummy_exprs, NULL, trans_infos))) {
    LOG_WARN("failed to get conds trans infos", K(ret));
  } else if (OB_FAIL(try_trans_helper2.fill_helper(stmt->get_query_ctx()))) {
    // after create_single_joined_table_stmt, may generate new stmt
    LOG_WARN("failed to fill try trans helper after pre operate", K(ret));
  } else {
    ObIArray<ObRawExpr*> &conds = ref_query->get_condition_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < trans_infos.count(); ++i) {
      ctx.or_expand_type_ = trans_infos.at(i).or_expand_type_;
      ctx.is_set_distinct_ = trans_infos.at(i).is_set_distinct_;
      ctx.expand_exprs_.reuse();
      ObDMLStmt *trans_stmt = ref_query;
      ObSelectStmt *union_stmt = NULL;
      StmtUniqueKeyProvider unique_key_provider;
      try_trans_helper2.unique_key_provider_ = &unique_key_provider;
      if (reached_max_times_for_or_expansion()) {
        /*do nothing*/
        OPT_TRACE("retry count reached max times:", try_times_);
      } else if (OB_FAIL(transform_or_expansion(ref_query,
                                                OB_INVALID_ID,
                                                trans_infos.at(i).pos_,
                                                ctx,
                                                union_stmt,
                                                unique_key_provider))) {
        LOG_WARN("failed to do transformation", K(ret));
      } else if (OB_FAIL(merge_stmt(trans_stmt, ref_query, union_stmt))) {
        LOG_WARN("failed to merge stmt", K(ret));
      } else if (OB_FALSE_IT(NULL == view_table ? origin_trans_stmt = trans_stmt
                                                : view_table->ref_query_ = static_cast<ObSelectStmt*>(trans_stmt))) {
      } else if (OB_FAIL(accept_transform(parent_stmts, stmt, origin_trans_stmt,
                                          NULL != ctx.hint_, false,
                                          trans_happened, &ctx))) {
        LOG_WARN("failed to accept transform", K(ret));
      } else if (trans_happened && OB_FAIL(add_transform_hint(*trans_stmt, &ctx))) {
        LOG_WARN("failed to add transform hint", K(ret));
      } else if (!trans_happened && OB_FAIL(try_trans_helper2.recover(stmt->get_query_ctx()))) {
        LOG_WARN("failed to recover params", K(ret));
      } else {
        ++try_times_;
        LOG_TRACE("transform or expansion in inner join", K(trans_happened), K(i), K(*conds.at(i)));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!trans_happened && OB_FAIL(try_trans_helper1.recover(stmt->get_query_ctx()))) {
      LOG_WARN("failed to recover params", K(ret));
    } else {
      ctx_->src_hash_val_.pop_back();
    }
  }
  return ret;
}

// try do or expansion for left join:
// 1. check is valid then deep copy stmt
// 2. create view from left join table v1
// 3. do or expansion in view create set stmt v2
// 4. add win func in v1, add or condition in stmt
// select * from t1 left join t2 on t1.c1 = t2.c1 or t1.c2 = t2.c2;
// -->
// select * from (
//     select v.*, row_number() over (partition by pk1 order by pk2 nulls last) as rn from (
//         select t1.pk pk1, t2.pk pk2 from t1 left join t2 on t1.c1 = t2.c1
//         union all
//         select t1.pk , t2.pk  from t1 left join t2 on lnnvl(t1.c2 = t2.c1) and t1.c2 = t2.c2
//     ) v2
// ) v1 where rn = 1 or pk2 is not null;
int ObTransformOrExpansion::try_do_transform_left_join(ObIArray<ObParentDMLStmt> &parent_stmts,
                                                       ObDMLStmt *&stmt,
                                                       JoinedTable *joined_table,
                                                       bool &trans_happened)
{
  int ret = OB_SUCCESS;
  trans_happened = false;
  ObDMLStmt *origin_trans_stmt = NULL;
  TableItem *view_table = NULL;
  ObSelectStmt *ref_query = NULL;
  JoinedTable *cur_joined_table = NULL;
  int64_t origin_select_item_count = 0;
  TableItem *not_null_side_table = NULL;
  ObSqlBitSet<> left_unique_pos;
  ObSqlBitSet<> right_flag_pos;
  bool is_valid = false;
  ObCostBasedRewriteCtx ctx;
  ObSEArray<OrExpandInfo, 2> trans_infos;
  ObSEArray<ObRawExpr*, 1> dummy_exprs;
  ObTryTransHelper try_trans_helper1;
  ObTryTransHelper try_trans_helper2;
  int64_t flag_view_sel_count = 0;
  ObSelectStmt *orig_flag_stmt = NULL;
  StmtUniqueKeyProvider unique_key_provider1;
  try_trans_helper1.unique_key_provider_ = &unique_key_provider1;
  if (OB_ISNULL(stmt) || OB_ISNULL(joined_table) || OB_ISNULL(ctx_) ||
      OB_ISNULL(ctx_->stmt_factory_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(joined_table), K(ctx_));
  } else if (OB_FAIL(has_valid_condition(*stmt, ctx, joined_table->get_join_conditions(),
                                         is_valid, NULL))) {
    LOG_WARN("failed to check has valid condition", K(ret));
  } else if (!is_valid) {
    /* do nothing */
    OPT_TRACE("can not expand join condition");
  } else if (OB_FAIL(check_child_table_valid(joined_table, not_null_side_table))) {
    LOG_WARN("failed to check child table valid", K(ret));
  } else if (NULL == not_null_side_table) {
    /* do nothing */
  } else if (OB_FAIL(ctx_->add_src_hash_val(ObTransformerCtx::SRC_STR_OR_EXPANSION_OUTER_JOIN))) {
    LOG_WARN("failed to add src hash val", K(ret));
  } else if (OB_FAIL(try_trans_helper1.fill_helper(stmt->get_query_ctx()))) {
    LOG_WARN("failed to fill try trans helper", K(ret));
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_,
                                                      stmt, origin_trans_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_FAIL(disable_pdml_for_upd_del_stmt(*origin_trans_stmt))) {
    LOG_WARN("failed to disable pdml for upd_del_stmt", K(ret));
  } else if (OB_FAIL(create_single_joined_table_stmt(origin_trans_stmt, joined_table->table_id_,
                                                     view_table, ref_query))) {
    LOG_WARN("failed to create view with table", K(ret));
  } else if (OB_ISNULL(ref_query) || OB_UNLIKELY(1 != ref_query->get_joined_tables().count())
             || OB_ISNULL(cur_joined_table = ref_query->get_joined_tables().at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected view table", K(ret), K(ref_query), K(cur_joined_table));
  } else if (OB_FALSE_IT(origin_select_item_count = ref_query->get_select_item_size())) {
  } else if (OB_FAIL(gather_transform_infos(ref_query, ctx, cur_joined_table->get_join_conditions(),
                                            dummy_exprs, cur_joined_table, trans_infos))) {
    LOG_WARN("failed to get conds trans infos", K(ret));
  } else if (OB_FAIL(add_select_item_to_ref_query(ref_query, not_null_side_table->table_id_,
                                                  unique_key_provider1,
                                                  left_unique_pos, right_flag_pos,
                                                  flag_view_sel_count, orig_flag_stmt))) {
    LOG_WARN("failed to set stmt unique", K(ret));
  } else if (OB_FAIL(try_trans_helper2.fill_helper(stmt->get_query_ctx()))) {
    // after create_single_joined_table_stmt, may generate new stmt
    LOG_WARN("failed to fill try trans helper after pre operate", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !trans_happened && i < trans_infos.count(); ++i) {
      ctx.or_expand_type_ = trans_infos.at(i).or_expand_type_;
      ctx.is_set_distinct_ = trans_infos.at(i).is_set_distinct_;
      ctx.expand_exprs_.reuse();
      ObSelectStmt *trans_ref_query = NULL;
      ObDMLStmt *trans_stmt = origin_trans_stmt;
      StmtUniqueKeyProvider unique_key_provider2;
      try_trans_helper2.unique_key_provider_ = &unique_key_provider2;
      if (reached_max_times_for_or_expansion()) {
        /*do nothing*/
        OPT_TRACE("retry count reached max times:", try_times_);
      } else if (OB_FAIL(transform_or_expansion(ref_query,
                                                joined_table->table_id_,
                                                trans_infos.at(i).pos_,
                                                ctx,
                                                trans_ref_query,
                                                unique_key_provider2))) {
        LOG_WARN("failed to do transformation", K(ret));
      } else if (OB_FAIL(do_transform_for_left_join(trans_ref_query, left_unique_pos,
                                                    right_flag_pos))) {
        LOG_WARN("failed to add win func and filter", K(ret));
      } else if (OB_FAIL(remove_stmt_select_item(trans_ref_query, origin_select_item_count))) {
        LOG_WARN("just stmt select item failed", K(ret));
      } else if (OB_FALSE_IT(NULL == view_table ? trans_stmt = trans_ref_query
                                                : view_table->ref_query_ = trans_ref_query)) {
      } else if (OB_FAIL(accept_transform(parent_stmts, stmt, trans_stmt,
                                          NULL != ctx.hint_, false,
                                          trans_happened, &ctx))) {
        LOG_WARN("failed to accept transform", K(ret));
      } else if (trans_happened && OB_FAIL(add_transform_hint(*trans_stmt, &ctx))) {
        LOG_WARN("failed to add transform hint", K(ret));
      } else if (!trans_happened && OB_FAIL(try_trans_helper2.recover(stmt->get_query_ctx()))) {
        LOG_WARN("failed to recover params", K(ret));
      } else {
        ++try_times_;
        LOG_TRACE("transform or expansion in left join", K(trans_happened), K(i), K(*joined_table));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!trans_happened && OB_FAIL(recover_flag_temp_table(ref_query,
                                                                  not_null_side_table->table_id_,
                                                                  flag_view_sel_count,
                                                                  orig_flag_stmt))) {
      LOG_WARN("failed to recover flag temp table", K(ret));
    } else if (!trans_happened && OB_FAIL(try_trans_helper1.recover(stmt->get_query_ctx()))) {
      LOG_WARN("failed to recover params", K(ret));
    } else {
      ctx_->src_hash_val_.pop_back();
    }
  }
  return ret;
}

int ObTransformOrExpansion::get_joined_table_pushdown_conditions(const TableItem *cur_table,
                                                                 const ObDMLStmt *trans_stmt,
                                                                 ObIArray<ObRawExpr *> &pushdown_conds)
{
  int ret = OB_SUCCESS;
  ObSqlBitSet<> table_set;
  const JoinedTable *cur_join_table = NULL;
  const TableItem *left_table = NULL;
  const TableItem *right_table = NULL;
  bool left_on_null_side = false;
  bool right_on_null_side = false;
  if (OB_ISNULL(cur_table) || OB_ISNULL(trans_stmt) || OB_UNLIKELY(!cur_table->is_joined_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected", K(ret), KP(cur_table), K(cur_table->is_joined_table()));
  } else if (FALSE_IT(cur_join_table = static_cast<const JoinedTable*>(cur_table))) {
  } else if (!cur_join_table->is_left_join() && !cur_join_table->is_inner_join()) {
    /* do nothing */
  } else if (OB_ISNULL(left_table = cur_join_table->left_table_) ||
             OB_ISNULL(right_table = cur_join_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected", K(ret), KP(left_table), KP(right_table));
  } else {
    if (left_table->is_basic_table() || left_table->is_generated_table()) {
      if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(trans_stmt,
                                                         left_table->table_id_,
                                                         left_on_null_side))) {
        LOG_WARN("failed to check table on null side", K(ret));
      } else if (!left_on_null_side &&
                 OB_FAIL(trans_stmt->get_table_rel_ids(*left_table, table_set))) {
        LOG_WARN("failed to get table rel ids", K(ret));
      }
    }
    if (OB_SUCC(ret) && (right_table->is_basic_table() || right_table->is_generated_table())) {
      if (OB_FAIL(ObOptimizerUtil::is_table_on_null_side(trans_stmt,
                                                         right_table->table_id_,
                                                         right_on_null_side))) {
        LOG_WARN("failed to check table on null side", K(ret));
      } else if (!right_on_null_side &&
                 OB_FAIL(trans_stmt->get_table_rel_ids(*right_table, table_set))) {
        LOG_WARN("failed to get table rel ids", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_stmt->get_condition_exprs().count(); i++) {
      ObRawExpr *cond = NULL;
      if (OB_ISNULL(cond = trans_stmt->get_condition_exprs().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (!cond->get_relation_ids().is_subset(table_set) ||
                 cond->has_flag(CNT_SUB_QUERY)) {
        /* do not push down */
      } else if (OB_FAIL(pushdown_conds.push_back(cond))) {
        LOG_WARN("failed to push cond", K(ret));
      } else { /* do nothing */ }
    }
  }
  return ret;
}

int ObTransformOrExpansion::create_single_joined_table_stmt(ObDMLStmt *trans_stmt,
                                                            uint64_t joined_table_id,
                                                            TableItem *&view_table,
                                                            ObSelectStmt *&ref_query)
{
  int ret = OB_SUCCESS;
  TableItem *cur_table = NULL;
  ObSEArray<ObRawExpr *, 4> push_conditions;
  view_table = NULL;
  ref_query = NULL;
  if (OB_ISNULL(trans_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(trans_stmt));
  } else if (OB_FAIL(trans_stmt->get_general_table_by_id(joined_table_id, cur_table))) {
    LOG_WARN("failed to get table", K(ret));
  } else if (OB_ISNULL(cur_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected", K(ret), KP(cur_table));
  } else if (OB_FAIL(get_joined_table_pushdown_conditions(cur_table,
                                                        trans_stmt,
                                                        push_conditions))) {
    LOG_WARN("failed to get single joined table condititons", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::remove_item(trans_stmt->get_condition_exprs(),
                                                  push_conditions))) {
    LOG_WARN("failed to remove pushed conditions", K(ret));
  } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                               trans_stmt,
                                                               view_table,
                                                               cur_table))) {
    LOG_WARN("failed to create empty view table", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                          trans_stmt,
                                                          view_table,
                                                          cur_table,
                                                          &push_conditions))) {
    LOG_WARN("failed to create inline view", K(ret));
  } else if (OB_ISNULL(view_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("view table is null", K(ret), K(view_table));
  } else {
    ref_query = view_table->ref_query_;
  }
  return ret;
}

// if is valid, return a not null not_null_side_table
int ObTransformOrExpansion::check_child_table_valid(JoinedTable *cur_table,
                                                    TableItem *&not_null_side_table)
{
  int ret = OB_SUCCESS;
  not_null_side_table = NULL;
  TableItem *left_table = NULL;
  ObSelectStmt *child_stmt = NULL;
  bool can_set_unique = false;
  if (OB_ISNULL(cur_table) || OB_ISNULL(left_table = cur_table->left_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(cur_table), K(left_table));
  } else if (OB_FAIL(get_table_not_on_null_side(cur_table->right_table_, not_null_side_table))) {
    LOG_WARN("failed to get table not on null side", K(ret));
  } else if (NULL == not_null_side_table) {
    /* do nothing */
  } else if (left_table->is_basic_table()) {
    /* do nothing */
  } else if (!left_table->is_generated_table() && !left_table->is_temp_table()) {
    not_null_side_table = NULL;
  } else if (OB_ISNULL(child_stmt = left_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(child_stmt));
  } else if (child_stmt->is_set_stmt()) {
    //union all can not set unique
    not_null_side_table = child_stmt->is_set_distinct() ? not_null_side_table : NULL;
  } else if (child_stmt->is_hierarchical_query() || child_stmt->has_rollup()) {
    not_null_side_table = NULL;
  } else if (OB_FAIL(StmtUniqueKeyProvider::check_can_set_stmt_unique(child_stmt, can_set_unique))) {
    LOG_WARN("failed to check can set stmt unique", K(ret));
  } else if (!can_set_unique) {
    not_null_side_table = NULL;
  }
  return ret;
}

// get a basic/generate target_table in cur_table, target_table is not on null side.
int ObTransformOrExpansion::get_table_not_on_null_side(TableItem *cur_table,
                                                       TableItem *&target_table)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = static_cast<JoinedTable*>(cur_table);
  target_table = NULL;
  if (OB_ISNULL(cur_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (cur_table->is_basic_table() || cur_table->is_temp_table() || cur_table->is_generated_table()) {
    target_table = cur_table;
  } else if (!cur_table->is_joined_table()) {
    /* do nothing */
  } else if (OB_FALSE_IT(joined_table = static_cast<JoinedTable*>(cur_table))) {
  } else if (joined_table->is_left_join()) {
    ret = SMART_CALL(get_table_not_on_null_side(joined_table->left_table_, target_table));
  } else if (joined_table->is_right_join()) {
    ret = SMART_CALL(get_table_not_on_null_side(joined_table->right_table_, target_table));
  } else if (!joined_table->is_inner_join()) {
    /* do nothing */
  } else if (OB_FAIL(SMART_CALL(get_table_not_on_null_side(joined_table->left_table_,
                                                           target_table)))) {
    LOG_WARN("failed to get in right table", K(ret));
  } else if (NULL != target_table) {
    /* do nothing */
  } else if (OB_FAIL(SMART_CALL(get_table_not_on_null_side(joined_table->right_table_,
                                                           target_table)))) {
    LOG_WARN("failed to get in right table", K(ret));
  }
  return ret;
}

// add left table unique key, right table falg expr to stmt select
int ObTransformOrExpansion::add_select_item_to_ref_query(ObSelectStmt *stmt,
                                                         const uint64_t flag_table_id,
                                                         StmtUniqueKeyProvider &unique_key_provider,
                                                         ObSqlBitSet<> &left_unique_pos,
                                                         ObSqlBitSet<> &right_flag_pos,
                                                         int64_t &flag_view_sel_count,
                                                         ObSelectStmt *&orig_flag_stmt)
{
  int ret = OB_SUCCESS;
  JoinedTable *joined_table = NULL;
  TableItem *right_table = NULL;
  TableItem *flag_table = NULL;
  ObSqlBitSet<> right_tables;
  ObSEArray<ObRawExpr*, 4> left_unique_exprs;
  ObSEArray<ObRawExpr*, 4> right_flag_exprs;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(ctx_->session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(stmt), K(ctx_));
  } else if (OB_UNLIKELY(1 != stmt->get_from_item_size()) ||
             OB_ISNULL(joined_table = stmt->get_joined_table(stmt->get_from_item(0).table_id_)) ||
             OB_UNLIKELY(!joined_table->is_left_join()) ||
             OB_ISNULL(right_table = joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect ref query", K(ret), K(joined_table), K(right_table));
  } else if (OB_FAIL(stmt->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(stmt->get_table_rel_ids(*right_table, right_tables))) {
    LOG_WARN("faield to get table rel ids", K(ret), K(*right_table));
  } else if (OB_FAIL(unique_key_provider.generate_unique_key(ctx_, stmt, right_tables,
                                                             left_unique_exprs))) {
    LOG_WARN("faield to get table rel ids", K(ret), K(*right_table));
  } else if (OB_ISNULL(flag_table = stmt->get_table_item_by_id(flag_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("faield to get table item", K(ret), K(flag_table), K(flag_table_id));
  } else if (flag_table->is_generated_table() || flag_table->is_temp_table()) {
    // add const to generate table select
    ObSelectStmt *view_stmt = NULL;
    ObConstRawExpr *const_expr = NULL;
    ObSEArray<ObRawExpr*, 4> select_list;
    orig_flag_stmt = flag_table->ref_query_;
    if (OB_ISNULL(view_stmt = flag_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view_stmt is null", K(ret), K(view_stmt));
    } else if (OB_FALSE_IT(flag_view_sel_count = view_stmt->get_select_item_size())) {
    } else if (view_stmt->is_set_stmt() &&
               OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_, view_stmt, flag_table->ref_query_))) {
      LOG_WARN("failed to create stmt with generated table", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_, ObNumberType,
                                              number::ObNumber::get_positive_one(), const_expr))) {
      LOG_WARN("failed to build const expr", K(ret));
    } else if (OB_FAIL(const_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize const number expr", K(ret));
    } else if (OB_FAIL(select_list.push_back(const_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_columns_for_view(ctx_, *flag_table, stmt,
                                                                 select_list, right_flag_exprs))) {
      LOG_WARN("failed to create columns for view", K(ret));
    }
  } else if (OB_FAIL(ObTransformUtils::generate_unique_key_for_basic_table(ctx_, stmt, flag_table,
                                                                           right_flag_exprs))) {
    LOG_WARN("failed to generate unique key for basic table", K(ret));
  } else if (OB_UNLIKELY(right_flag_exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect exprs", K(ret), K(right_flag_exprs));
  } else {
    ObRawExpr *expr = NULL;
    ObRawExpr *target_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && NULL == target_expr && i < right_flag_exprs.count(); ++i) {
      if (OB_ISNULL(expr = right_flag_exprs.at(i)) || OB_UNLIKELY(!expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect expr", K(ret), K(expr));
      } else if (OB_HIDDEN_PK_INCREMENT_COLUMN_ID ==
                 static_cast<ObColumnRefRawExpr*>(expr)->get_column_id()) {
        target_expr = expr;
      }
    }
    if (OB_SUCC(ret)) {
      target_expr = NULL == target_expr ? right_flag_exprs.at(0) : target_expr;
      right_flag_exprs.reuse();
      ret = right_flag_exprs.push_back(target_expr);
    }
  }

  // add to select list and get pos
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(left_unique_exprs.empty() || right_flag_exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect exprs", K(ret), K(left_unique_exprs), K(right_flag_exprs));
  } else {
    int64_t idx = OB_INVALID_INDEX;
    if (ObOptimizerUtil::find_item(select_exprs, right_flag_exprs.at(0), &idx)) {
      ret = right_flag_pos.add_member(idx);
    } else if (OB_FAIL(right_flag_pos.add_member(stmt->get_select_item_size()))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_, right_flag_exprs.at(0),
                                                            stmt))) {
      LOG_WARN("failed to create select item", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < left_unique_exprs.count(); ++i) {
      if (ObOptimizerUtil::find_item(select_exprs, left_unique_exprs.at(i), &idx)) {
        ret = left_unique_pos.add_member(idx);
      } else if (OB_FAIL(left_unique_pos.add_member(stmt->get_select_item_size()))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                              left_unique_exprs.at(i), stmt))) {
        LOG_WARN("failed to create select item", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::recover_flag_temp_table(ObSelectStmt *stmt,
                                                    const uint64_t flag_table_id,
                                                    const int64_t orig_sel_count,
                                                    ObSelectStmt *orig_flag_stmt)
{
  int ret = OB_SUCCESS;
  TableItem *flag_table = NULL;
  ObSelectStmt *view_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(stmt));
  } else if (OB_ISNULL(flag_table = stmt->get_table_item_by_id(flag_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("faield to get table item", K(ret), K(flag_table), K(flag_table_id));
  } else if (!flag_table->is_temp_table()) {
    // do nothing
  } else if (OB_ISNULL(view_stmt = flag_table->ref_query_) || OB_ISNULL(orig_flag_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(view_stmt), K(orig_flag_stmt));
  } else if (view_stmt != orig_flag_stmt) {
    if (OB_UNLIKELY(!orig_flag_stmt->is_set_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected origin flag stmt", K(ret), KPC(orig_flag_stmt));
    } else if (OB_UNLIKELY(orig_flag_stmt->get_select_item_size() != orig_sel_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select item count mismatch", K(ret), K(orig_sel_count), KPC(orig_flag_stmt));
    } else {
      flag_table->ref_query_ = orig_flag_stmt;
    }
  } else if (OB_UNLIKELY(orig_sel_count <= 0
                         || orig_sel_count > view_stmt->get_select_item_size())) {
    ret= OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected original select item count", K(ret), K(orig_sel_count), KPC(view_stmt));
  } else {
    ObOptimizerUtil::revert_items(view_stmt->get_select_items(), orig_sel_count);
  }
  return ret;
}

// do transform after get a union stmt:
// 1. add win func level
// 2. add filter level
int ObTransformOrExpansion::do_transform_for_left_join(ObSelectStmt *&stmt,
                                                       ObSqlBitSet<> &left_unique_pos,
                                                       ObSqlBitSet<> &right_flag_pos)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *view_stmt = NULL;
  ObSelectStmt *win_func_stmt = NULL;
  ObSelectStmt *filter_stmt = NULL;
  ObSEArray<ObRawExpr*, 2> partition_exprs;
  ObSEArray<ObRawExpr*, 1> order_exprs;
  ObSEArray<ObRawExpr*, 1> flag_exprs;
  ObRawExpr *rn_expr = NULL;
  ObWinFunRawExpr *win_expr = NULL;
  SelectItem select_item;
  select_item.alias_name_ = "RN";
  select_item.expr_name_ = "RN";
  if (OB_ISNULL(view_stmt = stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(view_stmt));
  } else if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_, view_stmt,
                                                                        win_func_stmt))) {
    LOG_WARN("create stmt with generated_table failed", K(ret));
  } else if (OB_FAIL(convert_exprs_to_win_func_level(win_func_stmt, left_unique_pos,
                                                     right_flag_pos, partition_exprs,
                                                     order_exprs))) {
    LOG_WARN("failed to convert exprs to win func level", K(ret));
  } else if (OB_FAIL(create_row_number_window_function(partition_exprs, order_exprs, win_expr))) {
    LOG_WARN("Failed to add select item", K(select_item), K(ret));
  } else if (OB_FAIL(win_func_stmt->add_window_func_expr(win_expr))) {
    LOG_WARN("failed to add window func expr", K(ret));
  } else if (OB_FALSE_IT(select_item.expr_ = win_expr)) {
  } else if (OB_FAIL(win_func_stmt->add_select_item(select_item))) {
    LOG_WARN("Failed to add select item", K(select_item), K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_, win_func_stmt,
                                                                        filter_stmt))) {
    LOG_WARN("create stmt with generated_table failed", K(ret));
  } else if (OB_FAIL(convert_exprs_to_filter_level(filter_stmt, select_item.expr_,
                                                   order_exprs, rn_expr, flag_exprs))) {
    LOG_WARN("failed to convert exprs to filter level", K(ret));
  } else if (OB_FAIL(add_filter_to_stmt(filter_stmt, rn_expr, flag_exprs))) {
    LOG_WARN("failed to add filter to stmt", K(ret));
  } else {
    stmt = filter_stmt;
  }
  return ret;
}

int ObTransformOrExpansion::add_filter_to_stmt(ObSelectStmt *stmt,
                                               ObRawExpr *rn_expr,
                                               ObIArray<ObRawExpr*> &flag_exprs)
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *const_one = NULL;
  ObRawExpr *equal_expr = NULL;
  ObOpRawExpr *is_not_null = NULL;
  ObSEArray<ObRawExpr*, 2> conds;
  ObRawExpr *or_expr = NULL;
  if (OB_ISNULL(stmt) ||  OB_ISNULL(ctx_) ||  OB_ISNULL(ctx_->expr_factory_)
      || OB_ISNULL(rn_expr) || OB_UNLIKELY(1 != flag_exprs.count())
      || OB_ISNULL(flag_exprs.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected param", K(ret), K(stmt), K(rn_expr), K(flag_exprs));
  } else if (is_oracle_mode() && ObRawExprUtils::build_const_number_expr(*ctx_->expr_factory_,
                                  ObNumberType, number::ObNumber::get_positive_one(), const_one)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (!is_oracle_mode() && ObRawExprUtils::build_const_int_expr(*ctx_->expr_factory_,
                                                        ObIntType, 1, const_one)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_common_binary_op_expr(*ctx_->expr_factory_, T_OP_EQ,
                                                                 rn_expr, const_one,
                                                                 equal_expr))) {
    LOG_WARN("failed to build common binary op expr", K(ret));
  } else if (OB_FAIL(conds.push_back(equal_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, flag_exprs.at(0),
                                                       is_not_null))) {
    LOG_WARN("failed to add is not null", K(ret));
  } else if (OB_FAIL(conds.push_back(is_not_null))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_or_exprs(*ctx_->expr_factory_, conds, or_expr))) {
    LOG_WARN("make or expr failed", K(ret));
  } else if (OB_ISNULL(or_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("or expr is null", K(ret));
  } else if (OB_FAIL(or_expr->formalize(ctx_->session_info_))) {
    LOG_WARN("failed to formalize windown function", K(ret));
  } else if (OB_FAIL(or_expr->pull_relation_id())) {
    LOG_WARN("failed to pull relation id and levels", K(ret));
  } else if (OB_FAIL(stmt->get_condition_exprs().push_back(or_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::convert_exprs_to_filter_level(ObSelectStmt *stmt,
                                                          ObRawExpr *rn_expr,
                                                          ObIArray<ObRawExpr*> &flag_exprs,
                                                          ObRawExpr *&upper_rn_expr,
                                                          ObIArray<ObRawExpr*> &upper_flag_exprs)
{
  int ret = OB_SUCCESS;
  TableItem *table = NULL;
  ObSelectStmt *child_stmt = NULL;
  ObSEArray<ObRawExpr*, 4> select_exprs;
  upper_rn_expr = NULL;
  upper_flag_exprs.reuse();
  if (OB_ISNULL(stmt) || OB_UNLIKELY(!stmt->is_single_table_stmt()) ||
      OB_ISNULL(table = stmt->get_table_item(0)) ||
      OB_ISNULL(child_stmt = table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt", K(ret), K(stmt), K(table), K(child_stmt));
  } else if (OB_ISNULL(rn_expr) || OB_UNLIKELY(flag_exprs.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected imput expr", K(ret), K(rn_expr), K(flag_exprs));
  } else if (OB_FAIL(select_exprs.assign(flag_exprs))) {
    LOG_WARN("failed to assign expr", K(ret));
  } else if (OB_FAIL(select_exprs.push_back(rn_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if(OB_FAIL(ObTransformUtils::convert_select_expr_to_column_expr(select_exprs, *child_stmt,
                                                  *stmt, table->table_id_, upper_flag_exprs))) {
    LOG_WARN("failed to convert expr to column epxr", K(ret));
  } else if (OB_FALSE_IT(upper_rn_expr = upper_flag_exprs.at(flag_exprs.count()))) {
    LOG_WARN("failed to assign expr", K(ret));
  } else {
    upper_flag_exprs.pop_back();
  }
  return ret;
}

int ObTransformOrExpansion::convert_exprs_to_win_func_level(ObSelectStmt *stmt,
                                                            ObSqlBitSet<> &left_unique_pos,
                                                            ObSqlBitSet<> &right_flag_pos,
                                                            ObIArray<ObRawExpr*> &partition_exprs,
                                                            ObIArray<ObRawExpr*> &order_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(stmt));
  } else {
    ObIArray<SelectItem> &select_items = stmt->get_select_items();
    for (int64_t i = 0; OB_SUCC(ret) && i < select_items.count(); ++i) {
      if (left_unique_pos.has_member(i)) {
        ret = partition_exprs.push_back(select_items.at(i).expr_);
      } else if (right_flag_pos.has_member(i)) {
        ret = order_exprs.push_back(select_items.at(i).expr_);
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::create_row_number_window_function(ObIArray<ObRawExpr *> &partition_exprs,
                                                              ObIArray<ObRawExpr *> &order_exprs,
                                                              ObWinFunRawExpr *&win_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(ctx_), K(ctx_->expr_factory_));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_WINDOW_FUNCTION, win_expr))) {
    LOG_WARN("create window function expr failed", K(ret));
  } else if (OB_ISNULL(win_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(win_expr));
  } else if (OB_FAIL(win_expr->set_partition_exprs(partition_exprs))) {
    LOG_WARN("fail to set partition exprs", K(ret));
  } else {
    Bound upper;
    Bound lower;
    upper.type_ = BOUND_UNBOUNDED;
    lower.type_ = BOUND_UNBOUNDED;
    upper.is_preceding_ = true;
    lower.is_preceding_ = false;
    win_expr->set_func_type(T_WIN_FUN_ROW_NUMBER);
    win_expr->set_window_type(WINDOW_RANGE);
    win_expr->set_is_between(true);
    win_expr->set_upper(upper);
    win_expr->set_lower(lower);
    for (int64_t i = 0; OB_SUCC(ret) && i < order_exprs.count(); ++i) {
      OrderItem sort_key(order_exprs.at(i), NULLS_LAST_ASC);
      if (OB_FAIL(win_expr->get_order_items().push_back(sort_key))) {
        LOG_WARN("failed to add sort key", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(win_expr->formalize(ctx_->session_info_))) {
      LOG_WARN("failed to formalize windown function", K(ret));
    } else if (OB_FAIL(win_expr->pull_relation_id())) {
      LOG_WARN("failed to pull relation id and levels", K(ret));
    }
  }
  return ret;
}

int ObTransformOrExpansion::adjust_transform_types(uint64_t &transform_types)
{
  int ret = OB_SUCCESS;
  if (cost_based_trans_tried_) {
    transform_types &= (~(1 << transformer_type_));
  }
  return ret;
}

int ObTransformOrExpansion::has_odd_function(const ObDMLStmt &stmt, bool &has)
{
  int ret = OB_SUCCESS;
  has = false;
  if (stmt.is_select_stmt()) {
    const ObSelectStmt &select_stmt = static_cast<const ObSelectStmt &>(stmt);
    has =  (select_stmt.is_contains_assignment()
            || NULL != select_stmt.get_select_into()
            || !select_stmt.get_cte_exprs().empty()
            || !select_stmt.get_cycle_items().empty()
            || !select_stmt.get_search_by_items().empty());
  }
  if (OB_SUCC(ret) && !has) {
    ObSEArray<ObSelectStmt *, 4> child_stmts;
    if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmt", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !has && i < child_stmts.count(); ++i) {
      if (OB_ISNULL(child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("child stmt is null", K(ret));
      } else if (OB_FAIL(SMART_CALL(has_odd_function(*child_stmts.at(i), has)))) {
        LOG_WARN("failed to check has odd function", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::check_basic_validity(const ObDMLStmt &stmt, bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool has_odd_func = false;
  bool check_status = true;
  is_valid = false;
  if (reached_max_times_for_or_expansion()) {
    is_valid = false;
    OPT_TRACE("retry count reached max times:", try_times_);
    LOG_TRACE("reached max times for or expansion.", K(is_valid));
  } else if (stmt.is_set_stmt()
             || stmt.get_from_item_size() == 0
             || !stmt.is_sel_del_upd()) {
    // do nothing
  } else if ((stmt.is_update_stmt() || stmt.is_delete_stmt()) &&
             OB_FAIL(check_upd_del_stmt_validity(static_cast<const ObDelUpdStmt&>(stmt),
                                                 check_status))) {
    LOG_WARN("failed to check upd del stmt validity", K(ret));
  } else if (!check_status) {
    /*do nothing */
  } else if (OB_FAIL(has_odd_function(stmt, has_odd_func))) {
    LOG_WARN("failed to check has odd function", K(ret));
} else if (has_odd_func) {
    /*do nothing */
    OPT_TRACE("stmt has odd function, will not expand or expr");
  } else {
    is_valid = true;
  }
  return ret;
}

// pre-check conditions
int ObTransformOrExpansion::has_valid_condition(ObDMLStmt &stmt,
                                                ObCostBasedRewriteCtx &ctx,
                                                const ObIArray<ObRawExpr*> &conds,
                                                bool &has_valid,
                                                ObIArray<ObRawExpr*> *expect_ordering)
{
  int ret = OB_SUCCESS;
  has_valid = false;
  if (!conds.empty()) {
    ctx.hint_ = static_cast<const ObOrExpandHint*>(get_hint(stmt.get_stmt_hint()));
    bool is_topk = false;
    bool can_set_distinct = false;
    OPT_TRACE("check conditions:", conds);
    if (NULL == expect_ordering) {
      /* do nothing */
    } else if (OB_FAIL(StmtUniqueKeyProvider::check_can_set_stmt_unique(&stmt, can_set_distinct))) {
      LOG_WARN("failed to check can set stmt unique", K(ret));
    } else if (!can_set_distinct) {
      /* do nothing */
      OPT_TRACE("stmt can not set unique");
    } else if (OB_FAIL(get_expect_ordering(stmt, *expect_ordering))) {
      LOG_WARN("failed to get expected ordering", K(ret));
    } else if (!expect_ordering->empty()) {
      is_topk = true;
    }

    bool using_same_cols = false;
    ObSEArray<ObRawExpr*, 4> common_cols;
    for (int64_t i = 0; OB_SUCC(ret) && !has_valid && i < conds.count(); ++i) {
      if (OB_FAIL(check_condition_valid_basic(&stmt, ctx, conds.at(i), is_topk,
                                              has_valid, common_cols, using_same_cols))) {
        LOG_WARN("failed to check condition valid basic", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::get_expect_ordering(const ObDMLStmt &stmt,
                                                ObIArray<ObRawExpr*> &expect_ordering)
{
  int ret = OB_SUCCESS;
  expect_ordering.reuse();
  EqualSets &equal_sets = ctx_->equal_sets_;
  ObArenaAllocator alloc;
  ObSEArray<ObRawExpr *, 4> const_exprs;
  const ObSelectStmt *sel_stmt = stmt.is_select_stmt()
                                 ? static_cast<const ObSelectStmt*>(&stmt) : NULL;
  if (NULL != sel_stmt && (sel_stmt->has_distinct() || sel_stmt->has_group_by()
                           || sel_stmt->has_window_function())) {
    /* do nothing */
  } else if (!stmt.has_order_by() || !stmt.has_limit()) {
    /* do nothing */
  } else if (OB_FAIL(const_cast<ObDMLStmt&>(stmt).get_stmt_equal_sets(equal_sets, alloc, true, true))) {
    LOG_WARN("failed to get stmt equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(stmt.get_condition_exprs(),
                                                          const_exprs))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  } else {
    const ObIArray<OrderItem> &order_items = stmt.get_order_items();
    bool is_valid = true;
    bool is_lossless = false;
    ObRawExpr *expr = NULL;
    bool is_const = false;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < order_items.count(); ++i) {
      if (OB_ISNULL(expr = order_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(expr, equal_sets, const_exprs, is_const))) {
        LOG_WARN("failed to check is_const_expr", K(ret));
      } else if (is_const) {
        /* do nothing */
      } else if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(expr, is_lossless))) {
        LOG_WARN("failed to check is losskess column cast", K(ret));
      } else if (is_lossless && OB_ISNULL(expr = expr->get_param_expr(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null");
      } else if (!expr->is_column_ref_expr()) {
        is_valid = false;
      } else if (OB_FAIL(expect_ordering.push_back(expr))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  equal_sets.reuse();
  return ret;
}

// expect_ordering contains column exprs from orig_stmt,
// this function convert them to column exprs from spj_stmt.
int ObTransformOrExpansion::convert_expect_ordering(ObDMLStmt *orig_stmt,
                                                    ObDMLStmt *spj_stmt,
                                                    ObIArray<ObRawExpr*> &expect_ordering)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(orig_stmt) || OB_ISNULL(spj_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (expect_ordering.empty() || orig_stmt == spj_stmt) {
    /* do nothing */
  } else {
    ObSEArray<ObRawExpr*, 4> spj_expect_ordering;
    ObColumnRefRawExpr *col = NULL;
    ObColumnRefRawExpr *spj_col = NULL;
    TableItem *table = NULL;
    ObRawExpr *expr = NULL;
    int64_t idx = -1;
    const int64_t table_size = spj_stmt->get_table_size();
    for (int64_t i = 0; OB_SUCC(ret) && i < expect_ordering.count(); ++i) {
      if (OB_ISNULL(expr = expect_ordering.at(i)) || !expr->is_column_ref_expr()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", K(ret), K(expr));
      } else if (OB_FALSE_IT(col = static_cast<ObColumnRefRawExpr*>(expr))) {
      } else if (OB_ISNULL(spj_col = spj_stmt->get_column_expr_by_id(col->get_table_id(),
                                                                     col->get_column_id()))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get column expr by id", K(ret), K(col->get_table_id()), K(col->get_column_id()));
      } else if (OB_FAIL(spj_expect_ordering.push_back(spj_col))) {
        LOG_WARN("failed to push back exprs", K(ret), K(spj_col));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(expect_ordering.assign(spj_expect_ordering))) {
      LOG_WARN("failed to assign exprs", K(ret), K(spj_expect_ordering.count()));
    }
  }
  return ret;
}

//or-expansion打开dml只能针对单表的更新、删除；对于多表的更新、删除目前不能打开，因为一个目前实现的方式一个视图
//只能设置一个base table item，无法处理多表的情况, 如：delete t1,t2 from t1,t2;
int ObTransformOrExpansion::check_upd_del_stmt_validity(const ObDelUpdStmt &stmt,
                                                        bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  ObQueryCtx *query_ctx = stmt.get_query_ctx();
  ObSEArray<ObDmlTableInfo*, 2> table_infos;
  if (OB_FAIL(const_cast<ObDelUpdStmt&>(stmt).get_dml_table_infos(table_infos))) {
    LOG_WARN("failed to get dml table infos", K(ret));
  } else if (1 != table_infos.count()) {
    is_valid = false;
    OPT_TRACE("multi dml table not support or expansion");
  }
  return ret;
}

// for update/delete stmt, disable pdml after or expansion
int ObTransformOrExpansion::disable_pdml_for_upd_del_stmt(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (stmt.is_update_stmt() || stmt.is_delete_stmt()) {
    ObSEArray<ObDmlTableInfo*, 2> table_infos;
    if (OB_FAIL(static_cast<ObDelUpdStmt&>(stmt).get_dml_table_infos(table_infos))) {
      LOG_WARN("failed to get dml table infos", K(ret));
    } else if (OB_UNLIKELY(table_infos.empty()) || OB_ISNULL(table_infos.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param", K(ret));
    } else if (NULL != stmt.get_part_expr(table_infos.at(0)->loc_table_id_,
                                          table_infos.at(0)->ref_table_id_)) {
      OPT_TRACE("disable parallel dml after or expansion");
      static_cast<ObDelUpdStmt&>(stmt).set_pdml_disabled();
    }
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::create_spj_view
 * 将 stmt 分解成两层：
 * 内层做 table scan, join 和 filter，构成一次 SPJ 查询
 * 外层做 distinct, group-by, order-by, window function 等非 SPJ 的操作
 */
int ObTransformOrExpansion::get_trans_view(ObDMLStmt *stmt,
                                           ObDMLStmt *&upper_stmt,
                                           ObSelectStmt *&child_stmt)
{
  int ret = OB_SUCCESS;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  child_stmt = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(stmt_factory = ctx_->stmt_factory_)
      || OB_ISNULL(expr_factory = ctx_->expr_factory_)
      || OB_UNLIKELY(!stmt->is_sel_del_upd())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("have invalid params", K(ret), K(ctx_), K(stmt), K(stmt_factory),
             K(expr_factory), K(stmt->is_sel_del_upd()));
  } else if (stmt->is_select_stmt() && static_cast<ObSelectStmt*>(stmt)->is_spj()) {
    upper_stmt = stmt;
    child_stmt = static_cast<ObSelectStmt*>(stmt);
  } else if (OB_FAIL(ObTransformUtils::deep_copy_stmt(
                       *stmt_factory, *expr_factory, stmt, upper_stmt))) {
    LOG_WARN("failed to deep copy stmt", K(ret));
  } else if (OB_ISNULL(upper_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(upper_stmt));
  } else if (OB_FAIL(disable_pdml_for_upd_del_stmt(*upper_stmt))) {
    LOG_WARN("failed to disable pdml for upd_del_stmt", K(ret));
  } else if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, upper_stmt, child_stmt, false))) {
    LOG_WARN("failed to create simple view", K(ret));
  } else if (OB_FAIL(upper_stmt->formalize_stmt_expr_reference(expr_factory, ctx_->session_info_))) {
    LOG_WARN("failed to formalize stmt expr reference", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::merge_stmt(ObDMLStmt *&upper_stmt,
                                       ObSelectStmt *input_stmt,
                                       ObSelectStmt *union_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(upper_stmt) || OB_ISNULL(union_stmt) || OB_ISNULL(input_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(upper_stmt), K(union_stmt), K(input_stmt));
  } else if (upper_stmt == input_stmt) {
    int64_t origin_select_item_count = input_stmt->get_select_item_size();
    if (origin_select_item_count == union_stmt->get_select_item_size()) {
      upper_stmt = union_stmt;
    } else {
      ObSelectStmt *temp_stmt = NULL;
      if (OB_FAIL(ObTransformUtils::create_stmt_with_generated_table(ctx_,
                                                                     union_stmt,
                                                                     temp_stmt))) {
        LOG_WARN("create stmt with generated_table failed", K(ret));
      } else if (OB_FAIL(remove_stmt_select_item(temp_stmt, origin_select_item_count))) {
        LOG_WARN("just stmt select item failed", K(ret));
      } else {
        upper_stmt = temp_stmt;
      }
    }
  } else if (OB_UNLIKELY(upper_stmt->get_table_size() != 1) ||
             OB_ISNULL(upper_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid upper stmt", K(ret));
  } else {
    upper_stmt->get_table_item(0)->ref_query_ = union_stmt;
  }
  return ret;
}

int ObTransformOrExpansion::remove_stmt_select_item(ObSelectStmt *select_stmt,
                                                    int64_t select_item_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || select_item_count < 0
      || select_stmt->get_select_item_size() < select_item_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(select_stmt), K(select_item_count),
                                 K(select_stmt->get_select_item_size()), K(ret));
  } else {
    int64_t i_select_item = select_stmt->get_select_item_size();
    do {
       if (OB_FAIL(select_stmt->get_select_items().remove(i_select_item - 1))) {
        LOG_WARN("remove select item failed", K(ret));
       } else {
         -- i_select_item;
       }
    } while (OB_SUCC(ret) && i_select_item != select_item_count);
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::check_condition_on_same_columns
 * 检查 or 条件的参数是不是同构的：
 * 1. 使用相同的列集合
 * 2. 所有的列都是关联同一张表的
 * e.g. select * from t where t.a = 1 or t.a = 2;
 *      select * from t where (1 < t.a and t.a < 2) or (5 < t.a and t.a < 6);
 *      select * from t where (t.a = 1 and t.b = 2) or (t.a = 10 and t.b = 20);
 * 以下情况认为 or 条件的参数不同构
 * e.g. select * from t where t.a = 1 or t.b = 2;
 *      select * from t, s where t.a = s.a or t.b = s.b;
 *  第二个SQL进行 or expansion 后，我们可以将 or 中的等值条件下降为等值连接条件。
 */
int ObTransformOrExpansion::check_condition_on_same_columns(const ObDMLStmt &stmt,
                                                            const ObRawExpr &expr,
                                                            bool &using_same_cols)
{
  int ret = OB_SUCCESS;
  using_same_cols = false;
  if (T_OP_OR == expr.get_expr_type()) {
    int64_t table_id = OB_INVALID_ID;
    ColumnBitSet column_bit_set;
    using_same_cols = true;
    for (int64_t i = 0; OB_SUCC(ret) && using_same_cols &&
         i < expr.get_param_count(); ++i) {
      bool from_same_table = true;
      ColumnBitSet tmp;
      if (OB_FAIL(extract_columns(expr.get_param_expr(i),  table_id, from_same_table, tmp))) {
        LOG_WARN("failed to extract columns info", K(ret));
      } else if (!from_same_table) {
        using_same_cols = false;
      } else if (0 == i) {
        column_bit_set.add_members(tmp);
        using_same_cols = column_bit_set.bit_count() > 0;
      } else if (!column_bit_set.equal(tmp)) {
        using_same_cols = false;
      }
    }
  } else if (T_OP_IN == expr.get_expr_type() &&
             OB_NOT_NULL(expr.get_param_expr(1)) &&
             T_OP_ROW == expr.get_param_expr(1)->get_expr_type()) {
    using_same_cols = true;
  }
  return ret;
}

int ObTransformOrExpansion::extract_columns(const ObRawExpr *expr,
                                            int64_t &table_id, bool &from_same_table,
                                            ColumnBitSet &col_bit_set)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null");
  } else if (expr->is_column_ref_expr()) {
    const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(expr);
    if (OB_INVALID_ID == table_id) {
      table_id = col_expr->get_table_id();
    }
    if (table_id != col_expr->get_table_id()) {
      from_same_table = false;
    } else if (OB_FAIL(col_bit_set.add_member(col_expr->get_column_id()))) {
      LOG_WARN("failed to add member", K(ret));
    }
  } else if (expr->has_flag(CNT_COLUMN)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(extract_columns(expr->get_param_expr(i),
                                             table_id, from_same_table, col_bit_set)))) {
        LOG_WARN("failed to extract columns", K(ret));
      } else if (!from_same_table) {
        break;
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::get_common_columns_in_condition
 * get common column exprs use in T_OP_OR/T_OP_IN:
 * 1. 使用相同的列集合
 */
int ObTransformOrExpansion::get_common_columns_in_condition(const ObDMLStmt *stmt,
                                                            const ObRawExpr *expr,
                                                            ObIArray<ObRawExpr*> &common_cols)
{
  int ret = OB_SUCCESS;
  common_cols.reuse();
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(expr));
  } else if (T_OP_OR == expr->get_expr_type()) {
    ObSEArray<ObRawExpr*, 4> pre_cols;
    ObSEArray<ObRawExpr*, 4> cur_cols;
    if (OB_UNLIKELY(1 >= expr->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr", K(ret), K(*expr));
    } else if (OB_FAIL(inner_get_common_columns_in_condition(stmt, expr->get_param_expr(0),
                                                           pre_cols))) {
      LOG_WARN("failed to inner get same columns in condition", K(ret));
    }
    for (int64_t i = 1; OB_SUCC(ret) && !pre_cols.empty() && i < expr->get_param_count(); ++i) {
      cur_cols.reuse();
      if (OB_FAIL(inner_get_common_columns_in_condition(stmt, expr->get_param_expr(i), cur_cols))) {
        LOG_WARN("failed to inner get same columns in condition", K(ret));
      } else if (OB_FAIL(ObOptimizerUtil::intersect(pre_cols, cur_cols, pre_cols))) {
        LOG_WARN("failed to intercet expr", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(common_cols.assign(pre_cols))) {
      LOG_WARN("failed to assign exprs", K(ret));
    }
  } else if (T_OP_IN == expr->get_expr_type() &&
             OB_FAIL(inner_get_common_columns_in_condition(stmt, expr, common_cols))) {
    LOG_WARN("failed to inner get same columns in condition", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::inner_get_common_columns_in_condition(const ObDMLStmt *stmt,
                                                                  const ObRawExpr *expr,
                                                                  ObIArray<ObRawExpr*> &cols)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(expr));
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(inner_get_common_columns_in_condition(stmt, expr->get_param_expr(i),
                                                                 cols)))) {
        LOG_WARN("failed to inner get same columns in condition", K(ret));
      }
    }
  } else if (T_OP_IN == expr->get_expr_type()) {
    const ObRawExpr *param = NULL;
    bool is_lossless = false;
    if (OB_ISNULL(expr->get_param_expr(1)) || OB_ISNULL(param = expr->get_param_expr(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret), K(expr->get_param_expr(1)));
    } else if (T_OP_ROW != expr->get_param_expr(1)->get_expr_type()) {
      /* do nothing */
    } else if (OB_FAIL(ObOptimizerUtil::is_lossless_column_cast(param, is_lossless))) {
      LOG_WARN("failed to check is lossless column cast", K(ret));
    } else if (is_lossless && FALSE_IT(param = param->get_param_expr(0))) {
    } else if (OB_ISNULL(param) || !param->is_column_ref_expr()) {
      /* do nothing */
    } else if (OB_FAIL(add_var_to_array_no_dup(cols, const_cast<ObRawExpr*>(param)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else if (expr->get_expr_type() == T_OP_EQ) {
    ObRawExpr *const_expr = NULL;
    if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(const_cast<ObRawExpr*>(expr),
                                                     const_expr))) {
      LOG_WARN("failed to compute const exprs", K(ret), K(*expr));
    } else if (NULL == const_expr || !const_expr->is_column_ref_expr()) {
      /* do nothing */
    } else if (OB_FAIL(add_var_to_array_no_dup(cols, const_expr))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  } else {
    /* do nothing */
  }
  return ret;
}

int ObTransformOrExpansion::is_valid_left_join_cond(const ObDMLStmt *stmt,
                                                    const ObRawExpr *expr,
                                                    ObSqlBitSet<> &right_table_set,
                                                    bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (!expr->get_relation_ids().overlap(right_table_set)) {
    /* do nothing */
  } else if (OB_FAIL(is_contain_join_cond(stmt, expr, is_valid))) {
    LOG_WARN("failed to check is contain join cond", K(ret));
  } else if (is_valid) {
    /* do nothing */
  } else if (OB_FAIL(is_valid_table_filter(expr, right_table_set, is_valid))) {
    LOG_WARN("failed to check is valid table filter", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::is_valid_inner_join_cond(const ObDMLStmt *stmt,
                                                     const ObRawExpr *expr,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (OB_FAIL(is_contain_join_cond(stmt, expr, is_valid))) {
    LOG_WARN("failed to check is contain join cond", K(ret));
  } else if (is_valid) {
    /* do nothing */
  } else if (OB_FAIL(is_simple_cond(stmt, expr, is_valid))) {
    LOG_WARN("failed to check is simple cond", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::is_simple_cond(const ObDMLStmt *stmt,
                                           const ObRawExpr *expr,
                                           bool &is_simple)
{
  int ret = OB_SUCCESS;
  is_simple = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (1 >= expr->get_relation_ids().num_members()) {
    is_simple = true;
  } else if (T_OP_AND == expr->get_expr_type()) {
    is_simple = true;
    int64_t N = expr->get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && is_simple && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(is_simple_cond(stmt,
                                            expr->get_param_expr(i),
                                            is_simple)))) {
        LOG_WARN("failed to check is simple cond", K(ret));
      }
    }
  }
  return ret;
}

// expr is table filter.
// for T_OP_AND expr, child exprs are all table filters.
int ObTransformOrExpansion::is_valid_table_filter(const ObRawExpr *expr,
                                                  ObSqlBitSet<> &right_table_set,
                                                  bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->get_relation_ids().is_subset(right_table_set)
             || !expr->get_relation_ids().overlap(right_table_set)) {
    is_contain = true;
  } else if (T_OP_AND == expr->get_expr_type()) {
    int64_t N = expr->get_param_count();
    is_contain = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_contain && i < N; ++i) {
      if (OB_FAIL(SMART_CALL(is_valid_table_filter(expr->get_param_expr(i),
                                                   right_table_set,
                                                   is_contain)))) {
        LOG_WARN("failed to check is simple cond", K(ret));
      }
    }
  }
  return ret;
}

// todo: can check join condition match index
int ObTransformOrExpansion::is_contain_join_cond(const ObDMLStmt *stmt,
                                                 const ObRawExpr *expr,
                                                 bool &is_contain)
{
  int ret = OB_SUCCESS;
  is_contain = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null expr", K(ret));
  } else if (expr->has_flag(IS_JOIN_COND)) {
    is_contain = true;
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_contain && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_contain_join_cond(stmt,
                                                  expr->get_param_expr(i),
                                                  is_contain)))) {
        LOG_WARN("failed to check is contain join cond", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::is_match_index(const ObDMLStmt *stmt,
                                          const ObRawExpr *expr,
                                          EqualSets &equal_sets,
                                          ObIArray<ObRawExpr*> &const_exprs,
                                          bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null ctx", K(ret));
  } else if (expr->has_flag(IS_SIMPLE_COND) ||
             expr->has_flag(IS_RANGE_COND) ||
             T_OP_IS == expr->get_expr_type()) {
    ObSEArray<ObRawExpr*, 2> column_exprs;
    ObColumnRefRawExpr *col_expr = NULL;
    if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, column_exprs))) {
      LOG_WARN("failed to extrace column exprs", K(ret));
    } else if (1 != column_exprs.count()) {
      //do nothing
    } else if (OB_ISNULL(column_exprs.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null expr", K(ret));
    } else if (!column_exprs.at(0)->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect column ref expr", K(*column_exprs.at(0)), K(ret));
    } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr*>(column_exprs.at(0)))) {
    } else if (OB_ISNULL(stmt->get_table_item_by_id(col_expr->get_table_id()))) {
      //do nothing
    } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                        stmt,
                                                        col_expr,
                                                        is_match,
                                                        &equal_sets,
                                                        &const_exprs))) {
      LOG_WARN("failed to check is match index", K(ret));
    }
  } else if (T_OP_IN == expr->get_expr_type()) {
    const ObRawExpr *right_expr = NULL;
      if (OB_UNLIKELY(2 != expr->get_param_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in expr should have 2 param", K(expr->get_param_count()), K(ret));
      } else if (OB_ISNULL(right_expr = expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (T_OP_ROW == right_expr->get_expr_type()) {
        bool is_const = true;
        for (int64_t i = 0;
             OB_SUCC(ret) && is_const && i < right_expr->get_param_count(); i++) {
          const ObRawExpr *temp_expr = right_expr->get_param_expr(i);
          if (OB_ISNULL(temp_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("null expr", K(ret));
          } else if (!temp_expr->is_const_expr()) {
            is_const = false;
          } else { /*do nothing*/ }
        }
        const ObColumnRefRawExpr *col_expr = NULL;
        if (OB_FAIL(ret) || !is_const) {
          //do nothing
        } else if (OB_ISNULL(expr->get_param_expr(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null expr", K(ret));
        } else if (!expr->get_param_expr(0)->is_column_ref_expr()) {
          //do nothing
        } else if (OB_FALSE_IT(col_expr = static_cast<const ObColumnRefRawExpr*>(expr->get_param_expr(0)))) {
        } else if (OB_ISNULL(stmt->get_table_item_by_id(col_expr->get_table_id()))) {
          //do nothing
        } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                            stmt,
                                                            col_expr,
                                                            is_match,
                                                            &equal_sets,
                                                            &const_exprs))) {
          LOG_WARN("failed to check is match index", K(ret));
        }
      } else { /*do nothing*/ }
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && !is_match && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_match_index(stmt,
                                            expr->get_param_expr(i),
                                            equal_sets,
                                            const_exprs,
                                            is_match)))) {
        LOG_WARN("failed to check is match index", K(ret));
      }
    }
  } else if (expr->has_flag(CNT_COLUMN) &&
             (expr->has_flag(IS_ROWID_SIMPLE_COND) ||
              expr->has_flag(IS_ROWID_RANGE_COND))) {
    //rowid = const or rowid belong const range can choose primary key.
    is_match = true;
  }
  return ret;
}

// todo: can also check refquery unnest hint
int ObTransformOrExpansion::is_valid_subquery_cond(const ObDMLStmt &stmt,
                                                   const ObRawExpr &expr,
                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (T_OP_EXISTS == expr.get_expr_type() || T_OP_NOT_EXISTS == expr.get_expr_type()) {
    const ObRawExpr *child_expr = NULL;
    if (OB_ISNULL(child_expr = static_cast<const ObOpRawExpr*>(&expr)->get_param_expr(0)) ||
        OB_UNLIKELY(!child_expr->is_query_ref_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr", K(child_expr), K(ret));
    } else {
      is_valid = static_cast<const ObQueryRefRawExpr*>(child_expr)->has_exec_param();
    }
  } else if (expr.has_flag(IS_WITH_ALL) || expr.has_flag(IS_WITH_ANY)
             || expr.has_flag(IS_WITH_SUBQUERY)) {
    const ObOpRawExpr *op_expr = static_cast<const ObOpRawExpr*>(&expr);
    const ObRawExpr *left_expr = NULL;
    const ObRawExpr *right_expr = NULL;
    if (OB_ISNULL(left_expr = op_expr->get_param_expr(0))
        || OB_ISNULL(right_expr = op_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected expr", K(left_expr), K(right_expr), K(ret));
    } else {
      is_valid |= left_expr->is_query_ref_expr() ?
                  static_cast<const ObQueryRefRawExpr*>(left_expr)->has_exec_param():
                  !left_expr->get_relation_ids().is_empty();
      is_valid |= right_expr->is_query_ref_expr() ?
                  static_cast<const ObQueryRefRawExpr*>(right_expr)->has_exec_param():
                  !right_expr->get_relation_ids().is_empty();
    }
  } else if (T_OP_AND == expr.get_expr_type()) {
    const ObRawExpr *child_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < expr.get_param_count(); ++i) {
      if (OB_ISNULL(child_expr = expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (OB_FAIL(SMART_CALL(is_valid_subquery_cond(stmt, *child_expr, is_valid)))) {
        LOG_WARN("failed to check is valid subquery cond", K(ret));
      }
    }
  }
  return ret;
}

/* 
 * topk:  1. common columns in equal condition / in.
 *        2. column expr in expect_ordering can match index after used common_cols.
 */
int ObTransformOrExpansion::is_valid_topk_cond(const ObDMLStmt &stmt,
                                               const ObIArray<ObRawExpr*> &expect_ordering,
                                               const ObIArray<ObRawExpr*> &common_cols,
                                               EqualSets &equal_sets,
                                               ObIArray<ObRawExpr*> &const_exprs,
                                               OrExpandInfo &trans_info)
{
  int ret = OB_SUCCESS;
  if (!common_cols.empty()) {
    bool is_const = false;
    ObRawExpr *expr = NULL;
    ObColumnRefRawExpr *col_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && NULL == col_expr && i < expect_ordering.count(); i++) {
      if (OB_ISNULL(expr = expect_ordering.at(i)) || OB_UNLIKELY(!expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(expr));
      } else if (OB_FAIL(ObOptimizerUtil::is_const_expr(expr, equal_sets, const_exprs, is_const))) {
        LOG_WARN("failed to check is const expr", K(ret));
      } else if (is_const) {
        /* do nothing */
      } else if (ObOptimizerUtil::find_equal_expr(common_cols, expr, equal_sets)) {
        /* do nothing */
      } else {
        col_expr = static_cast<ObColumnRefRawExpr*>(expr);
      }
    }
    if (OB_SUCC(ret) &&  NULL != col_expr) {
      ObSEArray<ObColumnRefRawExpr*, 4> col_exprs;
      bool is_match = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < common_cols.count(); i++) {
        if (OB_ISNULL(expr = common_cols.at(i)) || OB_UNLIKELY(!expr->is_column_ref_expr())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), K(expr));
        } else if (OB_FAIL(col_exprs.push_back(static_cast<ObColumnRefRawExpr*>(expr)))) {
          LOG_WARN("failed to push back exprs", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx_->sql_schema_guard_,
                                                          &stmt, col_expr, is_match,
                                                          &equal_sets, &const_exprs,
                                                          &col_exprs, true))) {
        LOG_WARN("failed to check is ordering match index", K(ret));
      } else if (is_match) {
        trans_info.is_set_distinct_ = true;
        trans_info.or_expand_type_ |= OR_EXPAND_TOP_K;
        trans_info.is_valid_topk_ = true;
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::check_condition_valid_basic
 *  basic check for condition:
 *  1. or_expr_count is less than MAX_STMT_NUM_FOR_OR_EXPANSION
 *  2. is OR expr, and not anti or condition for anti join
 *  3. is IN expr, contains no subquery, right expr is a const row
 */
int ObTransformOrExpansion::check_condition_valid_basic(const ObDMLStmt *stmt,
                                                        ObCostBasedRewriteCtx &ctx,
                                                        const ObRawExpr *expr,
                                                        const bool is_topk,
                                                        bool &is_valid,
                                                        ObIArray<ObRawExpr*> &common_cols,
                                                        bool &using_same_cols)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  using_same_cols = true;
  common_cols.reuse();
  int classify_count = 0;
  OPT_TRACE("check expr:", expr);
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(expr));
  } else if (expr->has_flag(CNT_ROWNUM)) {
    is_valid = false;
    OPT_TRACE("expr has rownum");
  } else if (T_OP_OR != expr->get_expr_type() && T_OP_IN != expr->get_expr_type()) {
    is_valid = false;
    OPT_TRACE("not or/in expr");
  } else if (T_OP_OR == expr->get_expr_type()) {
    if (OB_FAIL(pre_classify_or_expr(expr, classify_count))) {
      LOG_WARN("failed to classify or expr", K(ret));
    } else {
      is_valid = expr->get_param_count() > 1 && classify_count <= MAX_STMT_NUM_FOR_OR_EXPANSION;
    }
  } else if (is_topk && T_OP_IN == expr->get_expr_type() && !expr->has_flag(CNT_SUB_QUERY)) {
    const ObRawExpr *right_expr = NULL;
    if (OB_UNLIKELY(2 != expr->get_param_count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("in expr should have 2 param", K(expr->get_param_count()), K(ret));
    } else if (OB_ISNULL(right_expr = expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null expr", K(ret));
    } else if (T_OP_ROW == right_expr->get_expr_type() &&
               right_expr->get_param_count() <= MAX_STMT_NUM_FOR_OR_EXPANSION &&
               right_expr->get_param_count() >= 2) {
      const ObRawExpr *temp_expr = NULL;
      is_valid = right_expr->get_param_count() > 1;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < right_expr->get_param_count(); ++i) {
        if (OB_ISNULL(temp_expr = right_expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null expr", K(ret));
        } else if (temp_expr->is_const_expr()) {
          /*do nothing*/
        } else {
          is_valid = false;
          OPT_TRACE("in expr has none const param");
        }
      }
    } else { /*do nothing*/ }
  } else {
    OPT_TRACE("not or/in expr");
  }

  if (OB_FAIL(ret) || !is_valid) {
  } else if (is_topk && // top-K, get common cols in conditions
             OB_FAIL(get_common_columns_in_condition(stmt, expr, common_cols))) {
    LOG_WARN("failed to get get same columns in condition", K(ret));
  } else if (OB_FAIL(check_condition_on_same_columns(*stmt, *expr, using_same_cols))) {
    LOG_WARN("failed to check condition on same columns", K(ret));
  } else if (common_cols.empty() && using_same_cols && !expr->has_flag(CNT_SUB_QUERY)) {
    is_valid = false;
    OPT_TRACE("or expr from same table, will not expand");
  } else if (using_same_cols && T_OP_OR == expr->get_expr_type()
             && expr->get_param_count() > MAX_STMT_NUM_FOR_OR_EXPANSION) {
    is_valid = false;
    OPT_TRACE("or expr param count > MAX_STMT_NUM_FOR_OR_EXPANSION, will not expand");
  } else {
    is_valid = NULL == ctx.hint_ || ctx.hint_->enable_use_concat(*expr);
    if (!is_valid) {
      OPT_TRACE("hint disable transform");
    }
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::is_condition_valid
 *  1.  use_concat hint: do transfrom if match basic condition.
 *  2.  topk: expect_ordering is not empty, has same column
 *  3.  can do subquery pullup/can generate join condition/can use different index
 */
int ObTransformOrExpansion::is_condition_valid(const ObDMLStmt *stmt,
                                               ObCostBasedRewriteCtx &ctx,
                                               const ObRawExpr *expr,
                                               const ObIArray<ObRawExpr*> &expect_ordering,
                                               const JoinedTable *joined_table,
                                               EqualSets &equal_sets,
                                               ObIArray<ObRawExpr*> &const_exprs,
                                               OrExpandInfo &trans_info)
{
  int ret = OB_SUCCESS;
  const bool can_set_distinct = trans_info.is_set_distinct_;
  trans_info.or_expand_type_ = INVALID_OR_EXPAND_TYPE;
  trans_info.is_set_distinct_ = false;
  bool is_valid = false;
  ObSEArray<ObRawExpr*, 4> common_cols;
  bool using_same_cols = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) ||
      OB_UNLIKELY(NULL != joined_table && NULL == joined_table->right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(expr), K(joined_table));
  } else if (OB_FAIL(check_condition_valid_basic(stmt, ctx, expr,
                                                 !expect_ordering.empty() && can_set_distinct,
                                                 is_valid, common_cols, using_same_cols))) {
    LOG_WARN("failed to check condition valid basic", K(ret));
  } else if (!is_valid) {
    /*do nothing*/
  } else if (!common_cols.empty() && // 1. topk: can eliminate ordering
             OB_FAIL(is_valid_topk_cond(*stmt, expect_ordering, common_cols,
                                        equal_sets, const_exprs, trans_info))) {
    LOG_WARN("failed to check is valid topk cond", K(ret));
  } else if ((trans_info.or_expand_type_ & OR_EXPAND_TOP_K)
             && T_OP_OR == expr->get_expr_type()
             && expr->get_param_count() > MAX_STMT_NUM_FOR_OR_EXPANSION) {
    trans_info.or_expand_type_ = INVALID_OR_EXPAND_TYPE; // means is_valid == false
  } else if (NULL != ctx.hint_ && ctx.hint_->is_enable_hint()) {
    // 2. match basic condition, do transform if use hint.
    if (OB_FAIL(get_use_hint_expand_type(*expr, can_set_distinct, trans_info))) {
      LOG_WARN("failed to get expand type use hint", K(ret));
    }
  } else if (INVALID_OR_EXPAND_TYPE != trans_info.or_expand_type_) {
    /*do nothing*/
    LOG_TRACE("valid topk condition", K(*expr), K(trans_info));
  } else if (T_OP_OR != expr->get_expr_type()) {
    /*do nothing*/
  } else if (using_same_cols && !expr->has_flag(CNT_SUB_QUERY)) {
    /*do nothing*/
  } else {
    ObSqlBitSet<> right_table_set;
    ObJoinType join_type = UNKNOWN_JOIN;
    if (NULL == joined_table) {
      join_type = 1 < expr->get_relation_ids().num_members() ? INNER_JOIN : UNKNOWN_JOIN;
    } else if (joined_table->is_inner_join()) {
      join_type = INNER_JOIN;
    } else if (OB_FAIL(stmt->get_table_rel_ids(*joined_table->right_table_, right_table_set))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    } else {
      join_type = LEFT_OUTER_JOIN;
    }
    const int64_t N = expr->get_param_count();
    const ObRawExpr *child_expr = NULL;
    bool has_valid_subquery = false;
    int64_t subquery_count = 0;
    bool match_index = (join_type == UNKNOWN_JOIN);
    bool join_cond_valid = !match_index;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_ISNULL(child_expr = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret), K(child_expr));
      } else if (!join_cond_valid) {
        /*do nothing*/
      } else if (LEFT_OUTER_JOIN == join_type && OB_FAIL(is_valid_left_join_cond(stmt, child_expr,
                                                        right_table_set, join_cond_valid))) {
        LOG_WARN("failed to check is valid left join cond", K(ret));
      } else if (INNER_JOIN == join_type && OB_FAIL(is_valid_inner_join_cond(stmt,
                                                            child_expr, join_cond_valid))) {
        LOG_WARN("failed to check is valid inner join cond", K(ret));
      }

      if (OB_SUCC(ret) && match_index &&
          OB_FAIL(is_match_index(stmt, child_expr, equal_sets, const_exprs, match_index))) {
        LOG_WARN("failed to check is match index", K(ret));
      }
      if (OB_SUCC(ret) && !has_valid_subquery &&
          OB_FAIL(is_valid_subquery_cond(*stmt, *child_expr, has_valid_subquery))) {
        LOG_WARN("failed to check is valid subquery cond", K(ret));
      }
      if (OB_SUCC(ret) && child_expr->has_flag(CNT_SUB_QUERY)) {
        ++subquery_count;
      }
    }

    if (OB_SUCC(ret)) {
      if (!can_set_distinct && subquery_count > 1) {
        // do nothing
      } else {
        trans_info.is_set_distinct_ = subquery_count > 1;
        trans_info.or_expand_type_ |= join_cond_valid ? OR_EXPAND_JOIN : 0;
        trans_info.or_expand_type_ |= has_valid_subquery ? OR_EXPAND_SUB_QUERY : 0;
        trans_info.or_expand_type_ |= match_index ? OR_EXPAND_MULTI_INDEX : 0;
      }
      LOG_TRACE("after check is condition valid", K(join_cond_valid), K(match_index),
                                        K(has_valid_subquery), K(trans_info), K(*expr));
    }
  }
  return ret;
}

int ObTransformOrExpansion::get_use_hint_expand_type(const ObRawExpr &expr,
                                                     const bool can_set_distinct,
                                                     OrExpandInfo &trans_info)
{
  int ret = OB_SUCCESS;
  trans_info.or_expand_type_ = OR_EXPAND_HINT;
  if (!can_set_distinct) {
    trans_info.is_set_distinct_ = false;
  } else {
    trans_info.is_set_distinct_ |= (T_OP_IN == expr.get_expr_type());
    int64_t sub_num = 0;
    int64_t N = expr.get_param_count();
    for (int64_t i = 0; OB_SUCC(ret) && !trans_info.is_set_distinct_ && i < N; i++) {
      if (OB_ISNULL(expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (expr.get_param_expr(i)->has_flag(CNT_SUB_QUERY)) {
        ++sub_num;
        trans_info.is_set_distinct_ = sub_num > 1;
      }
    }
  }
  return ret;
}

// expand_anti_or_cond is generate when pullup where subquery: t1.c1 in (select t2.c1 ...)
// t1.c1 = t2.c1 or t1.c1 is null or t2.c1 is null
int ObTransformOrExpansion::is_expand_anti_or_cond(const ObRawExpr &expr,
                                                   bool &is_anti_or_cond)
{
  int ret = OB_SUCCESS;
  is_anti_or_cond = false;
  if (T_OP_OR == expr.get_expr_type()) {
    is_anti_or_cond = true;
    int64_t is_null_count = 0;
    const ObRawExpr *cond_expr = NULL;
    const ObRawExpr *param_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && is_anti_or_cond && i < expr.get_param_count(); i++) {
      if (OB_ISNULL(cond_expr = expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (T_OP_IS != cond_expr->get_expr_type()) {
        is_anti_or_cond = T_OP_EQ == cond_expr->get_expr_type() ||
                          IS_RANGE_CMP_OP(cond_expr->get_expr_type());
      } else if (OB_UNLIKELY(2 != cond_expr->get_param_count()) ||
                 OB_ISNULL(param_expr = cond_expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected expr", K(ret), K(*cond_expr));
      } else if (T_NULL == param_expr->get_expr_type()) {
        ++is_null_count;
      }
    }
    if (OB_SUCC(ret) && is_anti_or_cond) {
      is_anti_or_cond = is_null_count + 1 == expr.get_param_count();
    }
  }
  return ret;
}

// trans_id is OB_INVALID_ID, do transform in where condition;
// trans_id is a semi id, do transform in semi condition;
// trans_id is a joined table id, do transform in on condition;
int ObTransformOrExpansion::transform_or_expansion(ObSelectStmt *stmt,
                                                   const uint64_t trans_id,
                                                   const int64_t expr_pos,
                                                   ObCostBasedRewriteCtx &ctx,
                                                   ObSelectStmt *&trans_stmt,
                                                   StmtUniqueKeyProvider &unique_key_provider)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *copy_stmt = NULL;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  ObIArray<ObRawExpr*> *conds_exprs = NULL;
  ObSelectStmt *view_stmt = NULL;
  TableItem* view_table = NULL;
  int64_t view_expr_pos = expr_pos;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) ||
      OB_ISNULL(session_info = ctx_->session_info_) ||
      OB_ISNULL(stmt_factory = ctx_->stmt_factory_)  ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(stmt), K(ctx_),
        K(session_info), K(stmt_factory), K(expr_factory), K(ret));
  } else if (OB_FAIL(get_expand_conds(*stmt, trans_id, conds_exprs))) {
    LOG_WARN("failed to get expand conds", K(ret));
  } else if (OB_ISNULL(conds_exprs) || OB_UNLIKELY(expr_pos < 0 || expr_pos >= conds_exprs->count())
             || OB_ISNULL(ctx.orig_expr_ = conds_exprs->at(expr_pos))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid condition expr pos", K(expr_pos), K(conds_exprs), K(ctx.orig_expr_), K(ret));
  } else if (OB_FAIL(stmt_factory->create_stmt(copy_stmt))) {
    LOG_WARN("failed to create stmt factory", K(copy_stmt), K(ret));
  } else if (OB_ISNULL(copy_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret), K(copy_stmt));
  } else if (OB_FAIL(copy_stmt->deep_copy(*stmt_factory, *expr_factory, *stmt))) {
    LOG_WARN("failed to deep copy child statement", K(ret));
  } else if (OB_FAIL(get_expand_conds(*copy_stmt, trans_id, conds_exprs))) {
    LOG_WARN("failed to get expand conds", K(ret));
  } else if (FALSE_IT(view_stmt = copy_stmt)) {
    // never reach
  } else if (OB_INVALID_ID == trans_id && !ctx.is_valid_topk_ &&
             OB_FAIL(get_condition_related_view(copy_stmt, view_stmt, view_table,
                                                view_expr_pos, conds_exprs,
                                                ctx.is_set_distinct_))) {
    LOG_WARN("failed to create view for tables", K(ret));
  } else if (!ctx.is_set_distinct_ && OB_FAIL(preprocess_or_condition(*view_stmt, trans_id, view_expr_pos))) {
    LOG_WARN("failed to preprocess or condition", K(ret));
  } else if (ctx.is_set_distinct_ && !ctx.is_unique_ &&
             OB_FAIL(unique_key_provider.recursive_set_stmt_unique(view_stmt, ctx_, true))) {
    LOG_WARN("failed to set stmt unique", K(ret), KPC(view_stmt));
  } else if (!ctx.is_valid_topk_
             && OB_FAIL(classify_or_expr(*view_stmt, conds_exprs->at(view_expr_pos)))) {
    LOG_WARN("failed to classify or expr", K(ret), KPC(conds_exprs->at(view_expr_pos)));
  } else {
    const uint64_t or_expr_count = get_or_expr_count(*conds_exprs->at(view_expr_pos));
    ObSEArray<ObSelectStmt*, 2> child_stmts;
    ObSelectStmt *set_stmt = NULL;
    SemiInfo *semi_info = view_stmt->get_semi_info_by_id(trans_id);
    const ObSelectStmt::SetOperator set_type = NULL != semi_info && semi_info->is_anti_join()
                                               ? ObSelectStmt::INTERSECT : ObSelectStmt::UNION;
    for (int64_t i = 0; OB_SUCC(ret) && i < or_expr_count; i++) {
      ObSelectStmt *child_stmt = view_stmt;
      if (i < or_expr_count - 1 && OB_FAIL(stmt_factory->create_stmt(child_stmt))) {
        LOG_WARN("failed to create stmt factory", K(child_stmt), K(ret));
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null stmt", K(ret), K(child_stmt));
      } else if (i < or_expr_count - 1 &&
                 OB_FAIL(child_stmt->deep_copy(*stmt_factory, *expr_factory, *view_stmt))) {
        LOG_WARN("failed to deep copy child statement", K(ret));
      } else if (OB_FAIL(child_stmt->recursive_adjust_statement_id(ctx_->allocator_,
                                                                   ctx_->src_hash_val_,
                                                                   i + 1))) {
        LOG_WARN("failed to recursive adjust statement id", K(ret));
      } else if (OB_FAIL(get_expand_conds(*child_stmt, trans_id, conds_exprs))) {
        LOG_WARN("failed to get expand conds", K(ret));
      } else if (OB_FAIL(child_stmt->update_stmt_table_id(ctx_->allocator_, *view_stmt))) {
        //update stmt table id after find conds_exprs
        LOG_WARN("failed to update table id", K(ret));
      } else if (OB_FAIL(adjust_or_expansion_stmt(conds_exprs, view_expr_pos, i,
                                                  ctx, child_stmt))) {
        LOG_WARN("failed to adjust children stmt", K(ret));
      } else if (OB_FAIL(child_stmts.push_back(child_stmt))) {
        LOG_WARN("failed to push back stmt", K(ret));
      } else { /* do nothing */ }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTransformUtils::create_set_stmt(ctx_, set_type, ctx.is_set_distinct_,
                                                         child_stmts, set_stmt))) {
      LOG_WARN("failed to create union stmt", K(set_stmt), K(ret));
    } else if (OB_ISNULL(set_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(set_stmt));
    } else if (OB_FAIL(set_stmt->formalize_stmt(session_info))) {
      LOG_WARN("failed to formalize stmt", K(ret));
    } else {
      if (NULL != view_table) {
        view_table->ref_query_ = set_stmt;
        trans_stmt = copy_stmt;
      } else {
        trans_stmt = set_stmt;
      }
      ctx.trans_id_ = set_stmt->get_stmt_id();
    }
    //ignore for temp table optimization
    if (OB_SUCC(ret)) {
      if (OB_FAIL(append(ctx_->temp_table_ignore_stmts_, child_stmts))) {
        LOG_WARN("failed to append child stmts", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::adjust_or_expansion_stmt(ObIArray<ObRawExpr*> *conds_exprs,
                                                     const int64_t expr_pos,
                                                     const int64_t param_pos,
                                                     ObCostBasedRewriteCtx &ctx,
                                                     ObSelectStmt *&or_expansion_stmt)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObRawExpr *transformed_expr = NULL;
  ObSEArray<ObQueryRefRawExpr *, 4> removed_subqueries;
  ObSEArray<ObRawExpr*, 4> generated_exprs;
  if (OB_ISNULL(conds_exprs) || OB_ISNULL(ctx_)
      || OB_ISNULL(session_info = ctx_->session_info_)
      || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point error", K(ctx_), K(session_info), K(expr_factory), K(ret));
  } else if (OB_ISNULL(or_expansion_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null stmt", K(ret));
  } else if (OB_UNLIKELY(expr_pos < 0) || OB_UNLIKELY(expr_pos >= conds_exprs->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid condition expr pos", K(expr_pos), K(conds_exprs->count()), K(ret));
  } else if (OB_ISNULL(transformed_expr = conds_exprs->at(expr_pos))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null transformed expr", K(ret));
  } else if (OB_UNLIKELY(T_OP_IN != transformed_expr->get_expr_type() &&
                         T_OP_OR != transformed_expr->get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type", K(transformed_expr->get_expr_type()), K(ret));
  } else if (OB_FAIL(conds_exprs->remove(expr_pos))) {
    LOG_WARN("failed to remove condition expr", K(ret));
  } else if (T_OP_OR == transformed_expr->get_expr_type() &&
             OB_FAIL(create_expr_for_or_expr(*transformed_expr,
                                             param_pos,
                                             ctx,
                                             generated_exprs))) {
    LOG_WARN("failed to create expr", K(ret));
  } else if (T_OP_IN == transformed_expr->get_expr_type() &&
             OB_FAIL(create_expr_for_in_expr(*transformed_expr,
                                             param_pos,
                                             ctx,
                                             generated_exprs))) {
    LOG_WARN("failed to create expr", K(ret), K(generated_exprs));
  } else if (OB_FAIL(append(*conds_exprs, generated_exprs))) {
    LOG_WARN("failed to append expr", K(ret));
  } else if (OB_FAIL(or_expansion_stmt->formalize_stmt(session_info))) {
    LOG_WARN("failed to formalize stmt", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::create_expr_for_in_expr(const ObRawExpr &transformed_expr,
                                                    const int64_t param_pos,
                                                    ObCostBasedRewriteCtx &ctx,
                                                    ObIArray<ObRawExpr *> &generated_exprs)
{
  int ret = OB_SUCCESS;
  const ObRawExpr *left_expr = NULL;
  const ObRawExpr *right_expr = NULL;
  ObRawExpr *temp_expr = NULL;
  ObRawExprFactory *expr_factory = NULL;
  ObSQLSessionInfo *session_info = NULL;
  if (OB_ISNULL(ctx_) ||
      OB_ISNULL(expr_factory = ctx_->expr_factory_) ||
      OB_ISNULL(session_info = ctx_->session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ctx_), K(session_info), K(expr_factory), K(ret));
  } else if (OB_UNLIKELY(T_OP_IN != transformed_expr.get_expr_type()
                         || !ctx.is_set_distinct_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type or trans type", K(transformed_expr.get_expr_type()),
                                                   K(ctx.is_set_distinct_), K(ret));
  } else if (OB_UNLIKELY(2 != transformed_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in expr should have 2 param", K(transformed_expr.get_param_count()), K(ret));
  } else if (OB_ISNULL(left_expr = transformed_expr.get_param_expr(0)) ||
             OB_ISNULL(right_expr = transformed_expr.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null expr", K(left_expr), K(right_expr), K(ret));
  } else if (T_OP_ROW != right_expr->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(right_expr->get_expr_type()), K(ret));
  } else if (OB_UNLIKELY(param_pos < 0) || OB_UNLIKELY(param_pos >= right_expr->get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr pos", K(param_pos),
        K(right_expr->get_param_count()), K(ret));
  } else if (OB_FAIL(ObRawExprUtils::create_equal_expr(*expr_factory,
                                                      session_info,
                                                      left_expr,
                                                      right_expr->get_param_expr(param_pos),
                                                      temp_expr))) {
    LOG_WARN("failed to create double op expr", K(ret));
  } else if (OB_FAIL(ctx.expand_exprs_.push_back(temp_expr))) {
    LOG_WARN("failed to push back expand exprs", K(ret));
  } else if (OB_FAIL(generated_exprs.push_back(temp_expr))) {
    LOG_WARN("failed to push back temp expr", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::create_expr_for_or_expr(ObRawExpr &transformed_expr,
                                                    const int64_t param_pos,
                                                    ObCostBasedRewriteCtx &ctx,
                                                    common::ObIArray<ObRawExpr*> &generated_exprs)
{
  int ret = OB_SUCCESS;
  ObRawExprFactory *expr_factory = NULL;
  if (OB_ISNULL(ctx_) || OB_ISNULL(expr_factory = ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null point error", K(ctx_), K(expr_factory), K(ret));
  } else if (OB_UNLIKELY(T_OP_OR != transformed_expr.get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr type", K(transformed_expr.get_expr_type()), K(ret));
  } else if (OB_UNLIKELY(param_pos < 0) ||
             OB_UNLIKELY(param_pos >= transformed_expr.get_param_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr pos", K(param_pos),
        K(transformed_expr.get_param_count()), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i <= param_pos; i++) {
      ObRawExpr *expr = NULL;
      ObRawExpr *temp_expr = NULL;
      if (OB_ISNULL(expr = transformed_expr.get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null expr", K(ret));
      } else if (i == param_pos) {
        if (OB_FAIL(ctx.expand_exprs_.push_back(expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(ObTransformUtils::flatten_expr(expr, generated_exprs))) {
          LOG_WARN("failed to flatten expr", K(ret));
        } else { /*do nothing*/ }
      } else if (ctx.is_set_distinct_) {
       /*do nothing */
      } else if (OB_FAIL(ObRawExprUtils::build_lnnvl_expr(*expr_factory, expr, temp_expr))) {
        LOG_WARN("failed to create lnnvl expr", K(ret));
      } else if (OB_FAIL(generated_exprs.push_back(temp_expr))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

// pre-check conditions for semi/anti condition
int ObTransformOrExpansion::has_valid_semi_anti_cond(ObDMLStmt &stmt,
                                                     ObCostBasedRewriteCtx &ctx,
                                                     int64_t &begin_idx)
{
  int ret = OB_SUCCESS;
  begin_idx = OB_INVALID_INDEX;
  bool can_set_unique = false;
  bool has_lob = false;
  if (OB_FAIL(check_select_expr_has_lob(stmt, has_lob))) {
    LOG_WARN("failed to check stmt has lob", K(ret));
  } else if (has_lob) {
    // do nothing
    OPT_TRACE("select expr has lob expr");
  } else if (OB_FAIL(StmtUniqueKeyProvider::check_can_set_stmt_unique(&stmt, can_set_unique))) {
    LOG_WARN("failed to check can set stmt unique", K(ret));
  } else if (!can_set_unique) {
    // do nothing
    OPT_TRACE("stmt can not be unique");
  } else {
    ctx.hint_ = static_cast<const ObOrExpandHint*>(get_hint(stmt.get_stmt_hint()));
    bool has_valid = false;
    uint64_t trans_type = INVALID_OR_EXPAND_TYPE;
    SemiInfo *semi_info = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !has_valid && i < stmt.get_semi_info_size(); ++i) {
      if (OB_ISNULL(semi_info = stmt.get_semi_infos().at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(semi_info));
      } else {
        OPT_TRACE("check semi conditions:", semi_info->semi_conditions_);
        const int64_t N = semi_info->semi_conditions_.count();
        for (int64_t j = 0; OB_SUCC(ret) && !has_valid && j < N; ++j) {
          if (OB_FAIL(is_valid_semi_anti_cond(&stmt, ctx, semi_info->semi_conditions_.at(j),
                                              semi_info, trans_type))) {
            LOG_WARN("failed to check has valid condition", K(ret));
          } else if (INVALID_OR_EXPAND_TYPE != trans_type) {
            has_valid = true;
          }
        }
      }
      if (OB_SUCC(ret) && has_valid) {
        begin_idx = i;
      }
    }
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::is_valid_semi_anti_cond
 *  check expand expr can get join condition or table filter
 */
int ObTransformOrExpansion::is_valid_semi_anti_cond(const ObDMLStmt *stmt,
                                                    ObCostBasedRewriteCtx &ctx,
                                                    const ObRawExpr *expr,
                                                    const SemiInfo *semi_info,
                                                    uint64_t &trans_type)
{
  int ret = OB_SUCCESS;
  trans_type = INVALID_OR_EXPAND_TYPE;
  bool check_status = false;
  int classify_count = 0;
  OPT_TRACE("check expr:", expr);
  if (OB_ISNULL(stmt) || OB_ISNULL(expr) || OB_ISNULL(semi_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt), K(expr), K(semi_info));
  } else if (expr->has_flag(CNT_ROWNUM)) {
    /* do nothing */
    OPT_TRACE("expr has rownum");
  } else if (T_OP_OR != expr->get_expr_type() || expr->get_param_count() <= 1) {
    /* do nothing */
    OPT_TRACE("not or expr");
  } else if (OB_FAIL(check_condition_on_same_columns(*stmt, *expr, check_status))) {
    LOG_WARN("failed to check condition on same columns", K(ret));
  } else if (check_status) {
    /* do nothing */
    OPT_TRACE("or expr param from same table");
  } else if (OB_FAIL(pre_classify_or_expr(expr, classify_count))) {
    LOG_WARN("failed to classify or expr", K(ret));
  } else if (classify_count > MAX_STMT_NUM_FOR_OR_EXPANSION) {
    /* do nothing */
    OPT_TRACE("or expr classify count more than MAX_STMT_NUM_FOR_OR_EXPANSION");
  } else if (NULL != ctx.hint_) {
    trans_type = ctx.hint_->enable_use_concat(*expr) ? OR_EXPAND_HINT : INVALID_OR_EXPAND_TYPE;
    if (INVALID_OR_EXPAND_TYPE == trans_type) {
      OPT_TRACE("hint disable transform");
    }
  } else if (semi_info->is_anti_join() && OB_FAIL(is_expand_anti_or_cond(*expr, check_status))) {
    LOG_WARN("failed to check is expand anti or cond", K(ret));
  } else if (check_status) {
    /* do nothing */
    OPT_TRACE("is anti join or condition, do not expand");
  } else {
    const int64_t N = expr->get_param_count();
    const ObRawExpr *child_expr = NULL;
    ObSqlBitSet<> right_table_set;
    if (OB_FAIL(stmt->get_table_rel_ids(semi_info->right_table_id_, right_table_set))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    } else if (expr->get_relation_ids().is_subset(right_table_set)
               || !expr->get_relation_ids().overlap(right_table_set)) {
      /* semi/anti join or condition is a table filter. if it's a anti join left table filter,
        need not expand, else it can be expand as a normal where condition. */
    } else {
      check_status = true;
      for (int64_t i = 0; OB_SUCC(ret) && check_status && i < N; ++i) {
        if (OB_ISNULL(child_expr = expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret), K(child_expr));
        } else if (OB_FAIL(is_contain_join_cond(stmt, child_expr, check_status))) {
          LOG_WARN("failed to check is contain join cond", K(ret));
        } else if (check_status) {
          /* do nothing */
        } else if (OB_FAIL(is_valid_table_filter(child_expr, right_table_set, check_status))) {
          LOG_WARN("failed to check is contain table filter", K(ret));
        }
      }
      if (OB_SUCC(ret) && check_status) {
        trans_type = OR_EXPAND_JOIN;
        LOG_TRACE("get valid semi anti cond",  K(semi_info->is_anti_join()), K(*expr));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::gather_transform_infos(ObSelectStmt *stmt,
                                                   ObCostBasedRewriteCtx &ctx,
                                                   const common::ObIArray<ObRawExpr*> &candi_conds,
                                                   const ObIArray<ObRawExpr*> &expect_ordering,
                                                   const JoinedTable *joined_table,
                                                   ObIArray<OrExpandInfo> &trans_infos)
{
  int ret = OB_SUCCESS;
  ctx.is_unique_ = false;
  trans_infos.reuse();
  bool can_set_distinct = false;
  bool need_check_unique = false;
  ObArenaAllocator alloc;
  EqualSets &equal_sets = ctx_->equal_sets_;
  ObSEArray<ObRawExpr *, 4> const_exprs;
  OrExpandInfo trans_info;
  bool has_lob = false;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(stmt));
  } else if (OB_FAIL(stmt->get_stmt_equal_sets(equal_sets, alloc, true, true))) {
    LOG_WARN("failed to get stmt equal sets", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(stmt->get_condition_exprs(),
                                                          const_exprs))) {
    LOG_WARN("failed to compute const equivalent exprs", K(ret));
  } else if (OB_FAIL(check_select_expr_has_lob(*stmt, has_lob))) {
    LOG_WARN("failed to check stmt has lob", K(ret));
  } else if (has_lob) {
    can_set_distinct = false;
  } else if (OB_FAIL(StmtUniqueKeyProvider::check_can_set_stmt_unique(stmt, can_set_distinct))) {
    LOG_WARN("failed to check can set stmt unique", K(ret));
  }

  // get all trans_info
  for (int64_t i = 0; OB_SUCC(ret) && i < candi_conds.count(); ++i) {
    trans_info.pos_ = i;
    trans_info.is_set_distinct_ = can_set_distinct;
    if (OB_FAIL(is_condition_valid(stmt, ctx, candi_conds.at(i), expect_ordering, joined_table,
                                   equal_sets, const_exprs, trans_info))) {
      LOG_WARN("failed to check condition is valid", K(ret));
    } else if (INVALID_OR_EXPAND_TYPE == trans_info.or_expand_type_) {
      // do nothing
    } else if (OB_FAIL(trans_infos.push_back(trans_info))) {
      LOG_WARN("failed to push back trans info", K(ret));
    } else {
      need_check_unique |= trans_info.is_set_distinct_;
    }
  }
  equal_sets.reuse();

  // adjust ordering of trans_info in trans_infos
  int64_t head = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < trans_infos.count(); ++i) {
    if (trans_infos.at(i).is_set_distinct_ && i > head) {
      trans_info = trans_infos.at(i);
      trans_infos.at(i) = trans_infos.at(head);
      trans_infos.at(head++) = trans_info;
    }
  }
  if (OB_SUCC(ret) && need_check_unique &&
      OB_FAIL(ObTransformUtils::check_stmt_unique(stmt, ctx_->session_info_,
                                                  ctx_->schema_checker_, true /* strict */,
                                                  ctx.is_unique_))) {
    LOG_WARN("failed to check stmt unique", K(ret));
  }
  return ret;
}

int64_t ObTransformOrExpansion::get_or_expr_count(const ObRawExpr &expr)
{
  int64_t or_expr_count = 0;
  if (T_OP_OR == expr.get_expr_type()) {
    or_expr_count = expr.get_param_count();
  } else if (T_OP_IN == expr.get_expr_type() && OB_NOT_NULL(expr.get_param_expr(1))) {
    or_expr_count = expr.get_param_expr(1)->get_param_count();
  }
  return or_expr_count;
}

int ObTransformOrExpansion::get_expand_conds(ObSelectStmt &stmt,
                                             uint64_t trans_id,
                                             ObIArray<ObRawExpr*> *&conds_exprs)
{
  int ret = OB_SUCCESS;
  conds_exprs = NULL;
  SemiInfo *semi_info = NULL;
  JoinedTable *joined_table = NULL;
  if (OB_INVALID_ID == trans_id) {
    conds_exprs = &stmt.get_condition_exprs();
  } else if (NULL != (semi_info = stmt.get_semi_info_by_id(trans_id))) {
    conds_exprs = &semi_info->semi_conditions_;
  } else if (OB_ISNULL(joined_table = stmt.get_joined_table(trans_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect trans id", K(ret), K(trans_id));
  } else {
    conds_exprs = &joined_table->get_join_conditions();
  }
  return ret;
}

/**
 * @brief ObTransformOrExpansion::preprocess_or_condition
 * move params with subquery to the tail of the expr
 */
int ObTransformOrExpansion::preprocess_or_condition(ObSelectStmt &stmt,
                                                    const uint64_t trans_id,
                                                    const int64_t expr_pos)
{
  int ret = OB_SUCCESS;
  ObRawExpr *expr = NULL;
  ObIArray<ObRawExpr*> *conds_exprs = NULL;
  if (OB_FAIL(get_expand_conds(stmt, trans_id, conds_exprs))) {
    LOG_WARN("failed to get expand conds", K(ret));
  } else if (OB_ISNULL(conds_exprs)
             || OB_UNLIKELY(expr_pos < 0 || expr_pos >= conds_exprs->count())
             || OB_ISNULL(expr = conds_exprs->at(expr_pos))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid condition expr pos", K(expr_pos), K(conds_exprs), K(expr), K(ret));
  } else if (expr->has_flag(CNT_SUB_QUERY) && T_OP_OR == expr->get_expr_type()) {
    int64_t tail = expr->get_param_count() - 1;
    ObRawExpr *param = NULL;
    for (int64_t i = expr->get_param_count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      if (OB_ISNULL(param = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("param expr is null", K(ret));
      } else if (param->has_flag(CNT_SUB_QUERY)) {
        expr->get_param_expr(i) = expr->get_param_expr(tail);
        expr->get_param_expr(tail--) = param;
      }
    }
  }
  return ret;
}

/*
  1. normal where condition: single table scan use index
  2. inner join with join condition in where: 
  3. subquery where condition: subplan filter is less then origin plan
  4. topk: union child operator has no sort or has a prefix sort
  5. outer/semi/anti join: use nlj with exec param push down
                           or use merge/hash and origin plan is nlj
*/
int ObTransformOrExpansion::is_expected_plan(ObLogPlan *plan, void *check_ctx, bool is_trans_plan, bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObCostBasedRewriteCtx *ctx = static_cast<ObCostBasedRewriteCtx *>(check_ctx);
  ObLogicalOperator* log_set = NULL;
  is_valid = false;
  if (OB_ISNULL(ctx) || OB_ISNULL(plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (!is_trans_plan) {
    // do nothing
  } else if (OB_FAIL(find_trans_log_set(plan->get_plan_root(), ctx->trans_id_, log_set))) {
    LOG_WARN("failed to get join operator", K(ret));
  } else if (NULL == log_set) {
    //do nothing
  } else if (OB_FAIL(check_is_expected_plan(log_set, *ctx, is_valid))) {
    LOG_WARN("failed to check is expected plan", K(ret));
  } else {
    LOG_DEBUG("debug check or expansion is expected plan", K(is_valid), K(ctx->or_expand_type_),
                                                        K(*plan));
  }
  return ret;
}

int ObTransformOrExpansion::find_trans_log_set(ObLogicalOperator* op,
                                               const uint64_t trans_id,
                                               ObLogicalOperator *&log_set)
{
  int ret = OB_SUCCESS;
  log_set = NULL;
  if (OB_ISNULL(op) || OB_ISNULL(op->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null", K(ret), K(op));
  } else if (log_op_def::LOG_SET == op->get_type() && op->get_stmt()->is_set_stmt()
             && trans_id == op->get_stmt()->get_stmt_id()) {
    log_set = op;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && NULL == log_set && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(find_trans_log_set(op->get_child(i), trans_id, log_set)))) {
        LOG_WARN("failed to find operator", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::check_is_expected_plan(ObLogicalOperator* op,
                                                   ObCostBasedRewriteCtx &ctx,
                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  const uint64_t or_expand_type = ctx.or_expand_type_;
  if ((OR_EXPAND_TOP_K & or_expand_type)
      && OB_FAIL(is_expected_topk_plan(op, is_valid))) {
    LOG_WARN("failed to check is expected topk plan", K(ret));
  } else if (is_valid) {
    /* do nothing */
  } else if (OR_EXPAND_SUB_QUERY & or_expand_type) {
    is_valid = true;
  } else if (OR_EXPAND_JOIN & or_expand_type) {
    is_valid = true;
  } else if ((OR_EXPAND_MULTI_INDEX & or_expand_type)
             && OB_FAIL(is_expected_multi_index_plan(op, ctx, is_valid))) {
    LOG_WARN("failed to check is expected multi index plan", K(ret));
  }
  return ret;
}

int ObTransformOrExpansion::is_expected_topk_plan(ObLogicalOperator* op,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (NULL == op) {
    is_valid = false;
  } else if (log_op_def::LOG_SET == op->get_type()) {
    if (OB_ISNULL(op->get_stmt()) || OB_UNLIKELY(!op->get_stmt()->is_set_stmt())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect op", K(ret), K(*op));
    } else {
      // child query is a set from the same or expansion transform, need check recursive
      is_valid = true;
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < op->get_num_of_child(); ++i) {
        if (OB_FAIL(SMART_CALL(is_expected_topk_plan(op->get_child(i), is_valid)))) {
          LOG_WARN("failed to check is expected topk plan", K(ret));
        }
      }
    }
  } else {
    while (NULL != op && log_op_def::LOG_TABLE_SCAN != op->get_type() && !op->is_block_op()) {
      op = op->get_child(0);
    }
    if (NULL == op || op->is_block_op()) {
      is_valid = false;
    } else if (log_op_def::LOG_TABLE_SCAN == op->get_type()) {
      is_valid = true;
    }
  }
  return ret;
}

int ObTransformOrExpansion::is_expected_multi_index_plan(ObLogicalOperator* op,
                                                         ObCostBasedRewriteCtx &ctx,
                                                         bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  const int64_t N = ctx.expand_exprs_.count();
  ObSEArray<ObRawExpr*, 4> candi_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < N; ++i) {
    candi_exprs.reuse();
    if (OB_FAIL(get_candi_match_index_exprs(ctx.expand_exprs_.at(i), candi_exprs))) {
      LOG_WARN("failed to get candi match index exprs", K(ret));
    } else if (OB_FAIL(remove_filter_exprs(op, candi_exprs))) {
      LOG_WARN("failed to remove filter exprs", K(ret));
    } else if (candi_exprs.empty()) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformOrExpansion::remove_filter_exprs(ObLogicalOperator* op,
                                                ObIArray<ObRawExpr*> &candi_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || candi_exprs.empty()) {
    /* do nothing */
  } else if (!op->get_filter_exprs().empty() &&
             OB_FAIL(ObOptimizerUtil::except_exprs(candi_exprs,
                                                   op->get_filter_exprs(),
                                                   candi_exprs))) {
    LOG_WARN("failed to get except exprs", K(ret));
  } else if (log_op_def::LOG_JOIN == op->get_type() &&
             !static_cast<ObLogJoin*>(op)->get_join_filters().empty() &&
             OB_FAIL(ObOptimizerUtil::except_exprs(candi_exprs,
                                                   static_cast<ObLogJoin*>(op)->get_join_filters(),
                                                   candi_exprs))) {
    LOG_WARN("failed to get except exprs", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !candi_exprs.empty() && i < op->get_num_of_child(); ++i) {
      if (OB_FAIL(SMART_CALL(remove_filter_exprs(op->get_child(i), candi_exprs)))) {
        LOG_WARN("failed to remove filter exprs", K(ret));
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::is_candi_match_index_exprs(ObRawExpr *expr, bool &result)
{
  int ret = OB_SUCCESS;
  result = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect NULL", K(ret));
  } else if (expr->has_flag(IS_SIMPLE_COND) || expr->has_flag(IS_RANGE_COND)
             || T_OP_IN == expr->get_expr_type()) {
    if (expr->get_relation_ids().is_empty()) {
      // can not use as range candition
      result = false;
    }
  } else if (T_OP_OR == expr->get_expr_type() || T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && result && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(is_candi_match_index_exprs(expr->get_param_expr(i),
                                                        result)))) {
        LOG_WARN("failed to check is candi match index exprs", K(ret));
      }
    }
  } else if (expr->has_flag(IS_ROWID_SIMPLE_COND) ||
             expr->has_flag(IS_ROWID_RANGE_COND)) {
    //rowid = const or rowid belong const range can choose primary key.
  } else {
    result = false;
  }
  return ret;
}

int ObTransformOrExpansion::get_candi_match_index_exprs(ObRawExpr *expr,
                                                        ObIArray<ObRawExpr*> &candi_exprs)
{
  int ret = OB_SUCCESS;
  bool is_candi = true;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect NULL", K(ret));
  } else if (T_OP_AND == expr->get_expr_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      if (OB_FAIL(SMART_CALL(get_candi_match_index_exprs(expr->get_param_expr(i),
                                                         candi_exprs)))) {
        LOG_WARN("failed to get candi match index exprs", K(ret));
      }
    }
  } else if (OB_FAIL(is_candi_match_index_exprs(expr, is_candi))) {
    LOG_WARN("failed to check is candi match index exprs", K(ret));
  } else if (is_candi && OB_FAIL(candi_exprs.push_back(expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {/*do nothing*/}
  return ret;
}

// check select items contain lob type or other uncomparable type
int ObTransformOrExpansion::check_select_expr_has_lob(ObDMLStmt &stmt, bool &has_lob)
{
  int ret = OB_SUCCESS;
  has_lob = false;
  if (stmt.is_select_stmt()) {
    ObIArray<SelectItem> &select_items = static_cast<ObSelectStmt&>(stmt).get_select_items();
    ObRawExpr *select_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !has_lob && i < select_items.count(); ++i) {
      if (OB_ISNULL(select_expr = select_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret), K(select_items.at(i)));
      } else {
        has_lob = ObLongTextType == select_expr->get_data_type() ||
                  ObLobType == select_expr->get_data_type() ||
                  (is_oracle_mode() && ObJsonType == select_expr->get_data_type());
      }
    }
  } else if (!stmt.is_update_stmt() && !stmt.is_delete_stmt()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect stmt type", K(ret), K(stmt.get_stmt_type()));
  } else {
    const ObIArray<ColumnItem> &column_items = static_cast<ObDelUpdStmt&>(stmt).get_column_items();
    ObRawExpr *column_expr = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && !has_lob && i < column_items.count(); ++i) {
      if (OB_ISNULL(column_expr = column_items.at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null expr", K(ret), K(column_items.at(i)));
      } else {
        has_lob = ObLongTextType == column_expr->get_data_type() ||
                  ObLobType == column_expr->get_data_type() ||
                  (is_oracle_mode() && ObJsonType == column_expr->get_data_type());
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::construct_transform_hint(ObDMLStmt &stmt, void *trans_params)
{
  int ret = OB_SUCCESS;
  ObOrExpandHint *hint = NULL;
  const ObRawExpr *expr = NULL;
  ObCostBasedRewriteCtx *eval_cost_ctx = static_cast<ObCostBasedRewriteCtx*>(trans_params);
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->allocator_) || OB_ISNULL(eval_cost_ctx)
      || OB_ISNULL(expr = eval_cost_ctx->orig_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(ctx_), K(eval_cost_ctx), K(expr));
  } else if (OB_FAIL(ctx_->add_used_trans_hint(eval_cost_ctx->hint_))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (OB_FAIL(ObQueryHint::create_hint(ctx_->allocator_, T_USE_CONCAT, hint))) {
    LOG_WARN("failed to create hint", K(ret));
  } else if (OB_FAIL(ctx_->outline_trans_hints_.push_back(hint))) {
    LOG_WARN("failed to push back hint", K(ret));
  } else if (OB_FAIL(hint->set_expand_condition(*ctx_->allocator_, *expr))) {
    LOG_WARN("failed to set expand condition", K(ret));
  } else {
    hint->set_qb_name(ctx_->src_qb_name_);
  }
  return ret;
}

// Classify or expr to get union count before do_transform
int ObTransformOrExpansion::pre_classify_or_expr(const ObRawExpr *expr, int &count)
{
  int ret = OB_SUCCESS;
  ObSEArray<TableColBitSet, 4> table_col_bit_sets;
  count = 0;
  if (OB_ISNULL(expr) || T_OP_OR != expr->get_expr_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      TableColBitSet table_col_bit_set;
      bool from_same_table = true;
      int64_t table_id = OB_INVALID_ID;
      const ObRawExpr *branch = NULL;
      if (OB_ISNULL(branch = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (branch->has_flag(CNT_SUB_QUERY) || branch->get_relation_ids().is_empty()) {
        // conditions with subqueries will be classfied separately
        // irrelevant conditions will be classfied separately
        ++count;
      } else if (OB_FAIL(extract_columns(branch,
                                         table_id,
                                         from_same_table,
                                         table_col_bit_set.column_bit_set_))) {
        LOG_WARN("failed to extract columns info", K(ret), KPC(branch));
      } else if (!from_same_table) {
        // conditions of multiple tables will be classfied separately
        ++count;
      } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table id", K(ret), KPC(branch), K(table_id));
      } else if (FALSE_IT(table_col_bit_set.table_id_ = table_id)) {
      } else if (!ObOptimizerUtil::find_item(table_col_bit_sets,
                                             table_col_bit_set)) {
        if (OB_FAIL(table_col_bit_sets.push_back(table_col_bit_set))) {
          LOG_WARN("failed to push back bit set", K(ret));
        } else {
          ++count;
        }
      }
    }
  }
  return ret;
}

// Classify isomorphic predicates into the same class
// For example:
//     select * from t1,t2 where t1.a=1 or t1.a=t2.b or t1.a=t2.b+1 or t1.a>2 or t1.c=3
//  => select * from t1,t2 where (t1.a=1 or t1.a>2) or (t1.a=t2.b) or (t1.a=t2.b+1) or (t1.c=3)
int ObTransformOrExpansion::classify_or_expr(const ObDMLStmt &stmt, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected or expr", K(ret));
  } else if (2 == expr->get_param_count()
    || T_OP_OR != expr->get_expr_type()) {
    // do nothing
  } else {
    ObSEArray<ObRawExpr*, 4> expr_classes;
    ObSEArray<TableColBitSet, 4> table_col_bit_sets;
    bool classify_happened = false;
    for (int64_t i = 0; OB_SUCC(ret) &&
         i < expr->get_param_count(); ++i) {
      TableColBitSet table_col_bit_set;
      bool from_same_table = true;
      int64_t isomorphism_idx = OB_INVALID_ID;
      int64_t table_id = OB_INVALID_ID;
      ObRawExpr *branch = NULL;
      if (OB_ISNULL(branch = expr->get_param_expr(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (branch->has_flag(CNT_SUB_QUERY)) {
        // conditions with subqueries will be classfied separately
      } else if (branch->get_relation_ids().is_empty()) {
        // irrelevant conditions will be classfied separately
      } else if (OB_FAIL(extract_columns(branch,
                                         table_id,
                                         from_same_table,
                                         table_col_bit_set.column_bit_set_))) {
        LOG_WARN("failed to extract columns info", K(ret), KPC(branch));
      } else if (!from_same_table) {
        table_id =  OB_INVALID_ID;
        // conditions of multiple tables will be classfied separately
      } else if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid table id", K(ret), KPC(branch), K(table_id));
      } else if (FALSE_IT(table_col_bit_set.table_id_ = table_id)) {
        // never reach
      } else if (!ObOptimizerUtil::find_item(table_col_bit_sets,
                                             table_col_bit_set,
                                             &isomorphism_idx)) {
        isomorphism_idx = OB_INVALID_ID;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_INVALID_ID == isomorphism_idx) {
        // contain subquery or irrelevant expr or not from same table or not found,
        // create a new class
        if (OB_FAIL(table_col_bit_sets.push_back(table_col_bit_set))) {
          LOG_WARN("failed to push back bit set", K(ret));
        } else if (OB_FAIL(expr_classes.push_back(branch))) {
          LOG_WARN("failed to push back expr class", K(ret));
        }
      } else {
        //merge the expr into the partition
        classify_happened = true;
        ObRawExpr*& expr_class = expr_classes.at(isomorphism_idx);
        if (OB_FAIL(merge_expr_class(expr_class, branch))) {
          LOG_WARN("failed to merge expr class", K(ret));
        }
      }
    }

    if (OB_FAIL(ret) || !classify_happened) {
    } else if (1 == expr_classes.count()) {
      //do nothing
    } else {
      //modify the expr according to the partitions
      ObOpRawExpr *op_expr = static_cast<ObOpRawExpr *>(expr);
      op_expr->clear_child();
      for (int64_t i = 0; OB_SUCC(ret) && i < expr_classes.count(); i++) {
        if (OB_FAIL(op_expr->add_param_expr(expr_classes.at(i)))) {
          LOG_WARN("failed to add param expr", K(ret));
        }
      }
    }
    LOG_DEBUG("or expr after classify", KPC(expr));
  }
  return ret;
}

int ObTransformOrExpansion::merge_expr_class(ObRawExpr *&expr_class, ObRawExpr *expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *new_or_expr = NULL;
  if (OB_ISNULL(expr_class) || OB_ISNULL(expr) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (T_OP_OR == expr_class->get_expr_type()) {
    new_or_expr = static_cast<ObOpRawExpr *>(expr_class);
    if (OB_FAIL(new_or_expr->add_param_expr(expr))) {
      LOG_WARN("add param expr to or expr failed", K(ret));
    }
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_OP_OR, new_or_expr))) {
    LOG_WARN("failed to create or expr", K(ret));
  } else if (OB_FAIL(new_or_expr->add_param_expr(expr_class))) {
    LOG_WARN("add param expr to or expr failed", K(ret));
  } else if (OB_FAIL(new_or_expr->add_param_expr(expr))) {
    LOG_WARN("add param expr to or expr failed", K(ret));
  } else {
    expr_class = new_or_expr;
  }
  return ret;
}

int ObTransformOrExpansion::get_condition_related_tables(ObSelectStmt &stmt,
                                                         int64_t expr_pos,
                                                         const ObIArray<ObRawExpr*> &conds_exprs,
                                                         bool &create_view,
                                                         ObIArray<TableItem *> &or_expr_tables,
                                                         ObIArray<SemiInfo *> &or_semi_infos)
{
  int ret = OB_SUCCESS;
  create_view = false;
  or_expr_tables.reuse();
  or_semi_infos.reuse();
  bool contain_shared_subqueries = false;
  ObRelIds &or_expr_rel_ids = conds_exprs.at(expr_pos)->get_relation_ids();
  //  do not create view if or_expr has no rel_ids
  //  for example: select * from t1
  //               where (exists (select 1 from t2) or exists (select 1 from t3))
  //                     and (exists (select 1 from t4))
  if (!or_expr_rel_ids.is_empty()) {
    // create view only if:
    // 1. the expansion cond does not contain shared subqueries; and
    ObSEArray<ObQueryRefRawExpr *, 4> expand_subqueries;
    if (OB_FAIL(ObTransformUtils::extract_query_ref_expr(conds_exprs.at(expr_pos),
                                                         expand_subqueries,
                                                         true/*with_nested*/))) {
      LOG_WARN("failed to extract subqueries", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !contain_shared_subqueries && i < expand_subqueries.count(); i++) {
      if (OB_ISNULL(expand_subqueries.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret));
      } else if (expand_subqueries.at(i)->get_ref_count() > 1){
        contain_shared_subqueries = true;
      }
    }
    if (OB_SUCC(ret) && !contain_shared_subqueries) {
      // 2. a. other conds contain subqueries; or
      for (int64_t i = 0; !create_view && OB_SUCC(ret) && i < conds_exprs.count(); i++) {
        if (i != expr_pos) {
          if (OB_ISNULL(conds_exprs.at(i))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null", K(ret));
          } else {
            create_view = conds_exprs.at(i)->has_flag(CNT_SUB_QUERY);
          }
        }
      }
      //   b. rel_ids of expr is the proper subset of stmt table ids
      //      and is related to only one basic table
      ObSqlBitSet<> all_table_set;
      bool related_to_only_one = false;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(stmt.get_table_rel_ids(stmt.get_table_items(), all_table_set))) {
        LOG_WARN("failed to get table ids", K(ret));
      } else if (!or_expr_rel_ids.equal(all_table_set) && or_expr_rel_ids.num_members() == 1) {
        // the only one related table should not be in a joined table
        if (OB_FAIL(stmt.relids_to_table_items(or_expr_rel_ids, or_expr_tables))) {
          LOG_WARN("failed to get table items", K(ret));
        } else if (OB_UNLIKELY(or_expr_tables.count() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected table count", K(ret));
        } else if (OB_FAIL(check_valid_rel_table(stmt, or_expr_rel_ids, or_expr_tables.at(0), related_to_only_one))) {
          LOG_WARN("failed to check valid rel table", K(ret));
        } else if (related_to_only_one) {
          create_view = true;
        }
      }
      if (OB_SUCC(ret) && create_view && !related_to_only_one) {
        // There are exprs to be delayed
        // and the or expr is related to more than one tables.
        // We need create view for all tables.
        // e.g. select * from t1, t2, t3
        //      where t1.a = t2.a and t2.b = t3.b and
        //            (t1.c = 1 or t3.c = 1) and
        //            t1.d in (select d from t4 where t4.a > t1.a limit 10)
        // =>   select * from
        //        (select * from t1, t2, t3
        //         where t1.a = t2.a and t2.b = t3.b and
        //            (t1.c = 1 or t3.c = 1)) v
        //      where v.d in (select d from t4 where t4.a > v.a limit 10)
        or_expr_tables.reuse();
        if (OB_FAIL(or_semi_infos.assign(stmt.get_semi_infos()))) {
          LOG_WARN("failed to assign semi infos", K(ret));
        } else if (OB_FAIL(stmt.get_from_tables(or_expr_tables))) {
          LOG_WARN("failed to get from tables", K(ret));
        }
      }
    }
  }
  return ret;
}

// Create a view of partial tables which are related to the condition
// to avoid repeated computation of subqueries or joins
// For example:
//     select * from t1, t2 where (t1.a = 1 or t1.b = 1) and t1.c = t2.c
//  => select * from (select * from t1 where (t1.a = 1 or t1.b = 1)) v, t2 where v.c = t2.c
//
//     select * from t1 where (t1.a = 1 or t1.a = 2) and exists (select 1 from t2 where t1.a=t2.a)
//  => select * from (select * from t1 where (t1.a = 1 or t1.a = 2)) v
//     where exists (select 1 from t2 where v.a=t2.a)
int ObTransformOrExpansion::get_condition_related_view(ObSelectStmt *stmt,
                                                       ObSelectStmt *&view_stmt,
                                                       TableItem *&view_table,
                                                       int64_t& expr_pos,
                                                       ObIArray<ObRawExpr*> *&conds_exprs,
                                                       bool &is_set_distinct)
{
  int ret = OB_SUCCESS;
  ObStmtFactory *stmt_factory = NULL;
  ObRawExprFactory *expr_factory = NULL;
  view_stmt = stmt;
  view_table = NULL;
  bool create_view = false;
  ObSEArray<TableItem *, 4> or_expr_tables;
  ObSEArray<SemiInfo *, 4> or_semi_infos;
  ObSqlBitSet<> table_set;
  int64_t new_expr_pos = OB_INVALID_ID;
  if (OB_ISNULL(ctx_) || OB_ISNULL(stmt) || OB_ISNULL(stmt_factory = ctx_->stmt_factory_)
      || OB_ISNULL(expr_factory = ctx_->expr_factory_) || OB_ISNULL(conds_exprs)
      || OB_UNLIKELY(!stmt->is_select_stmt()) || OB_UNLIKELY(expr_pos < 0)
      || OB_UNLIKELY(expr_pos >= conds_exprs->count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("have invalid params", K(ret), K(ctx_), K(stmt), K(stmt_factory),
             K(expr_factory), K(conds_exprs), K(expr_pos),
             K(stmt->is_select_stmt()));
  } else if (OB_FAIL(get_condition_related_tables(*stmt, expr_pos, *conds_exprs,
                                                  create_view, or_expr_tables,
                                                  or_semi_infos))) {
    LOG_WARN("failed to get condition related tables", K(ret));
  } else if (!create_view) {
    // do nothing
  } else {
    // push down a predicate, if:
    // 1. it is the or expansion cond; or
    // 2. a. it does not contain not onetime subquery; and
    //    b. it is only related to view tables
    ObSEArray<ObRawExpr *, 4> push_conditions;
    if (OB_FAIL(stmt->get_table_rel_ids(or_expr_tables, table_set))) {
      LOG_WARN("failed to get table rel ids", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < conds_exprs->count(); i++) {
      ObRawExpr *cond = NULL;
      if (OB_ISNULL(cond = (conds_exprs->at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (i != expr_pos &&
                 (conds_exprs->at(i)->has_flag(CNT_SUB_QUERY) ||
                  !cond->get_relation_ids().is_subset(table_set))) {
        // do not push
      } else if (OB_FAIL(push_conditions.push_back(cond))) {
        LOG_WARN("failed to push cond", K(ret));
      } else if (i == expr_pos){
        new_expr_pos = push_conditions.count() - 1;
      }
    }
    // get push down conditions
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(ObOptimizerUtil::remove_item(*conds_exprs,
                                                    push_conditions))) {
      LOG_WARN("failed to remove pushed conditions", K(ret));
    } else if (OB_FAIL(ObTransformUtils::replace_with_empty_view(ctx_,
                                                                 stmt,
                                                                 view_table,
                                                                 or_expr_tables,
                                                                 &or_semi_infos))) {
      LOG_WARN("failed to create empty view table", K(ret));
    } else if (OB_FAIL(ObTransformUtils::create_inline_view(ctx_,
                                                            stmt,
                                                            view_table,
                                                            or_expr_tables,
                                                            &push_conditions,
                                                            &or_semi_infos))) {
      LOG_WARN("failed to create inline view", K(ret));
    } else if (OB_ISNULL(view_table) || OB_ISNULL(view_stmt = view_table->ref_query_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("view table is null", K(ret), K(view_table), K(view_stmt));
    } else {
      expr_pos = new_expr_pos;
      conds_exprs = &view_stmt->get_condition_exprs();
      if (is_set_distinct) {
        bool has_lob = false;
        if (OB_FAIL(check_select_expr_has_lob(*view_stmt, has_lob))) {
          LOG_WARN("failed to check lob", K(ret));
        } else {
          is_set_distinct = !has_lob;
        }
      }
    }
  }
  return ret;
}

// an expression computation will be delayed
// iff it contains a correlated subquery
int ObTransformOrExpansion::check_delay_expr(ObRawExpr* expr, bool &delay) {
  int ret = OB_SUCCESS;
  if (!delay) {
    if (expr->is_query_ref_expr()) {
      delay = expr->get_param_count() != 0;
    } else if (expr->has_flag(CNT_SUB_QUERY)){
      for (int64_t i = 0; OB_SUCC(ret) && !delay && i < expr->get_param_count(); i++) {
        if (OB_FAIL(SMART_CALL(check_delay_expr(expr->get_param_expr(i), delay)))) {
          LOG_WARN("failed to check expr to be delayed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTransformOrExpansion::check_valid_rel_table(ObSelectStmt &stmt,
                                                  ObRelIds &rel_ids,
                                                  TableItem *rel_table,
                                                  bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  bool join_subset = false;
  bool left_bottom = false;
  ObIArray<JoinedTable*>& joined_tables = stmt.get_joined_tables();
  for (int64_t i = 0; OB_SUCC(ret) && !join_subset && i < joined_tables.count(); i++) {
    ObSqlBitSet<> table_rel_ids;
    TableItem *table = NULL;
    if (OB_ISNULL(table = joined_tables.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (OB_FAIL(stmt.get_table_rel_ids(*table,
                                              table_rel_ids))) {
      LOG_WARN("failed to get table rel id", K(ret));
    } else if (FALSE_IT(join_subset = rel_ids.is_subset(table_rel_ids))) {
      // never reach
    } else if (join_subset &&
               OB_FAIL(check_left_bottom_table(stmt,
                                               rel_table,
                                               joined_tables.at(i),
                                               left_bottom))) {
      LOG_WARN("failed to check in join tables", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    is_valid = !(join_subset && !left_bottom);
  }
  return ret;
}

int ObTransformOrExpansion::check_left_bottom_table(ObSelectStmt &stmt,
                                                    TableItem *rel_table,
                                                    TableItem *table,
                                                    bool &left_bottom)
{
  int ret = OB_SUCCESS;
  left_bottom = false;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret));
  } else if (table->is_joined_table()) {
    JoinedTable *joined_table = static_cast<JoinedTable *>(table);
    if (joined_table->is_left_join() || joined_table->is_right_join()) {
      TableItem* left = joined_table->is_left_join() ?
                        joined_table->left_table_ :
                        joined_table->right_table_;
      if (OB_FAIL(SMART_CALL(check_left_bottom_table(stmt,
                                                     rel_table,
                                                     left,
                                                     left_bottom)))) {
        LOG_WARN("failed to check in joined tables", K(ret));
      }
    }
  } else if (rel_table == table){
    left_bottom = true;
  }
  return ret;
}

int ObTransformOrExpansion::check_stmt_valid_for_expansion(ObDMLStmt *stmt, bool &is_stmt_valid)
{
  int ret = OB_SUCCESS;
  bool has_lob = false;
  is_stmt_valid = true;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected param", K(ret));
  } else if (OB_FAIL(check_select_expr_has_lob(*stmt, has_lob))) {
    LOG_WARN("failed to check stmt has lob", K(ret));
  } else if (has_lob && stmt->is_select_stmt() &&
             static_cast<ObSelectStmt*>(stmt)->has_distinct()) {
    is_stmt_valid = false;
    OPT_TRACE("select expr has lob expr and distinct");
  }
  for (int64_t i = 0; OB_SUCC(ret) && is_stmt_valid && i < stmt->get_table_size(); i++) {
    TableItem *table_item = stmt->get_table_items().at(i);
    if (OB_ISNULL(table_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else if (table_item->is_fake_cte_table()) {
      is_stmt_valid = false;
      OPT_TRACE("contain fake cte table");
    }
  }
  return ret;
}

} /* namespace sql */
} /* namespace oceanbase */
