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
#include "ob_transform_min_max.h"
#include "ob_transformer_impl.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/oblog/ob_log_module.h"
#include "common/ob_common_utility.h"
#include "common/ob_smart_call.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/expr/ob_raw_expr_resolver_impl.h"
#include "sql/ob_sql_context.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "sql/optimizer/ob_optimizer_util.h"
using namespace oceanbase::common;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace sql
{

int ObTransformMinMax::MinMaxAggrHelper::assign(const MinMaxAggrHelper &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aggr_expr_ids_.assign(other.aggr_expr_ids_))) {
    LOG_WARN("failed to assign aggr expr ids", K(ret));
  } else {
    raw_expr_id_ = other.raw_expr_id_;
    raw_expr_ptr_ = other.raw_expr_ptr_;
  }
  return ret;
}

int ObTransformMinMax::MinMaxAggrHelper::alloc_helper(ObIAllocator &allocator, MinMaxAggrHelper* &helper)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  if (NULL == (buf = allocator.alloc(sizeof(MinMaxAggrHelper)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret));
  } else {
    helper = new(buf)MinMaxAggrHelper();
  }
  return ret;
}

ObTransformMinMax::ObTransformMinMax(ObTransformerCtx *ctx)
    : ObTransformRule(ctx, TransMethod::POST_ORDER, T_FAST_MINMAX)
{
}

ObTransformMinMax::~ObTransformMinMax()
{
}

int ObTransformMinMax::transform_one_stmt(common::ObIArray<ObParentDMLStmt> &parent_stmts,
                                          ObDMLStmt *&stmt,
                                          bool &trans_happened)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSEArray<MinMaxAggrHelper*, 2> selecthelpers;
  ObSEArray<MinMaxAggrHelper*, 2> havinghelpers;
  trans_happened = false;
  UNUSED(parent_stmts);
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(stmt), K(ctx_));
  } else if (!stmt->is_select_stmt()) {
    //do nothing
    OPT_TRACE("not select stmt");
  } else if (OB_FAIL(check_transform_validity(*ctx_,
                                              static_cast<ObSelectStmt *>(stmt),
                                              is_valid,
                                              &selecthelpers,
                                              &havinghelpers))) {
    LOG_WARN("failed to check transform validity", K(ret));
  } else if (!is_valid) {
    //do nothing
    OPT_TRACE("can not transform");
  } else if (OB_FAIL(do_transform(stmt, selecthelpers, havinghelpers))) {
    LOG_WARN("failed to transform column aggregate", K(ret));
  } else if (OB_FAIL(add_transform_hint(*stmt))) {
    LOG_WARN("failed to add transform hint", K(ret));
  } else {
    trans_happened = true;
  }
  //destruct helpers
  for (int64_t i = 0; i < selecthelpers.count(); ++i) {
    if (NULL != selecthelpers.at(i)) {
      selecthelpers.at(i)->~MinMaxAggrHelper();
      selecthelpers.at(i) = NULL;
    }
  }
  for (int64_t i = 0; i < havinghelpers.count(); ++i) {
    if (NULL != havinghelpers.at(i)) {
      havinghelpers.at(i)->~MinMaxAggrHelper();
      havinghelpers.at(i) = NULL;
    }
  }
  return ret;
}

int ObTransformMinMax::check_expr_validity(ObTransformerCtx &ctx,
                                           ObSelectStmt *select_stmt,
                                           const ObRawExpr *expr,
                                           const int64_t expr_id,
                                           bool &is_valid,
                                           MinMaxAggrHelper *&helper)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  if (expr->is_const_expr()) {
    if (OB_FAIL(MinMaxAggrHelper::alloc_helper(*ctx.allocator_, helper))) {
      LOG_WARN("failed to allocate helper", K(ret));
    } else if (OB_ISNULL(helper)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null helper", K(ret));
    } else {
      helper->aggr_expr_ids_.reuse();
      helper->raw_expr_id_ = expr_id;
      helper->raw_expr_ptr_ = const_cast<ObRawExpr*>(expr);
    }
  } else {
    ObSEArray<int64_t, 2> aggr_expr_ids;
    if (OB_FAIL(check_valid_aggr_expr(expr, select_stmt->get_aggr_items(), aggr_expr_ids, is_valid))) {
      LOG_WARN("failed to check valid aggr expr", K(ret));
    } else if (!is_valid) {
      // do nothing
    } else if (OB_FAIL(MinMaxAggrHelper::alloc_helper(*ctx.allocator_, helper))) {
      LOG_WARN("failed to allocate helper", K(ret));
    } else if (OB_ISNULL(helper)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null helper", K(ret));
    } else {
      if (OB_FAIL(helper->aggr_expr_ids_.assign(aggr_expr_ids))) {
        LOG_WARN("failed to assign aggr expr ids", K(ret));
      } else {
        helper->raw_expr_id_ = expr_id;
        helper->raw_expr_ptr_ = const_cast<ObRawExpr*>(expr);
      }
    }
  }
  return ret;
}                                           
int ObTransformMinMax::check_transform_validity(ObTransformerCtx &ctx,
                                                ObSelectStmt *select_stmt,
                                                bool &is_valid,
                                                ObIArray<MinMaxAggrHelper*> *selecthelpers,  /* = NULL */
                                                ObIArray<MinMaxAggrHelper*> *havinghelpers /* = NULL */)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_ISNULL(select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(select_stmt));
  } else if (select_stmt->has_recursive_cte() || select_stmt->has_hierarchical_query()) {
    OPT_TRACE("stmt has recusive cte or hierarchical query");
  } else if (select_stmt->get_from_item_size() != 1 ||
             select_stmt->get_from_item(0).is_joined_ ||
             !select_stmt->is_scala_group_by()) {
    OPT_TRACE("not a simple query");
  } else if (select_stmt->get_aggr_item_size() < 1) {
    OPT_TRACE("stmt has not agg expr");
  } else if (select_stmt->get_aggr_item_size() > 1 && select_stmt->get_condition_size() > 0) {
    OPT_TRACE("need to evaluate by cost estimation");
  } else {
    is_valid = true;
    ObArenaAllocator alloc;
    EqualSets &equal_sets = ctx.equal_sets_;
    ObSEArray<ObRawExpr *, 4> const_exprs;
    if (OB_FAIL(select_stmt->get_stmt_equal_sets(equal_sets, alloc, true))) {
      LOG_WARN("failed to get stmt equal sets", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::compute_const_exprs(select_stmt->get_condition_exprs(),
                                                            const_exprs))) {
      LOG_WARN("failed to compute const equivalent exprs", K(ret));
    } 
    // 1. check each aggr expr is min/max aggr expr and has index
    for (int i = 0; OB_SUCC(ret) && is_valid && i < select_stmt->get_aggr_item_size(); i++) {
      ObAggFunRawExpr* aggr_expr = select_stmt->get_aggr_item(i);
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("params have null", K(ret), KP(aggr_expr));
      } else if ((T_FUN_MAX != aggr_expr->get_expr_type() && T_FUN_MIN != aggr_expr->get_expr_type()) ||
              aggr_expr->get_real_param_count() != 1) {
        OPT_TRACE("aggr expr is not min/max expr");
        is_valid = false;
      } else if (OB_FAIL(is_valid_index_column(ctx, select_stmt, aggr_expr->get_param_expr(0), equal_sets, const_exprs, is_valid))) {
        LOG_WARN("failed to check is valid index column", K(ret));
      } else if (!is_valid) {
        OPT_TRACE("aggr expr is not include index column");
      } 
    }
    // 2. check each select item is const expr or contain one min/max aggr expr
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < select_stmt->get_select_item_size(); i++) {
      ObRawExpr *select_expr = select_stmt->get_select_item(i).expr_;
      MinMaxAggrHelper *helper = NULL;
      if (OB_ISNULL(select_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("params have null", K(ret));
      } else if (OB_FAIL(check_expr_validity(ctx, select_stmt, select_expr, i, is_valid, helper))) {
        LOG_WARN("failed to check expr validity", K(ret));
      } else if (NULL == selecthelpers) {
        // do nothing
      } else if (OB_FAIL(selecthelpers->push_back(helper))) {
        LOG_WARN("failed to push back having helper", K(ret));
      } 
    }
    // 3. check having exprs contain one min/max aggr expr
    if (select_stmt->has_having()) {
      for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < select_stmt->get_having_expr_size(); i++) {
        ObRawExpr *having_expr = select_stmt->get_having_exprs().at(i);
        MinMaxAggrHelper *helper = NULL;
        if (OB_ISNULL(having_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("params have null", K(ret));
        } else if (OB_FAIL(check_expr_validity(ctx, select_stmt, having_expr, i, is_valid, helper))) {
          LOG_WARN("failed to check expr validity", K(ret));
        } else if (NULL == havinghelpers) {
          // do nothing
        } else if (OB_FAIL(havinghelpers->push_back(helper))) {
          LOG_WARN("failed to push back having helper", K(ret));
        }
      }
    }
    equal_sets.reuse();
    if (OB_FAIL(ret)) {
      is_valid = false;
    }
  }
  return ret;
}

int ObTransformMinMax::create_new_ref_expr(ObQueryRefRawExpr *&ref_expr, 
                                           ObSelectStmt *aggr_ref_stmt, 
                                           ObRawExpr* ori_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(ctx_));
  } else if (OB_FAIL(ctx_->expr_factory_->create_raw_expr(T_REF_QUERY, ref_expr))) {
    LOG_WARN("failed to create ref query expr", K(ret));
  } else if (OB_ISNULL(ref_expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret));
  } else {
    ref_expr->set_output_column(aggr_ref_stmt->get_select_item_size());
    for (int64_t i = 0; OB_SUCC(ret) && i < aggr_ref_stmt->get_select_item_size(); ++i) {
      ObRawExpr *target_expr = aggr_ref_stmt->get_select_item(i).expr_;
      if (OB_ISNULL(target_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("target expr is null");
      } else {
        const ObExprResType &column_type = target_expr->get_result_type();
        if (OB_FAIL(ref_expr->add_column_type(column_type))) {
          LOG_WARN("add column type to subquery ref expr failed", K(ret));
        } else if (column_type.is_lob_storage() && !IS_CLUSTER_VERSION_BEFORE_4_1_0_0) {
          ObExprResType &last_item = ref_expr->get_column_types().at(ref_expr->get_column_types().count() - 1);
          last_item.set_has_lob_header();
        }
      }
    }
    ref_expr->set_ref_stmt(aggr_ref_stmt);
    ref_expr->set_alias_column_name(ori_expr->get_alias_column_name());
    ref_expr->set_expr_name(ori_expr->get_expr_name());
  }
  return ret;
}

int ObTransformMinMax::do_transform_one_stmt(ObSelectStmt *select_stmt, ObAggFunRawExpr *aggr_expr, ObSelectStmt *&ref_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(select_stmt) || OB_ISNULL(aggr_expr) ||
      OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(select_stmt), K(aggr_expr), K(ctx_));
  } else {
    ObSelectStmt *child_stmt = NULL;
    ObRawExpr *new_tmp_aggr_expr = NULL;
    ObSEArray<ObRawExpr*, 1> old_exprs;
    ObSEArray<ObRawExpr*, 1> new_exprs;
    ObRawExprCopier copier(*ctx_->expr_factory_);
    if (OB_FAIL(ObTransformUtils::create_simple_view(ctx_, select_stmt, child_stmt))) {
      LOG_WARN("failed to create simple view", K(ret));
    } else if (OB_FAIL(select_stmt->get_column_exprs(old_exprs))) {
      LOG_WARN("failed to get column exprs", K(ret));
    } else if (OB_FAIL(child_stmt->get_select_exprs(new_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    }
    if (OB_FAIL(copier.add_replaced_expr(old_exprs, new_exprs))) {
      LOG_WARN("failed to add replace pair", K(ret));
    } else if (OB_FAIL(copier.copy(aggr_expr, new_tmp_aggr_expr))) {
      LOG_WARN("failed to copy expr", K(ret));
    } else if (OB_FAIL(set_child_condition(child_stmt, new_tmp_aggr_expr))) {
      LOG_WARN("fail to set child condition", K(ret));
    } else if (OB_FAIL(set_child_order_item(child_stmt, new_tmp_aggr_expr))) {
      LOG_WARN("fail to set child order item", K(ret));
    } else if (OB_FAIL(ObTransformUtils::set_limit_expr(child_stmt, ctx_))) {
      LOG_WARN("fail to set child limit item", K(ret));
    } else {
      ref_stmt = child_stmt;
      LOG_TRACE("Succeed to do transform min max", K(*select_stmt));
    }
  }
  return ret;
}

int ObTransformMinMax::do_transform(ObDMLStmt *&stmt, ObIArray<MinMaxAggrHelper*> &selecthelpers, ObIArray<MinMaxAggrHelper*> &havinghelpers)
{
  int ret = OB_SUCCESS;
  ObSelectStmt *select_stmt = NULL;
  if (OB_ISNULL(stmt) || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->expr_factory_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(stmt), K(ctx_));
  } else if (OB_ISNULL(select_stmt = static_cast<ObSelectStmt*>(stmt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("static cast params null", K(ret), K(stmt));
  } else {
    // delete having exprs in the stmt, because having exprs cannot push down to subquery
    // will transform having exprs as where exprs in new stmt
    select_stmt->get_having_exprs().reuse();
    ObSelectStmt *new_stmt = NULL;
    // create new stmt
    if (OB_FAIL(ctx_->stmt_factory_->create_stmt(new_stmt))) {
      LOG_WARN("failed to create stmt", K(ret));
    } else if (OB_ISNULL(new_stmt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(new_stmt));
    } else if (OB_FAIL(new_stmt->get_stmt_hint().set_simple_view_hint(&stmt->get_stmt_hint()))) {
      LOG_WARN("failed to set simple view hint", K(ret));
    } else if (FALSE_IT(new_stmt->set_query_ctx(stmt->get_query_ctx()))) {
      // never reach
    } else if (OB_FAIL(new_stmt->adjust_statement_id(ctx_->allocator_,
                                                     ctx_->src_qb_name_,
                                                     ctx_->src_hash_val_))) {
      LOG_WARN("failed to adjust statement id", K(ret));
    }
    // copy enough select stmt used for sub query in select list
    ObSEArray<ObSelectStmt*, 4> query_array;
    bool has_not_pushed_origin_stmt = true;
    for (int i = 0; OB_SUCC(ret) && i < selecthelpers.count(); i++) {
      if (selecthelpers.at(i)->aggr_expr_ids_.empty()){
        // this expr is const expr, don't need replace as subquery
        query_array.push_back(NULL);
      } else if (has_not_pushed_origin_stmt) {
        if (OB_FAIL(query_array.push_back(select_stmt))) {
          LOG_WARN("failed to push back select stmt"); 
        } else {
          has_not_pushed_origin_stmt = false;
        }
      } else {
        ObDMLStmt* ref_query = NULL;
        if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                     *ctx_->expr_factory_,
                                                     select_stmt,
                                                     ref_query))) {
          LOG_WARN("failed to create select stmt");
        } else if (OB_FAIL(ref_query->update_stmt_table_id(*select_stmt))) {
          //update stmt table id after find conds_exprs
          LOG_WARN("failed to update table id", K(ret));
        } else if (OB_FAIL(query_array.push_back(static_cast<ObSelectStmt*>(ref_query)))) {
          LOG_WARN("failed to push back select stmt"); 
        } 
      }
    }
    // copy enough select stmt used for sub query in select list
    ObSEArray<ObSelectStmt*, 4> having_query_array;
    for (int i = 0; OB_SUCC(ret) && i < havinghelpers.count(); i++) {
      ObDMLStmt* ref_query = NULL;
      if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_,
                                                    *ctx_->expr_factory_,
                                                    select_stmt,
                                                    ref_query))) {
        LOG_WARN("failed to create select stmt");
      } else if (OB_FAIL(ref_query->update_stmt_table_id(*select_stmt))) {
        //update stmt table id after find conds_exprs
        LOG_WARN("failed to update table id", K(ret));
      } else if (OB_FAIL(having_query_array.push_back(static_cast<ObSelectStmt*>(ref_query)))) {
        LOG_WARN("failed to push back select stmt"); 
      } 
    }
    // used for checking if aggr expr has generate a sub query
    // if agg_ref_stmt_list item is not null, use it as shared
    ObSEArray<ObSelectStmt*, 4> agg_ref_stmt_list;
    for (int i = 0; OB_SUCC(ret) && i < select_stmt->get_aggr_item_size(); i++) {
      if(OB_FAIL(agg_ref_stmt_list.push_back(NULL))) {
        LOG_WARN("failed to push back null pointer"); 
      }
    }
    // treat select list
    for (int i = 0; OB_SUCC(ret) && i < selecthelpers.count(); i++) {
      MinMaxAggrHelper *helper = selecthelpers.at(i);
      if (OB_ISNULL(helper) || helper->raw_expr_id_ == -1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null helper pointer", K(ret));
      } else if (helper->aggr_expr_ids_.empty()) { // NULL == query_array.at(i)
        if (OB_FAIL(new_stmt->add_select_item(select_stmt->get_select_item(helper->raw_expr_id_)))) {
          LOG_WARN("failed to add select item", K(ret));
        }
      } else {
        ObSelectStmt* one_select_ref_query = query_array.at(i);
        ObSEArray<ObSelectStmt*, 4> one_select_query_array;
        ObRawExpr* ori_select_expr = NULL;
        if (OB_ISNULL(one_select_ref_query)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null query pointer", K(ret));
        } else if (OB_ISNULL(ori_select_expr = one_select_ref_query->get_select_item(helper->raw_expr_id_).expr_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null expr pointer", K(ret));
        } else if (OB_FAIL(one_select_query_array.push_back(one_select_ref_query))) {
          LOG_WARN("failed to push back select stmt");
        }
        for (int id = 1; OB_SUCC(ret) && id < helper->aggr_expr_ids_.count(); id++) {
          ObDMLStmt* one_aggr_ref_query = NULL;
          if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_, 
                                                       one_select_ref_query, one_aggr_ref_query))) {
            LOG_WARN("failed to create select stmt");
          } else if (OB_FAIL(one_aggr_ref_query->update_stmt_table_id(*one_select_ref_query))) {
            //update stmt table id after find conds_exprs
            LOG_WARN("failed to update table id", K(ret));
          } else if (OB_FAIL(one_select_query_array.push_back(static_cast<ObSelectStmt*>(one_aggr_ref_query)))) {
            LOG_WARN("failed to push back select stmt"); 
          } 
        }
        SelectItem select_item = one_select_ref_query->get_select_item(helper->raw_expr_id_);
        // replace ref expr into select expr
        for (int j = 0; OB_SUCC(ret) && j < helper->aggr_expr_ids_.count(); j++) {
          // delete all select item except needed select item
          ObSelectStmt *one_aggr_ref_stmt = one_select_query_array.at(j);
          if (OB_ISNULL(one_aggr_ref_stmt)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null pointer", K(ret));
          } else {
            ObAggFunRawExpr *aggr_expr = one_aggr_ref_stmt->get_aggr_item(helper->aggr_expr_ids_.at(j));
            ObAggFunRawExpr *ori_aggr_expr = one_select_ref_query->get_aggr_item(helper->aggr_expr_ids_.at(j));
            if (OB_ISNULL(aggr_expr) || OB_ISNULL(ori_aggr_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null pointer", K(ret));
            } else {
              one_aggr_ref_stmt->get_select_items().reuse();
              select_item.expr_ = aggr_expr;
              ObSelectStmt *&aggr_ref_stmt = agg_ref_stmt_list.at(helper->aggr_expr_ids_.at(j));
              ObQueryRefRawExpr *ref_expr = NULL;
              if (OB_FAIL(one_aggr_ref_stmt->get_select_items().push_back(select_item))) {
                LOG_WARN("failed to push back select item", K(ret));
              } else if (NULL != aggr_ref_stmt) {
                for (int k = 0; OB_SUCC(ret) && k < new_stmt->get_subquery_expr_size(); k++) {
                  if (OB_ISNULL(new_stmt->get_subquery_exprs().at(k))) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("params have null", K(ret));
                  } else if (aggr_ref_stmt == new_stmt->get_subquery_exprs().at(k)->get_ref_stmt()) {
                    ref_expr = new_stmt->get_subquery_exprs().at(k);
                  }
                }
                if (OB_ISNULL(ref_expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("ref expr should not be null", K(ret));
                }
              } else if (OB_FAIL(do_transform_one_stmt(one_aggr_ref_stmt, aggr_expr, aggr_ref_stmt))) {
                LOG_WARN("failed to transform column aggregate", K(ret));
              } else if (OB_FAIL(create_new_ref_expr(ref_expr, aggr_ref_stmt, ori_select_expr))) {
                LOG_WARN("failed to create new ref query expr", K(ret));
              } else if (OB_FAIL(new_stmt->add_subquery_ref(ref_expr))) {
                LOG_WARN("failed to add ref query expr to subquery ref list", K(ret));
              } 
              // build ref expr completely, then replace aggr expr
              if (OB_SUCC(ret) && OB_FAIL(replace_aggr_expr_by_subquery(ori_select_expr, ori_aggr_expr, ref_expr))) {
                LOG_WARN("failed to replace ref_expr into select_expr", K(ret));
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                                ori_select_expr,
                                                                new_stmt))) {
          LOG_WARN("failed to create select item from ref query", K(ret));
        } else {
          new_stmt->get_select_item(i).expr_name_ = select_item.expr_name_;
          new_stmt->get_select_item(i).alias_name_ = select_item.alias_name_;
        }
      }
    }
    // treat having list
    if (havinghelpers.count() > 0) {
      for (int i = 0; OB_SUCC(ret) && i < havinghelpers.count(); i++) {
        MinMaxAggrHelper *helper = havinghelpers.at(i);
        if (OB_ISNULL(helper)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null pointer", K(ret));
        } else {
          // replace ref expr into having expr
          ObSelectStmt* one_having_ref_query = having_query_array.at(i);
          ObSEArray<ObSelectStmt*, 4> one_having_expr_query_array;
          if (OB_FAIL(one_having_expr_query_array.push_back(one_having_ref_query))) {
            LOG_WARN("failed to push back select stmt"); 
          }
          for (int id = 1; OB_SUCC(ret) && id < helper->aggr_expr_ids_.count(); id++) {
            ObDMLStmt* one_aggr_ref_query = NULL;
            if (OB_FAIL(ObTransformUtils::deep_copy_stmt(*ctx_->stmt_factory_, *ctx_->expr_factory_, 
                                                         one_having_ref_query, one_aggr_ref_query))) {
              LOG_WARN("failed to create select stmt");
            } else if (OB_FAIL(one_aggr_ref_query->update_stmt_table_id(*one_having_ref_query))) {
              //update stmt table id after find conds_exprs
              LOG_WARN("failed to update table id", K(ret));
            } else if (OB_FAIL(one_having_expr_query_array.push_back(static_cast<ObSelectStmt*>(one_aggr_ref_query)))) {
              LOG_WARN("failed to push back select stmt"); 
            } 
          }
          for (int j = 0; OB_SUCC(ret) && j < helper->aggr_expr_ids_.count(); j++) {
            ObSelectStmt *&aggr_ref_stmt = agg_ref_stmt_list.at(helper->aggr_expr_ids_.at(j));
            ObQueryRefRawExpr *ref_expr = NULL;
            if (NULL != aggr_ref_stmt) {
              for (int k = 0; OB_SUCC(ret) && k < new_stmt->get_subquery_expr_size(); k++) {
                if (OB_ISNULL(new_stmt->get_subquery_exprs().at(k))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("params have null", K(ret));
                } else if (aggr_ref_stmt == new_stmt->get_subquery_exprs().at(k)->get_ref_stmt()) {
                  ref_expr = new_stmt->get_subquery_exprs().at(k);
                }
              }
              if (OB_ISNULL(ref_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("ref expr should not be null", K(ret));
              }
            } else {
              ObSelectStmt* one_aggr_ref_query = one_having_expr_query_array.at(j);
              ObAggFunRawExpr *aggr_expr = one_aggr_ref_query->get_aggr_item(helper->aggr_expr_ids_.at(j));
              if (OB_ISNULL(one_aggr_ref_query) || OB_ISNULL(aggr_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected null param", K(ret));
              } else {
                // set having_query select item
                one_aggr_ref_query->get_select_items().reuse();
                if (OB_FAIL(ObTransformUtils::create_select_item(*ctx_->allocator_,
                                                                  aggr_expr,
                                                                  one_aggr_ref_query))) {
                  LOG_WARN("failed to create select item from ref query", K(ret));
                } else if (OB_FAIL(do_transform_one_stmt(one_aggr_ref_query, 
                                                        aggr_expr, 
                                                        aggr_ref_stmt))) {
                  LOG_WARN("failed to transform column aggregate", K(ret));
                } else if (OB_FAIL(create_new_ref_expr(ref_expr, aggr_ref_stmt, aggr_expr))) {
                  LOG_WARN("failed to create new ref query expr", K(ret));
                } else if (OB_FAIL(new_stmt->add_subquery_ref(ref_expr))) {
                  LOG_WARN("failed to add ref query expr to subquery ref list", K(ret));
                }
              }
            }
            ObAggFunRawExpr *ori_aggr_expr = select_stmt->get_aggr_item(helper->aggr_expr_ids_.at(j));
            if (OB_ISNULL(ori_aggr_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected null param", K(ret));
            } else if (OB_FAIL(replace_aggr_expr_by_subquery(helper->raw_expr_ptr_, ori_aggr_expr, ref_expr))) {
              LOG_WARN("failed to replace ref_expr into having_expr", K(ret));
            }
          }
          if (OB_FAIL(new_stmt->add_condition_expr(helper->raw_expr_ptr_))) {
            LOG_WARN("failed to add condition expr for new stmt");
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      stmt = new_stmt;
    }
  }
  return ret;
}

int ObTransformMinMax::check_valid_aggr_expr(const ObRawExpr *expr,
                                             ObIArray<ObAggFunRawExpr *> &aggr_expr_array,
                                             ObIArray<int64_t> &aggr_expr_ids,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (OB_FAIL(SMART_CALL(inner_check_valid_aggr_expr(expr, aggr_expr_array, aggr_expr_ids, is_valid)))) {
    LOG_WARN("failed to check is_valid_expr", K(ret));
  } else if (aggr_expr_ids.count() == 0) {
    is_valid = false;
  }
  return ret;
}                        

int ObTransformMinMax::inner_check_valid_aggr_expr(const ObRawExpr *expr,
                                                   ObIArray<ObAggFunRawExpr *> &aggr_expr_array,
                                                   ObIArray<int64_t> &aggr_expr_ids,
                                                   bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be NULL", K(ret), KP(expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  // } else if (expr == aggr_expr) {
  //   is_valid = true;
  } else if (expr->has_flag(CNT_AGG)) {
    if (expr->has_flag(IS_AGG)) {
      for (int64_t j = 0; OB_SUCC(ret) && j < aggr_expr_array.count(); j++) {
        ObAggFunRawExpr *aggr_expr = aggr_expr_array.at(j);
        if (OB_ISNULL(aggr_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("params have null", K(ret));
        } else if (expr == aggr_expr) {
          is_valid = true;
          if (is_contain(aggr_expr_ids, j)) {
            continue; // duplicate min/max are shared in one expr
          } else if (OB_FAIL(aggr_expr_ids.push_back(j))) {
            LOG_WARN("failed to push back id", K(ret));
          }
        }
      }
    } else {
      const ObRawExpr *param = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_ISNULL(param = expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param is null", K(ret));
        } else if (param->is_const_expr()) {
          /* do nothing */
        } else if (param->has_flag(CNT_AGG)) {
          if (OB_FAIL(SMART_CALL(inner_check_valid_aggr_expr(param, aggr_expr_array, aggr_expr_ids, is_valid)))) {
            LOG_WARN("failed to check is_valid_expr", K(ret));
          } else if (!is_valid) {
            break;
          }
        } else {
          is_valid = false;
          break;
        }
      }
    }
  }
  return ret;
}

int ObTransformMinMax::replace_aggr_expr_by_subquery(ObRawExpr *&expr,
                                                     const ObAggFunRawExpr *aggr_expr,
                                                     ObRawExpr *ref_expr)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  if (OB_FAIL(SMART_CALL(replace_aggr_expr_by_subquery(expr, 
                                                      aggr_expr, 
                                                      ref_expr,
                                                      is_valid)))) {
    LOG_WARN("failed to check is_valid_expr", K(ret));
  } else if (!is_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not find right aggr expr", K(ret));
  }
  return ret;
}                                                     
int ObTransformMinMax::replace_aggr_expr_by_subquery(ObRawExpr *&expr,
                                                     const ObAggFunRawExpr *aggr_expr,
                                                     ObRawExpr *ref_expr,
                                                     bool &is_valid)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(aggr_expr) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr should not be NULL", K(ret), KP(expr), KP(aggr_expr));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  // } else if (expr == aggr_expr) {
  //   is_valid = true;
  //   expr = ref_expr;
  } else if (expr->has_flag(CNT_AGG)) {
    if (expr == aggr_expr) {
      expr = ref_expr;
      is_valid = true;
    } else if (!expr->has_flag(IS_AGG)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
        if (OB_ISNULL(expr->get_param_expr(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("param is null", K(ret));
        } else if (expr->get_param_expr(i)->is_const_expr() || expr->get_param_expr(i)->is_query_ref_expr()) {
          /* do nothing */
        } else if (expr->get_param_expr(i)->has_flag(CNT_AGG)) {
          if (OB_FAIL(SMART_CALL(replace_aggr_expr_by_subquery(expr->get_param_expr(i), 
                                                              aggr_expr, 
                                                              ref_expr,
                                                              is_valid)))) {
            LOG_WARN("failed to check is_valid_expr", K(ret));
          }
        }
      }
    }
  }
  return ret;
}              

int ObTransformMinMax::is_valid_index_column(ObTransformerCtx &ctx,
                                             const ObSelectStmt *stmt,
                                             const ObRawExpr *expr,
                                             EqualSets &equal_sets,
                                             ObIArray<ObRawExpr*> &const_exprs,
                                             bool &is_valid)
{
  int ret = OB_SUCCESS;
  const TableItem *table_item = NULL;
  const ObColumnRefRawExpr *col_expr = NULL;
  bool is_match_index = false;
  is_valid = false;
  if (OB_ISNULL(stmt) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(stmt), K(expr));
  } else if (!expr->is_column_ref_expr()) {
    /* do nothing */
  } else if (FALSE_IT(col_expr = static_cast<const ObColumnRefRawExpr *>(expr))) {
  } else if (OB_ISNULL(table_item = stmt->get_table_item_by_id(col_expr->get_table_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table item is null", K(ret));
  } else if (!table_item->is_basic_table()) {
    /* do nothing */
  } else if (OB_FAIL(ObTransformUtils::is_match_index(ctx.sql_schema_guard_,
                                                      stmt,
                                                      col_expr,
                                                      is_match_index,
                                                      &equal_sets, &const_exprs))) {
    LOG_WARN("failed to check whether column matches index", K(ret));
  } else if (is_match_index) {
    is_valid = true;
  }
  return ret;
}

int ObTransformMinMax::set_child_order_item(ObSelectStmt *stmt, ObRawExpr *aggr_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)
      || OB_ISNULL(aggr_expr->get_param_expr(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else {
    OrderItem new_order_item;
    new_order_item.expr_ = aggr_expr->get_param_expr(0);
    if (T_FUN_MAX == aggr_expr->get_expr_type()) {
      new_order_item.order_type_ = default_desc_direction();
    } else if (T_FUN_MIN == aggr_expr->get_expr_type()) {
      new_order_item.order_type_ = default_asc_direction();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("aggregate function type must by max or min", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(stmt->add_order_item(new_order_item))) {
        LOG_WARN("fail to add order item", K(ret), K(stmt), K(new_order_item));
      }
    }
  }
  return ret;
}

int ObTransformMinMax::set_child_condition(ObSelectStmt *stmt, ObRawExpr *aggr_expr)
{
  int ret = OB_SUCCESS;
  ObOpRawExpr *not_null_expr = NULL;
  ObRawExpr *aggr_param = NULL;
  bool is_not_null = false;
  ObArray<ObRawExpr *> constraints;
  if (OB_ISNULL(stmt) || OB_ISNULL(aggr_expr)
      || OB_ISNULL(aggr_param = aggr_expr->get_param_expr(0))
      || OB_ISNULL(ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params have null", K(ret), K(stmt), K(aggr_expr));
  } else if (OB_FAIL(ObTransformUtils::is_expr_not_null(ctx_, stmt, aggr_param, NULLABLE_SCOPE::NS_WHERE,
                                                        is_not_null, &constraints))) {
    LOG_WARN("failed to check expr not null", K(ret));
  } else if (is_not_null) {
    if (OB_FAIL(ObTransformUtils::add_param_not_null_constraint(*ctx_, constraints))) {
      LOG_WARN("failed to add param not null constraints", K(ret));
    }
  } else if (OB_FAIL(ObTransformUtils::add_is_not_null(ctx_, stmt, aggr_param, not_null_expr))) {
    LOG_WARN("failed to add is not null", K(ret));
  } else if (OB_FAIL(stmt->add_condition_expr(not_null_expr))) {
    LOG_WARN("failed to add condition expr", K(ret));
  }
  return ret;
}

} // sql
} // oceanbase
