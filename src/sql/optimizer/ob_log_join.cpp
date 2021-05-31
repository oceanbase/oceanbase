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

#define USING_LOG_PREFIX SQL_OPT
#include "share/system_variable/ob_sys_var_class_type.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_subplan_scan.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_operator_factory.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_log_table_scan.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql::log_op_def;
using oceanbase::share::schema::ObTableSchema;
using share::schema::ObSchemaGetterGuard;

int ObLogJoin::copy_without_child(ObLogicalOperator*& out)
{
  int ret = OB_SUCCESS;
  out = NULL;
  ObLogicalOperator* op = NULL;
  ObLogJoin* join = NULL;
  if (OB_FAIL(clone(op))) {
    LOG_WARN("failed to clone join operator", K(ret));
  } else if (OB_ISNULL(join = static_cast<ObLogJoin*>(op))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join is null", K(ret));
  } else {
    if (OB_FAIL(join->set_join_conditions(join_conditions_))) {
      LOG_WARN("failed to set join conditions for copy", K(ret));
    } else if (OB_FAIL(join->set_join_filters(join_filters_))) {
      LOG_WARN("failed to set join filters for copy", K(ret));
    } else if (OB_FAIL(join->set_nl_params(nl_params_))) {
      LOG_WARN("failed to set nl params for copy", K(ret));
    } else if (OB_FAIL(join->set_exec_params(exec_params_))) {
      LOG_WARN("failed to set nl params for copy", K(ret));
    } else if (OB_FAIL(append(join->merge_directions_, merge_directions_))) {
      LOG_WARN("failed to set merge directions for copy", K(ret));
    } else if (OB_FAIL(append(join->left_expected_ordering_, left_expected_ordering_))) {
      LOG_WARN("failed to set left expected_ordering for copy", K(ret));
    } else if (OB_FAIL(append(join->right_expected_ordering_, right_expected_ordering_))) {
      LOG_WARN("failed to set right expected_ordering for copy", K(ret));
    } else if (OB_FAIL(join->set_connect_by_prior_exprs(connect_by_prior_exprs_))) {
      LOG_WARN("Failed to set cby prior exprs", K(ret));
    } else if (OB_FAIL(join->set_connect_by_extra_exprs(connect_by_extra_exprs_))) {
      LOG_WARN("fialed to set connect by extra exprs", K(ret));
    } else {
      join->set_join_type(join_type_);
      join->set_join_algo(join_algo_);
      join->set_late_mat(late_mat_);
      join->set_join_distributed_method(join_dist_algo_);
      join->set_anti_or_semi_sel(anti_or_semi_sel_);
      out = join;
    }
  }
  return ret;
}

int ObLogJoin::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  bool is_basic = false;
  uint64_t candidate_method = 0;
  EqualSets sharding_input_esets;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  // join keys and types for hash-hash distribution
  ObSEArray<ObRawExpr*, 4> hash_left_join_keys;
  ObSEArray<ObRawExpr*, 4> hash_right_join_keys;
  ObSEArray<ObExprCalcType, 4> hash_calc_types;
  // set keys for non-hash-hash distribution
  ObSEArray<ObRawExpr*, 4> left_join_keys;
  ObSEArray<ObRawExpr*, 4> right_join_keys;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_plan()) || OB_ISNULL(left_child = get_child(first_child)) ||
      OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(ctx), K(get_plan()), K(left_child), K(right_child));
  } else if (OB_FAIL(compute_basic_sharding_info(ctx, is_basic))) {
    LOG_WARN("failed to compute basic sharding info", K(ret));
  } else if (is_basic) {
    LOG_TRACE("is basic sharding info", K(sharding_info_));
  } else if (OB_FAIL(get_join_keys(*ctx, left_join_keys, right_join_keys))) {
    LOG_WARN("failed to get join key", K(ret));
  } else if (OB_FAIL(get_hash_hash_distribution_info(hash_left_join_keys, hash_right_join_keys, hash_calc_types))) {
    LOG_WARN("failed to get calc types", K(ret));
  } else if (OB_FAIL(get_sharding_input_equal_sets(sharding_input_esets))) {
    LOG_WARN("failed to get sharding input esets", K(ret));
  } else if (OB_FAIL(get_candidate_join_distribution_method(
                 *get_plan(), sharding_input_esets, left_join_keys, right_join_keys, candidate_method))) {
    LOG_WARN("failed to get valid dist method", K(ret));
  } else if (OB_FAIL(choose_best_distribution_method(
                 *ctx, candidate_method, pq_map_hint_, join_dist_algo_, slave_mapping_type_))) {
    LOG_WARN("failed to choose best distribution method", K(ret));
  } else if (OB_FAIL(compute_sharding_and_allocate_exchange(ctx,
                 sharding_input_esets,
                 hash_left_join_keys,
                 hash_right_join_keys,
                 hash_calc_types,
                 left_join_keys,
                 right_join_keys,
                 *left_child,
                 *right_child,
                 join_dist_algo_,
                 slave_mapping_type_,
                 join_type_,
                 sharding_info_))) {
    LOG_WARN("failed to compute sharding and allocate exchange", K(ret));
  } else {
    is_partition_wise_ = (join_dist_algo_ == JoinDistAlgo::DIST_PARTITION_WISE) && !use_slave_mapping();
  }
  if (OB_SUCC(ret)) {
    if (is_nlj_without_param_down() && join_type_ != CONNECT_BY_JOIN &&
        (!left_child->get_is_at_most_one_row() || join_dist_algo_ != JoinDistAlgo::DIST_PULL_TO_LOCAL) &&
        OB_FAIL(check_and_allocate_material(second_child))) {
      LOG_WARN("failed to check whether need material", K(ret));
    } else if (OB_FAIL(update_weak_part_exprs(ctx))) {
      LOG_WARN("failed to update weak part exprs", K(ret));
    } else {
      sharding_info_.set_can_reselect_replica(false);
    }
  }

  return ret;
}

int ObLogJoin::build_gi_partition_pruning()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  if (OB_FAIL(get_child(second_child)->find_first_recursive(LOG_GRANULE_ITERATOR, op))) {
    LOG_WARN("find granule iterator in right failed", K(ret));
  } else if (NULL == op) {
    // granule iterator not found, do nothing
  } else {
    static_cast<ObLogGranuleIterator*>(op)->add_flag(GI_ENABLE_PARTITION_PRUNING);
  }
  return ret;
}

int ObLogJoin::get_join_keys(
    const AllocExchContext& ctx, ObIArray<ObRawExpr*>& left_key, ObIArray<ObRawExpr*>& right_key)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> conditions;
  ObSEArray<ObRawExpr*, 8> normal_conds;
  ObSEArray<ObRawExpr*, 8> nullsafe_conds;
  ObSEArray<ObRawExpr*, 8> nullsafe_left_key;
  ObSEArray<ObRawExpr*, 8> nullsafe_right_key;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  if (OB_ISNULL(left_child = get_child(first_child)) || OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
  } else if (OB_FAIL(append(conditions, get_equal_join_conditions())) ||
             OB_FAIL(append(conditions, get_other_join_conditions())) ||
             OB_FAIL(append(conditions, right_child->get_pushdown_filter_exprs()))) {
    LOG_WARN("failed to get all conditions", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::classify_equal_conds(conditions, normal_conds, nullsafe_conds))) {
    LOG_WARN("failed to classify equal conditions", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(normal_conds, left_child->get_table_set(), left_key, right_key))) {
    LOG_WARN("failed to get equal keys", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(
                 nullsafe_conds, left_child->get_table_set(), nullsafe_left_key, nullsafe_right_key))) {
    LOG_WARN("failed to get equal keys", K(ret));
  } else if (OB_FAIL(prune_weak_part_exprs(ctx, nullsafe_left_key, nullsafe_right_key, left_key, right_key))) {
    LOG_WARN("failed to prune weak part exprs", K(ret));
  }
  return ret;
}

int ObLogJoin::get_hash_hash_distribution_info(
    ObIArray<ObRawExpr*>& left_keys, ObIArray<ObRawExpr*>& right_keys, ObIArray<ObExprCalcType>& calc_types)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = NULL;
  if (OB_ISNULL(left_child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions_.count(); i++) {
      ObRawExpr* expr = NULL;
      ObRawExpr* left_expr = NULL;
      ObRawExpr* right_expr = NULL;
      if (OB_ISNULL(expr = join_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (!expr->has_flag(IS_JOIN_COND)) {
        /*do nothing*/
      } else if (OB_UNLIKELY(2 != expr->get_param_count()) || OB_ISNULL(left_expr = expr->get_param_expr(0)) ||
                 OB_ISNULL(right_expr = expr->get_param_expr(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(left_expr), K(right_expr), K(expr->get_param_count()));
      } else if (OB_FAIL(calc_types.push_back(expr->get_result_type().get_calc_meta()))) {
        LOG_WARN("failed to push back result type", K(ret));
      } else {
        if (!left_expr->get_relation_ids().is_subset(left_child->get_table_set())) {
          std::swap(left_expr, right_expr);
        }
        if (OB_FAIL(left_keys.push_back(left_expr))) {
          LOG_WARN("failed to push back exprs", K(ret));
        } else if (OB_FAIL(right_keys.push_back(right_expr))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

// only merge join will invoke this func to construct ordering of new_added sorting
int ObLogJoin::make_sort_keys(ObIArray<ObRawExpr*>& sort_expr, ObIArray<OrderItem>& order_keys)
{
  int ret = OB_SUCCESS;
  int64_t sort_expr_count = sort_expr.count();
  int64_t direction_count = merge_directions_.count();
  if (MERGE_JOIN != get_join_algo()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only merge join will invoke this function", K(get_join_algo()), K(ret));
  } else if (direction_count <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in merge join direction count should be positive", K(direction_count), K(ret));
  } else if (sort_expr_count < direction_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sort expr count should no less than direction count", K(sort_expr_count), K(direction_count), K(ret));
  } else {
    OrderItem order_item;
    for (int64_t i = 0; OB_SUCC(ret) && i < direction_count; ++i) {
      order_item.reset();
      order_item.expr_ = sort_expr.at(i);
      order_item.order_type_ = merge_directions_.at(i);
      if (OB_FAIL(order_keys.push_back(order_item))) {
        LOG_WARN("failed to push back order item", K(i), K(order_item), K(ret));
      }
    }

    for (int64_t j = direction_count; OB_SUCC(ret) && j < sort_expr_count; ++j) {
      order_item.reset();
      order_item.expr_ = sort_expr.at(j);
      order_item.order_type_ = default_asc_direction();
      if (OB_FAIL(order_keys.push_back(order_item))) {
        LOG_WARN("failed to push back order item", K(j), K(order_item), K(ret));
      }
    }
  }
  return ret;
}

int ObLogJoin::allocate_expr_pre(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> exprs;
  ObSEArray<ObRawExpr*, 8> left_join_exprs;
  ObSEArray<ObRawExpr*, 8> right_join_exprs;
  ObLogicalOperator* first_child = NULL;
  ObLogicalOperator* second_child = NULL;
  uint64_t left_producer_id = OB_INVALID_ID;
  uint64_t right_producer_id = OB_INVALID_ID;
  if (OB_ISNULL(first_child = get_child(0)) || OB_ISNULL(second_child = get_child(1)) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first_child), K(second_child), K(get_stmt()), K(ret));
  } else if (OB_FAIL(get_next_producer_id(first_child, left_producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else if (OB_FAIL(get_next_producer_id(second_child, right_producer_id))) {
    LOG_WARN("failed to get next producer id", K(ret));
  } else if (OB_FAIL(append(exprs, join_filters_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append(exprs, join_conditions_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else {

    for (int64_t i = 0; OB_SUCC(ret) && i < nl_params_.count(); i++) {
      if (OB_ISNULL(nl_params_.at(i).second)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(exprs.push_back(nl_params_.at(i).second))) {
        LOG_WARN("failed to push back expr", K(ret));
      } else { /*do nothing*/
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions_.count(); i++) {
      ObRawExpr* raw_expr = NULL;
      if (OB_ISNULL(raw_expr = join_conditions_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < raw_expr->get_param_count(); j++) {
          ObRawExpr* temp_expr = NULL;
          if (OB_ISNULL(temp_expr = raw_expr->get_param_expr(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (temp_expr->has_flag(IS_COLUMN)) {
            /*do nothing*/
          } else if (temp_expr->get_relation_ids().is_subset(first_child->get_table_set())) {
            ret = left_join_exprs.push_back(temp_expr);
          } else if (temp_expr->get_relation_ids().is_subset(second_child->get_table_set())) {
            ret = right_join_exprs.push_back(temp_expr);
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error",
                K(temp_expr->get_relation_ids()),
                K(first_child->get_table_set()),
                K(second_child->get_table_set()),
                K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_exprs_to_ctx(ctx, exprs))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else if (OB_FAIL(ObLogicalOperator::allocate_expr_pre(ctx))) {
        LOG_WARN("failed to allocate expr", K(ret));
      } else if (OB_FAIL(add_exprs_to_ctx(ctx, left_join_exprs, left_producer_id))) {
        LOG_WARN("failed to add exprs to ctx", K(ret));
      } else if (OB_FAIL(add_exprs_to_ctx(ctx, right_join_exprs, right_producer_id))) {
        LOG_WARN("faiiled to add exprs to ctx", K(ret));
      }
    }

    if (OB_SUCC(ret) && CONNECT_BY_JOIN == join_type_ && get_stmt()->is_select_stmt()) {
      ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(get_stmt());
      ObSEArray<ObRawExpr*, 4> left_extra_exprs;
      ObSEArray<ObRawExpr*, 4> right_extra_exprs;
      int64_t child_expr_count = connect_by_extra_exprs_.count() / 2;
      if (OB_UNLIKELY(child_expr_count * 2 != connect_by_extra_exprs_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child expr size", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < child_expr_count; ++i) {
        ObRawExpr* left = connect_by_extra_exprs_.at(i);
        ObRawExpr* right = connect_by_extra_exprs_.at(i + child_expr_count);
        if (OB_ISNULL(left) || OB_ISNULL(right)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid extra expr", K(ret));
        } else if (OB_FAIL(left_extra_exprs.push_back(left))) {
          LOG_WARN("failed to push left expr", K(ret));
        } else if (OB_FAIL(right_extra_exprs.push_back(right))) {
          LOG_WARN("failed to push right expr", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_exprs_to_ctx(ctx, left_extra_exprs, left_producer_id))) {
          LOG_WARN("failed to add exprs to ctx", K(ret));
        } else if (OB_FAIL(add_exprs_to_ctx(ctx, right_extra_exprs, right_producer_id))) {
          LOG_WARN("failed to add exprs to ctx", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        ObSEArray<ObRawExpr*, 8> param_exprs;
        for (int64_t i = 0; OB_SUCC(ret) && i < ctx.expr_producers_.count(); ++i) {
          ExprProducer& producer = ctx.expr_producers_.at(i);
          if (OB_ISNULL(producer.expr_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_INVALID_ID == producer.producer_id_ &&
                     ((producer.expr_->has_flag(IS_CONNECT_BY_ROOT)) ||
                         (producer.expr_->has_flag(IS_SYS_CONNECT_BY_PATH)) || (producer.expr_->has_flag(IS_PRIOR)) ||
                         (producer.expr_->has_flag(IS_PSEUDO_COLUMN) &&
                             static_cast<const ObPseudoColumnRawExpr*>(producer.expr_)
                                 ->is_hierarchical_query_type()))) {
            producer.producer_id_ = id_;
            for (int64_t j = 0; OB_SUCC(ret) && j < producer.expr_->get_param_count(); j++) {
              bool should_add = false;
              ObRawExpr* param_expr = const_cast<ObRawExpr*>(producer.expr_->get_param_expr(j));
              if (OB_ISNULL(param_expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("param expr is null", K(ret));
              } else if (OB_FAIL(check_param_expr_should_be_added(param_expr, should_add))) {
                LOG_WARN("failed to check whether param expr can be added", K(ret));
              } else if (should_add && OB_FAIL(param_exprs.push_back(param_expr))) {
                LOG_WARN("array push back failed", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(add_exprs_to_ctx(ctx, param_exprs))) {
          LOG_WARN("failed to add exprs to ctx", K(ret));
        }
      }
    }
    if (OB_SUCC(ret) && get_plan() && can_enable_gi_partition_pruning()) {
      if (OB_FAIL(alloc_partition_id_column(ctx))) {
        LOG_WARN("fail alloc part id expr for gi pruning", K(ret));
      }
    }
  }
  return ret;
}

uint64_t ObLogJoin::hash(uint64_t seed) const
{
  seed = ObOptimizerUtil::hash_exprs(seed, join_conditions_);
  seed = ObOptimizerUtil::hash_exprs(seed, join_filters_);
  seed = do_hash(join_type_, seed);
  seed = do_hash(join_algo_, seed);
  seed = do_hash(late_mat_, seed);
  HASH_ARRAY(merge_directions_, seed);
  for (int64_t i = 0; i < nl_params_.count(); i++) {
    seed = do_hash(nl_params_.at(i).first, seed);
    if (NULL != nl_params_.at(i).second) {
      seed = do_hash(*nl_params_.at(i).second, seed);
    }
  }
  HASH_PTR_ARRAY(pseudo_columns_, seed);
  seed = ObOptimizerUtil::hash_exprs(seed, connect_by_prior_exprs_);
  HASH_ARRAY(left_expected_ordering_, seed);
  HASH_ARRAY(right_expected_ordering_, seed);
  seed = ObLogicalOperator::hash(seed);
  seed = ObOptimizerUtil::hash_exprs(seed, connect_by_extra_exprs_);

  return seed;
}

int32_t ObLogJoin::get_explain_name_length() const
{
  int32_t length = 0;
  if (NESTED_LOOP_JOIN == join_algo_) {
    length += (int32_t)strlen("NESTED-LOOP ");
  } else if (HASH_JOIN == join_algo_) {
    length += (int32_t)strlen("HASH ");
  } else {
    length += (int32_t)strlen("MERGE ");
  }
  length += (int32_t)strlen(ob_join_type_str(join_type_));

  if (is_cartesian()) {
    ++length;
    length += (int32_t)strlen("CARTESIAN");
  }
  return length;
}

int ObLogJoin::get_explain_name_internal(char* buf, const int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (NESTED_LOOP_JOIN == join_algo_) {
    ret = BUF_PRINTF("NESTED-LOOP ");
  } else if (HASH_JOIN == join_algo_) {
    ret = BUF_PRINTF("HASH ");
  } else {
    ret = BUF_PRINTF("MERGE ");
  }
  if (OB_SUCC(ret)) {
    ret = BUF_PRINTF("%s", ob_join_type_str(join_type_));
  } else { /* Do nothing */
  }
  if (OB_SUCC(ret) && is_cartesian()) {
    ret = BUF_PRINTF(" ");
    if (OB_SUCC(ret)) {
      ret = BUF_PRINTF("CARTESIAN");
    } else { /* Do nothing */
    }
  } else { /* Do nothing */
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("BUF_PRINTF fails unexpectedly", K(ret));
  } else { /* Do nothing */
  }

  return ret;
}

int ObLogJoin::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(BUF_PRINTF(", "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("BUF_PRINTF fails", K(ret));
  } else { /* Do nothing */
  }
  if (OB_SUCC(ret)) {
    if (NESTED_LOOP_JOIN == get_join_algo()) {
      const ObIArray<ObRawExpr*>& conds = get_other_join_conditions();
      EXPLAIN_PRINT_EXPRS(conds, type);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* Do nothing */
        }
      } else { /* Do nothing */
      }
      if (OB_SUCC(ret)) {
        EXPLAIN_PRINT_EXEC_PARAMS(nl_params_, type);
      } else { /* Do nothing */
      }
      if (OB_SUCC(ret) && (EXPLAIN_EXTENDED == type || EXPLAIN_EXTENDED_NOADDR == type)) {
        bool use_batch_nlj = false;
        if (OB_FAIL(can_use_batch_nlj(use_batch_nlj))) {
          LOG_WARN("Failed to check use batch nlj", K(ret));
        } else if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else if (OB_FAIL(BUF_PRINTF("batch_join=%s", use_batch_nlj ? "true" : "false"))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* Do nothing */
        }
      }
    } else if (HASH_JOIN == get_join_algo()) {
      const ObIArray<ObRawExpr*>& equal_conds = get_equal_join_conditions();
      EXPLAIN_PRINT_EXPRS(equal_conds, type);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* Do nothing */
        }
      } else { /* Do nothing */
      }

      const ObIArray<ObRawExpr*>& other_conds = get_other_join_conditions();
      EXPLAIN_PRINT_EXPRS(other_conds, type);

    } else {
      const ObIArray<ObRawExpr*>& equal_conds = get_equal_join_conditions();
      EXPLAIN_PRINT_EXPRS(equal_conds, type);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* Do nothing */
        }
      } else { /* Do nothing */
      }
      const ObIArray<ObRawExpr*>& other_conds = get_other_join_conditions();
      EXPLAIN_PRINT_EXPRS(other_conds, type);
      if (OB_SUCC(ret) && EXPLAIN_EXTENDED == type) {
        const ObIArray<ObOrderDirection>& merge_directions = get_merge_directions();
        if (OB_FAIL(BUF_PRINTF(", "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
          LOG_WARN("BUF_PRINTF fails", K(ret));
        } else { /* Do nothing */
        }
        if (OB_SUCC(ret)) {
          EXPLAIN_PRINT_MERGE_DIRECTIONS(merge_directions);
        } else { /* Do nothing */
        }
      } else { /* Do nothing */
      }
    }
  } else { /* Do nothing */
  }

  return ret;
}

int ObLogJoin::check_output_dep_specific(ObRawExprCheckDep& checker)
{
  int ret = OB_SUCCESS;
  // join conditions
  for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions_.count(); i++) {
    if (OB_ISNULL(join_conditions_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join_conditions_.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*join_conditions_.at(i)))) {
      LOG_WARN("failed to check join_conditions expr", K(ret), K(i));
    } else {
    }
  }
  // join filters
  for (int64_t i = 0; OB_SUCC(ret) && i < join_filters_.count(); i++) {
    if (OB_ISNULL(join_filters_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("join_filters_.at(i) is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*join_filters_.at(i)))) {
      LOG_WARN("failed to check join_filters expr", K(ret), K(i));
    } else {
    }
  }

  // nl params
  for (int64_t i = 0; OB_SUCC(ret) && i < nl_params_.count(); i++) {
    if (OB_ISNULL(nl_params_.at(i).second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("nl_params_.at(i).second is null", K(ret), K(i));
    } else if (OB_FAIL(checker.check(*nl_params_.at(i).second))) {
      LOG_WARN("failed to check nl params expr", K(ret), K(i));
    } else {
    }
  }

  // nlj right gi pruning feature
  if (OB_SUCC(ret) && is_enable_gi_partition_pruning()) {
    if (OB_ISNULL(partition_id_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null unexpected", K(ret));
    } else if (OB_FAIL(checker.check(*partition_id_expr_))) {
      LOG_WARN("fail to check partition id expr", K(ret));
    }
  }
  // add extra output exprs for connect join
  if (CONNECT_BY_JOIN == join_type_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_extra_exprs_.count(); i++) {
      if (OB_ISNULL(connect_by_extra_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(checker.check(*connect_by_extra_exprs_.at(i)))) {
        LOG_WARN("failed to check extra output expr", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

int ObLogJoin::inner_replace_generated_agg_expr(const ObIArray<std::pair<ObRawExpr*, ObRawExpr*> >& to_replace_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(to_replace_exprs, get_join_conditions()))) {
    LOG_WARN("failed to extract subplan params in log join_conditions", K(ret));
  } else if (OB_FAIL(replace_exprs_action(to_replace_exprs, get_join_filters()))) {
    LOG_WARN("failed to extract subplan params in log join_filters", K(ret));
  } else {
    int64_t N = get_nl_params().count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExpr*& cur_expr = get_nl_params().at(i).second;
      if (OB_FAIL(replace_expr_action(to_replace_exprs, cur_expr))) {
        LOG_WARN("failed to extract subplan params in log join_filters", K(ret));
      } else { /* Do nothing */
      }
    }
  }
  return ret;
}

int ObLogJoin::set_left_expected_ordering(const common::ObIArray<OrderItem>& left_expected_ordering)
{
  int ret = OB_SUCCESS;
  left_expected_ordering_.reset();
  for (int i = 0; OB_SUCC(ret) && i < left_expected_ordering.count(); ++i) {
    if (OB_FAIL(left_expected_ordering_.push_back(left_expected_ordering.at(i)))) {
      LOG_WARN("failed to push back order item", K(ret));
    }
  }
  return ret;
}

int ObLogJoin::set_right_expected_ordering(const common::ObIArray<OrderItem>& right_expected_ordering)
{
  int ret = OB_SUCCESS;
  right_expected_ordering_.reset();
  for (int i = 0; OB_SUCC(ret) && i < right_expected_ordering.count(); ++i) {
    if (OB_FAIL(right_expected_ordering_.push_back(right_expected_ordering.at(i)))) {
      LOG_WARN("failed to push back order item", K(ret));
    }
  }
  return ret;
}

int ObLogJoin::transmit_op_ordering()
{
  int ret = OB_SUCCESS;
  reset_op_ordering();
  reset_local_ordering();
  ObLogicalOperator* left_child = get_child(first_child);
  ObLogicalOperator* right_child = get_child(second_child);
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_child or right_child is null", K(ret));
  } else if (NESTED_LOOP_JOIN == get_join_algo()) {  // nl keep left
    if (OB_FAIL(set_op_ordering(left_child->get_op_ordering()))) {
      LOG_WARN("failed to set op ordering", K(ret));
    } else { /*do nothing*/
    }
  } else if (HASH_JOIN == get_join_algo()) {  // hash join can not keep order
    /*do nothing*/
  } else if (MERGE_JOIN == get_join_algo()) {  // merge
    bool left_need_sort = true;
    bool right_need_sort = true;
    if (OB_FAIL(check_need_sort_below_node(0, left_expected_ordering_, left_need_sort))) {
      LOG_WARN("failed to check need sort", K(ret));
    } else if (left_need_sort) {
      if (OB_FAIL(allocate_sort_below(0, left_expected_ordering_))) {  // include set op ordering
        LOG_WARN("failed to allocate implicit sort", K(ret));
      }
    }
    if (OB_SUCC(ret)) {  // check right
      if (OB_FAIL(check_need_sort_below_node(1, right_expected_ordering_, right_need_sort))) {
        LOG_WARN("failed to check if need sort", K(ret));
      } else if (right_need_sort) {
        if (OB_FAIL(allocate_sort_below(1, right_expected_ordering_))) {
          LOG_WARN("failed to allocate implicit sort", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {  // set merge join's op_ordering
      if (INNER_JOIN == join_type_ || LEFT_OUTER_JOIN == join_type_ || LEFT_SEMI_JOIN == join_type_ ||
          LEFT_ANTI_JOIN == join_type_) {
        if (left_need_sort) {
          if (OB_FAIL(set_op_ordering(left_expected_ordering_))) {
            LOG_WARN("failed to set op ordering", K(ret));
          }
        } else if (OB_FAIL(set_op_ordering(get_child(first_child)->get_op_ordering()))) {
          LOG_WARN("failed to set op ordering", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogJoin::re_calc_cost()
{
  int ret = OB_SUCCESS;
  JoinAlgo algo = get_join_algo();
  const ObLogicalOperator* first_child = get_child(ObLogicalOperator::first_child);
  const ObLogicalOperator* second_child = get_child(ObLogicalOperator::second_child);
  if (OB_ISNULL(get_plan()) || OB_ISNULL(first_child) || OB_ISNULL(second_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(first_child), K(second_child), K(get_plan()), K(ret));
  } else if (MERGE_JOIN == algo || HASH_JOIN == algo ||
             (NESTED_LOOP_JOIN == algo && LOG_MATERIAL == second_child->get_type())) {
    set_cost(first_child->get_cost() + second_child->get_cost() + get_op_cost());
  } else {
    double op_cost = 0;
    double nl_cost = 0;
    bool need_mat = (log_op_def::LOG_MATERIAL == second_child->get_type());
    double anti_or_semi_sel;
    if (join_type_ == LEFT_SEMI_JOIN || join_type_ == LEFT_ANTI_JOIN) {
      anti_or_semi_sel = anti_or_semi_sel_;
    } else {
      anti_or_semi_sel = 1.0;
    }
    ObCostNLJoinInfo est_cost_info(first_child->get_card(),
        first_child->get_cost(),
        first_child->get_width(),
        second_child->get_card(),
        second_child->get_cost(),
        second_child->get_width(),
        first_child->get_table_set(),
        second_child->get_table_set(),
        join_type_,
        anti_or_semi_sel,
        nl_params_.count() > 0,
        need_mat,
        join_conditions_,
        join_filters_,
        get_est_sel_info());
    if (OB_FAIL(
            ObOptEstCost::cost_nestloop(est_cost_info, op_cost, nl_cost, &get_plan()->get_predicate_selectivities()))) {
      LOG_WARN("failed to get nestloop cost", K(ret));
    } else {
      cost_ = nl_cost;
      op_cost_ = op_cost;
    }
  }
  return ret;
}

int ObLogJoin::re_est_cost(const ObLogicalOperator* parent, double need_row_count, bool& re_est)
{
  int ret = OB_SUCCESS;
  UNUSED(parent);
  re_est = false;
  bool left_child_re_est = false;
  bool right_child_re_est = false;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null plan", K(get_plan()));
  } else if (need_row_count >= get_card()) {
    /* do nothing */
  } else if (OB_ISNULL(left_child = get_child(first_child)) || OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left_child), K(right_child));
  } else if (MERGE_JOIN == join_algo_) {
    double left_need_rows = sqrt(need_row_count / get_card()) * left_child->get_card();
    double right_need_rows = sqrt(need_row_count / get_card()) * right_child->get_card();
    if (OB_FAIL(left_child->re_est_cost(this, left_need_rows, left_child_re_est))) {
      LOG_WARN("re-estimate cost left child failed", K(ret));
    } else if (OB_FAIL(right_child->re_est_cost(this, right_need_rows, right_child_re_est))) {
      LOG_WARN("re-estimate cost right child failed", K(ret));
    } else {
      ObSEArray<OrderItem, 4, ModulePageAllocator, true> dummy_need_ordering;
      ObIArray<OrderItem>* left_need_ordering = &dummy_need_ordering;
      ObIArray<OrderItem>* right_need_ordering = &dummy_need_ordering;
      if (left_child->get_type() == LOG_SORT) {
        left_need_ordering = &(static_cast<ObLogSort*>(left_child)->get_sort_keys());
      }
      if (right_child->get_type() == LOG_SORT) {
        right_need_ordering = &(static_cast<ObLogSort*>(right_child)->get_sort_keys());
      }
      ObCostMergeJoinInfo est_cost_info(left_child->get_card(),
          left_child->get_cost(),
          left_child->get_width(),
          right_child->get_card(),
          right_child->get_cost(),
          right_child->get_width(),
          left_child->get_table_set(),
          right_child->get_table_set(),
          join_type_,
          join_conditions_,
          join_filters_,
          *left_need_ordering,
          *right_need_ordering,
          get_est_sel_info());
      double op_cost = 0.0;
      double mj_cost = 0.0;
      if (OB_ISNULL(get_plan())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("plan is NULL", K(ret));
      } else if (OB_FAIL(ObOptEstCost::cost_mergejoin(
                     est_cost_info, op_cost, mj_cost, &get_plan()->get_predicate_selectivities()))) {
        LOG_WARN("cost merge failed", K(ret));
      } else if (OB_UNLIKELY(op_cost > mj_cost)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("op cost > all merge join cost, unexpected", K(ret));
      } else {
        op_cost_ = op_cost;
        cost_ = mj_cost;
        card_ = need_row_count;
      }
    }
  } else if (HASH_JOIN == join_algo_) {
    if (LEFT_SEMI_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_) {
      // do nothing
    } else {
      double op_cost = 0.0;
      double hash_cost = 0.0;
      double right_need_rows = (need_row_count / get_card()) * right_child->get_card();
      if (OB_FAIL(right_child->re_est_cost(this, right_need_rows, right_child_re_est))) {
        LOG_WARN("Failed to re est cost", K(ret));
      } else {
        ObCostHashJoinInfo est_cost_info(left_child->get_card(),
            left_child->get_cost(),
            left_child->get_width(),
            right_child->get_card(),
            right_child->get_cost(),
            right_child->get_width(),
            left_child->get_table_set(),
            right_child->get_table_set(),
            join_type_,
            join_conditions_,
            join_filters_,
            get_est_sel_info());
        if (OB_FAIL(ObOptEstCost::cost_hashjoin(
                est_cost_info, op_cost, hash_cost, &get_plan()->get_predicate_selectivities()))) {
          LOG_WARN("failed to get hashjoin cost", K(ret));
        } else {
          cost_ = hash_cost;
          op_cost_ = op_cost;
          card_ = need_row_count;
          re_est = true;
        }
      }
    }
  } else if (NESTED_LOOP_JOIN == join_algo_) {
    double left_need_rows = (need_row_count / get_card()) * left_child->get_card();
    double op_cost = 0.0;
    double nl_cost = 0.0;
    double anti_or_semi_sel;
    if (join_type_ == LEFT_SEMI_JOIN || join_type_ == LEFT_ANTI_JOIN) {
      anti_or_semi_sel = anti_or_semi_sel_;
    } else {
      anti_or_semi_sel = 1.0;
    }
    if (OB_FAIL(left_child->re_est_cost(this, left_need_rows, left_child_re_est))) {
      LOG_WARN("Failed to re est cost", K(ret));
    } else {
      double right_child_cost = right_child->get_cost();
      bool need_mat = (log_op_def::LOG_MATERIAL == right_child->get_type());
      if (need_mat) {
        if (OB_ISNULL(right_child->get_child(first_child))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("material child should not be NULL", K(ret));
        } else {
          right_child_cost = right_child->get_child(first_child)->get_cost();
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        ObCostNLJoinInfo est_cost_info(left_need_rows,
            left_child->get_cost(),
            left_child->get_width(),
            right_child->get_card(),
            right_child_cost,
            right_child->get_width(),
            left_child->get_table_set(),
            right_child->get_table_set(),
            join_type_,
            anti_or_semi_sel,
            nl_params_.count() > 0,
            need_mat,
            join_conditions_,
            join_filters_,
            get_est_sel_info());
        if (OB_FAIL(ObOptEstCost::cost_nestloop(
                est_cost_info, op_cost, nl_cost, &get_plan()->get_predicate_selectivities()))) {
          LOG_WARN("failed to get nestloop cost", K(ret));
        } else {
          cost_ = nl_cost;
          op_cost_ = op_cost;
          card_ = need_row_count;
          re_est = true;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unrecognized type of join algorithm", K(ret), K_(join_algo));
  }
  return ret;
}

int ObLogJoin::print_use_late_materialization(planText& plan_text)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_one_line = plan_text.is_oneline_;
  OutlineType outline_type = plan_text.outline_type_;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL");
  } else if (is_late_mat()) {
    ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    if (OUTLINE_DATA == outline_type ||
        (USED_HINT == outline_type && stmt_hint.use_late_mat_ == OB_USE_LATE_MATERIALIZATION)) {
      stmt_hint.use_late_mat_ = OB_USE_LATE_MATERIALIZATION;
      if (!is_one_line && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_one_line)))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::USE_LATE_MATERIALIZATION))) {
        LOG_WARN("fail to print outline", K(ret));
      } else { /*do thing*/
      }
    }
  }
  return ret;
}

int ObLogJoin::print_outline(planText& plan_text)
{
  int ret = OB_SUCCESS;
  if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_3100 && OB_FAIL(print_use_late_materialization(plan_text))) {
    LOG_WARN("fail to print use late materialization", K(ret));
  } else if (is_added_outline_) {  // do nothing.
  } else if (OB_FAIL(print_leading(plan_text, LEFT_DEEP))) {
    LOG_WARN("fail to print leading", K(ret));
  } else if (OB_FAIL(print_use_join(plan_text, LEFT_DEEP, false))) {
    LOG_WARN("fail to print join method", K(ret));
  } else if (OB_FAIL(print_pq_distribute(plan_text, LEFT_DEEP, false))) {
    LOG_WARN("fail to print pq distribute", K(ret));
  } else if (OB_FAIL(print_material_nl(plan_text, LEFT_DEEP, false))) {
    LOG_WARN("fail to print material nl", K(ret));
  } else if (OB_FAIL(print_pq_map(plan_text, LEFT_DEEP, false))) {
    LOG_WARN("fail to print pq distribute", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogJoin::print_material_nl(planText& plan_text, JoinTreeType join_tree_type, bool is_need_print)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_oneline = plan_text.is_oneline_;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    bool is_need = false;
    bool need_mat = false;
    ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    if (OB_FAIL(is_need_print_material_nl(plan_text, stmt_hint, is_need, need_mat))) {
      LOG_WARN("fail to judge whether need print use join", K(ret));
    } else if (!is_need) {
    } else if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
    } else if (need_mat && OB_FAIL(BUF_PRINTF(ObStmtHint::USE_NL_MATERIAL))) {
    } else if (!need_mat && OB_FAIL(BUF_PRINTF(ObStmtHint::NO_USE_NL_MATERIAL))) {
    } else if (OB_FAIL(BUF_PRINTF("("))) {
    } else if (OB_FAIL(print_qb_name(plan_text))) {
      LOG_WARN("fail to print query block name", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(" ("))) {
    } else if (OB_FAIL(traverse_join_plan_pre(plan_text, MATERIAL_NL, join_tree_type, is_need_print))) {
      LOG_WARN("fail to traverse join plan", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("))"))) {
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    is_added_outline_ = true;
  }
  return ret;
}

int ObLogJoin::print_use_join(planText& plan_text, JoinTreeType join_tree_type, bool is_need_print)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_oneline = plan_text.is_oneline_;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL");
  } else {
    bool is_need = false;
    ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    if (OB_FAIL(is_need_print_use_join(plan_text, stmt_hint, is_need))) {
      LOG_WARN("fail to judge whether need print use join", K(ret));
    } else if (!is_need) {
    } else if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(JoinAlgo_to_hint_str(get_join_algo())))) {
    } else if (OB_FAIL(BUF_PRINTF("("))) {
    } else if (OB_FAIL(print_qb_name(plan_text))) {
      LOG_WARN("fail to print query block name", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(" ("))) {
    } else if (OB_FAIL(traverse_join_plan_pre(plan_text, JOIN_METHOD, join_tree_type, is_need_print))) {
      LOG_WARN("fail to traverse join plan", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("))"))) {
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    is_added_outline_ = true;
  }
  return ret;
}

int ObLogJoin::print_pq_distribute(planText& plan_text, JoinTreeType join_tree_type, bool is_need_print)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else {
    char* buf = plan_text.buf;
    int64_t& buf_len = plan_text.buf_len;
    int64_t& pos = plan_text.pos;
    bool is_oneline = plan_text.is_oneline_;
    ObPQDistributeMethod::Type left_dist_method;
    ObPQDistributeMethod::Type right_dist_method;
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    bool is_need = false;
    if (OB_FAIL(is_need_print_pq_dist(plan_text, stmt_hint, is_need))) {
      LOG_WARN("fail to judge whether need print pq dist", K(ret));
    } else if (!is_need) {
      /*do nothing*/
    } else if (OB_FAIL(get_pq_distribution_method(join_dist_algo_, left_dist_method, right_dist_method))) {
      LOG_WARN("failed to get distribution method", K(ret));
    } else if (ObPQDistributeMethod::MAX_VALUE == left_dist_method ||
               ObPQDistributeMethod::MAX_VALUE == right_dist_method) {
      /*do nothing*/
    } else if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::PQ_DISTRIBUTE))) {
    } else if (OB_FAIL(BUF_PRINTF("("))) {
    } else if (OB_FAIL(print_qb_name(plan_text))) {
    } else if (OB_FAIL(BUF_PRINTF(" ("))) {
    } else if (OB_FAIL(traverse_join_plan_pre(plan_text, PQ_DIST, join_tree_type, is_need_print))) {
      LOG_WARN("fail to print operator for outline", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(") "))) {
    } else if (OB_FAIL(BUF_PRINTF(ObPQDistributeMethod::get_type_string(left_dist_method)))) {
    } else if (OB_FAIL(BUF_PRINTF(" "))) {
    } else if (OB_FAIL(BUF_PRINTF(ObPQDistributeMethod::get_type_string(right_dist_method)))) {
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
    } else {
      is_added_outline_ = true;
    }
  }
  return ret;
}

int ObLogJoin::get_pq_distribution_method(const JoinDistAlgo join_dist_algo,
    ObPQDistributeMethod::Type& left_dist_method, ObPQDistributeMethod::Type& right_dist_method)
{
  int ret = OB_SUCCESS;
  left_dist_method = ObPQDistributeMethod::MAX_VALUE;
  right_dist_method = ObPQDistributeMethod::MAX_VALUE;
  if (JoinDistAlgo::DIST_PARTITION_NONE == join_dist_algo) {
    left_dist_method = ObPQDistributeMethod::PARTITION;
    right_dist_method = ObPQDistributeMethod::NONE;
  } else if (JoinDistAlgo::DIST_NONE_PARTITION == join_dist_algo) {
    left_dist_method = ObPQDistributeMethod::NONE;
    right_dist_method = ObPQDistributeMethod::PARTITION;
  } else if (JoinDistAlgo::DIST_BC2HOST_NONE == join_dist_algo) {
    left_dist_method = ObPQDistributeMethod::BC2HOST;
    right_dist_method = ObPQDistributeMethod::NONE;
  } else if (JoinDistAlgo::DIST_BROADCAST_NONE == join_dist_algo) {
    left_dist_method = ObPQDistributeMethod::BROADCAST;
    right_dist_method = ObPQDistributeMethod::NONE;
  } else if (JoinDistAlgo::DIST_NONE_BROADCAST == join_dist_algo) {
    left_dist_method = ObPQDistributeMethod::NONE;
    right_dist_method = ObPQDistributeMethod::BROADCAST;
  } else if (JoinDistAlgo::DIST_HASH_HASH == join_dist_algo) {
    left_dist_method = ObPQDistributeMethod::HASH;
    right_dist_method = ObPQDistributeMethod::HASH;
  } else { /*do nothing*/
  }
  return ret;
}

int ObLogJoin::print_pq_map(planText& plan_text, JoinTreeType join_tree_type, bool is_need_print)
{
  int ret = OB_SUCCESS;
  UNUSED(join_tree_type);
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_oneline = plan_text.is_oneline_;

  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL");
  } else {
    bool is_need = false;
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    if (OB_FAIL(is_need_print_pq_map(plan_text, stmt_hint, is_need))) {
      LOG_WARN("fail to judge whether need print pq dist", K(ret));
    } else if (!is_need) {
    } else {
      if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::PQ_MAP))) {
      } else if (OB_FAIL(BUF_PRINTF("("))) {
      } else if (OB_FAIL(print_qb_name(plan_text))) {
      } else if (OB_FAIL(BUF_PRINTF(" "))) {
      } else if (OB_FAIL(traverse_join_plan_pre(plan_text, PQ_MAP, join_tree_type, is_need_print))) {
        LOG_WARN("fail to print operator for outline", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(")"))) {
      } else { /*do nothing*/
      }
    }
    if (OB_SUCC(ret)) {
      is_added_outline_ = true;
    }
  }
  return ret;
}

int ObLogJoin::is_need_print_use_join(planText& plan_text, ObStmtHint& stmt_hint, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  OutlineType outline_type = plan_text.outline_type_;
  if (OUTLINE_DATA == outline_type) {
    is_need = true;
  } else if (USED_HINT == outline_type) {
    if (OB_ISNULL(get_child(second_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret));
    } else {
      const ObRelIds& use_idxs = get_child(second_child)->get_table_set();
      if (ObTransformUtils::is_subarray(use_idxs, stmt_hint.use_nl_idxs_)) {
        if (NESTED_LOOP_JOIN == get_join_algo()) {
          stmt_hint.valid_use_nl_idxs_.push_back(use_idxs);
          is_need = true;
        }
      }
      if (ObTransformUtils::is_subarray(use_idxs, stmt_hint.use_merge_idxs_)) {
        if (MERGE_JOIN == get_join_algo()) {
          stmt_hint.valid_use_merge_idxs_.push_back(use_idxs);
          is_need = true;
        }
      }
      if (ObTransformUtils::is_subarray(use_idxs, stmt_hint.use_hash_idxs_)) {
        if (HASH_JOIN == get_join_algo()) {
          stmt_hint.valid_use_hash_idxs_.push_back(use_idxs);
          is_need = true;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(outline_type));
  }
  return ret;
}

int ObLogJoin::is_need_print_material_nl(planText& plan_text, ObStmtHint& stmt_hint, bool& is_need, bool& use_mat)
{
  int ret = OB_SUCCESS;
  OutlineType outline_type = plan_text.outline_type_;
  if (OB_ISNULL(get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (NESTED_LOOP_JOIN != get_join_algo()) {
    is_need = false;
  } else if (OUTLINE_DATA == outline_type) {
    is_need = true;
    if (LOG_MATERIAL == get_child(second_child)->get_type()) {
      use_mat = true;
    } else {
      use_mat = false;
    }
  } else if (USED_HINT == outline_type) {
    const ObRelIds& use_idxs = get_child(second_child)->get_table_set();
    if (ObTransformUtils::is_subarray(use_idxs, stmt_hint.use_nl_materialization_idxs_)) {
      if (OB_FAIL(stmt_hint.valid_use_nl_materialization_idxs_.push_back(use_idxs))) {
        LOG_WARN("failed to append valid use material nl idxs.", K(ret));
      } else {
        is_need = true;
        use_mat = true;
      }
    } else if (ObTransformUtils::is_subarray(use_idxs, stmt_hint.no_use_nl_materialization_idxs_)) {
      if (OB_FAIL(stmt_hint.valid_no_use_nl_materialization_idxs_.push_back(use_idxs))) {
        LOG_WARN("failed to append valid no use material nl idxs.", K(ret));
      } else {
        is_need = true;
        use_mat = false;
      }
    } else {
      is_need = false;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(outline_type));
  }
  return ret;
}

int ObLogJoin::is_need_print_leading(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  OutlineType outline_type = plan_text.outline_type_;
  if (OUTLINE_DATA == outline_type) {
    is_need = !is_added_leading_outline_;
  } else if (USED_HINT == outline_type) {
    is_need = stmt_hint.is_used_leading() && !IS_OUTER_JOIN(join_type_) && !is_added_leading_hints_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(outline_type));
  }
  return ret;
}

int ObLogJoin::is_need_print_ordered(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  OutlineType outline_type = plan_text.outline_type_;
  if (OUTLINE_DATA == outline_type) {
    is_need = false;
  } else if (USED_HINT == outline_type) {
    is_need = stmt_hint.join_ordered_ && !IS_OUTER_JOIN(join_type_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(outline_type));
  }
  return ret;
}

int ObLogJoin::is_need_print_pq_dist(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  OutlineType outline_type = plan_text.outline_type_;
  if (OUTLINE_DATA == outline_type) {
    is_need = true;
  } else if (USED_HINT == outline_type) {
    if (OB_ISNULL(get_child(second_child))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("child is null", K(ret));
    } else {
      const ObRelIds& use_idxs = get_child(second_child)->get_table_set();
      for (int64_t idx = 0; idx < stmt_hint.valid_pq_distributes_idxs_.count(); idx++) {
        if (use_idxs.equal(stmt_hint.valid_pq_distributes_idxs_.at(idx))) {
          is_need = true;
          break;
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected outline type", K(ret), K(outline_type));
  }
  return ret;
}

int ObLogJoin::is_need_print_pq_map(planText& plan_text, const ObStmtHint& stmt_hint, bool& is_need)
{
  int ret = OB_SUCCESS;
  is_need = false;
  OutlineType outline_type = plan_text.outline_type_;
  if (OB_ISNULL(get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else {
    const ObRelIds& use_idxs = get_child(second_child)->get_table_set();
    if (USED_HINT == outline_type) {
      if (use_idxs.is_subset(stmt_hint.valid_pq_maps_idxs_) && use_slave_mapping()) {
        is_need = true;
      }
    } else if (OUTLINE_DATA == outline_type) {
      if (use_slave_mapping()) {
        is_need = true;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected outline type", K(ret), K(outline_type));
    }
  }
  return ret;
}

int ObLogJoin::print_leading(planText& plan_text, JoinTreeType join_tree_type)
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_oneline = plan_text.is_oneline_;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL");
  } else {
    const ObStmtHint& stmt_hint = stmt_->get_stmt_hint();
    bool is_need = false;
    if (OB_FAIL(is_need_print_ordered(plan_text, stmt_hint, is_need))) {
      LOG_WARN("fail to judge whether need print ordered", K(ret));
    } else if (is_need) {
      if (OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::ORDERED_HINT))) {
      } else { /*do nothing*/
      }
    } else if (OB_FAIL(is_need_print_leading(plan_text, stmt_hint, is_need))) {
      LOG_WARN("fail to judge whether need print leading", K(ret));
    } else if (!is_need) {  // no need to print
    } else if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
    } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::LEADING_HINT))) {
    } else if (OB_FAIL(BUF_PRINTF("("))) {
    } else if (OB_FAIL(print_qb_name(plan_text))) {
      LOG_WARN("fail to print query block name", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(" "))) {
    } else if (OB_FAIL(traverse_join_plan_ldr(plan_text, LEADING, join_tree_type))) {
      LOG_WARN("fail to traverse join plan", K(ret));
    } else if (OB_FAIL(BUF_PRINTF(")"))) {
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    is_added_outline_ = true;
  }
  return ret;
}

int ObLogJoin::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  ObDMLStmt* stmt = NULL;
  ObSelectStmt* select_stmt = NULL;
  if (OB_ISNULL(stmt = get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is NULL", K(ret));
  } else if (CONNECT_BY_JOIN != join_type_) {
    // do nothing
  } else if (OB_UNLIKELY(!stmt->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", KPC(stmt), K(ret));
  } else if (OB_UNLIKELY(false == (select_stmt = static_cast<ObSelectStmt*>(stmt))->is_hierarchical_query())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid stmt", KPC(stmt), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < select_stmt->get_pseudo_column_like_exprs().count(); ++i) {
      ObRawExpr* cur_expr = select_stmt->get_pseudo_column_like_exprs().at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid pseudo column expr", K(ret));
      } else if (cur_expr->is_pseudo_column_expr() &&
                 static_cast<ObPseudoColumnRawExpr*>(cur_expr)->is_hierarchical_query_type()) {
        if (OB_FAIL(pseudo_columns_.push_back(static_cast<ObPseudoColumnRawExpr*>(cur_expr)))) {
          LOG_WARN("fail to add pseudo column expr", KPC(cur_expr), K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate_expr_post", K(ret));
    }
  }

  ObSQLSessionInfo* session = NULL;
  CK(NULL != get_plan());
  // See: comment of ObLogPlan::extract_level_pseudo_as_param(), the old engine replace
  // expr at code generation. We can not alter raw expr in static engine CG, so we move
  // the replace to the end of EXPR_ALLOC.
  OX(session = get_plan()->get_optimizer_context().get_session_info());
  if (OB_SUCC(ret) && CONNECT_BY_JOIN == join_type_ && session->use_static_typing_engine() &&
      select_stmt->is_hierarchical_query() && !get_exec_params().empty()) {
    FOREACH_CNT_X(raw_expr, join_filters_, OB_SUCC(ret))
    {
      bool replaced = false;
      if (OB_ISNULL(*raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is NULL", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::replace_level_column(*raw_expr, get_exec_params().at(0).second, replaced))) {
        LOG_WARN("replace level column failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogJoin::allocate_granule_pre(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing", K(ctx));
  } else if (use_slave_mapping()) {
    ctx.slave_mapping_type_ = slave_mapping_type_;
  } else if (!ctx.is_in_partition_wise_state() && !ctx.is_in_pw_affinity_state() && is_partition_wise_) {
    /**
     *        (partition wise join)
     *                   |
     *                 JOIN(1)
     *                   |
     *             --------------
     *             |            |
     *           JOIN(2)       ...
     *             |
     *            ...
     *   JOIN(1) come into this code block.
     *   he will set 'this' ptr to gi allocate ctx as a reset token,
     *   and in the allocate-granule post stage JOIN(1) will
     *   reset the state of gi-allocate ctx.
     */
    ctx.set_in_partition_wise_state(this);
    LOG_TRACE("in find partition wise state", K(sharding_info_), K(ctx));
  } else if (ctx.is_in_partition_wise_state()) {
    /**
     *       (partition wise join with pkey reshuffle)
     *                   |
     *                 JOIN(1)
     *                   |
     *             --------------
     *             |            |
     *           JOIN(2)       ...
     *             |
     *        ------------
     *        |          |
     *       ...        EX(pkey)
     *                   |
     *                  ...
     *   JOIN(2) come into this code block.
     *   If there is a repartition(by key) below this partition wise plan,
     *   for some complex reason, we can not do partition wise join/union.
     *   We allocate gi above the table scan as usual, and set a affinitize property to these GI.
     */
    if (DIST_BROADCAST_NONE == join_dist_algo_ || DIST_NONE_BROADCAST == join_dist_algo_ ||
        DIST_PARTITION_NONE == join_dist_algo_ || DIST_NONE_PARTITION == join_dist_algo_) {
      if (OB_FAIL(ctx.set_pw_affinity_state())) {
        LOG_WARN("set affinity state failed", K(ret), K(ctx));
      }
      LOG_TRACE("partition wise affinity", K(ret));
    }
  }
  return ret;
}

int ObLogJoin::allocate_granule_post(AllocGIContext& ctx)
{
  int ret = OB_SUCCESS;
  /**
   *       (partition wise join)
   *                   |
   *                 JOIN(1)
   *                   |
   *             --------------
   *             |            |
   *           JOIN(2)       ...
   *             |
   *            ...
   *   JOIN(1) will reset the state of gi allocate ctx.
   *   As the ctx has record the state was changed by JOIN(2),
   *   so JOIN(2) can not reset this state.
   */
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing", K(sharding_info_));
  } else if (ctx.is_in_partition_wise_state()) {
    if (ctx.is_op_set_pw(this)) {
      ctx.alloc_gi_ = true;
      if (OB_FAIL(allocate_granule_nodes_above(ctx))) {
        LOG_WARN("allocate gi above table scan failed", K(ret));
      }
      IGNORE_RETURN ctx.reset_info();
    }
  } else if (ctx.is_in_pw_affinity_state()) {
    if (ctx.is_op_set_pw(this)) {
      ctx.alloc_gi_ = true;
      if (OB_FAIL(allocate_gi_recursively(ctx))) {
        LOG_WARN("allocate gi above table scan failed", K(ret));
      }
      IGNORE_RETURN ctx.reset_info();
    }
  } else if (DIST_NONE_PARTITION == join_dist_algo_) {
    if (OB_FAIL(set_granule_nodes_affinity(ctx, 0))) {
      LOG_WARN("set granule nodes affinity failed", K(ret));
    }
    LOG_TRACE("set left child gi to affinity");
  } else if (DIST_PARTITION_NONE == join_dist_algo_) {
    if (OB_FAIL(set_granule_nodes_affinity(ctx, 1))) {
      LOG_WARN("set granule nodes affinity failed", K(ret));
    } else if (can_enable_gi_partition_pruning()) {
      if (OB_FAIL(build_gi_partition_pruning())) {
        LOG_WARN("fail deterimine right child partition id", K(ret));
      }
    }
    LOG_TRACE("set right child gi to affinity");
  } else if (DIST_BC2HOST_NONE == join_dist_algo_) {
    ObLogicalOperator* op = NULL;
    if (OB_FAIL(get_child(second_child)->find_first_recursive(LOG_GRANULE_ITERATOR, op))) {
      LOG_WARN("find granule iterator in right failed", K(ret));
    } else if (NULL == op) {
      // granule iterator not found, do nothing
    } else {
      static_cast<ObLogGranuleIterator*>(op)->add_flag(GI_ACCESS_ALL);
    }
  } else if (is_nlj_with_param_down()) {
    ObLogicalOperator* op = NULL;
    if (OB_FAIL(get_child(second_child)->find_first_recursive(LOG_GRANULE_ITERATOR, op))) {
      LOG_WARN("find granule iterator in right failed", K(ret));
    } else if (NULL == op) {
      // granule iterator not found, do nothing
    } else {
      static_cast<ObLogGranuleIterator*>(op)->add_flag(GI_NLJ_PARAM_DOWN);
    }
  }
  return ret;
}

int ObLogJoin::bloom_filter_partition_type(
    const ObShardingInfo& right_child_sharding_info, ObIArray<ObRawExpr*>& right_keys, PartitionFilterType& type)
{
  int ret = OB_SUCCESS;
  bool one_level_partition_covered = true;
  bool two_level_partition_covered = true;
  const ObIArray<ObRawExpr*>& partition_keys = right_child_sharding_info.get_partition_keys();
  const ObIArray<ObRawExpr*>& sub_partition_keys = right_child_sharding_info.get_sub_partition_keys();
  int64_t M = partition_keys.count();
  int64_t N = sub_partition_keys.count();
  int64_t right_count = right_keys.count();
  /*
   * check the right key is equal to the one level partition key
   * */
  if (0 == M) {
    one_level_partition_covered = false;
  } else {
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && one_level_partition_covered && i < M; ++i) {
      is_found = false;
      if (OB_ISNULL(partition_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition_keys contain null", K(i), K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < right_count; ++j) {
          if (OB_ISNULL(right_keys.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("right_join_key is NULL", K(i), KP(right_keys.at(j)), K(ret));
          } else if (partition_keys.at(i) == right_keys.at(j)) {
            is_found = true;
          }
        }
        if (!is_found) {
          one_level_partition_covered = false;
        }
      }
    }
  }
  /*
   * check the left key is equal to the two level partition key
   * */
  if (0 == N) {
    two_level_partition_covered = false;
  } else {
    bool is_found = false;
    for (int64_t i = 0; OB_SUCC(ret) && two_level_partition_covered && i < N; ++i) {
      is_found = false;
      if (OB_ISNULL(sub_partition_keys.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("partition_keys contain null", K(i), K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && !is_found && j < right_count; ++j) {
          if (OB_ISNULL(right_keys.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("right_join_key is NULL", K(i), KP(right_keys.at(j)), K(ret));
          } else if (sub_partition_keys.at(i) == right_keys.at(j)) {
            is_found = true;
          }
        }
        if (!is_found) {
          two_level_partition_covered = false;
        }
      }
    }
  }
  /*
   * get type of partition filter
   * */
  type = one_level_partition_covered ? OneLevelPartitionKey : Forbidden;
  type = ((type == OneLevelPartitionKey) && two_level_partition_covered) ? TwoLevelPartitionKey : type;
  return ret;
}

bool ObLogJoin::is_block_input(const int64_t child_idx) const
{
  return HASH_JOIN == join_algo_ && 0 == child_idx;
}

int ObLogJoin::can_use_batch_nlj(bool& use_batch_nlj)
{
  int ret = OB_SUCCESS;
  const ParamStore* params = NULL;
  use_batch_nlj = false;
  if (NESTED_LOOP_JOIN == join_algo_ && CONNECT_BY_JOIN != join_type_ && !IS_SEMI_ANTI_JOIN(join_type_)) {
    ObSQLSessionInfo* session_info = NULL;
    ObLogTableScan* ts = NULL;
    if (OB_ISNULL(get_plan()) || OB_ISNULL(session_info = get_plan()->get_optimizer_context().get_session_info()) ||
        OB_ISNULL(params = get_plan()->get_optimizer_context().get_params())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(get_plan()), K(session_info), K(ret));
    } else if (OB_FAIL(session_info->get_nlj_batching_enabled(use_batch_nlj))) {
      LOG_WARN("failed to get join cache size variable", K(ret));
    } else if (use_batch_nlj) {
      if (OB_ISNULL(get_child(1))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid right child", K(get_child(1)), K(ret));
      } else if (get_child(1)->is_table_scan()) {
        ts = static_cast<ObLogTableScan*>(get_child(1));
        // normal table scan on real table
        use_batch_nlj =
            (!is_virtual_table(ts->get_ref_table_id()) && !ts->get_is_fake_cte_table() && !ts->is_for_update());
      } else {
        use_batch_nlj = false;
      }
    }
    if (OB_SUCC(ret) && use_batch_nlj) {
      const ObIArray<ObRawExpr*>& table_filters = ts->get_filter_exprs();
      bool contain_nl_param = false;
      if (OB_FAIL(ObOptimizerUtil::is_contain_nl_params(table_filters, params->count(), contain_nl_param))) {
        LOG_WARN("fail to check nl param", K(ret), K(table_filters), K(params->count()));
      } else if (contain_nl_param) {
        use_batch_nlj = false;
      }
    }
  }
  return ret;
}

int ObLogJoin::is_left_unique(bool& left_unique) const
{
  int ret = OB_SUCCESS;
  left_unique = false;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_stmt()), K(get_child(first_child)));
  } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(left_expected_ordering_,
                 get_child(first_child)->get_table_set(),
                 get_child(first_child)->get_fd_item_set(),
                 get_child(first_child)->get_ordering_output_equal_sets(),
                 get_child(first_child)->get_output_const_exprs(),
                 left_unique))) {
    LOG_WARN("fail to check unique condition", K(ret));
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogJoin::get_candidate_join_distribution_method(ObLogPlan& log_plan, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& left_join_keys, const ObIArray<ObRawExpr*>& right_join_keys, uint64_t& candidate_method)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  candidate_method = 0;
  // get all candidate method
  if (OB_ISNULL(left_child = get_child(first_child)) || OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
  } else if (is_nlj_with_param_down()) {
    add_join_dist_flag(candidate_method, DIST_PULL_TO_LOCAL);
    add_join_dist_flag(candidate_method, DIST_PARTITION_WISE);
    add_join_dist_flag(candidate_method, DIST_PARTITION_NONE);
    bool is_table_scan = false;
    if (OB_FAIL(check_is_table_scan(*right_child, is_table_scan))) {
      LOG_WARN("failed to check is table scan", K(ret));
    } else if (INNER_JOIN == join_type_ && is_table_scan) {
      /*
       * todo we should use strategy to choose
       * between DIST_BC2HOST_NONE and DIST_BROADCAST_NONE in future
       */
      add_join_dist_flag(candidate_method, DIST_BC2HOST_NONE);
    }
  } else {
    // without BC2HOST
    ADD_JOIN_DIST_METHOD_WITHOUT_BC2HOST(candidate_method);
    if (is_nlj_without_param_down()) {
      // nested loop join without parameter down can not use hash/hash
      // todo release this constrain
      remove_join_dist_flag(candidate_method, DIST_HASH_HASH);
    }
    if (IS_LEFT_STYLE_JOIN(join_type_)) {
      // without BC2HOST DIST_BROADCAST_NONE
      remove_join_dist_flag(candidate_method, DIST_BROADCAST_NONE);
    }
    if (IS_RIGHT_STYLE_JOIN(join_type_)) {
      // without BC2HOST DIST_NONE_BROADCAST
      remove_join_dist_flag(candidate_method, DIST_NONE_BROADCAST);
    }
  }

  if (OB_SUCC(ret)) {
    if (1 >= log_plan.get_optimizer_context().get_parallel()) {
      REMOVE_PX_PARALLEL_DFO_DIST_METHOD(candidate_method);
    }
  }

  // remove invalid method
  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_PARTITION_WISE)) {
    bool is_partition_wise = false;
    if (OB_FAIL(check_if_match_join_partition_wise(
            log_plan, equal_sets, left_join_keys, right_join_keys, is_partition_wise))) {
      LOG_WARN("failed to check if match partition wise join", K(ret));
    } else if (!is_partition_wise) {
      remove_join_dist_flag(candidate_method, DIST_PARTITION_WISE);
    }
  }
  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_PARTITION_NONE)) {
    bool left_match_repart = false;
    if (OB_FAIL(check_if_match_repart(equal_sets, left_join_keys, right_join_keys, *right_child, left_match_repart))) {
      LOG_WARN("failed to check if match repart", K(ret));
    } else if (!left_match_repart) {
      remove_join_dist_flag(candidate_method, DIST_PARTITION_NONE);
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret) && has_join_dist_flag(candidate_method, DIST_NONE_PARTITION)) {
    bool right_match_repart = false;
    if (OB_FAIL(check_if_match_repart(equal_sets, right_join_keys, left_join_keys, *left_child, right_match_repart))) {
      LOG_WARN("failed to check_and_extract_repart_info", K(ret));
    } else if (!right_match_repart) {
      // this join can't do pwj, remove flag DIST_PARTITION_NONE.
      remove_join_dist_flag(candidate_method, DIST_NONE_PARTITION);
    }
  }

  // get hint method
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_hint_join_distribution_method(candidate_method, pq_map_hint_))) {
      LOG_WARN("failed to get hint dist method", K(ret));
    } else {
      bool use_seq_exec = false;
      if (has_join_dist_flag(candidate_method, DIST_PULL_TO_LOCAL) &&
          !has_join_dist_flag(candidate_method, DIST_PARTITION_WISE) &&
          OB_FAIL(should_use_sequential_execution(use_seq_exec))) {
        LOG_WARN("failed to check should use sequential execution", K(ret));
      } else if (use_seq_exec) {
        candidate_method = DIST_PULL_TO_LOCAL;
      }
      if (CONNECT_BY_JOIN == join_type_) {
        candidate_method = DIST_PULL_TO_LOCAL;
      }
      LOG_TRACE("succeed to get candidate methods",
          "DIST_PARTITION_WISE",
          has_join_dist_flag(candidate_method, DIST_PARTITION_WISE),
          "DIST_PARTITION_NONE",
          has_join_dist_flag(candidate_method, DIST_PARTITION_NONE),
          "DIST_NONE_PARTITION",
          has_join_dist_flag(candidate_method, DIST_NONE_PARTITION),
          "DIST_HASH_HASH",
          has_join_dist_flag(candidate_method, DIST_HASH_HASH),
          "DIST_BROADCAST_NONE",
          has_join_dist_flag(candidate_method, DIST_BROADCAST_NONE),
          "DIST_NONE_BROADCAST",
          has_join_dist_flag(candidate_method, DIST_NONE_BROADCAST),
          "DIST_PULL_TO_LOCAL",
          has_join_dist_flag(candidate_method, DIST_PULL_TO_LOCAL),
          "DIST_BC2HOST_NONE",
          has_join_dist_flag(candidate_method, DIST_BC2HOST_NONE));
    }
  }
  return ret;
}

int ObLogJoin::check_if_match_join_partition_wise(ObLogPlan& log_plan, const EqualSets& equal_sets,
    const ObIArray<ObRawExpr*>& left_keys, const ObIArray<ObRawExpr*>& right_keys, bool& is_partition_wise)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  is_partition_wise = false;
  if (OB_ISNULL(left_child = get_child(first_child)) || OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(ret));
  } else if (left_child->get_sharding_info().is_match_all() && right_child->get_sharding_info().is_distributed() &&
             NULL != right_child->get_sharding_info().get_phy_table_location_info() &&
             !IS_LEFT_STYLE_JOIN(join_type_)) {
    is_partition_wise = true;
  } else if (left_child->get_sharding_info().is_distributed() &&
             NULL != left_child->get_sharding_info().get_phy_table_location_info() &&
             right_child->get_sharding_info().is_match_all() && !IS_RIGHT_STYLE_JOIN(join_type_)) {
    is_partition_wise = true;
  } else if (left_child->get_sharding_info().is_distributed() && right_child->get_sharding_info().is_distributed() &&
             OB_FAIL(ObShardingInfo::check_if_match_partition_wise(log_plan,
                 equal_sets,
                 left_keys,
                 right_keys,
                 left_child->get_sharding_info(),
                 right_child->get_sharding_info(),
                 is_partition_wise))) {
    LOG_WARN("failed to check is match partition wise join", K(ret));
  } else {
    LOG_TRACE("succeed to check match partition wise join", K(is_partition_wise));
  }
  return ret;
}

int ObLogJoin::get_hint_join_distribution_method(uint64_t& candidate_method, bool& is_slave_mapping)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* left_child = NULL;
  ObLogicalOperator* right_child = NULL;
  uint64_t temp_method = candidate_method;
  JoinDistAlgo hint_method = DIST_INVALID_METHOD;
  is_slave_mapping = false;
  if (OB_ISNULL(get_stmt()) || OB_ISNULL(left_child = get_child(first_child)) ||
      OB_ISNULL(right_child = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(left_child), K(right_child), K(get_stmt()), K(ret));
  } else {
    // get distributed hint method
    ObIArray<ObPQDistributeIndex>& pq_hints = get_stmt()->get_stmt_hint().pq_distributes_idxs_;
    for (int64_t i = 0; OB_SUCC(ret) && DIST_INVALID_METHOD == hint_method && i < pq_hints.count(); i++) {
      ObPQDistributeIndex& hint = pq_hints.at(i);
      if (hint.is_join_ && right_child->get_table_set().equal(hint.rel_ids_)) {
        if (ObPQDistributeMethod::PARTITION == hint.outer_ && ObPQDistributeMethod::NONE == hint.inner_) {
          hint_method = DIST_PARTITION_NONE;
        } else if (ObPQDistributeMethod::NONE == hint.outer_ && ObPQDistributeMethod::PARTITION == hint.inner_) {
          hint_method = DIST_NONE_PARTITION;
        } else if (ObPQDistributeMethod::BROADCAST == hint.outer_ && ObPQDistributeMethod::NONE == hint.inner_) {
          hint_method = DIST_BROADCAST_NONE;
        } else if (ObPQDistributeMethod::NONE == hint.outer_ && ObPQDistributeMethod::BROADCAST == hint.inner_) {
          hint_method = DIST_NONE_BROADCAST;
        } else if (ObPQDistributeMethod::HASH == hint.outer_ && ObPQDistributeMethod::HASH == hint.inner_) {
          hint_method = DIST_HASH_HASH;
        } else { /*do nothing*/
        }
      }
    }
    // get slave mapping
    if (OB_SUCC(ret)) {
      auto& pq_map_idxs = get_stmt()->get_stmt_hint().pq_map_idxs_;
      if (!pq_map_idxs.is_empty() && LOG_TABLE_SCAN == left_child->get_type() &&
          LOG_TABLE_SCAN == right_child->get_type() && right_child->get_table_set().is_subset(pq_map_idxs)) {
        is_slave_mapping = true;
        remove_join_dist_flag(temp_method, DIST_HASH_HASH);
        remove_join_dist_flag(temp_method, DIST_BC2HOST_NONE);
        if (!has_join_dist_flag(temp_method, DIST_PARTITION_NONE)) {
          remove_join_dist_flag(temp_method, DIST_BROADCAST_NONE);
        }
        if (!has_join_dist_flag(temp_method, DIST_NONE_PARTITION)) {
          remove_join_dist_flag(temp_method, DIST_NONE_BROADCAST);
        }
      }
    }
    if (OB_FAIL(ret)) {
      /*do nothing*/
    } else if ((DIST_INVALID_METHOD != hint_method && (0 == (hint_method & temp_method))) ||
               (is_slave_mapping && 0 == temp_method)) {
      // hint does not work
      is_slave_mapping = false;
    } else if (DIST_INVALID_METHOD != hint_method && is_slave_mapping) {
      candidate_method = hint_method;
      if (OB_FAIL(get_stmt()->get_stmt_hint().valid_pq_distributes_idxs_.push_back(right_child->get_table_set()))) {
        LOG_WARN("failed to push table set", K(ret));
      } else if (OB_FAIL(get_stmt()->get_stmt_hint().valid_pq_maps_idxs_.add_members(right_child->get_table_set()))) {
        LOG_WARN("failed to push back table set", K(ret));
      }
    } else if (DIST_INVALID_METHOD != hint_method && !is_slave_mapping) {
      candidate_method = hint_method;
      ret = get_stmt()->get_stmt_hint().valid_pq_distributes_idxs_.push_back(right_child->get_table_set());
    } else if (DIST_INVALID_METHOD == hint_method && is_slave_mapping) {
      candidate_method = temp_method;
      ret = get_stmt()->get_stmt_hint().valid_pq_maps_idxs_.add_members(right_child->get_table_set());
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObLogJoin::compute_table_set()
{
  int ret = OB_SUCCESS;
  if (IS_SEMI_ANTI_JOIN(join_type_)) {
    ObLogicalOperator* child = NULL;
    if (IS_LEFT_SEMI_ANTI_JOIN(join_type_)) {
      child = get_child(ObLogicalOperator::first_child);
    } else {
      child = get_child(ObLogicalOperator::second_child);
    }
    if (OB_ISNULL(child)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null child", K(ret));
    } else {
      set_table_set(&child->get_table_set());
    }
  } else if (OB_FAIL(ObLogicalOperator::compute_table_set())) {
    LOG_WARN("failed to compute table set", K(ret));
  }
  return ret;
}

int ObLogJoin::update_weak_part_exprs(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* tmp = NULL;
  if (OB_ISNULL(ctx) || OB_ISNULL(get_stmt()) || OB_ISNULL(get_child(first_child)) ||
      OB_ISNULL(get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join child is null", K(ret), K(ctx));
  } else if (get_join_type() == LEFT_OUTER_JOIN) {
    tmp = get_child(second_child);
  } else if (get_join_type() == RIGHT_OUTER_JOIN) {
    tmp = get_child(first_child);
  } else if (get_join_type() == FULL_OUTER_JOIN) {
    tmp = this;
  }
  if (OB_SUCC(ret) && NULL != tmp) {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_stmt()->get_column_size(); ++i) {
      ObRawExpr* col = NULL;
      if (OB_ISNULL(col = get_stmt()->get_column_items().at(i).expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is null", K(ret));
      } else if (!tmp->get_table_set().is_superset(col->get_relation_ids())) {
        // do nothing
      } else if (OB_FAIL(ctx->weak_part_exprs_.push_back(col))) {
        LOG_WARN("failed to push back weak part expr", K(ret));
      }
    }
  }
  return ret;
}

int ObLogJoin::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  if (stmt_->is_select_stmt()) {
    const ObSelectStmt* select_stmt = static_cast<const ObSelectStmt*>(stmt_);
    ObSEArray<ObRawExpr*, 8> connect_by_root_exprs;
    ObSEArray<ObRawExpr*, 8> sys_connect_by_path_exprs;
    if (OB_FAIL(select_stmt->get_connect_by_root_exprs(connect_by_root_exprs))) {
      LOG_WARN("failed to get connect by root exprs", K(ret));
    } else if (OB_FAIL(select_stmt->get_sys_connect_by_path_exprs(sys_connect_by_path_exprs))) {
      LOG_WARN("failed to get sys connect by path exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < pseudo_columns_.count(); i++) {
      OZ(raw_exprs.append(static_cast<ObRawExpr*>(pseudo_columns_.at(i))));
    }
    OZ(raw_exprs.append(connect_by_prior_exprs_));
    for (int64_t i = 0; OB_SUCC(ret) && i < exec_params_.count(); i++) {
      CK(OB_NOT_NULL(exec_params_.at(i).second));
      OZ(raw_exprs.append(exec_params_.at(i).second));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < connect_by_root_exprs.count(); i++) {
      OZ(raw_exprs.append(connect_by_root_exprs.at(i)));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < sys_connect_by_path_exprs.count(); i++) {
      OZ(raw_exprs.append(sys_connect_by_path_exprs.at(i)));
    }
  }
  return ret;
}

int ObLogJoin::generate_link_sql_pre(GenLinkStmtContext& link_ctx)
{
  /**
   * from_strs: "?" => "(" "?" "[left / right / full] join" "?" "on" "cond1" "and cond2" ")"
   *            cond may have @1 if join is nested loop, @1 should be filled in other op
   *            like table_scan.
   *            on cond comes from equal_conds and other_conds.
   * where_strs: like other all ops, where cond comes from filter.
   */
  int ret = OB_SUCCESS;
  ObLinkStmt* link_stmt = link_ctx.link_stmt_;
  ObLogicalOperator* right_child = get_child(second_child);
  if (OB_ISNULL(link_stmt) || !link_stmt->is_inited()) {
    // do nothing.
  } else if (dblink_id_ != link_ctx.dblink_id_) {
    link_ctx.dblink_id_ = OB_INVALID_ID;
    link_ctx.link_stmt_ = NULL;
  } else if (OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right child is NULL", K(ret));
  } else if (OB_FAIL(link_stmt->force_fill_select_strs())) {
    LOG_WARN("failed to fill link stmt select strs", K(ret));
  } else if (OB_FAIL(link_stmt->fill_from_strs(
                 join_type_, join_conditions_, join_filters_, right_child->get_pushdown_filter_exprs()))) {
    LOG_WARN("failed to fill link stmt from strs",
        K(ret),
        K(join_type_),
        K(join_conditions_),
        K(join_filters_),
        K(right_child->get_pushdown_filter_exprs()));
  } else if (OB_FAIL(link_stmt->fill_where_strs(filter_exprs_))) {
    LOG_WARN("failed to fill link stmt where strs", K(ret), K(filter_exprs_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < nl_params_.count(); i++) {
      if (OB_FAIL(link_stmt->append_nl_param_idx(nl_params_.at(i).first))) {
        LOG_WARN("failed to append nl paranm idx", K(ret), K(i), K(nl_params_.at(i).first));
      }
    }
  }
  return ret;
}

int ObLogJoin::alloc_partition_id_column(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  if (OB_FAIL(get_left_exch_repartition_table_id(table_id))) {
    LOG_WARN("fail get repart tid", K(ret));
  } else if (OB_FAIL(alloc_partition_id_expr(table_id, ctx, partition_id_expr_))) {
    LOG_WARN("fail alloc partition id expr", K(ret));
  }
  return ret;
}

int ObLogJoin::get_left_exch_repartition_table_id(uint64_t& table_id)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator* op = NULL;
  if (OB_FAIL(get_child(first_child)->find_first_recursive(LOG_EXCHANGE, op))) {
    LOG_WARN("find granule iterator in right failed", K(ret));
  } else if (NULL == op) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log exchange not found", K(ret));
  } else {
    table_id = static_cast<ObLogExchange*>(op)->get_repartition_table_id();
  }
  return ret;
}
