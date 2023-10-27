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
#include "sql/optimizer/ob_log_join_filter.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_log_granule_iterator.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"
#include "sql/optimizer/ob_join_order.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql::log_op_def;
using oceanbase::share::schema::ObTableSchema;
using share::schema::ObSchemaGetterGuard;

int ObLogJoin::build_gi_partition_pruning()
{
  int ret = OB_SUCCESS;
  // 1. join 上标记 request part id
  // 2. join 上标记右侧 gi 为 partition pruning 模式
  ObLogicalOperator *receive = NULL;
  ObLogicalOperator *transmit = NULL;
  if (OB_FAIL(get_child(first_child)->find_first_recursive(LOG_EXCHANGE, receive))) {
    LOG_WARN("find granule iterator in right failed", K(ret));
  } else if (OB_UNLIKELY(NULL == receive || 1 != receive->get_num_of_child())
             || OB_ISNULL(transmit = receive->get_child(0))
             || OB_UNLIKELY(LOG_EXCHANGE != transmit->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log exchange not found", K(ret));
  } else if (OB_FAIL(set_granule_repart_ref_table_id_recursively(get_child(second_child),
             static_cast<ObLogExchange *>(transmit)->get_repartition_ref_table_id()))) {
    LOG_WARN("set granule repart table id failed", K(ret));
  }
  return ret;
}

int ObLogJoin::set_granule_repart_ref_table_id_recursively(ObLogicalOperator *op, int64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logical operator is null", K(ret));
  } else if (op->get_type() == LOG_GRANULE_ITERATOR) {
    ObLogGranuleIterator *gi_op = static_cast<ObLogGranuleIterator *>(op);
    gi_op->set_repartition_ref_table_id(ref_table_id);
    gi_op->add_flag(GI_ENABLE_PARTITION_PRUNING);
  } else if (op->get_type() == LOG_EXCHANGE) {
    // do nothing.
  } else {
    for (int64_t i = 0; i < op->get_num_of_child() && OB_SUCC(ret); i++) {
      if (OB_FAIL(SMART_CALL(set_granule_repart_ref_table_id_recursively(op->get_child(i), ref_table_id)))) {
        LOG_WARN("set granule repart table id failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLogJoin::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(all_exprs, join_conditions_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (OB_FAIL(append_array_no_dup(all_exprs, join_filters_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (CONNECT_BY_JOIN == join_type_ && OB_FAIL(get_connect_by_exprs(all_exprs))) {
    LOG_WARN("failed to add connect by exprs", K(ret));
  } else if (can_enable_gi_partition_pruning() && OB_FAIL(generate_join_partition_id_expr())) {
    LOG_WARN("failed to generate join partition id expr", K(ret));
  } else if (NULL != partition_id_expr_ && OB_FAIL(all_exprs.push_back(partition_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < nl_params_.count(); i++) {
      if (OB_ISNULL(nl_params_.at(i)) ||
          OB_ISNULL(nl_params_.at(i)->get_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (OB_FAIL(all_exprs.push_back(nl_params_.at(i)->get_ref_expr()))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
        LOG_WARN("failed to get op exprs", K(ret));
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

int ObLogJoin::get_connect_by_exprs(ObIArray<ObRawExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(get_stmt()), K(ret));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt *>(get_stmt());
    if (OB_FAIL(select_stmt->get_connect_by_root_exprs(get_connect_by_root_exprs()))) {
      LOG_WARN("failed to get connect by root exprs", K(ret));
    } else if (OB_FAIL(select_stmt->get_sys_connect_by_path_exprs(get_sys_connect_by_path_exprs()))) {
      LOG_WARN("failed to get sys connect by path exprs", K(ret));
    } else if (OB_FAIL(select_stmt->get_prior_exprs(get_prior_exprs()))) {
      LOG_WARN("failed to get prior exprs", K(ret));
    } else if (OB_FAIL(append(exprs, connect_by_root_exprs_)) ||
               OB_FAIL(append(exprs, sys_connect_by_path_exprs_)) ||
               OB_FAIL(append(exprs, prior_exprs_)) ||
               OB_FAIL(append(exprs, connect_by_pseudo_columns_)) ||
               OB_FAIL(append_array_no_dup(exprs, connect_by_prior_exprs_)) ||
               OB_FAIL(append_array_no_dup(exprs, connect_by_extra_exprs_))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

uint64_t ObLogJoin::hash(uint64_t seed) const
{
  seed = do_hash(join_type_, seed);
  seed = do_hash(join_algo_, seed);
  seed = do_hash(join_dist_algo_, seed);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogJoin::get_explain_name_internal(char *buf,
                                         const int64_t buf_len,
                                         int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (NESTED_LOOP_JOIN == join_algo_) {
    ret = BUF_PRINTF("NESTED-LOOP ");
  } else if (MERGE_JOIN == join_algo_) {
    ret = BUF_PRINTF("MERGE ");
  } else if (HASH_JOIN == join_algo_ &&
             DIST_BC2HOST_NONE == join_dist_algo_) {
    ret = BUF_PRINTF("SHARED HASH ");
  } else {
    ret = BUF_PRINTF("HASH ");
  }
  if (OB_SUCC(ret)) {
    ret = BUF_PRINTF("%.*s ",
                     ob_join_type_str(join_type_).length(),
                     ob_join_type_str(join_type_).ptr());
  } else { /* Do nothing */ }
  if(OB_SUCC(ret) && is_cartesian()) {
    ret = BUF_PRINTF("CARTESIAN ");
  }
  if (OB_SUCC(ret) &&
      nullptr != join_path_ &&
      HASH_JOIN == join_algo_ &&
      join_path_->is_naaj_) {
    if (join_path_->is_sna_) {
      ret = BUF_PRINTF("SNA");
    } else {
      ret = BUF_PRINTF("NA");
    }
  }
  return ret;
}

int ObLogJoin::get_plan_item_info(PlanText &plan_text,
                                  ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    BEGIN_BUF_PRINT;
    if (OB_FAIL(get_explain_name_internal(buf, buf_len, pos))) {
      LOG_WARN("failed to get explain name", K(ret));
    } else {
      END_BUF_PRINT(plan_item.operation_, plan_item.operation_len_);
    }
  }
  if (OB_SUCC(ret)) {
    BEGIN_BUF_PRINT;
    if (NESTED_LOOP_JOIN == get_join_algo()) {
      const ObIArray<ObRawExpr *> &conds = get_other_join_conditions();
      EXPLAIN_PRINT_EXPRS(conds, type);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        EXPLAIN_PRINT_EXEC_EXPRS(nl_params_, type);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else if (OB_FAIL(BUF_PRINTF("use_batch=%s", can_use_batch_nlj_? "true" : "false"))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else { /* Do nothing */ }
    } else if (HASH_JOIN == get_join_algo()) {
      const ObIArray<ObRawExpr *> &equal_conds = get_equal_join_conditions();
      const ObIArray<ObRawExpr *> &other_conds = get_other_join_conditions();
      EXPLAIN_PRINT_EXPRS(equal_conds, type);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        EXPLAIN_PRINT_EXPRS(other_conds, type);
      }
    } else {
      const ObIArray<ObRawExpr *> &equal_conds = get_equal_join_conditions();
      const ObIArray<ObRawExpr *> &other_conds = get_other_join_conditions();
      const ObIArray<ObOrderDirection> &merge_directions = get_merge_directions();
      EXPLAIN_PRINT_EXPRS(equal_conds, type);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF(", "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        EXPLAIN_PRINT_EXPRS(other_conds, type);
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
        LOG_WARN("BUF_PRINTF fails", K(ret));
      } else {
        EXPLAIN_PRINT_MERGE_DIRECTIONS(merge_directions);
      }
    }
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
  }
  return ret;
}

int ObLogJoin::adjust_join_conds(ObIArray<ObRawExpr *> &dest_exprs)
{
  int ret = OB_SUCCESS;
  int64_t dest_num = dest_exprs.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < dest_num; ++i) {
    ObRawExpr *&cur_expr = dest_exprs.at(i);
    ObRawExpr *lexpr = NULL;
    ObRawExpr *rexpr = NULL;
    if (OB_ISNULL(lexpr = cur_expr->get_param_expr(0)) ||
        OB_ISNULL(rexpr = cur_expr->get_param_expr(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(lexpr), K(rexpr), K(ret));
    } else if (!(T_OP_EQ == cur_expr->get_expr_type() ||
              T_OP_NSEQ == cur_expr->get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid argument", K(cur_expr->get_expr_type()), K(ret));
    } else if (T_OP_EQ == cur_expr->get_expr_type()) {
      ObSEArray<ObRawExpr*, 4> left_columns;
      ObSEArray<ObRawExpr*, 4> right_columns;
      if (OB_FAIL(ObRawExprUtils::extract_column_exprs(lexpr, left_columns))) {
        LOG_WARN("extract column exprs failed", K(ret), K(lexpr));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(rexpr, right_columns))) {
        LOG_WARN("extract column exprs failed", K(ret), K(rexpr));
      } else {
        bool is_conclude_gen_col = false;
        for (int64_t j = 0; OB_SUCC(ret) && !is_conclude_gen_col &&
              j < left_columns.count(); ++j) {
          ObRawExpr *dep_column = left_columns.at(j);
          if (OB_ISNULL(dep_column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("deps_column is null");
          } else if (!dep_column->is_column_ref_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dep column is invalid", K(ret), KPC(dep_column));
          } else if (static_cast<ObColumnRefRawExpr *>(dep_column)->is_generated_column()) {
            is_conclude_gen_col = true;
          }
        }
        for (int64_t j = 0; OB_SUCC(ret) && !is_conclude_gen_col &&
              j < right_columns.count(); ++j) {
          ObRawExpr *dep_column = right_columns.at(j);
          if (OB_ISNULL(dep_column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("deps_column is null");
          } else if (!dep_column->is_column_ref_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("dep column is invalid", K(ret), KPC(dep_column));
          } else if (static_cast<ObColumnRefRawExpr *>(dep_column)->is_generated_column()) {
            is_conclude_gen_col = true;
          }
        }
        if (OB_SUCC(ret) && is_conclude_gen_col) {
          bool is_opposite = false;
          if (OB_FAIL(calc_equal_cond_opposite(
                *cur_expr, is_opposite))) {
            LOG_WARN("failed to calc equal condition opposite", K(ret));
          } else {
            LOG_INFO("do is_opposite", K(ret), K(is_opposite));
            // Before generating column replacement, determine whether the dependent expression
            // is a constant expression. If so, you need to change the left and right node positions
            // in advance.
            if (is_opposite) {
              std::swap(cur_expr->get_param_expr(0), cur_expr->get_param_expr(1));
            }
          }
        }
      }
    }

  }
  return ret;
}

int ObLogJoin::calc_equal_cond_opposite(const ObRawExpr &raw_expr,
                                               bool &is_opposite)
{
  int ret = OB_SUCCESS;
  is_opposite = false;
  const ObLogicalOperator *left_child = NULL;
  const ObLogicalOperator *right_child = NULL;
  const ObRawExpr *lexpr = NULL;
  const ObRawExpr *rexpr = NULL;
  if (OB_ISNULL(lexpr = raw_expr.get_param_expr(0)) ||
      OB_ISNULL(rexpr = raw_expr.get_param_expr(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(lexpr), K(rexpr), K(ret));
  } else if (!(T_OP_EQ == raw_expr.get_expr_type() ||
            T_OP_NSEQ == raw_expr.get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(raw_expr.get_expr_type()), K(ret));
  } else if (OB_ISNULL(left_child = this->get_child(0)) ||
      OB_ISNULL(right_child = this->get_child(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(left_child), K(right_child), K(ret));
  } else if (lexpr->get_relation_ids().is_subset(left_child->get_table_set())
      && rexpr->get_relation_ids().is_subset(right_child->get_table_set())) {
    is_opposite = false;
  } else if (lexpr->get_relation_ids().is_subset(right_child->get_table_set())
              && rexpr->get_relation_ids().is_subset(left_child->get_table_set())) {
    is_opposite = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid equal condition", K(this), K(raw_expr), K(ret));
  }

  return ret;
}

int ObLogJoin::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, get_join_conditions()))) {
    LOG_WARN("failed to extract subplan params in log join_conditions", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, get_join_filters()))) {
    LOG_WARN("failed to extract subplan params in log join_filters", K(ret));
  } else {
    int64_t N = get_nl_params().count();
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ObRawExpr *&cur_expr = get_nl_params().at(i)->get_ref_expr();
      if (OB_FAIL(replace_expr_action(replacer, cur_expr))) {
        LOG_WARN("failed to extract subplan params in log join_filters", K(ret));
      } else { /* Do nothing */ }
    }
  }
  // add extra replace expr
  if (OB_SUCC(ret) && (CONNECT_BY_JOIN == join_type_)) {
    if (OB_FAIL(replace_exprs_action(replacer, get_connect_by_root_exprs()))) {
      LOG_WARN("failed to replace connect by root exprs", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, get_sys_connect_by_path_exprs()))) {
      LOG_WARN("failed to replace sys connect by path exprs", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, get_prior_exprs()))) {
      LOG_WARN("failed to replace prior exprs", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, get_connect_by_pseudo_columns()))) {
      LOG_WARN("failed to replace connect by pseudo columns", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, get_connect_by_prior_exprs()))) {
      LOG_WARN("failed to replace connect by prior exprs", K(ret));
    } else if (OB_FAIL(replace_exprs_action(replacer, get_connect_by_extra_exprs()))) {
      LOG_WARN("failed to replace sys connect by extra exprs", K(ret));
    }
  }
  return ret;
}

int ObLogJoin::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  EstimateCostInfo left_param;
  EstimateCostInfo right_param;
  double left_output_rows = 0.0;
  double right_output_rows = 0.0;
  double left_cost = 0.0;
  double right_cost = 0.0;
  ObLogicalOperator *left_child = get_child(ObLogicalOperator::first_child);
  ObLogicalOperator *right_child = get_child(ObLogicalOperator::second_child);
  const int64_t parallel = param.need_parallel_;
  if (OB_ISNULL(left_child) || OB_ISNULL(right_child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null join path", K(ret));
  } else if (OB_ISNULL(join_path_)) {
    card = get_card();
    op_cost = get_op_cost();
    cost = get_cost();
  } else if (OB_FAIL(join_path_->get_re_estimate_param(param, 
                                                       left_param, 
                                                       right_param,
                                                       true))) {
    LOG_WARN("failed to get re estimate param", K(ret));
  } else if (OB_FAIL(SMART_CALL(left_child->re_est_cost(left_param,
                                              left_output_rows,
                                              left_cost)))) {
    LOG_WARN("failed to re estimate cost", K(ret));
  } else if (OB_FAIL(join_path_->try_set_batch_nlj_for_right_access_path(true))) {
    LOG_WARN("failed to try set batch nlj for right access path", K(ret));
  } else if (OB_FAIL(SMART_CALL(right_child->re_est_cost(right_param,
                                              right_output_rows,
                                              right_cost)))) {
    LOG_WARN("failed to re estimate cost", K(ret));
  } else if (OB_FAIL(join_path_->try_set_batch_nlj_for_right_access_path(false))) {
    LOG_WARN("failed to try set batch nlj for right access path", K(ret));
  } else if (OB_FAIL(join_path_->re_estimate_rows(left_output_rows, 
                                                 right_output_rows, 
                                                 card))) {
    LOG_WARN("failed to re estimate rows", K(ret));
  } else if (NESTED_LOOP_JOIN == join_algo_) {
    if (OB_FAIL(join_path_->cost_nest_loop_join(parallel,
                                                left_output_rows,
                                                left_cost, 
                                                right_output_rows, 
                                                right_cost,
                                                true,
                                                op_cost, 
                                                cost))) {
      LOG_WARN("failed to cost nest loop join", K(*this), K(ret));
    }
  } else if(MERGE_JOIN == join_algo_) {
    if (OB_FAIL(join_path_->cost_merge_join(parallel,
                                            left_output_rows,
                                            left_cost, 
                                            right_output_rows, 
                                            right_cost, 
                                            true,
                                            op_cost, 
                                            cost))) {
      LOG_WARN("failed to cost merge join", K(*this), K(ret));
    }
  } else if(HASH_JOIN == join_algo_) {
    if (OB_FAIL(join_path_->cost_hash_join(parallel,
                                            left_output_rows,
                                            left_cost, 
                                            right_output_rows, 
                                            right_cost, 
                                            true,
                                            op_cost, 
                                            cost))) {
      LOG_WARN("failed to cost hash join", K(*this), K(ret));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown join algorithm", K(join_algo_));
  }
  if (OB_SUCC(ret)) {
    if (param.need_row_count_ >=0 && param.need_row_count_ < get_card()) {
      card = param.need_row_count_;
    }
  }
  return ret;
}

// in log join, print hint below:
//  1. leading
//  2. use join
//  3. pq distribute
//  4. pq map hint
//  5. nl material
//  6. join filter
int ObLogJoin::print_outline_data(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  const ObDMLStmt *stmt = NULL;
  ObItemType use_join_type = T_INVALID;
  ObLogicalOperator *left_child = NULL;
  ObLogicalOperator *right_child = NULL;
  const ObRelIds *tables= NULL;
  ObString qb_name;
  if (is_late_mat()) {
    // need not print outline for late material join
  } else {
   if (OB_ISNULL(get_plan())
      || OB_ISNULL(stmt = get_plan()->get_stmt())
      || OB_ISNULL(left_child = get_child(first_child))
      || OB_ISNULL(right_child = get_child(second_child))
      || OB_ISNULL(tables = &right_child->get_table_set())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(get_plan()), K(stmt), K(right_child));
    } else if (OB_FAIL(stmt->get_qb_name(qb_name))) {
      LOG_WARN("fail to get qb_name", K(ret), K(stmt->get_stmt_id()));
    } else if (NESTED_LOOP_JOIN == get_join_algo()) {
      use_join_type = T_USE_NL;
    } else if (MERGE_JOIN == get_join_algo()) {
      use_join_type = T_USE_MERGE;
    } else if (HASH_JOIN == get_join_algo()) {
      use_join_type = T_USE_HASH;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected join algo", K(ret), K(get_join_algo()));
    }

    // 1. print leading
    if (OB_SUCC(ret) && !get_plan()->has_added_leading()) {
      if (OB_FAIL(BUF_PRINTF("%s%s(@\"%.*s\" ", ObQueryHint::get_outline_indent(plan_text.is_oneline_),
                                                ObHint::get_hint_name(T_LEADING),
                                                qb_name.length(), qb_name.ptr()))) {
        LOG_WARN("fail to print leading hint head", K(ret));
      } else if (OB_FAIL(print_leading_tables(*stmt, plan_text, this))) {
        LOG_WARN("fail to print leading tables", K(ret));
      } else if (OB_FAIL(BUF_PRINTF(")"))) {
      } else {
        get_plan()->set_added_leading();
      }
    }
    if (OB_FAIL(ret)) {
    // 2. print join algo
    } else if (OB_FAIL(print_join_hint_outline(*stmt,
                                              use_join_type,
                                              qb_name,
                                              *tables,
                                              plan_text))) {
      LOG_WARN("fail to print use join hint", K(ret));
    // 3. print pq distribute hint
    } else if (ObJoinHint::need_print_dist_algo(get_dist_method()) &&
              OB_FAIL(print_join_hint_outline(*stmt,
                                              T_PQ_DISTRIBUTE,
                                              qb_name,
                                              *tables,
                                              plan_text))) {
      LOG_WARN("fail to print pq distribute hint", K(ret));
    // 4. print pq map hint
    } else if (is_using_slave_mapping() &&
              OB_FAIL(print_join_hint_outline(*stmt,
                                              T_PQ_MAP,
                                              qb_name,
                                              *tables,
                                              plan_text))) {
      LOG_WARN("fail to print pq distribute hint", K(ret));
    // 5. print use nl material
    } else if (NESTED_LOOP_JOIN == get_join_algo() &&
              LOG_MATERIAL == right_child->get_type() &&
              OB_FAIL(print_join_hint_outline(*stmt,
                                              T_USE_NL_MATERIALIZATION,
                                              qb_name,
                                              *tables,
                                              plan_text))) {
      LOG_WARN("fail to print pq distribute hint", K(ret));
    } else {
    // 6. print (part) join filter hint
      const ObIArray<JoinFilterInfo> &infos = get_join_filter_infos();
      for (int64_t i = 0; OB_SUCC(ret) && i < infos.count(); ++i) {
        if (infos.at(i).can_use_join_filter_ &&
            OB_FAIL(print_join_filter_hint_outline(*stmt,
                                                  qb_name,
                                                  left_child->get_table_set(),
                                                  infos.at(i).filter_table_id_,
                                                  infos.at(i).pushdown_filter_table_,
                                                  infos.at(i).table_id_,
                                                  false,
                                                  plan_text))) {
          LOG_WARN("fail to print join filter hint", K(ret));
        } else if (infos.at(i).need_partition_join_filter_ &&
                  OB_FAIL(print_join_filter_hint_outline(*stmt,
                                                          qb_name,
                                                          left_child->get_table_set(),
                                                          infos.at(i).filter_table_id_,
                                                          infos.at(i).pushdown_filter_table_,
                                                          infos.at(i).table_id_,
                                                          true,
                                                          plan_text))) {
          LOG_WARN("fail to print part join filter hint", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogJoin::print_used_hint(PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObHint*, 8> used_hints;
  if (is_late_mat()) {
    // need not print outline for late material join
  } else if (OB_FAIL(add_used_leading_hint(used_hints))) {
    LOG_WARN("failed to add used leading hint", K(ret), K(get_join_algo()));
  } else if (OB_FAIL(append_used_join_hint(used_hints))) {
    LOG_WARN("failed to append used hint", K(ret));
  } else if (OB_FAIL(append_used_join_filter_hint(used_hints))) {
    LOG_WARN("failed to add used join filter hint", K(ret));
  } else {
    const ObHint *hint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < used_hints.count(); ++i) {
      if (OB_ISNULL(hint = used_hints.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(hint));
      } else if (OB_FAIL(hint->print_hint(plan_text))) {
        LOG_WARN("failed to print hint in log join", K(ret), K(*hint));
      }
    }
  }
  return ret;
}

int ObLogJoin::add_used_leading_hint(ObIArray<const ObHint*> &used_hints)
{
  int ret = OB_SUCCESS;
  const LogLeadingHint *leading_hint = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()));
  } else if (OB_FALSE_IT(leading_hint = &get_plan()->get_log_plan_hint().join_order_)) {
  } else if (get_plan()->has_added_leading()
             || NULL == leading_hint->hint_) {
    /* do nothing */
  } else {
    get_plan()->set_added_leading();
    bool used_hint = false;
    const ObLogicalOperator *op = this;
    while (OB_SUCC(ret) && NULL != op) {
      if (op->get_table_set().equal(leading_hint->leading_tables_)) {
        used_hint = 1 == leading_hint->leading_tables_.num_members();  // leading hint with single table like leading(t1)
        if (!used_hint && LOG_JOIN == op->get_type()
            && OB_FAIL(check_used_leading(leading_hint->leading_infos_, op, used_hint))) {
          LOG_WARN("failed to check used leading hint", K(ret));
        } else {
          op = NULL;
        }
      } else if (LOG_JOIN == op->get_type()) {
        op = find_child_join(op->get_child(first_child)); // only check left table recursively
      } else {
        op = NULL;
      }
    }

    if (OB_SUCC(ret) && used_hint && OB_FAIL(used_hints.push_back(leading_hint->hint_))) {
      LOG_WARN("failed to push back hint", K(ret));
    }
  }
  return ret;
}

int ObLogJoin::check_used_leading(const ObIArray<LeadingInfo> &leading_infos,
                                  const ObLogicalOperator *op,
                                  bool &used_hint)
{
  int ret = OB_SUCCESS;
  used_hint = true;
  ObLogicalOperator *l_child = NULL;
  ObLogicalOperator *r_child = NULL;
  if (OB_ISNULL(op = find_child_join(op))
      || OB_UNLIKELY(LOG_JOIN != op->get_type())
      || OB_ISNULL(l_child = op->get_child(first_child))
      || OB_ISNULL(r_child = op->get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op", K(ret), K(op), K(l_child), K(r_child));
  } else if (!find_leading_info(leading_infos,
                                l_child->get_table_set(),
                                r_child->get_table_set())) {
    used_hint = false;
  } else if (l_child->get_table_set().num_members() > 1 &&
             OB_FAIL(SMART_CALL(check_used_leading(leading_infos, l_child, used_hint)))) {
    LOG_WARN("failed to check used leading", K(ret));
  } else if (used_hint && r_child->get_table_set().num_members() > 1 &&
             OB_FAIL(SMART_CALL(check_used_leading(leading_infos, r_child, used_hint)))) {
    LOG_WARN("failed to check used leading", K(ret));
  }
  return ret;
}

bool ObLogJoin::find_leading_info(const ObIArray<LeadingInfo> &leading_infos,
                                  const ObRelIds &l_set,
                                  const ObRelIds &r_set)
{
  bool find = false;
  for (int64_t i = 0; !find && i < leading_infos.count(); ++i) {
    if (l_set.equal(leading_infos.at(i).left_table_set_)
        && r_set.equal(leading_infos.at(i).right_table_set_)) {
      find = true;
    }
  }
  return find;
}

const ObLogicalOperator *ObLogJoin::find_child_join(const ObLogicalOperator *op) {
  while (NULL != op && !is_scan_operator(op->get_type()) && LOG_JOIN != op->get_type()) {
    op = op->get_child(first_child);
  }
  return op;
}

bool ObLogJoin::is_scan_operator(log_op_def::ObLogOpType type)
{
  return LOG_TABLE_SCAN == type || LOG_SUBPLAN_SCAN == type ||
         LOG_FUNCTION_TABLE == type || LOG_UNPIVOT == type ||
         LOG_TEMP_TABLE_ACCESS == type || LOG_JSON_TABLE == type;
}

int ObLogJoin::append_used_join_hint(ObIArray<const ObHint*> &used_hints)
{
  int ret = OB_SUCCESS;
  const LogJoinHint *log_join_hint = NULL;
  ObLogicalOperator *child_op = NULL;
  if (OB_ISNULL(get_plan()) || OB_ISNULL(child_op = get_child(second_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(get_plan()), K(child_op));
  } else if (NULL != (log_join_hint = get_plan()->get_log_plan_hint().get_join_hint(child_op->get_table_set()))) {
    bool find = false;
    const ObJoinHint *join_hint = NULL;
    // add used use join hint
    if (get_join_algo() & log_join_hint->local_methods_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < log_join_hint->local_method_hints_.count(); ++i) {
        if (OB_ISNULL(join_hint = log_join_hint->local_method_hints_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected NULL", K(ret), K(join_hint));
        } else if (!join_hint->is_match_local_algo(get_join_algo())) {
          /* do nothing */
        } else if (OB_FAIL(used_hints.push_back(join_hint))) {
          LOG_WARN("failed to append pq distribute hint", K(ret));
        }
      }
    }
    // add used use/no_use nl_material hint
    if (OB_SUCC(ret) && NULL != log_join_hint->nl_material_) {
      if (NESTED_LOOP_JOIN == get_join_algo() && LOG_MATERIAL == child_op->get_type()) {
        if (log_join_hint->nl_material_->is_enable_hint()
            && OB_FAIL(used_hints.push_back(log_join_hint->nl_material_))) {
          LOG_WARN("failed to append nl material hint", K(ret));
        }
      } else if (log_join_hint->nl_material_->is_disable_hint()
                 && OB_FAIL(used_hints.push_back(log_join_hint->nl_material_))) {
        LOG_WARN("failed to append nl material hint", K(ret));
      }
    }
    // add used pq_map hint
    if (OB_SUCC(ret) && is_using_slave_mapping() && NULL != log_join_hint->slave_mapping_) {
      if (OB_FAIL(used_hints.push_back(log_join_hint->slave_mapping_))) {
        LOG_WARN("failed to append pq map hint", K(ret));
      }
    }
    // add pq dist hint
    for (int64_t i = 0; !find && OB_SUCC(ret) && i < log_join_hint->dist_method_hints_.count(); ++i) {
      if (OB_ISNULL(join_hint = log_join_hint->dist_method_hints_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(join_hint));
      } else if (get_dist_method() != join_hint->get_dist_algo()) {
        /* do nothing */
      } else if (OB_FAIL(used_hints.push_back(join_hint))) {
        LOG_WARN("failed to append pq distribute hint", K(ret));
      } else {
        find = true;
      }
    }
  }
  return ret;
}

int ObLogJoin::append_used_join_filter_hint(ObIArray<const ObHint*> &used_hints)
{
  int ret = OB_SUCCESS;
  const int64_t N = get_join_filter_infos().count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    const JoinFilterInfo &info = get_join_filter_infos().at(i);
    if (info.can_use_join_filter_ && NULL != info.force_filter_ &&
        OB_FAIL(add_var_to_array_no_dup(used_hints, static_cast<const ObHint*>(info.force_filter_)))) {
      LOG_WARN("failed to add hint", K(ret));
    } else if (info.need_partition_join_filter_ && NULL != info.force_part_filter_ &&
               OB_FAIL(add_var_to_array_no_dup(used_hints, static_cast<const ObHint*>(info.force_part_filter_)))) {
      LOG_WARN("failed to add hint", K(ret));
    }
  }
  return ret;
}

int ObLogJoin::print_leading_tables(const ObDMLStmt &stmt,
                                    PlanText &plan_text,
                                    const ObLogicalOperator *op)
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  int64_t table_num = -1;
  if (OB_ISNULL(op) || OB_UNLIKELY(1 > (table_num = op->get_table_set().num_members()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op", K(ret), K(op), K(table_num));
  } else if (1 == table_num) {
    if (OB_FAIL(print_join_tables_in_hint(stmt, plan_text, op->get_table_set()))) {
      LOG_WARN("fail to print join tables", K(ret));
    }
  } else if (OB_ISNULL(op = find_child_join(op)) || OB_UNLIKELY(LOG_JOIN != op->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op", K(ret), K(op));
  } else if (OB_FAIL(BUF_PRINTF("("))) {
  } else if (OB_FAIL(SMART_CALL(print_leading_tables(stmt, plan_text,
                                                     op->get_child(first_child))))) {
    LOG_WARN("fail to print leading tables", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" "))) {
  } else if (OB_FAIL(SMART_CALL(print_leading_tables(stmt, plan_text,
                                                     op->get_child(second_child))))) {
    LOG_WARN("fail to print leading tables", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(")"))) {
  } else { /* do nothing */ }
  return ret;
}

int ObLogJoin::print_join_hint_outline(const ObDMLStmt &stmt,
                                       const ObItemType hint_type,
                                       const ObString &qb_name,
                                       const ObRelIds &table_set,
                                       PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  const char* algo_str = T_PQ_DISTRIBUTE == hint_type
                         ? ObJoinHint::get_dist_algo_str(get_dist_method())
                         : NULL;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  if (OB_FAIL(BUF_PRINTF("%s%s(@\"%.*s\" ", ObQueryHint::get_outline_indent(plan_text.is_oneline_),
                                            ObHint::get_hint_name(hint_type),
                                            qb_name.length(), qb_name.ptr()))) {
    LOG_WARN("fail to print pq map hint head", K(ret));
  } else if (OB_FAIL(print_join_tables_in_hint(stmt, plan_text, table_set))) {
    LOG_WARN("fail to print join tables", K(ret));
  } else if (NULL != algo_str && OB_FAIL(BUF_PRINTF(" %s", algo_str))) {
    LOG_WARN("fail to print distribute method", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(")"))) {
  } else { /* do nothing */ }
  return ret;
}

int ObLogJoin::print_join_filter_hint_outline(const ObDMLStmt &stmt,
                                              const ObString &qb_name,
                                              const ObRelIds &left_table_set,
                                              const uint64_t filter_table_id,
                                              const ObTableInHint &child_table_hint,
                                              const uint64_t child_table_id,
                                              const bool is_part_hint,
                                              PlanText &plan_text)
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  if (OB_FAIL(BUF_PRINTF("%s%s(@\"%.*s\" ", ObQueryHint::get_outline_indent(plan_text.is_oneline_),
                                            ObHint::get_hint_name(is_part_hint ? T_PX_PART_JOIN_FILTER
                                                                              : T_PX_JOIN_FILTER),
                                            qb_name.length(), qb_name.ptr()))) {
    LOG_WARN("fail to print pq map hint head", K(ret));
  } else if (OB_FAIL(print_outline_table(plan_text, stmt.get_table_item_by_id(filter_table_id)))) {
    LOG_WARN("fail to print table", K(ret));
  } else if (OB_FAIL(BUF_PRINTF(" "))) {
  } else if (OB_FAIL(print_join_tables_in_hint(stmt, plan_text, left_table_set))) {
    LOG_WARN("fail to print join tables", K(ret));
  } else if (filter_table_id != child_table_id &&
             (OB_FAIL(BUF_PRINTF(" ") ||
              OB_FAIL(child_table_hint.print_table_in_hint(plan_text))))) {
    LOG_WARN("fail to print pushdown table hint", K(ret));
  }else if (OB_FAIL(BUF_PRINTF(")"))) {
  } else { /* do nothing */ }
  return ret;
}

int ObLogJoin::print_join_tables_in_hint(const ObDMLStmt &stmt,
                                         PlanText &plan_text,
                                         const ObRelIds &table_set)
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  bool multi_table = table_set.num_members() > 1;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(get_plan()));
  } else if (multi_table && OB_FAIL(BUF_PRINTF("("))) {
  } else {
    bool is_first_table = true;
    const ObIArray<TableItem*> &table_items = stmt.get_table_items();
    const TableItem *table = NULL;
    ObSEArray<const TableItem*, 2> join_tables;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_items.count(); ++i) {
      if (OB_ISNULL(table = table_items.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(table));
      } else if (!table_set.has_member(stmt.get_table_bit_index(table->table_id_))) {
        /* do nothing */
      } else if (OB_FAIL(join_tables.push_back(table))) {
        LOG_WARN("failed to push back table", K(ret));
      }
    }
    auto cmp_func = [](const TableItem *lhs, const TableItem *rhs) {
      if (NULL != lhs && NULL != rhs) {
        return lhs->get_table_name().compare(rhs->get_table_name()) > 0;
      } else {
        return false;
      }
    };
    std::sort(join_tables.begin(), join_tables.end(), cmp_func);
    for (int64_t i = 0; OB_SUCC(ret) && i < join_tables.count(); ++i) {
      if (OB_ISNULL(table = join_tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(table));
      } else if (!is_first_table && OB_FAIL(BUF_PRINTF(" "))) {
      } else if (OB_FAIL(print_outline_table(plan_text, table))) {
        LOG_WARN("fail to print join table", K(ret));
      } else {
        is_first_table = false;
      }
    }
    if (OB_SUCC(ret) && multi_table && OB_FAIL(BUF_PRINTF(")"))) {
    } else { /* do nothing */ }
  }
  return ret;
}

int ObLogJoin::allocate_granule_pre(AllocGIContext &ctx)
{
  int ret = OB_SUCCESS;
  if (!ctx.exchange_above()) {
    LOG_TRACE("no exchange above, do nothing", K(ctx));
  } else if (is_using_slave_mapping()) {
    ctx.slave_mapping_type_ = slave_mapping_type_;
  } else if (!ctx.is_in_partition_wise_state()
             && !ctx.is_in_pw_affinity_state()
             && DistAlgo::DIST_PARTITION_WISE == join_dist_algo_) {
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
    LOG_TRACE("in find partition wise state", K(ctx));
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
    if (DIST_BROADCAST_NONE == join_dist_algo_ ||
        (DIST_BC2HOST_NONE == join_dist_algo_ && HASH_JOIN == join_algo_) ||
        DIST_NONE_BROADCAST == join_dist_algo_ ||
        DIST_PARTITION_NONE == join_dist_algo_ ||
        DIST_NONE_PARTITION == join_dist_algo_) {
      if (OB_FAIL(ctx.set_pw_affinity_state())) {
        LOG_WARN("set affinity state failed", K(ret), K(ctx));
      }
      LOG_TRACE("partition wise affinity", K(ret));
    }
  }
  return ret;
}

int ObLogJoin::allocate_granule_post(AllocGIContext &ctx)
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
    LOG_TRACE("no exchange above, do nothing");
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
    }
    LOG_TRACE("set right child gi to affinity");
  } else if (DIST_BC2HOST_NONE == join_dist_algo_ && HASH_JOIN != join_algo_) {
    ObLogicalOperator *op = NULL;
    if (OB_FAIL(get_child(second_child)->find_first_recursive(LOG_GRANULE_ITERATOR, op))) {
      LOG_WARN("find granule iterator in right failed", K(ret));
    } else if (NULL == op) {
      // granule iterator not found, do nothing
    } else {
      static_cast<ObLogGranuleIterator *>(op)->add_flag(GI_ACCESS_ALL);
    }
  } else if (is_nlj_with_param_down()) {
    if (DIST_PULL_TO_LOCAL != join_dist_algo_) {
      ObLogicalOperator *op = NULL;
      if (OB_FAIL(get_child(second_child)->find_first_recursive(LOG_GRANULE_ITERATOR, op))) {
        LOG_WARN("find granule iterator in right failed", K(ret));
      } else if (NULL == op) {
        // granule iterator not found, do nothing
      } else {
        static_cast<ObLogGranuleIterator *>(op)->add_flag(GI_NLJ_PARAM_DOWN);
        static_cast<ObLogGranuleIterator *>(op)->add_flag(GI_FORCE_PARTITION_GRANULE);
      }
    }
  }
  if (OB_SUCC(ret) && can_enable_gi_partition_pruning()) {
    // 如果是 nlj，并且右支是 local 分区表 get，则启用 gi part filter 能力，
    // 并且 nlj 向左支 request 一个 part id
    // 通知 GI 在迭代 partition granule 时进入 partition pruning 模式
    if (OB_FAIL(build_gi_partition_pruning())) {
      LOG_WARN("fail determine right child partition id", K(ret));
    }
  }
	return ret;
}

int ObLogJoin::bloom_filter_partition_type(const ObShardingInfo &right_child_sharding_info,
                                           ObIArray<ObRawExpr *> &right_keys,
                                           PartitionFilterType &type)
{
  int ret = OB_SUCCESS;
  bool one_level_partition_covered = true;
  bool two_level_partition_covered = true;
  const ObIArray<ObRawExpr *> &partition_keys = right_child_sharding_info.get_partition_keys();
  const ObIArray<ObRawExpr *> &sub_partition_keys = right_child_sharding_info.get_sub_partition_keys();
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

int ObLogJoin::is_left_unique(bool &left_unique) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> left_exprs;
  ObSEArray<ObRawExpr*, 8> right_exprs;
  ObSEArray<bool, 8> null_safe_info;
  ObLogicalOperator *left_child = NULL;
  left_unique = false;
  if (OB_ISNULL(left_child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null", K(ret), K(get_child(first_child)));
  } else if (OB_FAIL(ObOptimizerUtil::get_equal_keys(join_conditions_,
                                                     left_child->get_table_set(),
                                                     left_exprs,
                                                     right_exprs,
                                                     null_safe_info))) {
    LOG_WARN("failed to get equal keys", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::is_exprs_unique(left_exprs,
                                                      left_child->get_table_set(),
                                                      left_child->get_fd_item_set(),
                                                      left_child->get_output_equal_sets(),
                                                      left_child->get_output_const_exprs(),
                                                      left_unique))) {
    LOG_WARN("fail to check unique condition", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObLogJoin::compute_table_set()
{
  int ret = OB_SUCCESS;
  if (IS_SEMI_ANTI_JOIN(join_type_)) {
    ObLogicalOperator *child = NULL;
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

int ObLogJoin::generate_join_partition_id_expr()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *op = NULL;
  if (OB_FAIL(get_child(first_child)->find_first_recursive(LOG_EXCHANGE, op))) {
    LOG_WARN("find granule iterator in right failed", K(ret));
  } else if (NULL == op
             || NULL == op->get_child(0)
             || LOG_EXCHANGE != op->get_child(0)->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log exchange not found", K(ret));
  } else if (OB_FAIL(generate_pseudo_partition_id_expr(partition_id_expr_))) {
    LOG_WARN("fail alloc partition id expr", K(ret));
  } else {
    static_cast<ObLogExchange *>(op->get_child(0))->set_partition_id_expr(partition_id_expr_);
  }
  return ret;
}

int ObLogJoin::compute_property(Path *path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(path)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::compute_property(path))) {
    LOG_WARN("failed to compute property", K(ret));
  } else if (OB_UNLIKELY(!path->is_join_path())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected join path", K(ret));
  }
  return ret;
}

int ObLogJoin::allocate_startup_expr_post()
{
  int ret = OB_SUCCESS;
  if (INNER_JOIN == join_type_) {
    if (OB_FAIL(ObLogicalOperator::allocate_startup_expr_post())) {
      LOG_WARN("failed to allocate startup expr post", K(ret));
    }
  } else if (LEFT_OUTER_JOIN == join_type_ ||
             CONNECT_BY_JOIN == join_type_ ||
             LEFT_SEMI_JOIN == join_type_ ||
             LEFT_ANTI_JOIN == join_type_) {
    if (OB_FAIL(allocate_startup_expr_post(first_child))) {
      LOG_WARN("failed to allocate startup expr post", K(ret));
    }
  } else if (RIGHT_OUTER_JOIN == join_type_ ||
             RIGHT_SEMI_JOIN == join_type_ ||
             RIGHT_ANTI_JOIN == join_type_) {
    if (OB_FAIL(allocate_startup_expr_post(second_child))) {
      LOG_WARN("failed to allocate startup expr post", K(ret));
    }
  } else if (FULL_OUTER_JOIN == join_type_) {
    //do nothing
  }
  return ret;
}

int ObLogJoin::set_use_batch(ObLogicalOperator* root)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret));
  } else if (root->is_table_scan()) {
    ObLogTableScan *ts = static_cast<ObLogTableScan*>(root);
    // dblink can not support batch nlj
    bool has_param = false;
    ObSEArray<int64_t, 1> idx_array;
    if (OB_FAIL(ts->extract_bnlj_param_idxs(idx_array))) {
      LOG_WARN("extract param indexes failed", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && !has_param && i < nl_params_.count(); i++) {
      int64_t param_idx = nl_params_.at(i)->get_param_index();
      for (int64_t j = 0; OB_SUCC(ret) && j < idx_array.count(); j++) {
        if (param_idx == idx_array.at(j)) {
          has_param = true;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (ts->has_index_scan_filter() && ts->get_index_back() && ts->get_is_index_global()) {
        // For the global index lookup, if there is a pushdown filter when scanning the index,
        // batch cannot be used.
        ts->set_use_batch(false);
      } else {
        ts->set_use_batch(ts->use_batch() || (can_use_batch_nlj_ && has_param));
      }
    }
  } else if (root->get_num_of_child() == 1) {
    if(OB_FAIL(SMART_CALL(set_use_batch(root->get_child(first_child))))) {
      LOG_WARN("failed to check use batch nlj", K(ret));
    }
  } else if (log_op_def::LOG_SET == root->get_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < root->get_num_of_child(); ++i) {
      ObLogicalOperator *child = root->get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child", K(ret));
      } else if (OB_FAIL(SMART_CALL(set_use_batch(child)))) {
        LOG_WARN("failed to check use batch nlj", K(ret));
      }
    }
  } else if (log_op_def::LOG_JOIN == root->get_type()) {
    ObLogJoin *join = NULL;
    ObLogicalOperator *left_child = NULL;
    ObLogicalOperator *rigtht_child = NULL;
    if (OB_ISNULL(join = static_cast<ObLogJoin *>(root))
        || OB_ISNULL(left_child = join->get_child(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input", K(ret));
    } else if (OB_FAIL(SMART_CALL(set_use_batch(left_child)))) {
      LOG_WARN("failed to check use batch nlj", K(ret));
    } else if (!join->can_use_batch_nlj()) {
      // do nothing
    } else if (OB_ISNULL(rigtht_child = join->get_child(1))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid child", K(ret));
    } else if (OB_FAIL(SMART_CALL(set_use_batch(rigtht_child)))) {
      LOG_WARN("failed to check use batch nlj", K(ret));
    }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogJoin::check_and_set_use_batch()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = NULL;
  ObLogPlan *plan = NULL;
  if (OB_ISNULL(plan = get_plan())
      || OB_ISNULL(session_info = plan->get_optimizer_context().get_session_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!can_use_batch_nlj_) {
    // do nothing
  } else if (OB_FAIL(session_info->get_nlj_batching_enabled(can_use_batch_nlj_))) {
    LOG_WARN("failed to get enable batch variable", K(ret));
  } else if (NESTED_LOOP_JOIN != get_join_algo()) {
    can_use_batch_nlj_ = false;
  }
  // check use batch
  if (OB_SUCC(ret) && can_use_batch_nlj_) {
    bool contains_invalid_startup = false;
    bool contains_limit = false;
    if (get_child(1)->get_type() == log_op_def::LOG_GRANULE_ITERATOR) {
      can_use_batch_nlj_ = false;
    } else if (OB_FAIL(plan->contains_startup_with_exec_param(get_child(1),
                                                              contains_invalid_startup))) {
      LOG_WARN("failed to check contains invalid startup", K(ret));
    } else if (contains_invalid_startup) {
      can_use_batch_nlj_ = false;
    } else if (OB_FAIL(plan->contains_limit_or_pushdown_limit(get_child(1), contains_limit))) {
      LOG_WARN("failed to check contains limit", K(ret));
    } else if (contains_limit) {
      can_use_batch_nlj_ = false;
    } else if (OB_FAIL(check_if_disable_batch(get_child(1), can_use_batch_nlj_))) {
      LOG_WARN("failed to check if disable batch", K(ret));
    }
  }
  // set use batch
  if (OB_SUCC(ret) && can_use_batch_nlj_) {
    if (OB_FAIL(set_use_batch(get_child(1)))) {
      LOG_WARN("failed to set use batch nlj", K(ret));
    }
  }
  return ret;
}

int ObLogJoin::check_if_disable_batch(ObLogicalOperator* root, bool &can_use_batch_nlj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (!can_use_batch_nlj) {
    // do nothing
  } else if (root->is_table_scan()) {
    ObLogTableScan *ts = NULL;
    ObLogPlan *plan = NULL;
    ObTablePartitionInfo *info = NULL;
    if (OB_ISNULL(ts = static_cast<ObLogTableScan *>(root)) ||
        OB_ISNULL(plan = get_plan()) ||
        OB_ISNULL(info = ts->get_table_partition_info())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input", K(ret));
    } else if (ts->has_index_scan_filter() && ts->get_index_back() && ts->get_is_index_global()) {
      // For the global index lookup, if there is a pushdown filter when scanning the index,
      // batch cannot be used.
      can_use_batch_nlj = false;
    } else {
      SMART_VAR(ObTablePartitionInfo, tmp_info) {
        ObTablePartitionInfo *tmp_info_ptr = &tmp_info;
        if (OB_FAIL(plan->gen_das_table_location_info(ts, tmp_info_ptr))) {
          LOG_WARN("failed to gen das table location info", K(ret));
        } else {
          if (tmp_info.get_table_location().use_das() &&
              tmp_info.get_table_location().get_has_dynamic_exec_param()) {
            // dynamic partition pruning, no need to check
          } else if (10 < info->get_phy_tbl_location_info().get_phy_part_loc_info_list().count()) {
            can_use_batch_nlj = false;
          }
        }
      }
    }
  } else if (1 == root->get_num_of_child()) {
    if (OB_FAIL(SMART_CALL(check_if_disable_batch(root->get_child(0), can_use_batch_nlj)))) {
      LOG_WARN("failed to check if disable batch", K(ret));
    }
  } else if (log_op_def::LOG_SET == root->get_type()) {
    for (int64_t i = 0; OB_SUCC(ret) && can_use_batch_nlj && i < root->get_num_of_child(); ++i) {
      ObLogicalOperator *child = root->get_child(i);
      if (OB_ISNULL(child)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child", K(ret));
      } else if (OB_FAIL(SMART_CALL(check_if_disable_batch(child, can_use_batch_nlj)))) {
        LOG_WARN("failed to check if disable batch", K(ret));
      }
    }
  } else if (log_op_def::LOG_JOIN == root->get_type()) {
    ObLogJoin *join = NULL;
    if (OB_ISNULL(join = static_cast<ObLogJoin *>(root))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid input", K(ret));
    } else if (!join->can_use_batch_nlj()) {
      can_use_batch_nlj = false;
      LOG_TRACE("child join not support batch_nlj", K(root->get_name()));
    } else if (OB_FAIL(SMART_CALL(check_if_disable_batch(root->get_child(0), can_use_batch_nlj)))) {
      LOG_WARN("failed to check use batch nlj", K(ret));
    } else if (OB_FAIL(SMART_CALL(check_if_disable_batch(root->get_child(1), can_use_batch_nlj)))) {
      LOG_WARN("failed to check use batch nlj for right op", K(ret));
    }
  } else {
    can_use_batch_nlj = false;
  }
  return ret;
}

bool ObLogJoin::is_my_exec_expr(const ObRawExpr *expr)
{
  return ObOptimizerUtil::find_item(nl_params_, expr);
}

int ObLogJoin::allocate_startup_expr_post(int64_t child_idx)
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = get_child(child_idx);
  if (OB_ISNULL(child)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null child", K(ret));
  } else if (child->get_startup_exprs().empty()) {
    //do nothing
  } else {
    ObSEArray<ObRawExpr*, 4> non_startup_exprs, new_startup_exprs;
    ObIArray<ObRawExpr*> &startup_exprs = child->get_startup_exprs();
    for (int64_t i = 0; OB_SUCC(ret) && i < startup_exprs.count(); ++i) {
      if (OB_ISNULL(startup_exprs.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null expr", K(ret));
      } else if (startup_exprs.at(i)->has_flag(CNT_ROWNUM)) {
        if (OB_FAIL(non_startup_exprs.push_back(startup_exprs.at(i)))) {
          LOG_WARN("fail to push back non startup expr",K(ret));
        }
      } else if (startup_exprs.at(i)->has_flag(CNT_DYNAMIC_PARAM)) {
        bool found = false;
        if (is_nlj_with_param_down()
            && OB_FAIL(ObOptimizerUtil::check_contain_my_exec_param(startup_exprs.at(i), get_nl_params(), found))) {
          LOG_WARN("fail to check if contain my exec param");
        } else if (found && OB_FAIL(non_startup_exprs.push_back(startup_exprs.at(i)))) {
          LOG_WARN("fail to push back non startup expr",K(ret));
        } else if (!found && OB_FAIL(new_startup_exprs.push_back(startup_exprs.at(i)))) {
          LOG_WARN("fail to push back non startup expr",K(ret));
        }
      } else if (OB_FAIL(new_startup_exprs.push_back(startup_exprs.at(i)))) {
        LOG_WARN("failed to push back expr", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObOptimizerUtil::append_exprs_no_dup(get_startup_exprs(), new_startup_exprs))) {
        LOG_WARN("failed to add startup exprs", K(ret));
      } else if (OB_FAIL(child->get_startup_exprs().assign(non_startup_exprs))) {
        LOG_WARN("failed to assign exprs", K(ret));
      }
    }
  }
  return ret;
}
