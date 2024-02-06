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

#include "sql/optimizer/ob_log_expr_values.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/optimizer/ob_opt_est_cost.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/optimizer/ob_del_upd_log_plan.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/rewrite/ob_transform_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_INSERT_VALUES(values, column_count, type)                    \
  {                                                                                \
    if (OB_ISNULL(values)) {                                                       \
      ret = OB_ERR_UNEXPECTED;                                                     \
    } else if (OB_FAIL(BUF_PRINTF(#values"("))) {                                  \
      LOG_WARN("fail to print to buf", K(ret));                                    \
    } else  {                                                                      \
      int64_t N = values->count();                                                 \
      int64_t M = column_count;                                                    \
      if (N == 0 || M == 0) {                                                      \
        if (OB_FAIL(BUF_PRINTF("nil"))) {                                          \
          LOG_WARN("fail to print to buf", K(ret));                                \
        }                                                                          \
      } else if (OB_UNLIKELY(0 != N % M)) {                                        \
        ret = OB_ERR_UNEXPECTED;                                                   \
        LOG_WARN("invalid value count", K(ret), "value_count", N, "row_count", M); \
      } else {                                                                     \
        for (int64_t i = 0; OB_SUCC(ret) && i < N / M; i++) {                      \
          if (OB_FAIL(BUF_PRINTF("{"))) {                                          \
            LOG_WARN("fail to print to buf", K(ret));                              \
          }                                                                        \
          for (int64_t j = 0; OB_SUCC(ret) && j < M; j++) {                        \
            int64_t expr_idx = i * M + j;                                          \
            if (OB_UNLIKELY(expr_idx >= values->count()) || OB_UNLIKELY(expr_idx < 0)) { \
              ret = OB_ERR_UNEXPECTED;                                             \
            } else if (OB_ISNULL(values->at(expr_idx))) {                          \
              ret = OB_ERR_UNEXPECTED;                                             \
            } else {                                                               \
              if (OB_FAIL(values->at(expr_idx)->get_name(buf, buf_len, pos, type))) { \
              } else {                                                             \
                if (j < M - 1) {                                                   \
                  if (OB_FAIL(BUF_PRINTF(", "))) {                                 \
                    LOG_WARN("fail to print to buf", K(ret));                      \
                  }                                                                \
                }                                                                  \
              }                                                                    \
            }                                                                      \
          }                                                                        \
          if (OB_FAIL(BUF_PRINTF("}"))) {                                          \
            LOG_WARN("fail to print to buf", K(ret));                              \
          } else if (i < N / M - 1) {                                              \
            if (OB_FAIL(BUF_PRINTF(", "))) {                                       \
              LOG_WARN("fail to print to buf", K(ret));                            \
            }                                                                      \
          }                                                                        \
        }                                                                          \
      }                                                                            \
      if (OB_SUCC(ret)) {                                                          \
        if (OB_FAIL(BUF_PRINTF(")"))) {                                            \
          LOG_WARN("fail to print to buf", K(ret));                                \
        }                                                                          \
      }                                                                            \
    }                                                                              \
  }


int ObLogExprValues::add_values_expr(const common::ObIArray<ObRawExpr *> &value_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(value_exprs_, value_exprs))) {
    LOG_WARN("failed to append expr", K(ret));
  } else if (get_stmt()->is_insert_stmt() && is_ins_values_batch_opt()) {
    const ObInsertStmt *insert_stmt = static_cast<const ObInsertStmt*>(get_stmt());
    ObRawExpr *stmt_id_expr = NULL;
    if (OB_ISNULL(stmt_id_expr = insert_stmt->get_ab_stmt_id_expr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stmt_id_expr is null", K(ret));
    } else if (OB_FAIL(value_exprs_.push_back(stmt_id_expr))) {
      LOG_WARN("fail to push stmt_id_expr", K(ret));
    }
  }
  return ret;
}

int ObLogExprValues::add_values_desc(const common::ObIArray<ObColumnRefRawExpr *> &value_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(value_desc_, value_desc))) {
    LOG_WARN("failed to append desc", K(ret));
  }
  return ret;
}

int ObLogExprValues::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(my_plan_) || OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect parameter", K(my_plan_), K(get_stmt()));
  } else if (!get_stmt()->is_select_stmt()) {
    set_fd_item_set(&empty_fd_item_set_);
  } else {
    ObFdItemSet *fd_item_set = NULL;
    ObSEArray<ObRawExpr*, 8> select_exprs;
    if (OB_FAIL(static_cast<const ObSelectStmt *>(get_stmt())->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_fd_item_set(fd_item_set))) {
      LOG_WARN("failed to create fd item set", K(ret));
    } else if (!ObTransformUtils::need_compute_fd_item_set(select_exprs)) {
      //do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
        ObSEArray<ObRawExpr *, 1> value_exprs;
        ObExprFdItem *fd_item = NULL;
        if (OB_FAIL(value_exprs.push_back(select_exprs.at(i)))) {
          LOG_WARN("failed to push back expr", K(ret));
        } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_expr_fd_item(fd_item,
                                                                               true,
                                                                               value_exprs,
                                                                               select_exprs))) {
          LOG_WARN("failed to create fd item", K(ret));
        } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
          LOG_WARN("failed to push back fd item", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
        /*do nothing*/
      } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
        LOG_WARN("failed to deduce fd item set", K(ret));
      } else {
        set_fd_item_set(fd_item_set);
      }
    }
  }
  return ret;
}

int ObLogExprValues::compute_equal_set()
{
  int ret = OB_SUCCESS;
  set_output_equal_sets(&empty_expr_sets_);
  return ret;
}

int ObLogExprValues::compute_table_set()
{
  int ret = OB_SUCCESS;
  set_table_set(&empty_table_set_);
  return ret;
}

int ObLogExprValues::compute_op_ordering()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!get_stmt()->is_select_stmt()) {
    /*do nothing*/
  } else {
    ObSEArray<ObRawExpr*, 4> select_exprs;
    if (OB_FAIL(static_cast<const ObSelectStmt *>(get_stmt())->get_select_exprs(select_exprs))) {
      LOG_WARN("failed to get select exprs", K(ret));
    } else if (OB_FAIL(ObOptimizerUtil::make_sort_keys(select_exprs,
                                                       op_ordering_))) {
      LOG_WARN("failed to copy sort keys", K(ret));
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObLogExprValues::est_cost()
{
  int ret = OB_SUCCESS;
  double card = 0.0;
  double op_cost = 0.0;
  double cost = 0.0;
  EstimateCostInfo param;
  param.need_parallel_ = get_parallel();
  if (OB_FAIL(do_re_est_cost(param, card, op_cost, cost))) {
    LOG_WARN("failed to get re est cost infos", K(ret));
  } else {
    set_card(card);
    set_op_cost(op_cost);
    set_cost(cost);
  }
  return ret;
}

int ObLogExprValues::do_re_est_cost(EstimateCostInfo &param, double &card, double &op_cost, double &cost)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan()) ||
      OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (get_stmt()->is_insert_stmt() || is_values_table_) {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    card = get_stmt()->is_insert_stmt() ? static_cast<const ObInsertStmt*>(get_stmt())->get_insert_row_count() :
                                          get_values_row_count();
    op_cost = ObOptEstCost::cost_get_rows(get_card(), opt_ctx.get_cost_model_type());
    cost = op_cost;
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    card = 1.0;
    op_cost = ObOptEstCost::cost_filter_rows(get_card(), filter_exprs_, opt_ctx.get_cost_model_type());
    cost = op_cost;
  }
  return ret;
}

int ObLogExprValues::compute_sharding_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    strong_sharding_ = get_plan()->get_optimizer_context().get_match_all_sharding();
  }
  return ret;
}

int ObLogExprValues::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!get_stmt()->is_insert_stmt()) {
    is_at_most_one_row_ = get_values_row_count() <= 1;
  } else { /*do nothing*/ }

  return ret;
}

int ObLogExprValues::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get op exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs, value_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (get_stmt()->is_insert_stmt()) {
    const ObInsertStmt *insert_stmt = static_cast<const ObInsertStmt*>(get_stmt());
    if (OB_FAIL(append(all_exprs, insert_stmt->get_values_desc()))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else { /*do nothing*/ }
  } else if (is_values_table_) {
    if (OB_FAIL(append(all_exprs, value_desc_))) {
      LOG_WARN("failed to append exprs", K(ret));
    } else { /*do nothing*/ }
  } else { /*do nothing*/ }
  return ret;
}

int ObLogExprValues::allocate_expr_post(ObAllocExprContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(get_stmt()), K(ret));
  } else if (get_stmt()->is_insert_stmt() || is_values_table_) {
    const ObIArray<ObColumnRefRawExpr*> &values_desc = get_stmt()->is_insert_stmt() ?
                                  static_cast<const ObInsertStmt*>(get_stmt())->get_values_desc() : value_desc_;
    for (int64_t i = 0; OB_SUCC(ret) && i < values_desc.count(); ++i) {
      ObColumnRefRawExpr *value_col = values_desc.at(i);
      if (OB_FAIL(mark_expr_produced(value_col, branch_id_, id_, ctx))) {
        LOG_WARN("makr expr produced failed", K(ret));
      } else if (!is_plan_root() && OB_FAIL(output_exprs_.push_back(value_col))) {
        LOG_WARN("failed to push back exprs", K(ret));
      } else { /*do nothing*/ }
    }
  }
  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
    LOG_WARN("failed to allocate expr post", K(ret));
  } else if (contain_array_binding_param() && OB_FAIL(construct_array_binding_values())) {
    LOG_WARN("construct array binding values failed", K(ret));
  } else if (value_exprs_.empty() && OB_FAIL(append(value_exprs_, get_output_exprs()))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else if (value_exprs_.empty() && OB_FAIL(allocate_dummy_output())) {
    LOG_WARN("failed to allocate dummy output", K(ret));
  } else if (OB_FAIL(construct_sequence_values())) {
    LOG_WARN("failed to construct sequence values", K(ret));
  } else if (OB_FAIL(mark_probably_local_exprs())) {
    LOG_WARN("failed to mark local exprs", K(ret));
  } else { /*do nothing*/ }

  return ret;
}

int ObLogExprValues::construct_sequence_values()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is null", K(ret));
  } else if (get_stmt()->is_insert_stmt() && !value_exprs_.empty() && output_exprs_.count() >= 2) {
    int64_t seq_expr_idx = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs_.count(); ++i) {
      if (OB_ISNULL(output_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("output expr is null", K(ret));
      } else if (T_FUN_SYS_SEQ_NEXTVAL == output_exprs_.at(i)->get_expr_type()) {
        seq_expr_idx = i;
        break;
      }
    }
    if (OB_SUCC(ret) && -1 != seq_expr_idx) {
      ObRawExpr *seq_expr = output_exprs_.at(seq_expr_idx);
      const int64_t value_count = value_exprs_.count();
      const int64_t output_count = output_exprs_.count();
      if (OB_UNLIKELY((0 != value_count % (output_count - 1)) || (0 == seq_expr_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected value count", K(value_count), K(output_count), K(seq_expr_idx));
      } else {
        const int64_t group_num = value_count / (output_count - 1);
        ObSEArray<ObRawExpr*, 8> new_value_exprs;
        if (OB_FAIL(new_value_exprs.reserve(group_num * output_count))) {
          LOG_WARN("failed to reserve array", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < value_exprs_.count(); ++i) {
            if (OB_FAIL(new_value_exprs.push_back(value_exprs_.at(i)))) {
              LOG_WARN("failed to push back expr", K(ret));
            } else if (((i + 1) % seq_expr_idx == 0) &&
                          OB_FAIL(new_value_exprs.push_back(seq_expr))) {
              LOG_WARN("failed to push back sequence expr", K(ret));
            }
          }
          if (OB_SUCC(ret) && OB_FAIL(value_exprs_.assign(new_value_exprs))) {
            LOG_WARN("failed to assign new value exprs", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLogExprValues::construct_array_binding_values()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt()) ||
      OB_ISNULL(get_plan()) ||
      OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array binding param only in select stmt", K(ret), KPC(get_stmt()));
  } else {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(get_stmt());
    if (OB_FAIL(value_exprs_.assign(select_stmt->get_query_ctx()->ab_param_exprs_))) {
      LOG_WARN("assign ab param exprs to value exprs failed", K(ret));
    } else if (OB_FAIL(get_plan()->get_optimizer_context().get_all_exprs().append(value_exprs_))) {
      LOG_WARN("fail to append ab param exprs", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs_.count(); ++i) {
      ObRawExpr *raw_expr = NULL;
      if (OB_ISNULL(raw_expr = output_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret));
      } else if (raw_expr->get_expr_type() == T_PSEUDO_STMT_ID) {
        if (OB_FAIL(value_exprs_.push_back(raw_expr))) {
          LOG_WARN("failed to add expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObLogExprValues::extract_err_log_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!get_stmt()->is_insert_stmt()) {
    // do nothing
  } else if (OB_FAIL(ObLogDelUpd::generate_errlog_info(
                       static_cast<const ObDelUpdStmt&>(*get_stmt()),
                       get_err_log_define())))  {
    LOG_WARN("failed to generate errlog info", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < get_err_log_define().err_log_value_exprs_.count(); ++i) {
    ObRawExpr *&expr = get_err_log_define().err_log_value_exprs_.at(i);
    if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else if (expr->get_expr_type() == T_FUN_COLUMN_CONV) {
      expr = expr->get_param_expr(ObExprColumnConv::VALUE_EXPR);
    }
  }
  return ret;
}

int ObLogExprValues::get_plan_item_info(PlanText &plan_text,
                                        ObSqlPlanItem &plan_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::get_plan_item_info(plan_text, plan_item))) {
    LOG_WARN("failed to get plan item info", K(ret));
  } else {
    const ObIArray<ObRawExpr*> *values = &get_value_exprs();
    BEGIN_BUF_PRINT;
    EXPLAIN_PRINT_INSERT_VALUES(values, output_exprs_.count(), type);
    END_BUF_PRINT(plan_item.special_predicates_,
                  plan_item.special_predicates_len_);
    if (OB_SUCC(ret) && is_values_table_) {
      const ObString &name = get_table_name();
      BUF_PRINT_OB_STR(name.ptr(),
                       name.length(),
                       plan_item.object_alias_,
                       plan_item.object_alias_len_);
    }
  }
  return ret;
}

int ObLogExprValues::mark_probably_local_exprs()
{
  int ret = OB_SUCCESS;
  FOREACH_CNT_X(e, value_exprs_, OB_SUCC(ret)) {
    CK(NULL != *e);
    OZ((*e)->add_flag(IS_PROBABLY_LOCAL));
  }

  return ret;
}

int ObLogExprValues::allocate_dummy_output()
{
  int ret = OB_SUCCESS;
  ObConstRawExpr *dummy_expr = NULL;
  if (OB_ISNULL(get_plan())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (is_oracle_mode() && ObRawExprUtils::build_const_number_expr(
                                   get_plan()->get_optimizer_context().get_expr_factory(),
                                   ObNumberType, number::ObNumber::get_positive_one(),
                                   dummy_expr)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (!is_oracle_mode() && ObRawExprUtils::build_const_int_expr(
                                  get_plan()->get_optimizer_context().get_expr_factory(),
                                  ObIntType,
                                  1,
                                  dummy_expr)) {
    LOG_WARN("failed to build const expr", K(ret));
  } else if (OB_ISNULL(dummy_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(dummy_expr->extract_info())) {
    LOG_WARN("failed to extract info for dummy expr", K(ret));
  } else if (OB_FAIL(value_exprs_.push_back(dummy_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(output_exprs_.push_back(dummy_expr))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(get_plan()->get_optimizer_context().get_all_exprs().append(dummy_expr))) {
    LOG_WARN("failed to append exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

bool ObLogExprValues::is_ins_values_batch_opt() const
{
  bool bret = false;
  if (get_stmt() != nullptr && get_stmt()->is_insert_stmt()) {
    bret = get_stmt()->get_query_ctx()->ins_values_batch_opt_;
  }
  return bret;
}

int ObLogExprValues::get_array_param_group_id(int64_t &group_id, bool &find)
{
  int ret = OB_SUCCESS;
  const ObExecContext *exec_ctx = NULL;
  find = false;
  group_id = -1;
  if (OB_ISNULL(my_plan_) ||
      OB_ISNULL(exec_ctx = my_plan_->get_optimizer_context().get_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(my_plan_), KP(exec_ctx));
  } else if (exec_ctx->has_dynamic_values_table()) {
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < value_exprs_.count(); i++) {
      if (OB_ISNULL(value_exprs_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw_expr is null", K(ret));
      } else if (value_exprs_.at(i)->is_const_raw_expr()) {
        const ObConstRawExpr *const_expr = static_cast<const ObConstRawExpr *>(value_exprs_.at(i));
        if (const_expr->get_array_param_group_id() >= 0) {
          find = true;
          group_id = const_expr->get_array_param_group_id();
        }
      }
    }
  }
  return ret;
}

bool ObLogExprValues::contain_array_binding_param() const
{
  bool bret = false;
  if (get_stmt() != nullptr && get_stmt()->is_select_stmt()) {
    const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(get_stmt());
    bret = select_stmt->contain_ab_param();
  }
  return bret;
}

int ObLogExprValues::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, value_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  }
  return ret;
}

int ObLogExprValues::compute_op_parallel_and_server_info()
{
  int ret = common::OB_SUCCESS;
  if (get_num_of_child() == 0) {
    ret = set_parallel_and_server_info_for_match_all();
  } else {
    ret = ObLogicalOperator::compute_op_parallel_and_server_info();
  }
  return ret;
}

int ObLogExprValues::is_my_fixed_expr(const ObRawExpr *expr, bool &is_fixed)
{
  is_fixed = ObOptimizerUtil::find_item(value_desc_, expr);
  return OB_SUCCESS;
}

} // namespace sql
}// namespace oceanbase
