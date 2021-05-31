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

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
/**
 *  Print log info with expressions
 */
#define EXPLAIN_PRINT_INSERT_VALUES(values, column_count, type)                          \
  {                                                                                      \
    if (OB_ISNULL(values)) {                                                             \
      ret = OB_ERR_UNEXPECTED;                                                           \
    } else if (OB_FAIL(BUF_PRINTF(#values "("))) {                                       \
      LOG_WARN("fail to print to buf", K(ret));                                          \
    } else {                                                                             \
      int64_t N = values->count();                                                       \
      int64_t M = column_count;                                                          \
      if (N == 0) {                                                                      \
        if (OB_FAIL(BUF_PRINTF("nil"))) {                                                \
          LOG_WARN("fail to print to buf", K(ret));                                      \
        }                                                                                \
      } else if (OB_UNLIKELY(0 != N % M)) {                                              \
        ret = OB_ERR_UNEXPECTED;                                                         \
        LOG_WARN("invalid value count", K(ret), "value_count", N, "row_count", M);       \
      } else {                                                                           \
        for (int64_t i = 0; OB_SUCC(ret) && i < N / M; i++) {                            \
          if (OB_FAIL(BUF_PRINTF("{"))) {                                                \
            LOG_WARN("fail to print to buf", K(ret));                                    \
          }                                                                              \
          for (int64_t j = 0; OB_SUCC(ret) && j < M; j++) {                              \
            int64_t expr_idx = i * M + j;                                                \
            if (OB_UNLIKELY(expr_idx >= values->count()) || OB_UNLIKELY(expr_idx < 0)) { \
              ret = OB_ERR_UNEXPECTED;                                                   \
            } else if (OB_ISNULL(values->at(expr_idx))) {                                \
              ret = OB_ERR_UNEXPECTED;                                                   \
            } else {                                                                     \
              if (OB_FAIL(values->at(expr_idx)->get_name(buf, buf_len, pos, type))) {    \
              } else {                                                                   \
                if (j < M - 1) {                                                         \
                  if (OB_FAIL(BUF_PRINTF(", "))) {                                       \
                    LOG_WARN("fail to print to buf", K(ret));                            \
                  }                                                                      \
                }                                                                        \
              }                                                                          \
            }                                                                            \
          }                                                                              \
          if (OB_FAIL(BUF_PRINTF("}"))) {                                                \
            LOG_WARN("fail to print to buf", K(ret));                                    \
          } else if (i < N / M - 1) {                                                    \
            if (OB_FAIL(BUF_PRINTF(", "))) {                                             \
              LOG_WARN("fail to print to buf", K(ret));                                  \
            }                                                                            \
          }                                                                              \
        }                                                                                \
      }                                                                                  \
      if (OB_SUCC(ret)) {                                                                \
        if (OB_FAIL(BUF_PRINTF(")"))) {                                                  \
          LOG_WARN("fail to print to buf", K(ret));                                      \
        }                                                                                \
      }                                                                                  \
    }                                                                                    \
  }

int ObLogExprValues::allocate_exchange_post(AllocExchContext* ctx)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  sharding_info_.set_location_type(OB_TBL_LOCATION_ALL);
  return ret;
}

int ObLogExprValues::add_values_expr(const common::ObIArray<ObRawExpr*>& value_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append(value_exprs_, value_exprs))) {
    LOG_WARN("failed to append expr", K(ret));
  }
  return ret;
}

int ObLogExprValues::add_str_values_array(const common::ObIArray<ObRawExpr*>& expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(str_values_array_.reserve(expr.count()))) {
    LOG_WARN("fail to prepare_allocate", K(ret), K(expr.count()));
  } else {
    for (int64_t i = 0; i < expr.count() && OB_SUCC(ret); ++i) {
      ObRawExpr* raw_expr = expr.at(i);
      if (OB_ISNULL(raw_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("raw_exprr is null", K(ret), K(raw_expr));
      } else if (T_FUN_COLUMN_CONV != raw_expr->get_expr_type()) {
        // do nothing
        LOG_DEBUG("ignore", K(ret), K(i), KPC(raw_expr));
      } else if (OB_UNLIKELY(raw_expr->get_param_count() != ObExprColumnConv::PARAMS_COUNT_WITH_COLUMN_INFO &&
                             raw_expr->get_param_count() != ObExprColumnConv::PARAMS_COUNT_WITHOUT_COLUMN_INFO)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("convert expr have invalid param number", K(ret));
      } else if (raw_expr->get_param_expr(4)->is_column_ref_expr()) {
        ObStrValues str_values(&my_plan_->get_allocator());
        if (raw_expr->get_result_type().is_enum_or_set()) {
          ObExprOperator* op = NULL;
          if (OB_ISNULL(op = static_cast<ObSysFunRawExpr*>(raw_expr)->get_op())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get_op", K(ret), K(expr));
          } else {
            ObExprColumnConv* column_conv = static_cast<ObExprColumnConv*>(op);
            str_values.assign(column_conv->get_str_values());
            if (OB_FAIL(str_values.assign(column_conv->get_str_values()))) {
              LOG_WARN("failed to push_back", K(ret), KPC(column_conv));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(str_values_array_.push_back(str_values))) {
            LOG_WARN("failed to push_back", K(ret), K(str_values));
          }
        }
      } else if (raw_expr->get_param_expr(4)->has_flag(CNT_COLUMN)) {
        ObStrValues str_values(&my_plan_->get_allocator());
        if (OB_FAIL(str_values_array_.push_back(str_values))) {
          LOG_WARN("failed to push_back", K(ret), K(str_values));
        }
        LOG_DEBUG("ignore", K(ret), K(i), KPC(raw_expr->get_param_expr(4)));
      }
    }
    LOG_DEBUG("finish add_str_values_array",
        K(ret),
        K(expr.count()),
        "count",
        str_values_array_.count(),
        K(str_values_array_));
  }
  return ret;
}

int ObLogExprValues::compute_fd_item_set()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> select_exprs;
  int32_t stmt_level = -1;
  ObFdItemSet* fd_item_set = NULL;
  if (OB_ISNULL(my_plan_) || OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect parameter", K(my_plan_), K(get_stmt()));
  } else if (OB_FAIL(static_cast<ObSelectStmt*>(get_stmt())->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_fd_item_set(fd_item_set))) {
    LOG_WARN("failed to create fd item set", K(ret));
  } else {
    stmt_level = get_stmt()->get_current_level();
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    ObSEArray<ObRawExpr*, 1> value_exprs;
    ObExprFdItem* fd_item = NULL;
    if (OB_FAIL(value_exprs.push_back(select_exprs.at(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    } else if (OB_FAIL(my_plan_->get_fd_item_factory().create_expr_fd_item(
                   fd_item, true, value_exprs, stmt_level, select_exprs))) {
      LOG_WARN("failed to create fd item", K(ret));
    } else if (OB_FAIL(fd_item_set->push_back(fd_item))) {
      LOG_WARN("failed to push back fd item", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    /*do nothing*/
  } else if (OB_FAIL(deduce_const_exprs_and_ft_item_set(*fd_item_set))) {
    LOG_WARN("falied to deduce fd item set", K(ret));
  } else {
    set_fd_item_set(fd_item_set);
  }
  return ret;
}

int ObLogExprValues::compute_equal_set()
{
  int ret = OB_SUCCESS;
  set_ordering_output_equal_sets(&empty_expr_sets_);
  set_sharding_output_equal_sets(&empty_expr_sets_);
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
  ObArray<ObRawExpr*> select_exprs;
  if (OB_ISNULL(get_stmt()) || OB_UNLIKELY(!get_stmt()->is_select_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt is invalid", K(ret));
  } else if (OB_FAIL(static_cast<ObSelectStmt*>(get_stmt())->get_select_exprs(select_exprs))) {
    LOG_WARN("failed to get select exprs", K(ret));
  } else if (OB_FAIL(ObOptimizerUtil::copy_sort_keys(select_exprs, op_ordering_))) {
    LOG_WARN("failed to copy sort keys", K(ret));
  }
  return ret;
}

int ObLogExprValues::est_cost()
{
  int ret = OB_SUCCESS;
  // set cost and card
  set_card(1.0);
  set_cost(ObOptEstCost::get_cost_params().CPU_OPERATOR_COST *
               static_cast<double>(get_stmt()->get_condition_exprs().count()) +
           ObOptEstCost::get_cost_params().CPU_TUPLE_COST);
  set_op_cost(get_cost());
  // TODO
  return ret;
}

int ObLogExprValues::compute_one_row_info()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (!get_stmt()->is_insert_stmt()) {
    is_at_most_one_row_ = true;
  } else { /*do nothing*/
  }

  return ret;
}

int ObLogExprValues::allocate_dummy_output()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLogicalOperator::allocate_dummy_output())) {
    LOG_WARN("fail to allocate dummy output", K(ret));
  } else if (OB_FAIL(append(value_exprs_, output_exprs_))) {
    LOG_WARN("failed to append exprs", K(ret));
  }
  return ret;
}

int ObLogExprValues::allocate_expr_post(ObAllocExprContext& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt is null");
  } else if (get_stmt()->is_insert_stmt()) {
    ObInsertStmt* insert_stmt = static_cast<ObInsertStmt*>(get_stmt());
    ObIArray<ObColumnRefRawExpr*>& values_desc = insert_stmt->get_values_desc();
    for (int64_t i = 0; OB_SUCC(ret) && i < values_desc.count(); ++i) {
      ObColumnRefRawExpr* value_col = values_desc.at(i);
      bool expr_is_required = false;
      if (OB_FAIL(mark_expr_produced(value_col, branch_id_, id_, ctx, expr_is_required))) {
        LOG_WARN("makr expr produced failed", K(ret));
      } else if (!is_plan_root() && expr_is_required) {
        if (OB_FAIL(add_var_to_array_no_dup(output_exprs_, static_cast<ObRawExpr*>(value_col)))) {
          LOG_WARN("add expr no duplicate key failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLogicalOperator::allocate_expr_post(ctx))) {
      LOG_WARN("failed to allocate expr post", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_exprs_.count(); i++) {
        ObRawExpr* raw_expr = NULL;
        if (OB_ISNULL(raw_expr = output_exprs_.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (raw_expr->has_flag(CNT_COLUMN)) {
          /*do nothing*/
        } else if (OB_FAIL(value_exprs_.push_back(raw_expr))) {
          LOG_WARN("failed to add expr", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObLogExprValues::print_my_plan_annotation(char* buf, int64_t& buf_len, int64_t& pos, ExplainType type)
{
  const ObIArray<ObRawExpr*>* values = &get_value_exprs();
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(buf));
  } else if (OB_ISNULL(values)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get value expr", K(values));
  } else if (OB_FAIL(BUF_PRINTF("\n      "))) {
    LOG_WARN("fail to do buf_print", K(ret));
  } else {
    EXPLAIN_PRINT_INSERT_VALUES(values, output_exprs_.count(), type);
  }
  if (OB_FAIL(ret)) {
    pos = tmp_pos;
  }
  return ret;
}

uint64_t ObLogExprValues::hash(uint64_t seed) const
{
  seed = do_hash(need_columnlized_, seed);
  seed = ObOptimizerUtil::hash_exprs(seed, value_exprs_);
  seed = ObLogicalOperator::hash(seed);

  return seed;
}

int ObLogExprValues::inner_append_not_produced_exprs(ObRawExprUniqueSet& raw_exprs) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(get_stmt())) {
    ret = OB_NOT_INIT;
    LOG_WARN("stmt is null", K(ret));
  } else if (get_stmt()->is_insert_stmt()) {
    const ObInsertStmt* insert_stmt = static_cast<const ObInsertStmt*>(get_stmt());
    const ObIArray<ObColumnRefRawExpr*>& values_desc = insert_stmt->get_values_desc();
    for (int64_t i = 0; OB_SUCC(ret) && i < values_desc.count(); i++) {
      OZ(raw_exprs.append(static_cast<ObRawExpr*>(values_desc.at(i))));
    }
  }
  OZ(raw_exprs.append(value_exprs_));

  return ret;
}

int ObLogExprValues::check_range_param_continuous(bool& use_range_param) const
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = get_output_exprs().count();
  int64_t last_param_index = OB_INVALID_INDEX;
  if (column_cnt > 1 && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2230) {
    use_range_param = true;
    for (int64_t i = 0; OB_SUCC(ret) && use_range_param && i <= value_exprs_.count() - column_cnt; i += column_cnt) {
      last_param_index = OB_INVALID_INDEX;
      for (int64_t j = i; OB_SUCC(ret) && use_range_param && j < i + column_cnt; ++j) {
        if (OB_ISNULL(value_exprs_.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("value expr is null", K(ret), K(value_exprs_), K(i), K(j));
        } else if (!value_exprs_.at(j)->is_const_expr()) {
          use_range_param = false;
        } else {
          const ObConstRawExpr* param_expr = static_cast<const ObConstRawExpr*>(value_exprs_.at(j));
          if (!param_expr->get_value().is_unknown()) {
            use_range_param = false;
          } else if (param_expr->get_result_type().get_param().is_ext()) {
            use_range_param = false;
          } else if (param_expr->get_value().get_unknown() != last_param_index + 1 &&
                     OB_INVALID_INDEX != last_param_index) {
            use_range_param = false;
          } else {
            last_param_index = param_expr->get_value().get_unknown();
          }
        }
        LOG_DEBUG("check range param continuous", K(use_range_param), K(last_param_index));
      }
    }
  } else {
    use_range_param = false;
  }
  return ret;
}

int ObLogExprValues::get_value_param_range(int64_t row_index, int64_t& param_idx_start, int64_t& param_idx_end) const
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = get_output_exprs().count();
  int64_t value_idx_start = row_index * column_cnt;
  if (OB_UNLIKELY(value_idx_start < 0) || OB_UNLIKELY(value_idx_start >= value_exprs_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value index is invalid", K(row_index), K(column_cnt), K(value_idx_start), K(ret));
  } else if (!value_exprs_.at(value_idx_start)->is_const_expr()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("value expr is invalid", K(ret), KPC(value_exprs_.at(value_idx_start)));
  } else {
    const ObConstRawExpr* param_expr = static_cast<const ObConstRawExpr*>(value_exprs_.at(value_idx_start));
    if (OB_FAIL(param_expr->get_value().get_unknown(param_idx_start))) {
      LOG_WARN("get param index start value failed", K(ret), K(param_expr->get_value()));
    } else {
      param_idx_end = param_idx_start + column_cnt - 1;
      LOG_DEBUG("get value param range", K(row_index), K(param_idx_start), K(param_idx_end));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
