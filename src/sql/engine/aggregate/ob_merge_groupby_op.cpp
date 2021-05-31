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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/aggregate/ob_merge_groupby_op.h"
#include "lib/number/ob_number_v2.h"

namespace oceanbase {
namespace sql {

OB_SERIALIZE_MEMBER(
    (ObMergeGroupBySpec, ObGroupBySpec), group_exprs_, rollup_exprs_, is_distinct_rollup_expr_, has_rollup_);

DEF_TO_STRING(ObMergeGroupBySpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("groupby_spec");
  J_COLON();
  pos += ObGroupBySpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(group_exprs), K_(rollup_exprs), K_(is_distinct_rollup_expr), K_(has_rollup));
  J_OBJ_END();
  return pos;
}

int ObMergeGroupBySpec::add_group_expr(ObExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(group_exprs_.push_back(expr))) {
    LOG_ERROR("failed to push_back expr");
  }
  return ret;
}

int ObMergeGroupBySpec::add_rollup_expr(ObExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(rollup_exprs_.push_back(expr))) {
    LOG_ERROR("failed to push_back expr");
  }
  return ret;
}

void ObMergeGroupByOp::reset()
{
  is_end_ = false;
  cur_output_group_id_ = OB_INVALID_INDEX;
  first_output_group_id_ = 0;
  last_child_output_.reset();
  curr_groupby_datums_.reset();
}

int ObMergeGroupByOp::init()
{
  int ret = OB_SUCCESS;
  const int64_t col_count =
      (MY_SPEC.has_rollup_ ? (MY_SPEC.group_exprs_.count() + MY_SPEC.rollup_exprs_.count() + 1) : 0);
  if (OB_FAIL(aggr_processor_.init_group_rows(col_count))) {
    LOG_WARN("failed to initialize init_group_rows", K(ret));
  } else if (OB_FAIL(
                 curr_groupby_datums_.prepare_allocate(MY_SPEC.group_exprs_.count() + MY_SPEC.rollup_exprs_.count()))) {
    LOG_WARN("failed to initialize curr_groupby_datums_", K(ret));
  } else {
    last_child_output_.reuse_ = true;
  }
  return ret;
}

int ObMergeGroupByOp::inner_open()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByOp::inner_open())) {
    LOG_WARN("failed to inner_open", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  }
  return ret;
}

int ObMergeGroupByOp::inner_close()
{
  reset();
  return ObGroupByOp::inner_close();
}

void ObMergeGroupByOp::destroy()
{
  reset();
  ObGroupByOp::destroy();
}

int ObMergeGroupByOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByOp::switch_iterator())) {
    LOG_WARN("failed to switch_iterator", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  }
  return ret;
}

int ObMergeGroupByOp::rescan()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByOp::rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else if (OB_FAIL(init())) {
    LOG_WARN("failed to init", K(ret));
  }
  return ret;
}

int ObMergeGroupByOp::rewrite_rollup_column(ObExpr*& diff_expr)
{
  int ret = OB_SUCCESS;
  // output roll-up results here
  const bool is_distinct_expr =
      (cur_output_group_id_ < MY_SPEC.group_exprs_.count()
              ? true
              : MY_SPEC.is_distinct_rollup_expr_.at(cur_output_group_id_ - MY_SPEC.group_exprs_.count()));
  if (!is_distinct_expr) {
    ObExpr* null_expr =
        const_cast<ObExpr*>(cur_output_group_id_ < MY_SPEC.group_exprs_.count()
                                ? MY_SPEC.group_exprs_[cur_output_group_id_]
                                : MY_SPEC.rollup_exprs_[cur_output_group_id_ - MY_SPEC.group_exprs_.count()]);
    null_expr->locate_expr_datum(eval_ctx_).set_null();
    null_expr->get_eval_info(eval_ctx_).evaluated_ = true;
  }
  diff_expr = const_cast<ObExpr*>(cur_output_group_id_ < MY_SPEC.group_exprs_.count()
                                      ? MY_SPEC.group_exprs_[cur_output_group_id_]
                                      : MY_SPEC.rollup_exprs_[cur_output_group_id_ - MY_SPEC.group_exprs_.count()]);
  // for SELECT GROUPING(z0_test0) FROM Z0CASE GROUP BY z0_test0, ROLLUP(z0_test0);
  if (cur_output_group_id_ >= MY_SPEC.group_exprs_.count()) {
    for (int64_t i = 0; diff_expr != NULL && i < MY_SPEC.group_exprs_.count(); ++i) {
      if (MY_SPEC.group_exprs_[i] == diff_expr) {
        diff_expr = NULL;
      }
    }
  }
  return ret;
}

int ObMergeGroupByOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const int64_t stop_output_group_id = MY_SPEC.group_exprs_.count();
  const int64_t col_count = MY_SPEC.group_exprs_.count() + MY_SPEC.rollup_exprs_.count();
  const int64_t group_id = MY_SPEC.has_rollup_ ? col_count : 0;
  LOG_DEBUG("before inner_get_next_row",
      "aggr_hold_size",
      aggr_processor_.get_aggr_hold_size(),
      "aggr_used_size",
      aggr_processor_.get_aggr_used_size());

  if (MY_SPEC.has_rollup_ && cur_output_group_id_ >= first_output_group_id_ &&
      cur_output_group_id_ >= stop_output_group_id) {

    ObExpr* diff_expr = NULL;
    if (OB_FAIL(rewrite_rollup_column(diff_expr))) {
      LOG_WARN("failed to rewrite_rollup_column", K(ret));
    } else if (OB_FAIL(rollup_and_calc_results(cur_output_group_id_, diff_expr))) {
      LOG_WARN("failed to rollup and calculate results", K(cur_output_group_id_), K(ret));
    } else {
      --cur_output_group_id_;
      LOG_DEBUG("finish ouput rollup row", K(cur_output_group_id_), K(first_output_group_id_), K(ret));
    }
  } else if (is_end_) {
    ret = OB_ITER_END;
  } else {
    // output group results here
    bool is_break = false;
    int64_t first_diff_pos = OB_INVALID_INDEX;
    ObAggregateProcessor::GroupRow* group_row = NULL;
    if (OB_FAIL(aggr_processor_.get_group_row(group_id, group_row))) {
      LOG_WARN("failed to get_group_row", K(ret));
    } else if (OB_ISNULL(group_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group_row is null", K(ret));
    } else if (NULL != last_child_output_.store_row_) {
      if (OB_FAIL(last_child_output_.store_row_->to_expr(child_->get_spec().output_, eval_ctx_))) {
        LOG_WARN("Failed to get next row", K(ret));
      }
    } else {
      if (OB_FAIL(child_->get_next_row())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next row", K(ret));
        }
      }
    }
    LOG_DEBUG("finish merge prepare 1", K(child_->get_spec().output_), KPC(last_child_output_.store_row_));

    if (OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (OB_FAIL(aggr_processor_.prepare(*group_row))) {
        LOG_WARN("failed to prepare", K(ret));
      } else if (OB_FAIL(prepare_curr_groupby_datum())) {
        LOG_WARN("failed to prepare_curr_groupby_datum", K(ret));
      }

      while (OB_SUCC(ret) && !is_break && OB_SUCC(child_->get_next_row())) {
        clear_evaluated_flag();
        if (OB_FAIL(try_check_status())) {
          LOG_WARN("check status failed", K(ret));
        } else if (OB_FAIL(check_same_group(first_diff_pos))) {
          LOG_WARN("failed to check group", K(ret));
        } else if (OB_INVALID_INDEX == first_diff_pos) {
          // same group
          if (OB_FAIL(aggr_processor_.process(*group_row))) {
            LOG_WARN("failed to calc aggr", K(ret));
          }
        } else {
          // different group
          if (OB_FAIL(last_child_output_.save_store_row(child_->get_spec().output_, eval_ctx_, 0))) {
            LOG_WARN("failed to store child output", K(ret));
          } else if (OB_FAIL(restore_groupby_datum(first_diff_pos))) {
            LOG_WARN("failed to restore_groupby_datum", K(ret));
          } else if (OB_FAIL(rollup_and_calc_results(group_id))) {
            LOG_WARN("failed to rollup and calculate results", K(group_id), K(ret));
          } else {
            is_break = true;
            if (MY_SPEC.has_rollup_) {
              first_output_group_id_ = first_diff_pos + 1;
              cur_output_group_id_ = group_id - 1;
            }
          }
        }
      }  // end while

      if (OB_ITER_END == ret) {
        // the last group
        is_end_ = true;
        if (OB_FAIL(restore_groupby_datum(0))) {
          LOG_WARN("failed to restore_groupby_datum", K(ret));
        } else if (OB_FAIL(rollup_and_calc_results(group_id))) {
          LOG_WARN("failed to rollup and calculate results", K(group_id), K(ret));
        } else {
          if (MY_SPEC.has_rollup_) {
            first_output_group_id_ = 0;
            cur_output_group_id_ = group_id - 1;
          }
        }
        LOG_DEBUG("finish iter end", K(first_output_group_id_), K(cur_output_group_id_));
      }
    }
  }
  LOG_TRACE("after inner_get_next_row",
      "aggr_hold_size",
      aggr_processor_.get_aggr_hold_size(),
      "aggr_used_size",
      aggr_processor_.get_aggr_used_size());
  return ret;
}

int ObMergeGroupByOp::prepare_curr_groupby_datum()
{
  int ret = OB_SUCCESS;
  ExprFixedArray& group_exprs = const_cast<ExprFixedArray&>(MY_SPEC.group_exprs_);
  ExprFixedArray& rollup_exprs = const_cast<ExprFixedArray&>(MY_SPEC.rollup_exprs_);
  const int64_t group_count = group_exprs.count();
  const int64_t rollup_count = rollup_exprs.count();

  if (OB_UNLIKELY(curr_groupby_datums_.count() != (group_count + rollup_count))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("last_groupby_datum is unexpected ",
        "count",
        curr_groupby_datums_.count(),
        K(group_count),
        K(rollup_count),
        K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < group_count; ++i) {
    ObDatum& last_datum = curr_groupby_datums_.at(i);
    ObDatum* result = NULL;
    ObExpr* expr = group_exprs.at(i);
    if (OB_FAIL(expr->eval(eval_ctx_, result))) {
      LOG_WARN("eval failed", K(ret));
    } else if (OB_FAIL(aggr_processor_.clone_cell(last_datum, *result))) {
      LOG_WARN("clone_cell failed", K(ret));
    } else {
      LOG_DEBUG("clone_cell succ", K(i), KPC(result), K(last_datum));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < rollup_count; ++i) {
    ObDatum& last_datum = curr_groupby_datums_.at(i + group_count);
    ObDatum* result = NULL;
    ObExpr* expr = rollup_exprs.at(i);
    if (OB_FAIL(expr->eval(eval_ctx_, result))) {
      LOG_WARN("eval failed", K(ret));
    } else if (OB_FAIL(aggr_processor_.clone_cell(last_datum, *result))) {
      LOG_WARN("clone_cell failed", K(ret));
    } else {
      LOG_DEBUG("clone_cell succ", K(i), KPC(result), K(last_datum));
    }
  }
  LOG_DEBUG("finish prepare_curr_groupby_datum", K(curr_groupby_datums_), K(group_exprs), K(rollup_exprs), K(ret));
  return ret;
}

int ObMergeGroupByOp::check_same_group(int64_t& diff_pos)
{
  int ret = OB_SUCCESS;
  diff_pos = OB_INVALID_INDEX;
  ExprFixedArray& group_exprs = const_cast<ExprFixedArray&>(MY_SPEC.group_exprs_);
  ExprFixedArray& rollup_exprs = const_cast<ExprFixedArray&>(MY_SPEC.rollup_exprs_);
  const int64_t group_count = group_exprs.count();
  const int64_t rollup_count = rollup_exprs.count();

  ObDatum* result = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < group_count; ++i) {
    const ObDatum& last_datum = curr_groupby_datums_.at(i);
    ObExpr* expr = group_exprs.at(i);
    if (OB_ISNULL(expr) || OB_ISNULL(expr->basic_funcs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", K(ret));
    } else if (OB_FAIL(expr->eval(eval_ctx_, result))) {
      LOG_WARN("eval failed", K(ret));
    } else if (last_datum.is_null() && result->is_null()) {
      continue;
    } else if (last_datum.is_null() || result->is_null() ||
               expr->basic_funcs_->null_first_cmp_(last_datum, *result) != 0) {
      diff_pos = i;
      break;
    }
  }

  // need a new expr for distinct??@
  if (OB_INVALID_INDEX == diff_pos) {
    result = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < rollup_exprs.count(); ++i) {
      const ObDatum& last_datum = curr_groupby_datums_.at(i + group_count);
      ObExprCmpFuncType cmp_func = rollup_exprs.at(i)->basic_funcs_->null_first_cmp_;
      ObExpr* expr = rollup_exprs.at(i);
      if (OB_ISNULL(expr) || OB_ISNULL(expr->basic_funcs_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr node is null", K(ret));
      } else if (OB_FAIL(expr->eval(eval_ctx_, result))) {
        LOG_WARN("eval failed", K(ret));
      } else if (last_datum.is_null() && result->is_null()) {
        continue;
      } else if (last_datum.is_null() || result->is_null() ||
                 expr->basic_funcs_->null_first_cmp_(last_datum, *result) != 0) {
        diff_pos = i + group_count;
        break;
      }
    }
  }
  LOG_DEBUG("finish check_same_group",
      K(diff_pos),
      K(curr_groupby_datums_),
      KPC(result),
      K(group_exprs),
      K(rollup_exprs),
      K(ret));

  return ret;
}

int ObMergeGroupByOp::restore_groupby_datum(const int64_t diff_pos)
{
  int ret = OB_SUCCESS;
  ExprFixedArray& group_exprs = const_cast<ExprFixedArray&>(MY_SPEC.group_exprs_);
  ExprFixedArray& rollup_exprs = const_cast<ExprFixedArray&>(MY_SPEC.rollup_exprs_);
  const int64_t group_count = group_exprs.count();
  const int64_t rollup_count = rollup_exprs.count();

  for (int64_t i = 0; OB_SUCC(ret) && i < group_count; ++i) {
    ObDatum& last_datum = curr_groupby_datums_.at(i);
    ObExpr* expr = group_exprs.at(i);
    ObDatum& result = expr->locate_expr_datum(eval_ctx_);
    result.set_datum(last_datum);
    expr->get_eval_info(eval_ctx_).evaluated_ = true;
    LOG_DEBUG("succ to restore", K(i), KPC(expr), K(result), K(last_datum));
  }
  for (int64_t i = std::max(0L, diff_pos - group_count); OB_SUCC(ret) && i < rollup_count; ++i) {
    ObDatum& last_datum = curr_groupby_datums_.at(i + group_count);
    ObExpr* expr = rollup_exprs.at(i);
    ObDatum& result = expr->locate_expr_datum(eval_ctx_);
    result.set_datum(last_datum);
    expr->get_eval_info(eval_ctx_).evaluated_ = true;
    LOG_DEBUG("succ to restore", K(i), KPC(expr), K(result), K(last_datum));
  }
  LOG_DEBUG(
      "finish restore_groupby_datum", K(curr_groupby_datums_), K(group_exprs), K(rollup_exprs), K(diff_pos), K(ret));
  return ret;
}

int ObMergeGroupByOp::rollup_and_calc_results(const int64_t group_id, const ObExpr* diff_expr /*= NULL*/)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = MY_SPEC.group_exprs_.count() + MY_SPEC.rollup_exprs_.count();
  if (OB_UNLIKELY(group_id < 0 || group_id > col_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(group_id), K(col_count), K(ret));
  } else if (MY_SPEC.has_rollup_ && group_id > 0) {
    if (OB_FAIL(aggr_processor_.rollup_process(group_id, diff_expr))) {
      LOG_WARN("failed to rollup aggregation results", K(ret));
    }
    diff_expr = 0;
  }
  if (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(aggr_processor_.collect(group_id, diff_expr))) {
      LOG_WARN("failed to collect aggr result", K(group_id), K(ret));
    } else if (OB_FAIL(aggr_processor_.reuse_group(group_id))) {
      LOG_WARN("failed to reuse group", K(group_id), K(ret));
    } else {
      LOG_DEBUG(
          "finish rollup_and_calc_results", K(group_id), K(is_end_), "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
