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

#include "ob_limit_op.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/basic/ob_material_op.h"
#include "sql/engine/sort/ob_sort_op.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObLimitSpec::ObLimitSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
    limit_expr_(NULL),
    offset_expr_(NULL),
    percent_expr_(NULL),
    calc_found_rows_(false),
    is_top_limit_(false),
    is_fetch_with_ties_(false),
    sort_columns_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObLimitSpec, ObOpSpec),
                    limit_expr_,
                    offset_expr_,
                    percent_expr_,
                    calc_found_rows_,
                    is_top_limit_,
                    is_fetch_with_ties_,
                    sort_columns_);

ObLimitOp::ObLimitOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    limit_(-1),
    offset_(0),
    input_cnt_(0),
    output_cnt_(0),
    total_cnt_(0),
    is_percent_first_(false),
    pre_sort_columns_(exec_ctx.get_allocator())
{
}

int ObLimitOp::inner_open()
{
  int ret = OB_SUCCESS;
  bool is_null_value = false;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit operator has no child", K(ret));
  } else if (OB_FAIL(get_int_val(MY_SPEC.limit_expr_, eval_ctx_, limit_, is_null_value))) {
    LOG_WARN("get limit values failed", K(ret));
  } else if (!is_null_value && OB_FAIL(get_int_val(MY_SPEC.offset_expr_, eval_ctx_,
                                                   offset_, is_null_value))) {
    LOG_WARN("get offset values failed", K(ret));
  } else if (is_null_value) {
    offset_ = 0;
    limit_ = 0;
  } else {
    is_percent_first_ = NULL != MY_SPEC.percent_expr_;
    // revise limit, offset because rownum < -1 is rewritten as limit -1
    // 如果limit小于0, offset_也没有什么意义，这里一起判断
    //offset 2 rows fetch next -3 rows only --> is meaningless
    offset_ = offset_ < 0 ? 0 : offset_;
    if (MY_SPEC.limit_expr_ != NULL) {//不能统一直接设置为0,因为需要支持仅仅只有offset情形
      limit_ = limit_ < 0 ? 0 : limit_;
    }
    pre_sort_columns_.reuse_ = true;
  }

  return ret;
}

int ObLimitOp::inner_rescan()
{
  input_cnt_ = 0;
  output_cnt_ = 0;
  return ObOperator::inner_rescan();
}

int ObLimitOp::get_int_val(ObExpr *expr, ObEvalCtx &eval_ctx, int64_t &val, bool &is_null_value)
{
  int ret = OB_SUCCESS;
  if (NULL != expr) {
    OB_ASSERT(ob_is_int_tc(expr->datum_meta_.type_));
    ObDatum *datum = NULL;
    if (OB_FAIL(expr->eval(eval_ctx, datum))) {
      LOG_WARN("expr evaluate failed", K(ret), K(expr));
    } else if (datum->null_) {
      is_null_value = true;
      val = 0;
    } else {
      val = *datum->int_;
    }
  }
  return ret;
}

int ObLimitOp::get_double_val(ObExpr *expr, ObEvalCtx &eval_ctx, double &val)
{
  int ret = OB_SUCCESS;
  if (NULL != expr) {
    OB_ASSERT(ob_is_double_tc(expr->datum_meta_.type_));
    ObDatum *datum = NULL;
    if (OB_FAIL(expr->eval(eval_ctx, datum))) {
      LOG_WARN("expr evaluate failed", K(ret), K(expr));
    } else if (datum->null_) {
      val = 0.0;
    } else {
      val = *datum->double_;
    }
  }
  return ret;
}

// copy from ObLimit::inner_get_next_row
int ObLimitOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  LOG_DEBUG("limitop get_next_row start", K(limit_), K(offset_));
  while (OB_SUCC(ret) && input_cnt_ < offset_) {
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("child_op failed to get next row", K(input_cnt_), K(offset_), K(ret));
      }
    } else if (is_percent_first_ && OB_FAIL(convert_limit_percent())) {
      LOG_WARN("failed to convert limit percent", K(ret));
    } else {
      ++input_cnt_;
    }
  } // end while

  /*由于支持了oracle 12c的fetch功能，因此下面的执行流程比较复杂，这里简单解释一下：
  * 1.is_percent_first_：代表的fetch是否指定的百分比取行数，比如：select * from t1 fetch next 50 percent rows only;
  *   取总行数的50%出来，这个时候需要is_percent_first_来表明是否用的百分比，同时我们的下层block算子(sort、hash group by等)
  *   都是在get_next_row时指定设置的，因此需要在第一次时去设置对应的limit数量，同时设置完后将is_percent_first_重新设置为false；
  * 2.is_fetch_with_ties_：表示在拿到所需要的limit数量时，需要继续下探是否存在按照order by排序列值相等的情形，
  *   比如表t1有3行数据 c1 c2 c3
  *                   1  2  3
  *                   1  2  4
  *                   2  2  3
  *   这个时候假如按照表t1的c1列排序，同时设置了只输出一列，但是指定了with ties(sql为:select * from t1 order by c1 fetch next 1 rows with ties);
  *   那么需要将每次从child op拿取rows同时保存拿到的最后一行数据，等拿到了指定的数量之后，继续按照保存好的最后一行数据
  *   下探child op的rows,直到拿到按照order by排序列的值不相等或者拿完了child op的rows为止；比如上述例子中拿到行：1 2 3
  *   会继续下探行：1 2 4，发现排序列c1值相等，会继续下探拿行：2 2 3，这个时候排序列c1值不等，整个get_next_row结束。
  *
   */

  int64_t left_count = 0;
  if (OB_SUCC(ret)) {
    if (is_percent_first_ || output_cnt_ < limit_ || limit_ < 0) {
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("child_op failed to get next row",
                   K(ret), K_(limit), K_(offset), K_(input_cnt), K_(output_cnt));
        }
      } else if (is_percent_first_ && OB_FAIL(convert_limit_percent())) {
        LOG_WARN("failed to convert limit percent", K(ret));
      } else if (limit_ == 0) {
        ret = OB_ITER_END;
      } else {
        ++output_cnt_;
        LOG_DEBUG("output row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
        //如果需要支持fetch with ties功能，需要拷贝limit拿出的最后一行保存下来供后续使用
        if (MY_SPEC.is_fetch_with_ties_ && output_cnt_ == limit_ &&
            OB_FAIL(pre_sort_columns_.save_store_row(MY_SPEC.sort_columns_, eval_ctx_))) {
          LOG_WARN("failed to deep copy limit last rows", K(ret));
        }
      }
    //说明需要继续判断input rows能否按照order by items等值输出
    } else if (limit_ > 0 && MY_SPEC.is_fetch_with_ties_) {
      bool is_equal = false;
      if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("child_op failed to get next row",
                   K(ret), K_(limit), K_(offset), K_(input_cnt), K_(output_cnt));
        }
      } else if (OB_FAIL(is_row_order_by_item_value_equal(is_equal))) {
        LOG_WARN("failed to is row order by item value equal", K(ret));
      } else if (is_equal) {
        ++output_cnt_;
      } else {
        //溢出的按照order by排序相等的row已经找完
        ret = OB_ITER_END;
      }
    } else {
      // 结果条数已经满足
      ret = OB_ITER_END;
      if (MY_SPEC.calc_found_rows_) {
        while (OB_SUCC(child_->get_next_row())) {
          ++left_count;
        }
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row from child", K(ret));
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    if (MY_SPEC.is_top_limit_) {
      total_cnt_ = left_count + output_cnt_ + input_cnt_;
      ObPhysicalPlanCtx *plan_ctx = NULL;
      if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("get physical plan context failed");
      } else {
        NG_TRACE_EXT(found_rows,
                     OB_ID(total_count), total_cnt_, OB_ID(input_count), input_cnt_);
        plan_ctx->set_found_rows(total_cnt_);
      }
    }
  }
  return ret;
}

// Batch version of ObLimitOp::inner_get_next_row
int ObLimitOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  LOG_DEBUG("limitop get_next_batch start", K(limit_), K(offset_), K(batch_cnt),
             K(is_percent_first_), K(output_cnt_), K(MY_SPEC.calc_found_rows_));
  const ObBatchRows *child_brs = nullptr;
  clear_evaluated_flag();
  while (OB_SUCC(ret) && input_cnt_ < offset_) {
    // Note: batch_cnt is NEVER bigger than offset
    if (input_cnt_ + batch_cnt > offset_) {
      batch_cnt = offset_ - input_cnt_;
    }
    if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
      LOG_WARN("child_op failed to get next row", K(input_cnt_), K(offset_), K(ret));
    } else if (is_percent_first_ && OB_FAIL(convert_limit_percent())) {
      LOG_WARN("failed to convert limit percent", K(ret));
    } else {
      input_cnt_ += (child_brs->size_ - child_brs->skip_->accumulate_bit_cnt(child_brs->size_));
    }
    if (child_brs->end_) {
      brs_.end_ = true;
      break;
    }
  } // end while

  LOG_DEBUG("limitop get_next_batch", K(brs_), K(input_cnt_), K(batch_cnt), K(output_cnt_));
  auto skip_fetch_rows = false;
  if (input_cnt_ > offset_ && output_cnt_ == 0) {
    // offset error handling: child operator return more rows than expected
    // mark offset rows as skipped and continue limit logic
    brs_.copy(child_brs);
    input_cnt_ -= (brs_.size_ - brs_.skip_->accumulate_bit_cnt(brs_.size_));
    for (auto i = 0; i < child_brs->size_; i++) {
      if (brs_.skip_->at(i)) {
        continue;
      } else {
        ++input_cnt_;
        if (input_cnt_ <= offset_) {
          brs_.skip_->set(i);
        } else {
          break;
        }
      }
    }
    // stop getting rows from child and setting brs_ when skip_fetch_rows is true
    skip_fetch_rows = true;
  }

  // now the limit part
  int64_t left_count = 0;
  batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (OB_UNLIKELY(brs_.end_) && !skip_fetch_rows) {
    brs_.size_ = 0;
    LOG_DEBUG("Offset num is bigger than child output num, return empty rows",
              K(offset_), K(input_cnt_), K(child_brs->size_),
              K(child_brs->end_));
  } else if (OB_SUCC(ret)) {
    if (is_percent_first_ || output_cnt_ < limit_ || limit_ < 0) {
      // adjust iterating count for last batch
      if (output_cnt_ + batch_cnt > limit_ && limit_ >= 0) {
        batch_cnt = limit_ - output_cnt_;
      }
      if (is_percent_first_) {
        // Fetch one row for percent first fetch, make sure %output_cnt_ never exceed %limit_
        // in this round.
        batch_cnt = 1;
      }

      if (!skip_fetch_rows && OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
        LOG_WARN("child_op failed to get next row", K(ret), K(limit_), K(batch_cnt));
      } else if (is_percent_first_ && OB_FAIL(convert_limit_percent())) {
        LOG_WARN("failed to convert limit percent", K(ret));
      } else if (limit_ == 0) {
        brs_.size_ = 0;
        brs_.end_ = true;
      } else {
        if (!skip_fetch_rows) {
          // skip copy brs_ from child as it is already copied in offset error
          // handing branch
          brs_.copy(child_brs);
        }
        output_cnt_ += (brs_.size_ - brs_.skip_->accumulate_bit_cnt(brs_.size_));
        if (output_cnt_ == limit_) {
          // Don't mark brs_.end_, end iterating in next round
          if (MY_SPEC.is_fetch_with_ties_) {
            ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
            batch_info_guard.set_batch_size(brs_.size_);
            batch_info_guard.set_batch_idx(find_last_available_row_cnt(*(brs_.skip_), brs_.size_));
            if (OB_FAIL(pre_sort_columns_.save_store_row(MY_SPEC.sort_columns_, eval_ctx_))) {
              LOG_WARN("failed to deep copy limit last rows", K(ret));
            }
          }
        } else if (OB_UNLIKELY(
                       limit_ != -1 /*limit=-1 means no limit in oracle mode*/ &&
                       output_cnt_ > limit_)) {
          // Notice: here is the error hanlding branch
          // Child branch should NOT return rows more than batch_cnt.
          // If it return more row, something out of expect take place.
          // In order to keep limit logic NOT broken, update the batch size
          // within limit operator to get correct result.
          LOG_TRACE("child operator return more rows than expected",
                   K(output_cnt_), K(limit_), K(brs_), K(batch_cnt),
                   K(child_->get_spec().get_type()));

          // recaculate output_cnt_ and update output brs_.size_
          output_cnt_ -= (brs_.size_ - brs_.skip_->accumulate_bit_cnt(brs_.size_));
          for (auto i = 0; i < child_brs->size_; i++) {
            if (brs_.skip_->at(i)) {
              continue;
            } else {
              ++output_cnt_;
              if (output_cnt_ >= limit_) {
                brs_.size_ = i + 1;
                break;
              }
            }
          }
        }
      }
      skip_fetch_rows = false;
    } else if (limit_ > 0 && output_cnt_ >= limit_ &&
               MY_SPEC.is_fetch_with_ties_) {
      // keep fetching until a different value is found
      batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
      bool keep_iterating = false;
      uint32_t matched_row_count = 0;
      if (OB_FAIL(child_->get_next_batch(batch_cnt, child_brs))) {
          LOG_WARN("child_op failed to get next row",
                   K(ret), K(limit_), K(batch_cnt), K(child_brs->size_));
      } else if (OB_FAIL(compare_value_in_batch(keep_iterating, *(child_brs->skip_),
                                            child_brs->size_, matched_row_count))) {
        LOG_WARN("failed to is row order by item value equal", K(ret));
      }
      brs_.copy(child_brs);
      if (!keep_iterating) {
        brs_.end_ = true;
        brs_.size_ = matched_row_count;
      }
      output_cnt_ += brs_.size_;
    } else {
      brs_.end_ = true;
      if (MY_SPEC.calc_found_rows_) {
        batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
        while (OB_SUCC(child_->get_next_batch(batch_cnt, child_brs))) {
          left_count += (child_brs->size_ -
                         child_brs->skip_->accumulate_bit_cnt(child_brs->size_));
          if (child_brs->end_) {
            break;
          }
        }
        if (OB_SUCCESS != ret) {
          LOG_WARN("fail to get next row from child", K(ret));
        }
      }
    }
  }
  if (brs_.end_) {
    if (MY_SPEC.is_top_limit_) {
      total_cnt_ = left_count + output_cnt_ + input_cnt_;
      ObPhysicalPlanCtx *plan_ctx = NULL;
      if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("get physical plan context failed");
      } else {
        NG_TRACE_EXT(found_rows,
                     OB_ID(total_count), total_cnt_, OB_ID(input_count), input_cnt_);
        plan_ctx->set_found_rows(total_cnt_);
      }
    }
  }
  LOG_DEBUG("limitop get_next_batch finished", K(batch_cnt), K(output_cnt_),
              K(brs_), K(limit_), K(input_cnt_));
  return ret;
}

int ObLimitOp::is_row_order_by_item_value_equal(bool &is_equal)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (MY_SPEC.sort_columns_.empty()) {
    // %sort_columns_ is empty if order by const value, set is_equal to true directly.
    // pre_sort_columns_.store_row_ is NULL here.
    is_equal = true;
  } else {
    is_equal = true;
    CK(NULL != pre_sort_columns_.store_row_
       && pre_sort_columns_.store_row_->cnt_ == MY_SPEC.sort_columns_.count());
    for (int64_t i = 0; OB_SUCC(ret) && is_equal && i < MY_SPEC.sort_columns_.count(); ++i) {
      const ObExpr *expr = MY_SPEC.sort_columns_.at(i);
      ObDatum *datum = NULL;
      if (OB_FAIL(expr->eval(eval_ctx_, datum))) {
        LOG_WARN("expression evaluate failed", K(ret));
      } else if (OB_FAIL(expr->basic_funcs_->null_first_cmp_(
                 pre_sort_columns_.store_row_->cells()[i], *datum, cmp_ret))) {
        LOG_WARN("compare failed", K(ret));
      } else {
        is_equal = 0 == cmp_ret;
      }
    }
  }
  return ret;
}

// batch version for is_row_order_by_item_value_equal
int ObLimitOp::compare_value_in_batch(bool &keep_iterating,
                                      const ObBitVector &skip,
                                      const int64_t batch_size,
                                      uint32_t &row_count_matched)
{
  int ret = OB_SUCCESS;
  keep_iterating = true;
  if (MY_SPEC.sort_columns_.empty()) {
    // %sort_columns_ is empty if order by const value, set is_equal to true directly.
    // pre_sort_columns_.store_row_ is NULL here.
  } else {
    CK(NULL != pre_sort_columns_.store_row_
       && pre_sort_columns_.store_row_->cnt_ == MY_SPEC.sort_columns_.count());

    const int64_t size = MY_SPEC.sort_columns_.count();
    ObSEArray<ObDatumVector, 16> datum_vectors;
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.sort_columns_.count(); ++i) {
      ObExpr *expr = MY_SPEC.sort_columns_.at(i);
      if (OB_FAIL(expr->eval_batch(eval_ctx_, skip, batch_size))) {
        LOG_WARN("expression evaluate failed", K(ret));
      } else {
        datum_vectors.push_back(expr->locate_expr_datumvector(eval_ctx_));
      }
    }

    for (uint32_t row_idx = 0; OB_SUCC(ret) && keep_iterating && row_idx < batch_size; row_idx++) {
      if (skip.at(row_idx)) {
        continue;
      }
      for (int64_t col_idx = 0; OB_SUCC(ret) && keep_iterating && col_idx < datum_vectors.count();
                   ++col_idx) {
        ObExpr *expr = MY_SPEC.sort_columns_.at(col_idx);
        int cmp_ret = 0;
        if (OB_FAIL(expr->basic_funcs_->null_first_cmp_(
                    pre_sort_columns_.store_row_->cells()[col_idx],
                    *(datum_vectors[col_idx].at(row_idx)), cmp_ret))) {
          LOG_WARN("compare failed", K(ret));
        } else {
          keep_iterating = (0 == cmp_ret);
        }
      }
      if (keep_iterating) {
          row_count_matched++;
      }
    }
  }
  return ret;
}

//针对percent需要这里根据总行数转换为对应的limit count
int ObLimitOp::convert_limit_percent()
{
  int ret = OB_SUCCESS;
  double percent = 0.0;
  if (OB_FAIL(get_double_val(MY_SPEC.percent_expr_, eval_ctx_, percent))) {
    LOG_WARN("failed to get double value", K(ret));
  } else if (percent > 0) {
    int64_t tot_count = 0;
    if (OB_UNLIKELY(limit_ != -1) || OB_ISNULL(child_) ||
        OB_UNLIKELY(child_->get_spec().get_type() != PHY_MATERIAL &&
                    child_->get_spec().get_type() != PHY_SORT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(limit_), K(child_));
    } else if (child_->get_spec().get_type() == PHY_MATERIAL &&
               OB_FAIL(static_cast<ObMaterialOp *>(child_)->get_material_row_count(tot_count))) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (child_->get_spec().get_type() == PHY_SORT &&
               FALSE_IT(tot_count = static_cast<ObSortOp *>(child_)->get_sort_row_count())) {
      LOG_WARN("failed to get op row count", K(ret));
    } else if (OB_UNLIKELY(tot_count < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid child op row count", K(tot_count), K(ret));
    } else if (percent < 100) {
      //兼容oracle,向上取整
      int64_t percent_int64 = static_cast<int64_t>(percent);
      int64_t offset = (tot_count * percent / 100 - tot_count * percent_int64 / 100) > 0 ? 1 : 0;
      limit_ = tot_count * percent_int64 / 100 +  offset;
      is_percent_first_ = false;
    } else {
      limit_ = tot_count;
      is_percent_first_ = false;
    }
  } else {
    limit_ = 0;
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
