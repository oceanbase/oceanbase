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

#include "sql/engine/aggregate/ob_merge_distinct_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObMergeDistinctSpec::ObMergeDistinctSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObDistinctSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObMergeDistinctSpec, ObDistinctSpec));

ObMergeDistinctOp::ObMergeDistinctOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    first_got_row_(true),
    alloc_(ObModIds::OB_SQL_MERGE_GROUPBY,
      OB_MALLOC_NORMAL_BLOCK_SIZE, exec_ctx.get_my_session()->get_effective_tenant_id(), ObCtxIds::WORK_AREA),
    last_row_(alloc_)
{
}

int ObMergeDistinctOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (MY_SPEC.is_block_mode_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge distinct not support block mode", K(ret));
  } else {
    last_row_.reuse_ = true;
  }
  return ret;
}

int ObMergeDistinctOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  first_got_row_ = true;
  last_row_.reset();
  alloc_.reset();
  if (OB_FAIL(ObOperator::inner_rescan())) {
  }
  return ret;
}

int ObMergeDistinctOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_distinct_row = false;
  if (!first_got_row_) {
    while (OB_SUCC(ret) && !got_distinct_row) {
      clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_row())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("child operator get next row failed", K(ret));
        }
      } else {
        // compare current_row and last_row
        bool equal = false;
        if (OB_FAIL(cmp_.equal(&MY_SPEC.distinct_exprs_, last_row_.store_row_, equal))) {
          LOG_WARN("failed to cmp row", K(ret));
        } else if (!equal) {
          got_distinct_row = true;
          /* save this row to local buffer. last_row_buf_ reused */
          if (OB_SUCC(ret) &&
              OB_FAIL(last_row_.save_store_row(MY_SPEC.distinct_exprs_, eval_ctx_, 0))) {
            LOG_WARN("failed to storage row to operator context", K(ret));
          }
        }
      }
    } /* end while */
  } else { /* first row, always output */
    if (OB_FAIL(cmp_.init(&eval_ctx_, &MY_SPEC.cmp_funcs_))) {
      LOG_WARN("failed to init compare functions", K(ret));
    } else if (OB_FAIL(child_->get_next_row())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(last_row_.save_store_row(MY_SPEC.distinct_exprs_, eval_ctx_, 0))) {
      LOG_WARN("failed to store row", K(ret));
    } else {
      first_got_row_ = false;
    }
  }
  return ret;
}

int ObMergeDistinctOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t batch_size = std::min(max_row_cnt, MY_SPEC.max_batch_size_);
  clear_evaluated_flag();
  const ObBatchRows *child_brs = nullptr;
  bool got_batch = false;
  while (OB_SUCC(ret) && !got_batch) {
    if (!first_got_row_) {
      bool has_last = true;
      if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
        LOG_WARN("failed to get next batch", K(ret));
      } else if (child_brs->end_ && 0 == child_brs->size_) {
        ret = OB_ITER_END;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.distinct_exprs_.count(); ++i) {
          if (OB_FAIL(MY_SPEC.distinct_exprs_.at(i)->eval_batch(eval_ctx_,
                                                                *child_brs->skip_,
                                                                child_brs->size_))) {
            LOG_WARN("failed to eval batch", K(i), K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(deduplicate_for_batch(has_last, child_brs))) {
          LOG_WARN("failed to deduplicate batch", K(ret));
        } else {
          got_batch = true;
        }
      }
    } else {
      bool has_last = false;
      first_got_row_ = false;
      if (OB_FAIL(cmp_.init(&eval_ctx_, &MY_SPEC.cmp_funcs_))) {
        LOG_WARN("failed to init cmp", K(ret));
      } else if (OB_FAIL(child_->get_next_batch(batch_size, child_brs))) {
        LOG_WARN("failed to get next batch", K(ret));
      } else if (child_brs->end_ && 0 == child_brs->size_) {
        ret = OB_ITER_END;
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.distinct_exprs_.count(); ++i) {
          if (OB_FAIL(MY_SPEC.distinct_exprs_.at(i)->eval_batch(eval_ctx_,
                                                                *child_brs->skip_,
                                                                child_brs->size_))) {
            LOG_WARN("failed to eval batch", K(i), K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(deduplicate_for_batch(has_last, child_brs))) {
          LOG_WARN("failed to deduplicate batch", K(ret));
        } else {
          got_batch = true;
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    brs_.end_ = true;
    brs_.size_ = 0;
  }
  return ret;
}

int ObMergeDistinctOp::deduplicate_for_batch(bool has_last, const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  int64_t curr_idx = 0;
  bool equal = false;
  if (OB_ISNULL(cmp_.eval_ctx_) || OB_ISNULL(cmp_.cmp_funcs_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(!has_last)) {
    //first batch, locate 1st valid line
    brs_.size_ = child_brs->size_;
    while (curr_idx < child_brs->size_ && child_brs->skip_->at(curr_idx)) {
      brs_.skip_->set(curr_idx);
      ++curr_idx;
    }
    //now we find at least 1 valid row, record its idx and locate next
    int64_t last_idx = curr_idx;
    for (curr_idx = last_idx + 1; OB_SUCC(ret) && curr_idx < child_brs->size_; ++curr_idx) {
      if (child_brs->skip_->at(curr_idx)) {
        brs_.skip_->set(curr_idx);
        continue;
      }
      if (OB_FAIL(cmp_.equal_in_batch(&MY_SPEC.distinct_exprs_, last_idx, curr_idx, equal))) {
        LOG_WARN("failed to cmp row", K(ret));
      } else if (equal) {
        brs_.skip_->set(curr_idx);
      } else {
        last_idx = curr_idx;
      }
    }
    //we found all distinct row in this batch, store last row and return
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_idx(last_idx);
    if (OB_FAIL(last_row_.save_store_row(MY_SPEC.distinct_exprs_, eval_ctx_, 0))) {
      LOG_WARN("failed to save last row");
    }
  } else {
    //we have a stored row may from last batch, first locate a valid row
    brs_.size_ = child_brs->size_;
    equal = true;
    while (curr_idx < child_brs->size_ && OB_SUCC(ret) && equal) {
      equal = child_brs->skip_->at(curr_idx);
      if (!equal && OB_FAIL(cmp_.equal_in_batch(&MY_SPEC.distinct_exprs_, last_row_.store_row_, curr_idx, equal))) {
        LOG_WARN("failed to cmp row", K(ret));
      }
      if (OB_SUCC(ret) && equal) {
        brs_.skip_->set(curr_idx);
        ++curr_idx;
      }
    }
    //now we have got 1 row can be return or all rows are skiped
    if (curr_idx < child_brs->size_) {
      int64_t last_idx = curr_idx;
      for (curr_idx = last_idx + 1; OB_SUCC(ret) && curr_idx < child_brs->size_; ++curr_idx) {
        if (child_brs->skip_->at(curr_idx)) {
          brs_.skip_->set(curr_idx);
          continue;
        }
        if (OB_FAIL(cmp_.equal_in_batch(&MY_SPEC.distinct_exprs_, last_idx, curr_idx, equal))) {
          LOG_WARN("failed to cmp row", K(ret));
        } else if (equal) {
          brs_.skip_->set(curr_idx);
        } else {
          last_idx = curr_idx;
        }
      }
      //we found all distinct row in this batch, store last row and return
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
      batch_info_guard.set_batch_idx(last_idx);
      if (OB_FAIL(last_row_.save_store_row(MY_SPEC.distinct_exprs_, eval_ctx_, 0))) {
        LOG_WARN("failed to save last row");
      }
    }

  }
  return ret;
}

void ObMergeDistinctOp::destroy()
{
  last_row_.reset();
  alloc_.reset();
  ObOperator::destroy();
}

int ObMergeDistinctOp::inner_close()
{
  int ret = OB_SUCCESS;
  last_row_.reset();
  alloc_.reset();
  if (OB_FAIL(ObOperator::inner_close())) {
  }
  return ret;
}

int ObMergeDistinctOp::Compare::init(ObEvalCtx *eval_ctx, const ObIArray<ObCmpFunc> *cmp_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx) || OB_ISNULL(cmp_funcs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to init compare funcs", K(ret));
  } else {
    eval_ctx_ = eval_ctx;
    cmp_funcs_ = cmp_funcs;
    ret_code_ = OB_SUCCESS;
  }
  return ret;
}

/**
 * 这里需要注意一种特殊场景，就是distinct 1如何处理
 * 现在处理逻辑是：last_row为nullptr，即r为null，然后不比较，同时认为l的列为0
 **/
int ObMergeDistinctOp::Compare::equal(
  const ObIArray<ObExpr*> *l,
  const ObChunkDatumStore::StoredRow *r,
  bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  if (OB_ISNULL(l) || OB_ISNULL(eval_ctx_) || OB_ISNULL(cmp_funcs_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(r)) {
    if (0 != l->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: it must be distinct const", K(ret));
    } else {
      // 表示是distinct 常量，所以没有distinct列，则永远相等
      // case: select distinct 1 from t1;
      equal = true;
    }
  } else {
    const ObDatum *rcells = r->cells();
    int cmp = 0;
    ObDatum *other_datum = nullptr;
    for (int64_t i = 0; 0 == cmp && i < cmp_funcs_->count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(l->at(i)->eval(*eval_ctx_, other_datum))) {
        LOG_WARN("failed to get expr value", K(ret), K(i));
      } else if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*other_datum, rcells[i], cmp))) {
        LOG_WARN("do cmp failed", K(ret), K(i));
      }
    }
    equal = OB_SUCC(ret) ? (0 == cmp) : false;
  }
  return ret;
}

int ObMergeDistinctOp::Compare::equal_in_batch(const common::ObIArray<ObExpr*> *set_exprs,
                                                const int64_t last_idx,
                                                const int64_t curr_idx,
                                                bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  if (0 == set_exprs->count()) {
    // 表示是distinct 常量，所以没有distinct列，则永远相等
    // case: select distinct 1 from t1;
    equal = true;
  } else {
    int cmp = 0;
    ObDatum *r_datum = nullptr;
    ObDatum *l_datum = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < cmp_funcs_->count(); i++) {
      l_datum = &set_exprs->at(i)->locate_expr_datum(*eval_ctx_, last_idx);
      r_datum = &set_exprs->at(i)->locate_expr_datum(*eval_ctx_, curr_idx);
      if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*l_datum, *r_datum, cmp))) {
        LOG_WARN("do cmp failed", K(ret), K(i), KPC(l_datum), KPC(r_datum));
      }
    }
    equal = (0 == cmp);
  }
  return ret;
}

int ObMergeDistinctOp::Compare::equal_in_batch(const common::ObIArray<ObExpr*> *set_exprs,
                                                const ObChunkDatumStore::StoredRow *r,
                                                const int64_t curr_idx,
                                                bool &equal)
{
  int ret = OB_SUCCESS;
  equal = false;
  if (0 == set_exprs->count()) {
    // 表示是distinct 常量，所以没有distinct列，则永远相等
    // case: select distinct 1 from t1;
    equal = true;
  } else {
    int cmp = 0;
    const ObDatum *r_datum = nullptr;
    const ObDatum *l_datum = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && 0 == cmp && i < cmp_funcs_->count(); i++) {
      l_datum = &set_exprs->at(i)->locate_expr_datum(*eval_ctx_, curr_idx);
      r_datum = &r->cells()[i];
      if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*l_datum, *r_datum, cmp))) {
        LOG_WARN("do cmp failed", K(ret), K(i), KPC(l_datum), KPC(r_datum));
      }
    }
    equal = (0 == cmp);
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
