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

#include "sql/engine/set/ob_merge_set_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObMergeSetSpec::ObMergeSetSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObSetSpec(alloc, type)
{
}

OB_SERIALIZE_MEMBER((ObMergeSetSpec, ObSetSpec));

ObMergeSetOp::ObMergeSetOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    alloc_(ObModIds::OB_SQL_MERGE_GROUPBY,
      OB_MALLOC_NORMAL_BLOCK_SIZE, exec_ctx.get_my_session()->get_effective_tenant_id(), ObCtxIds::WORK_AREA),
    last_row_(alloc_),
    cmp_(),
    need_skip_init_row_(false),
    last_row_idx_(-1),
    use_last_row_(false)
{}

int ObMergeSetOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_) || OB_ISNULL(right_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: left or right is null", K(ret), K(left_), K(right_));
  } else {
    const ObMergeSetSpec &spec = static_cast<const ObMergeSetSpec&>(get_spec());
    if (OB_FAIL(cmp_.init(&spec.sort_collations_, &spec.sort_cmp_funs_))) {
      LOG_WARN("failed to init compare function", K(ret));
    }
  }
  return ret;
}

int ObMergeSetOp::inner_close()
{
  return ObOperator::inner_close();
}

int ObMergeSetOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  last_row_.reset();
  alloc_.reset();
  need_skip_init_row_ = false;
  last_row_idx_ = -1;
  if (OB_FAIL(ObOperator::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

void ObMergeSetOp::destroy()
{
  last_row_.reset();
  alloc_.reset();
  ObOperator::destroy();
}

/**
 * 拿数据时，由child_op来驱动，如果是left，则拿的就是left的数据，则后续就是需要用left来获取output的ObExprs
 * 相反，如果是right op，则拿的就是right的数据，则后续需要用right来获取output的ObExprs
 **/
int ObMergeSetOp::do_strict_distinct(
  ObOperator &child_op,
  const ObIArray<ObExpr*> &compare_row,
  const ObIArray<ObExpr*> *&output_row,
  int &cmp)
{
  int ret = OB_SUCCESS;
  bool is_break = false;
  while (OB_SUCC(ret) && !is_break) {
    if (OB_FAIL(child_op.get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(cmp_(
        compare_row, child_op.get_spec().output_, eval_ctx_, cmp))) {
      LOG_WARN("strict compare with last_row failed", K(ret), K(compare_row));
    } else if (0 != cmp) {
      is_break = true;
    }
  }
  if (OB_SUCC(ret)) {
    output_row = &child_op.get_spec().output_;
  }
  return ret;
}

int ObMergeSetOp::convert_row(
  const ObChunkDatumStore::StoredRow *sr, const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr) || sr->cnt_ != exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: store row is null or column count is not match", K(ret));
  } else {
    for (uint32_t i = 0; i < sr->cnt_; ++i) {
      exprs.at(i)->locate_expr_datum(eval_ctx_) = sr->cells()[i];
      exprs.at(i)->set_evaluated_projected(eval_ctx_);
    }
    LOG_DEBUG("trace convert row", K(ret), K(sr->cnt_), K(ROWEXPR2STR(eval_ctx_, exprs)));
  }
  return ret;
}

int ObMergeSetOp::convert_row(const common::ObIArray<ObExpr*> &src_exprs,
                              const common::ObIArray<ObExpr*> &dst_exprs,
                              const int64_t src_idx,
                              const int64_t dst_idx)
{
  int ret = OB_SUCCESS;
  if (dst_exprs.count() != src_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: exprs is not match", K(ret), K(src_exprs.count()),
      K(dst_exprs.count()));
  } else {
    ObDatum *src_datum = nullptr;
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_idx(src_idx);
    for (uint32_t i = 0; i < dst_exprs.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(src_exprs.at(i)->eval(eval_ctx_, src_datum))) {
        LOG_WARN("failed to eval expr", K(ret), K(i));
      } else {
        dst_exprs.at(i)->locate_expr_datum(eval_ctx_, dst_idx) = *src_datum;
        dst_exprs.at(i)->set_evaluated_flag(eval_ctx_);
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG("trace convert row", K(ret), K(ROWEXPR2STR(eval_ctx_, src_exprs)));
    }
  }
  return ret;
}

int ObMergeSetOp::Compare::init(
  const common::ObIArray<ObSortFieldCollation> *sort_collations,
  const common::ObIArray<common::ObCmpFunc> *cmp_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sort_collations || nullptr == cmp_funcs)
      || sort_collations->count() != cmp_funcs->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compare info is null", K(ret), K(sort_collations), K(cmp_funcs));
  } else {
    sort_collations_ = sort_collations;
    cmp_funcs_ = cmp_funcs;
  }
  return ret;
}

int ObMergeSetOp::Compare::operator()(
  const ObChunkDatumStore::StoredRow &l,
  const common::ObIArray<ObExpr*> &r,
  ObEvalCtx &eval_ctx,
  int &cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObDatum *lcells = l.cells();
  ObDatum *r_datum = nullptr;
  for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); i++) {
    int64_t idx = sort_collations_->at(i).field_idx_;
    if (OB_FAIL(r.at(idx)->eval(eval_ctx, r_datum))) {
      LOG_WARN("failed to get expr value", K(ret), K(i));
    } else if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(lcells[idx], *r_datum, cmp))) {
      LOG_WARN("failed to compare", K(ret), K(i));
    } else {
      if (0 != cmp) {
        cmp = sort_collations_->at(i).is_ascending_ ? cmp : -cmp;
        break;
      }
    }
  }
  return ret;
}

int ObMergeSetOp::Compare::operator()(
  const common::ObIArray<ObExpr*> &l,
  const common::ObIArray<ObExpr*> &r,
  ObEvalCtx &eval_ctx,
  int &cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  ObDatum *l_datum = nullptr;
  ObDatum *r_datum = nullptr;
  for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); i++) {
    int64_t idx = sort_collations_->at(i).field_idx_;
    if (OB_FAIL(l.at(idx)->eval(eval_ctx, l_datum))) {
      LOG_WARN("failed to get expr value", K(ret), K(i));
    } else if (OB_FAIL(r.at(idx)->eval(eval_ctx, r_datum))) {
    } else if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*l_datum, *r_datum, cmp))) {
      LOG_WARN("failed to compare", K(ret), K(i));
    } else {
      LOG_DEBUG("debug compare merge set op", K(EXPR2STR(eval_ctx, *l.at(idx))),
        K(EXPR2STR(eval_ctx, *r.at(idx))), K(cmp));
      if (0 != cmp) {
        cmp = sort_collations_->at(i).is_ascending_ ? cmp : -cmp;
        break;
      }
    }
  }
  return ret;
}

int ObMergeSetOp::Compare::operator() (const common::ObIArray<ObExpr*> &l,
                                       const common::ObIArray<ObExpr*> &r,
                                       const int64_t l_idx,
                                       const int64_t r_idx,
                                       ObEvalCtx &eval_ctx,
                                       int &cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  ObDatum *l_datum = nullptr;
  ObDatum *r_datum = nullptr;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
  for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); i++) {
    int64_t idx = sort_collations_->at(i).field_idx_;
    batch_info_guard.set_batch_idx(l_idx);
    if (OB_FAIL(l.at(idx)->eval(eval_ctx, l_datum))) {
      LOG_WARN("failed to get expr value", K(ret), K(i));
    } else if (FALSE_IT(batch_info_guard.set_batch_idx(r_idx))) {
    } else if (OB_FAIL(r.at(idx)->eval(eval_ctx, r_datum))) {
      LOG_WARN("failed to get expr value", K(ret), K(i));
    } else if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*l_datum, *r_datum, cmp))) {
      LOG_WARN("failed to compare", K(ret), K(i));
    } else if (0 != cmp) {
      cmp = sort_collations_->at(i).is_ascending_ ? cmp : -cmp;
      break;
    }
  }
  return ret;
}

int ObMergeSetOp::Compare::operator() (const ObChunkDatumStore::StoredRow &l,
                                       const common::ObIArray<ObExpr*> &r,
                                       const int64_t r_idx,
                                       ObEvalCtx &eval_ctx,
                                       int &cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObDatum *l_datum = nullptr;
  ObDatum *r_datum = nullptr;
  const ObDatum *l_cells = l.cells();
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
  batch_info_guard.set_batch_idx(r_idx);
  for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); i++) {
    int64_t idx = sort_collations_->at(i).field_idx_;
    l_datum = &l_cells[idx];
    batch_info_guard.set_batch_idx(r_idx);
    if (OB_FAIL(r.at(idx)->eval(eval_ctx, r_datum))) {
    } else if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*l_datum, *r_datum, cmp))) {
      LOG_WARN("failed to compare", K(ret), K(i));
    } else if (0 != cmp) {
      cmp = sort_collations_->at(i).is_ascending_ ? cmp : -cmp;
      break;
    }
  }
  return ret;
}


int ObMergeSetOp::convert_batch(const common::ObIArray<ObExpr*> &src_exprs,
                                const common::ObIArray<ObExpr*> &dst_exprs,
                                ObBatchRows &brs,
                                bool is_union_all)
{
  int ret = OB_SUCCESS;
  if (0 == brs.size_) {
  } else if (dst_exprs.count() != src_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: exprs is not match", K(ret), K(src_exprs.count()),
      K(dst_exprs.count()));
  } else {
    if (!is_union_all) {
      // reorg to dense
      int64_t size = 0;
      for (int64_t j = 0; j < brs.size_; ++j) {
        if (brs.skip_->at(j)) {
          continue;
        }
        for (uint32_t i = 0; i < dst_exprs.count(); ++i) {
          ObDatum &src = src_exprs.at(i)->locate_expr_datum(eval_ctx_, j);
          dst_exprs.at(i)->locate_expr_datum(eval_ctx_, size) = src;
        }
        ++size;
      } // end for
      for (uint32_t i = 0; i < dst_exprs.count(); ++i) {
        dst_exprs.at(i)->set_evaluated_projected(eval_ctx_);
      }
      brs.skip_->reset(brs.size_);
      brs.size_ = size;
    } else {
      for (uint32_t i = 0; i < dst_exprs.count(); ++i) {
        ObDatum *dst = dst_exprs.at(i)->locate_batch_datums(eval_ctx_);
        if (!src_exprs.at(i)->is_batch_result()) {
          ObDatum &src = src_exprs.at(i)->locate_expr_datum(eval_ctx_, 0);
          for (int64_t j = 0; j < brs.size_; ++j) {
            if (brs.skip_->at(j)) {
              continue;
            }
            dst_exprs.at(i)->locate_expr_datum(eval_ctx_, j) = src;
          }
        } else {
          ObDatum *src = src_exprs.at(i)->locate_batch_datums(eval_ctx_);
          MEMCPY(dst, src, sizeof(ObDatum) * brs.size_);
        }
        dst_exprs.at(i)->set_evaluated_projected(eval_ctx_);
      } // end for
    }
  }
  return ret;
}

int ObMergeSetOp::locate_next_left_inside(ObOperator &child_op,
                                          const int64_t last_idx,
                                          const ObBatchRows &row_brs,
                                          int64_t &curr_idx,
                                          bool &is_first)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_first)) {
    //return 1st valid row idx
    is_first = false;
    curr_idx = 0;
    for (; curr_idx < row_brs.size_; ++curr_idx) {
      if (!row_brs.skip_->at(curr_idx)) {
        break;
      }
    }
    OB_ASSERT(curr_idx < row_brs.size_);
  } else {
    bool got_row = false;
    int cmp = 0;
    while (OB_SUCC(ret) && !got_row) {
      //try move to next row in batch, if cant, return iter_end
      for (; curr_idx < row_brs.size_; ++curr_idx) {
        if (!row_brs.skip_->at(curr_idx)) {
          break;
        }
      }
      if (curr_idx == row_brs.size_) {
        ret = OB_ITER_END;
      } else if (OB_UNLIKELY(use_last_row_ && nullptr == last_row_.store_row_)
                || OB_LIKELY(!use_last_row_ && last_idx < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get wrong last idx", K(ret), K(use_last_row_), KP(last_row_.store_row_), K(last_idx));
      } else if (use_last_row_
                && OB_FAIL(cmp_(*last_row_.store_row_, child_op.get_spec().output_,
                                curr_idx, eval_ctx_, cmp))) {
        LOG_WARN("failed to compare row", K(ret));
      } else if (!use_last_row_
                 && OB_FAIL(cmp_(child_op.get_spec().output_, child_op.get_spec().output_,
                                 last_idx, curr_idx, eval_ctx_, cmp))) {
        LOG_WARN("failed to compare row", K(ret));
      } else if (0 != cmp) {
        got_row = true;
      } else {
        ++curr_idx;
      }
    }
  }
  return ret;
}

int ObMergeSetOp::locate_next_right(ObOperator &child_op,
                                    const int64_t batch_size,
                                    const ObBatchRows *&child_brs,
                                    int64_t &curr_idx)
{
  int ret = OB_SUCCESS;
  //first batch
  if (OB_ISNULL(child_brs) || child_brs->size_ == curr_idx/*last row in batch*/) {
    if (OB_FAIL(child_op.get_next_batch(batch_size, child_brs))) {
      LOG_WARN("failed to get next batch", K(ret));
    } else if (child_brs->end_ && 0 == child_brs->size_) {
      ret = OB_ITER_END;
    } else {
      curr_idx = 0;
      //locate 1st valid row and return, we're sure at least 1 row will be got
      for (; curr_idx < child_brs->size_; ++curr_idx) {
        if (!child_brs->skip_->at(curr_idx)) {
          break;
        }
      }
      OB_ASSERT(curr_idx < child_brs->size_);
    }
  } else {
    //try move to next row in batch, if cant, get next batch
    for (; curr_idx < child_brs->size_; ++curr_idx) {
      if (!child_brs->skip_->at(curr_idx)) {
        break;
      }
    }
    if (curr_idx == child_brs->size_) {
      if (OB_FAIL(child_op.get_next_batch(batch_size, child_brs))) {
        LOG_WARN("failed to get next batch", K(ret));
      } else if (child_brs->end_ && 0 == child_brs->size_) {
        ret = OB_ITER_END;
      } else {
        curr_idx = 0;
        //locate 1st valid row and return, we're sure at least 1 row will be got
        for (; curr_idx < child_brs->size_; ++curr_idx) {
          if (!child_brs->skip_->at(curr_idx)) {
            break;
          }
        }
        OB_ASSERT(curr_idx < child_brs->size_);
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
