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

#include "sql/engine/set/ob_merge_set_vec_op.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObMergeSetVecSpec::ObMergeSetVecSpec(ObIAllocator &alloc, const ObPhyOperatorType type)
: ObSetSpec(alloc, type)
{
}

OB_SERIALIZE_MEMBER((ObMergeSetVecSpec, ObSetSpec));

ObMergeSetVecOp::ObMergeSetVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(exec_ctx, spec, input),
    alloc_(ObModIds::OB_SQL_MERGE_GROUPBY,
      OB_MALLOC_NORMAL_BLOCK_SIZE, exec_ctx.get_my_session()->get_effective_tenant_id(), ObCtxIds::WORK_AREA),
    cmp_(),
    need_skip_init_row_(false),
    last_row_idx_(-1),
    use_last_row_(false),
    last_row_(alloc_)
{}

int ObMergeSetVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_) || OB_ISNULL(right_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: left or right is null", K(ret), K(left_), K(right_));
  } else {
    const ObMergeSetVecSpec &spec = static_cast<const ObMergeSetVecSpec&>(get_spec());
    if (OB_FAIL(cmp_.init(&spec.sort_collations_, &spec.sort_cmp_funs_))) {
      LOG_WARN("failed to init compare function", K(ret));
    }
  }
  return ret;
}

int ObMergeSetVecOp::inner_close()
{
  return ObOperator::inner_close();
}

int ObMergeSetVecOp::inner_rescan()
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

void ObMergeSetVecOp::destroy()
{
  last_row_.reset();
  alloc_.reset();
  ObOperator::destroy();
}

int ObMergeSetVecOp::Compare::init(
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

int ObMergeSetVecOp::Compare::operator() (const common::ObIArray<ObExpr*> &l,
                                       const common::ObIArray<ObExpr*> &r,
                                       const int64_t l_idx,
                                       const int64_t r_idx,
                                       ObEvalCtx &eval_ctx,
                                       int &cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); i++) {
    int64_t idx = sort_collations_->at(i).field_idx_;
    ObIVector *lvec = l.at(idx)->get_vector(eval_ctx);
    ObIVector *rvec = r.at(idx)->get_vector(eval_ctx);
    bool r_isnull = rvec->is_null(r_idx);
    const char *r_v = NULL;
    ObLength r_len = 0;
    if (sort_collations_->at(i).null_pos_ == NULL_LAST) {
      if (OB_FAIL(lvec->null_last_cmp(*l.at(idx), l_idx, r_isnull, rvec->get_payload(r_idx),
          rvec->get_length(r_idx), cmp))) {
        LOG_WARN("failed to compare", K(ret), K(idx));
      } else if (0 != cmp) {
        cmp = sort_collations_->at(i).is_ascending_ ? cmp : -cmp;
        break;
      }
    } else if (sort_collations_->at(i).null_pos_ == NULL_FIRST) {
      if (OB_FAIL(lvec->null_first_cmp(*l.at(idx), l_idx, r_isnull, rvec->get_payload(r_idx),
          rvec->get_length(r_idx), cmp))) {
        LOG_WARN("failed to compare", K(ret), K(idx));
      } else if (0 != cmp) {
        cmp = sort_collations_->at(i).is_ascending_ ? cmp : -cmp;
        break;
      }
    }
  }
  return ret;
}

int ObMergeSetVecOp::Compare::operator() (const ObCompactRow &l_store_rows,
                                       const RowMeta &meta,
                                       const common::ObIArray<ObExpr*> &r,
                                       const int64_t r_idx,
                                       ObEvalCtx &eval_ctx,
                                       int &cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  ObDatum *r_datum = nullptr;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx);
  batch_info_guard.set_batch_idx(r_idx);
  for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); i++) {
    int64_t idx = sort_collations_->at(i).field_idx_;
    ObIVector *rvec = r.at(idx)->get_vector(eval_ctx);
    bool l_isnull = l_store_rows.is_null(idx);
    const char *l_v = NULL;
    ObLength l_len = 0;
    l_store_rows.get_cell_payload(meta, idx, l_v, l_len);
    if (sort_collations_->at(i).null_pos_ == NULL_LAST) {
      if (OB_FAIL(rvec->null_last_cmp(*r.at(idx), r_idx, l_isnull, l_v, l_len, cmp))) {
        LOG_WARN("failed to compare", K(ret), K(idx));
      } else if (0 != cmp) {
        // here cmp right to left, so cmp result is different with the result of (cmp left to right)
        // the sort_collations meaning is to decide which is cand op?
        cmp = sort_collations_->at(i).is_ascending_ ? -cmp : cmp;
        break;
      }
    } else if (sort_collations_->at(i).null_pos_ == NULL_FIRST) {
      if (OB_FAIL(rvec->null_first_cmp(*r.at(idx), r_idx, l_isnull, l_v, l_len, cmp))) {
        LOG_WARN("failed to compare", K(ret), K(idx));
      } else if (0 != cmp) {
        // here cmp right to left, so cmp result is different with the result of (cmp left to right)
        // the sort_collations meaning is to decide which is cand op?
        cmp = sort_collations_->at(i).is_ascending_ ? -cmp : cmp;
        break;
      }
    }

  }
  return ret;
}

int ObMergeSetVecOp::convert_batch(const common::ObIArray<ObExpr*> &src_exprs,
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
    // for merge intersect and merge except and union_all,
    // only set the expr header, reuse child_op data
    for (int i = 0; OB_SUCC(ret) && i < dst_exprs.count(); i++) {
      ObExpr *from = src_exprs.at(i);
      ObExpr *to = dst_exprs.at(i);
      if (OB_FAIL(from->eval_vector(eval_ctx_, brs_))) {
        LOG_WARN("eval batch failed", K(ret));
      } else {
        VectorHeader &from_vec_header = from->get_vector_header(eval_ctx_);
        VectorHeader &to_vec_header = to->get_vector_header(eval_ctx_);
        if (from_vec_header.format_ == VEC_UNIFORM_CONST) {
          ObDatum *from_datum =
            static_cast<ObUniformBase *>(from->get_vector(eval_ctx_))->get_datums();
          OZ(to->init_vector(eval_ctx_, VEC_UNIFORM, brs_.size_));
          ObUniformBase *to_vec = static_cast<ObUniformBase *>(to->get_vector(eval_ctx_));
          ObDatum *to_datums = to_vec->get_datums();
          for (int64_t j = 0; j < brs_.size_ && OB_SUCC(ret); j++) {
            to_datums[j] = *from_datum;
          }
        } else if (from_vec_header.format_ == VEC_UNIFORM) {
          ObUniformBase *uni_vec = static_cast<ObUniformBase *>(from->get_vector(eval_ctx_));
          ObDatum *src = uni_vec->get_datums();
          ObDatum *dst = to->locate_batch_datums(eval_ctx_);
          if (src != dst) {
            MEMCPY(dst, src, brs_.size_ * sizeof(ObDatum));
          }
          OZ(to->init_vector(eval_ctx_, VEC_UNIFORM, brs_.size_));
        } else if (OB_FAIL(to_vec_header.assign(from_vec_header))) {
          LOG_WARN("assign vector header failed", K(ret));
        }
        // init eval info
        if (OB_SUCC(ret)) {
          const ObEvalInfo &from_info = from->get_eval_info(eval_ctx_);
          ObEvalInfo &to_info = to->get_eval_info(eval_ctx_);
          to_info = from_info;
          to_info.set_projected(true);
          to_info.cnt_ = brs_.size_;
        }
      }
    }
  }
  return ret;
}

int ObMergeSetVecOp::locate_next_left_inside(ObOperator &child_op,
                                          const int64_t last_idx,
                                          const ObBatchRows &row_brs,
                                          int64_t &curr_idx)
{
  int ret = OB_SUCCESS;
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
    } else {
      got_row = true;
    }
  }
  return ret;
}

int ObMergeSetVecOp::locate_next_right(ObOperator &child_op,
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

int ObMergeSetVecOp::distinct_for_batch(ObOperator &child_op, const ObBatchRows &row_brs,
                                        bool &is_first,
                                        const common::ObIArray<ObExpr*> &compare_expr,
                                        const int64_t compare_idx,
                                        ObBatchRows &result_brs)
{
  int ret = OB_SUCCESS;
  int64_t last_idx = -1;
  int64_t curr_idx = 0;
  int64_t first_active_idx = -1;
  int64_t last_cmp_idx = first_active_idx;
  int64_t child_skip_cnt = row_brs.skip_->accumulate_bit_cnt(row_brs.size_);
  int last_row_cmp_ret = -1;

  // 1.get first_no_skip
  result_brs.size_ = row_brs.size_;
  result_brs.skip_->set_all(row_brs.size_); // default skip
  if (!row_brs.all_rows_active_) {
    while (curr_idx < row_brs.size_) {
      if (!row_brs.skip_->at(curr_idx)) {
        first_active_idx = curr_idx;
        break;
      }
      curr_idx++;
    }
  } else {
    first_active_idx = 0;
  }

  // 2. deduplicate last row
  if (!is_first) {
    if (compare_idx < 0) {
      if (OB_FAIL(cmp_(*last_row_.compact_row_, *last_row_.ref_row_meta_,
                      child_op.get_spec().output_,
                      curr_idx, eval_ctx_, last_row_cmp_ret))) {
        LOG_WARN("failed to compare row", K(ret));
      }
    } else if (compare_idx >= 0) {
      if (OB_FAIL(cmp_(compare_expr, child_op.get_spec().output_,
                      compare_idx, curr_idx, eval_ctx_, last_row_cmp_ret))) {
        LOG_WARN("failed to compare row", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 == last_row_cmp_ret) {
      // skip
    } else {
      result_brs.skip_->unset(first_active_idx);
      last_idx = first_active_idx;
    }
  } else {
    // no last, out the first_active_idx row
    result_brs.skip_->unset(first_active_idx);
    last_idx = first_active_idx;
    is_first = false;
  }

  // 3.cmp curr_idx and last_idx col by col
  if (OB_FAIL(ret)) {
  } else {
    // 3.cmp curr_idx and last_idx col by col
    for (int col_idx = 0; col_idx < child_op.get_spec().output_.count() && OB_SUCC(ret); col_idx++){
      bool curr_out = false;
      int cmp_ret;
      ObIVector *vec = child_op.get_spec().output_.at(col_idx)->get_vector(eval_ctx_);
      last_cmp_idx = first_active_idx;
      curr_idx = first_active_idx + 1;
      const sql::ObExpr &col_expr = *child_op.get_spec().output_.at(col_idx);
      switch (vec->get_format()) {
        case VEC_FIXED : {
          // 对big_int进行特化
          if (ob_is_integer_type(child_op.get_spec().output_.at(col_idx)->datum_meta_.type_)) {
            ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> *fixed_vec =
              static_cast<ObFixedLengthVector<int64_t, VectorBasicOp<VEC_TC_INTEGER>> *> (vec);
            if (OB_FAIL(compare_in_column_with_format<FixedLengthVectorBigInt>(fixed_vec, &row_brs,
                first_active_idx, col_idx, last_idx, col_expr, result_brs))) {
              LOG_WARN("compare in column with format failed", K(ret));
            }
          } else {
            if (OB_FAIL(compare_in_column_with_format<ObIVector>(vec, &row_brs, first_active_idx,
                col_idx, last_idx, col_expr, result_brs))) {
              LOG_WARN("compare in column with format failed", K(ret));
            }
          }
          break;
        }
        case VEC_DISCRETE : {
          if (ob_is_string_tc(child_op.get_spec().output_.at(col_idx)->datum_meta_.type_)) {
            ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> *string_vec =
              static_cast<ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> *> (vec);
            if (OB_FAIL(compare_in_column_with_format<DiscreteVectorString>(string_vec, &row_brs,
                first_active_idx, col_idx, last_idx, col_expr, result_brs))) {
              LOG_WARN("compare in column with format failed", K(ret));
            }
          } else {
            if (OB_FAIL(compare_in_column_with_format<ObIVector>(vec, &row_brs, first_active_idx,
                col_idx, last_idx, col_expr, result_brs))) {
              LOG_WARN("compare in column with format failed", K(ret));
            }
          }
          break;
        }
        default : {
          if (OB_FAIL(compare_in_column_with_format<ObIVector>(vec, &row_brs, first_active_idx,
              col_idx, last_idx, col_expr, result_brs))) {
            LOG_WARN("compare in column with format failed", K(ret));
          }
        }
      }
      if (col_idx < child_op.get_spec().output_.count() - 1 &&
          result_brs.skip_->accumulate_bit_cnt(row_brs.size_) <= child_skip_cnt + (last_row_cmp_ret == 0 ? 1 : 0)) {
        break;
      }
    }
  }
  return ret;
}

template<typename InputVec, bool ALL_ROWS_ACTIVE, bool FIRST_COL, bool HAS_NULL>
int ObMergeSetVecOp::compare_in_column(InputVec * vec, int64_t first_active_idx,
                                      const ObBatchRows *child_brs, int64_t &last_idx,
                                      const sql::ObExpr &col_expr, ObBatchRows &result_brs)
{
  int ret = OB_SUCCESS;
  int null_type = 0;
  int cmp_ret = 0;
  int64_t last_cmp_idx = first_active_idx;
  int64_t curr_idx = first_active_idx + 1;
  for (; curr_idx < child_brs->size_ && OB_SUCC(ret) ; curr_idx++) {
    if (ALL_ROWS_ACTIVE && FIRST_COL) { // skip and out are false, do not continue, need compare
    } else if (ALL_ROWS_ACTIVE && !FIRST_COL) { // skip is false, judge out
      if (!result_brs.skip_->at(curr_idx)) {
        // curr_idx row is out, do not need compare, but need update last_cmp_idx
        last_cmp_idx = curr_idx;
        continue;
      }
    } else if (!ALL_ROWS_ACTIVE && FIRST_COL) { // out is false, judge skip
      if (child_brs->skip_->at(curr_idx)) { continue; } // skip row
    } else if (!ALL_ROWS_ACTIVE && !FIRST_COL) {
      if (child_brs->skip_->at(curr_idx)) { continue; }
      if (!result_brs.skip_->at(curr_idx)) {
        last_cmp_idx = curr_idx;
        continue;
      }
    }

    // curr_idx row is neither skip nor out, cmp with last_cmp_idx
    if (HAS_NULL) {
      if (OB_FAIL(vec->null_last_cmp(col_expr, curr_idx, vec->is_null(last_cmp_idx),
                                    vec->get_payload(last_cmp_idx), vec->get_length(last_cmp_idx), cmp_ret))) {
        LOG_WARN("null_last_cmp failed", K(curr_idx), K(last_cmp_idx), K(cmp_ret), K(ret));
      }
    } else {
      if (OB_FAIL(vec->no_null_cmp(col_expr, curr_idx, last_cmp_idx, cmp_ret))) {
        LOG_WARN("no_null_cmp failed", K(curr_idx), K(last_cmp_idx), K(cmp_ret), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (cmp_ret == 0) { // equal, do nothing
    } else {
      result_brs.skip_->unset(curr_idx);
      if (last_idx < curr_idx) {
        last_idx = curr_idx;
      }
    }
    last_cmp_idx = curr_idx;
  }

  return ret;
}

template<typename InputVec>
int ObMergeSetVecOp::compare_in_column_with_format(InputVec *vec, const ObBatchRows *child_brs,
    int64_t first_active_idx, int64_t col_idx, int64_t &last_idx, const sql::ObExpr &col_expr,
    ObBatchRows &result_brs)
{
  int ret = OB_SUCCESS;
  if (vec->has_null()) {
    if (child_brs->all_rows_active_ && col_idx == 0) {
      if (OB_FAIL((compare_in_column<InputVec, true, true, true>)(vec, first_active_idx,
          child_brs, last_idx, col_expr, result_brs))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (child_brs->all_rows_active_ && col_idx != 0) {
      if (OB_FAIL((compare_in_column<InputVec, true, false, true>)(vec, first_active_idx,
          child_brs, last_idx, col_expr, result_brs))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (!child_brs->all_rows_active_ && col_idx == 0) {
      if (OB_FAIL((compare_in_column<InputVec, false, true, true>)(vec, first_active_idx,
          child_brs, last_idx, col_expr, result_brs))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (!child_brs->all_rows_active_ && col_idx != 0) {
      if (OB_FAIL((compare_in_column<InputVec, false, false, true>)(vec, first_active_idx,
          child_brs, last_idx, col_expr, result_brs))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    }
  } else {
    if (child_brs->all_rows_active_ && col_idx == 0) {
      if (OB_FAIL((compare_in_column<InputVec, true, true, false>)(vec, first_active_idx,
          child_brs, last_idx, col_expr, result_brs))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (child_brs->all_rows_active_ && col_idx != 0) {
      if (OB_FAIL((compare_in_column<InputVec, true, false, false>)(vec, first_active_idx,
          child_brs, last_idx, col_expr, result_brs))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (!child_brs->all_rows_active_ && col_idx == 0) {
      if (OB_FAIL((compare_in_column<InputVec, false, true, false>)(vec, first_active_idx,
          child_brs, last_idx, col_expr, result_brs))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    } else if (!child_brs->all_rows_active_ && col_idx != 0) {
      if (OB_FAIL((compare_in_column<InputVec, false, false, false>)(vec, first_active_idx,
          child_brs, last_idx, col_expr, result_brs))) {
        LOG_WARN("failed to cmp compare in column", K(ret), K(col_idx));
      }
    }
  }
  return ret;
}


} // end namespace sql
} // end namespace oceanbase
