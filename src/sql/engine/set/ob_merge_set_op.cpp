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

namespace oceanbase {
using namespace common;
namespace sql {

ObMergeSetSpec::ObMergeSetSpec(ObIAllocator& alloc, const ObPhyOperatorType type) : ObSetSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObMergeSetSpec, ObSetSpec));

ObMergeSetOp::ObMergeSetOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      alloc_(ObModIds::OB_SQL_MERGE_GROUPBY, OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID, ObCtxIds::WORK_AREA),
      last_row_(alloc_),
      cmp_(),
      need_skip_init_row_(false)
{}

int ObMergeSetOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_) || OB_ISNULL(right_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: left or right is null", K(ret), K(left_), K(right_));
  } else {
    const ObMergeSetSpec& spec = static_cast<const ObMergeSetSpec&>(get_spec());
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

int ObMergeSetOp::rescan()
{
  int ret = OB_SUCCESS;
  last_row_.reset();
  alloc_.reset();
  need_skip_init_row_ = false;
  if (OB_FAIL(ObOperator::rescan())) {
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

int ObMergeSetOp::do_strict_distinct(
    ObOperator& child_op, const ObIArray<ObExpr*>& compare_row, const ObIArray<ObExpr*>*& output_row, int& cmp)
{
  int ret = OB_SUCCESS;
  bool is_break = false;
  while (OB_SUCC(ret) && !is_break) {
    if (OB_FAIL(child_op.get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(cmp_(compare_row, child_op.get_spec().output_, eval_ctx_, cmp))) {
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

int ObMergeSetOp::convert_row(const ObChunkDatumStore::StoredRow* sr, const common::ObIArray<ObExpr*>& exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sr) || sr->cnt_ != exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: store row is null or column count is not match", K(ret));
  } else {
    for (uint32_t i = 0; i < sr->cnt_; ++i) {
      exprs.at(i)->locate_expr_datum(eval_ctx_) = sr->cells()[i];
      exprs.at(i)->get_eval_info(eval_ctx_).evaluated_ = true;
    }
    LOG_DEBUG("trace convert row", K(ret), K(sr->cnt_), K(ROWEXPR2STR(eval_ctx_, exprs)));
  }
  return ret;
}

int ObMergeSetOp::convert_row(const common::ObIArray<ObExpr*>& src_exprs, const common::ObIArray<ObExpr*>& dst_exprs)
{
  int ret = OB_SUCCESS;
  if (dst_exprs.count() != src_exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: exprs is not match", K(ret), K(src_exprs.count()), K(dst_exprs.count()));
  } else {
    ObDatum* src_datum = nullptr;
    for (uint32_t i = 0; i < dst_exprs.count() && OB_SUCC(ret); ++i) {
      if (OB_FAIL(src_exprs.at(i)->eval(eval_ctx_, src_datum))) {
        LOG_WARN("failed to eval expr", K(ret), K(i));
      } else {
        dst_exprs.at(i)->locate_expr_datum(eval_ctx_) = *src_datum;
        dst_exprs.at(i)->get_eval_info(eval_ctx_).evaluated_ = true;
      }
    }
    if (OB_SUCC(ret)) {
      LOG_DEBUG(
          "trace convert row", K(ret), K(ROWEXPR2STR(eval_ctx_, dst_exprs)), K(ROWEXPR2STR(eval_ctx_, src_exprs)));
    }
  }
  return ret;
}

int ObMergeSetOp::Compare::init(
    const common::ObIArray<ObSortFieldCollation>* sort_collations, const common::ObIArray<common::ObCmpFunc>* cmp_funcs)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sort_collations || nullptr == cmp_funcs) ||
      sort_collations->count() != cmp_funcs->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("compare info is null", K(ret), K(sort_collations), K(cmp_funcs));
  } else {
    sort_collations_ = sort_collations;
    cmp_funcs_ = cmp_funcs;
  }
  return ret;
}

int ObMergeSetOp::Compare::operator()(
    const ObChunkDatumStore::StoredRow& l, const common::ObIArray<ObExpr*>& r, ObEvalCtx& eval_ctx, int& cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  const ObDatum* lcells = l.cells();
  ObDatum* r_datum = nullptr;
  for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); i++) {
    int64_t idx = sort_collations_->at(i).field_idx_;
    if (OB_FAIL(r.at(idx)->eval(eval_ctx, r_datum))) {
      LOG_WARN("failed to get expr value", K(ret), K(i));
    } else {
      cmp = cmp_funcs_->at(i).cmp_func_(lcells[idx], *r_datum);
      if (0 != cmp) {
        cmp = sort_collations_->at(i).is_ascending_ ? cmp : -cmp;
        break;
      }
    }
  }
  return ret;
}

int ObMergeSetOp::Compare::operator()(
    const common::ObIArray<ObExpr*>& l, const common::ObIArray<ObExpr*>& r, ObEvalCtx& eval_ctx, int& cmp)
{
  int ret = OB_SUCCESS;
  cmp = 0;
  ObDatum* l_datum = nullptr;
  ObDatum* r_datum = nullptr;
  for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); i++) {
    int64_t idx = sort_collations_->at(i).field_idx_;
    if (OB_FAIL(l.at(idx)->eval(eval_ctx, l_datum))) {
      LOG_WARN("failed to get expr value", K(ret), K(i));
    } else if (OB_FAIL(r.at(idx)->eval(eval_ctx, r_datum))) {
    } else {
      cmp = cmp_funcs_->at(i).cmp_func_(*l_datum, *r_datum);
      LOG_DEBUG(
          "debug compare merge set op", K(EXPR2STR(eval_ctx, *l.at(idx))), K(EXPR2STR(eval_ctx, *r.at(idx))), K(cmp));
      if (0 != cmp) {
        cmp = sort_collations_->at(i).is_ascending_ ? cmp : -cmp;
        break;
      }
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
