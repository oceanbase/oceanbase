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

namespace oceanbase {
using namespace common;
namespace sql {

ObMergeDistinctSpec::ObMergeDistinctSpec(ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObDistinctSpec(alloc, type)
{}

OB_SERIALIZE_MEMBER((ObMergeDistinctSpec, ObDistinctSpec));

ObMergeDistinctOp::ObMergeDistinctOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
    : ObOperator(exec_ctx, spec, input),
      first_got_row_(true),
      alloc_(ObModIds::OB_SQL_MERGE_GROUPBY, OB_MALLOC_NORMAL_BLOCK_SIZE, OB_SERVER_TENANT_ID, ObCtxIds::WORK_AREA),
      last_row_(alloc_)
{}

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

int ObMergeDistinctOp::rescan()
{
  int ret = OB_SUCCESS;
  first_got_row_ = true;
  last_row_.reset();
  alloc_.reset();
  if (OB_FAIL(ObOperator::rescan())) {}
  return ret;
}

int ObMergeDistinctOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool got_distinct_row = false;
  bool equal = false;
  if (!first_got_row_) {
    while (OB_SUCC(ret) && !got_distinct_row) {
      clear_evaluated_flag();
      if (OB_FAIL(child_->get_next_row())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("child operator get next row failed", K(ret));
        }
      } else {
        // compare current_row and last_row
        if (!cmp_.equal(&MY_SPEC.distinct_exprs_, last_row_.store_row_)) {
          got_distinct_row = true;
          ret = cmp_.ret_code_;
          /* save this row to local buffer. last_row_buf_ reused */
          if (OB_SUCC(ret) && OB_FAIL(last_row_.save_store_row(MY_SPEC.distinct_exprs_, eval_ctx_, 0))) {
            LOG_WARN("failed to storage row to operator context", K(ret));
          }
        }
      }
    }      /* end while */
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
  if (OB_FAIL(ObOperator::inner_close())) {}
  return ret;
}

int ObMergeDistinctOp::Compare::init(ObEvalCtx* eval_ctx, const ObIArray<ObCmpFunc>* cmp_funcs)
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

bool ObMergeDistinctOp::Compare::equal(const ObIArray<ObExpr*>* l, const ObChunkDatumStore::StoredRow* r)
{
  int ret = OB_SUCCESS;
  bool equal = false;
  if (OB_ISNULL(l) || OB_ISNULL(eval_ctx_) || OB_ISNULL(cmp_funcs_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(r)) {
    if (0 != l->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: it must be distinct const", K(ret));
    } else {
      // case: select distinct 1 from t1;
      equal = true;
    }
  } else {
    const ObDatum* rcells = r->cells();
    int cmp = 0;
    ObDatum* other_datum = nullptr;
    for (int64_t i = 0; 0 == cmp && i < cmp_funcs_->count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(l->at(i)->eval(*eval_ctx_, other_datum))) {
        LOG_WARN("failed to get expr value", K(ret), K(i));
      } else {
        cmp = cmp_funcs_->at(i).cmp_func_(*other_datum, rcells[i]);
      }
    }
    equal = OB_SUCC(ret) ? (0 == cmp) : false;
  }
  ret_code_ = ret;
  return equal;
}

}  // end namespace sql
}  // end namespace oceanbase
