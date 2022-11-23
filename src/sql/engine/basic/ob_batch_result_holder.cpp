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

#include "sql/engine/basic/ob_batch_result_holder.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/container/ob_se_array_iterator.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObBatchResultHolder::init(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    // do nothing
  } else if (exprs.count() == 0) {
    // do nothing
  } else {
    exprs_ = &exprs;
    eval_ctx_ = &eval_ctx;
    int64_t batch_size = eval_ctx.max_batch_size_;
    datums_ = static_cast<ObDatum *>(eval_ctx.exec_ctx_.get_allocator().alloc(
        batch_size * exprs.count() * sizeof(*datums_)));
    if (OB_ISNULL(datums_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(batch_size), K(exprs.count()));
    }
    inited_ = true;
    saved_size_ = 0;
  }
  return ret;
}

int ObBatchResultHolder::save(int64_t size)
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_) {
    // empty expr_: do nothing
  } else if (NULL == datums_) {
    ret = OB_NOT_INIT;
  } else if (size > 0) {
    int64_t off = 0;
    for (int64_t i = 0; i < exprs_->count(); i++) {
      ObExpr *e = exprs_->at(i);
      const int cnt = e->is_batch_result() ? size : 1;
      MEMCPY(datums_ + off, e->locate_batch_datums(*eval_ctx_), sizeof(ObDatum) * cnt);
      off += cnt;
    }
    saved_size_ = size;
  }
  return ret;
}

int ObBatchResultHolder::restore()
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_) {
    // empty expr_: do nothing
  } else if (NULL == datums_) {
    ret = OB_NOT_INIT;
  } else if (saved_size_ > 0) {
    int64_t off = 0;
    for (int64_t i = 0; i < exprs_->count(); i++) {
      ObExpr *e = exprs_->at(i);
      const int cnt = e->is_batch_result() ? saved_size_ : 1;
      MEMCPY(e->locate_batch_datums(*eval_ctx_), datums_ + off, sizeof(ObDatum) * cnt);
      off += cnt;
    }
  }
  return ret;
}

int ObBatchResultHolder::check_datum_modified()
{
  int ret = OB_SUCCESS;
  if (NULL == exprs_) {
    // empty expr_: do nothing
  } else if (NULL == datums_) {
    // not init: do nothing
  } else if (saved_size_ > 0) {
    int64_t off = 0;
    for (int64_t i = 0; i < exprs_->count() && OB_SUCC(ret); i++) {
      ObExpr *e = exprs_->at(i);
      const int cnt = e->is_batch_result() ? saved_size_ : 1;
      if (OB_UNLIKELY(0 != memcmp(e->locate_batch_datums(*eval_ctx_), datums_ + off,
                                  sizeof(ObDatum) * cnt))) {
        ret = OB_ERR_UNEXPECTED;
        int j;
        for (j = 0; j < cnt; ++j) {
          if (0 != memcmp(e->locate_batch_datums(*eval_ctx_) + j, datums_ + off + j,
                          sizeof(ObDatum))) {
            break;
          }
        }
        if (j >= cnt) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status", K(ret), K(j), K(cnt));
        } else {
          const ObDatum datum_saved = (datums_ + off)[j];
          const ObDatum datum_in_expr = e->locate_batch_datums(*eval_ctx_)[j];
          LOG_WARN("Datum modified", K(ret), K(i), K(j), K(datum_saved.pack_), KP(datum_saved.ptr_),
                                    K(datum_in_expr.pack_), KP(datum_in_expr.ptr_));
        }
        break;
      }
      off += cnt;
    }
  }
  reset(); // reset after first check
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
