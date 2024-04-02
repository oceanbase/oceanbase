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
#include "src/sql/engine/basic/chunk_store/ob_compact_store.h"

namespace oceanbase
{
namespace sql
{

int ObCompactStore::add_batch_fallback(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                                       const ObBitVector &skip, const int64_t batch_size,
                                       const common::ObIArray<int64_t> &selector, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(ctx);
  batch_info_guard.set_batch_size(batch_size);
  for (int64_t i = 0; i < size && OB_SUCC(ret); i++) {
    int64_t idx = selector.at(i);
    batch_info_guard.set_batch_idx(idx);
    ObChunkDatumStore::StoredRow *srow = NULL;
    if (OB_FAIL(add_row(exprs, ctx))) {
      LOG_WARN("add row failed", K(ret), K(i), K(idx));
    }
  }
  return ret;
}

int ObCompactStore::add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                              const ObBitVector &skip, const int64_t batch_size,
                              int64_t &stored_rows_count)
{
  int ret = OB_SUCCESS;
  int64_t size = 0;
  common::ObSEArray<int64_t, 2> selector;
  CK(is_inited());
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < batch_size && OB_SUCC(ret); i++) {
      if (skip.at(i)) {
        continue;
      } else if (OB_FAIL(selector.push_back(i))){
        LOG_WARN("fail to push back selector", K(ret));
      } else {
        size++;
      }
    }
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
      ObExpr *e = exprs.at(i);
      if (OB_ISNULL(e)) {
      } else if (OB_FAIL(e->eval_batch(ctx, skip, batch_size))) {
        LOG_WARN("evaluate batch failed", K(ret));
      } else {
        if (!e->is_batch_result()) {
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    stored_rows_count = size;
    if (OB_FAIL(add_batch_fallback(exprs, ctx, skip, batch_size,
                          selector, size))) {
      LOG_WARN("add batch failed");
    }
  }
  return ret;
}

int ObCompactStore::add_row(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    const ObCompactRow *crow = nullptr;
    ObCompactRow *dummy_crow = nullptr;
    if (OB_FAIL(convertor_.get_compact_row_from_expr(exprs, ctx, crow))) {
      LOG_WARN("fail to get compact row from expr", K(ret), KP(crow));
    } else if (OB_FAIL(static_cast<ObTempRowStore*>(this)->add_row(crow, dummy_crow))) {
      LOG_WARN("fail to add row", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should init writer first", K(ret));
  }
  return ret;
}

int ObCompactStore::add_row(const ObChunkDatumStore::StoredRow &src_sr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    const ObCompactRow *crow = nullptr;
    ObCompactRow *dummy_crow = nullptr;
    if (OB_FAIL(convertor_.stored_row_to_compact_row(&src_sr, crow))) {
      LOG_WARN("fail to get compact row from expr", K(ret), KP(crow));
    } else if (OB_FAIL(add_row(crow, dummy_crow))) {
      LOG_WARN("fail to add row", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should init writer first", K(ret));
  }

  return ret;
}

int ObCompactStore::get_next_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (!start_iter_) {
    if (OB_FAIL(init_iter())) {
      LOG_WARN("fail to init iter", K(ret));
    } else {
      start_iter_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    sr = nullptr;
    if (iter_.has_next()) {
      const ObCompactRow *crow = nullptr;
      if (OB_FAIL(iter_.get_next_row(crow))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get row", K(ret));
        }
      } else if (OB_FAIL(convertor_.compact_row_to_stored_row(crow, sr))) {
        LOG_WARN("fail to convert crow to srow", K(ret), KP(crow), KP(sr));
      }
    } else {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

int ObCompactStore::init(const int64_t mem_limit,
                         const uint64_t tenant_id,
                         const int64_t mem_ctx_id,
                         const char *label,
                         const bool enable_dump,
                         const uint32_t row_extra_size,
                         const bool reorder_fixed_expr /*true*/,
                         const bool enable_trunc,
                         const ObCompressorType compress_type,
                         const ExprFixedArray *exprs)
{
  int ret = OB_SUCCESS;
  inited_ = true;
  OZ(ObTempRowStore::init(mem_limit, enable_dump, tenant_id, mem_ctx_id, label, compress_type, enable_trunc));
  if (OB_NOT_NULL(exprs)) {
    ObTempBlockStore::set_inner_allocator_attr(ObMemAttr(tenant_id, label, mem_ctx_id));
    OZ(row_meta_.init(*exprs, row_extra_size, reorder_fixed_expr));
  }
  OZ(init_convertor());
  LOG_INFO("success to init compact store", K(enable_dump), K(enable_trunc), K(compress_type),
            K(exprs), K(ret));
  return ret;
}

void ObCompactStore::reset()
{
  start_iter_ = false;
  inited_ = false;
}

int ObCompactStore::init_convertor()
{
  return convertor_.init(this, &row_meta_);
}

OB_DEF_SERIALIZE(ObCompactStore)
{
  int ret = OB_ERR_UNEXPECTED;
  return ret;
}

OB_DEF_DESERIALIZE(ObCompactStore)
{
  int ret = OB_ERR_UNEXPECTED;
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCompactStore)
{
  int64_t len = 0;
  return len;
}

}
}
