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

#include "ob_temp_column_store.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "storage/ddl/ob_direct_load_struct.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

int ObTempColumnStore::ColumnBlock::calc_vector_size(const ObIVector *vec,
                                                     const uint16_t *selector,
                                                     const ObLength length,
                                                     const int64_t size,
                                                     int64_t &batch_mem_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == vec)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vec));
  } else {
    const VectorFormat format = vec->get_format();
    switch (format) {
    case VEC_FIXED:
      batch_mem_size += calc_size(static_cast<const ObFixedLengthBase*>(vec), selector, size);
      break;
    case VEC_DISCRETE:
      batch_mem_size += calc_size(static_cast<const ObDiscreteBase*>(vec), selector, size);
      break;
    case VEC_CONTINUOUS:
      batch_mem_size += calc_size(static_cast<const ObContinuousBase*>(vec), selector, size);
      break;
    case VEC_UNIFORM:
      batch_mem_size += calc_size<false>(static_cast<const ObUniformBase*>(vec), selector, size, length);
      break;
    case VEC_UNIFORM_CONST:
      batch_mem_size += calc_size<true>(static_cast<const ObUniformBase*>(vec), selector, size, length);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", K(ret), K(format));
      break;
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::vector_to_buf(const ObIVector *vec,
                                                  const uint16_t *selector,
                                                  const ObLength length,
                                                  const int64_t size,
                                                  char *head,
                                                  int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == vec || nullptr == head)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(vec), KP(head));
  } else {
    const VectorFormat format = vec->get_format();
    switch (format) {
    case VEC_FIXED:
      ret = to_buf(static_cast<const ObFixedLengthBase*>(vec), selector, size, head, pos);
      break;
    case VEC_DISCRETE:
      ret = to_buf(static_cast<const ObDiscreteBase*>(vec), selector, size, head, pos);
      break;
    case VEC_CONTINUOUS:
      ret = to_buf(static_cast<const ObContinuousBase*>(vec), selector, size, head, pos);
      break;
    case VEC_UNIFORM:
      ret = to_buf<false>(static_cast<const ObUniformBase*>(vec), selector, size, length, head, pos);
      break;
    case VEC_UNIFORM_CONST:
      ret = to_buf<true>(static_cast<const ObUniformBase*>(vec), selector, size, length, head, pos);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), K(format));
      break;
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::vector_from_buf(char *buf,
                                                    int64_t &pos,
                                                    const int64_t size,
                                                    ObIVector *vec)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == buf || nullptr == vec)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), KP(vec));
  } else {
    const VectorFormat format = vec->get_format();
    switch (format) {
    case VEC_FIXED:
      ret = from_buf(buf, pos, size, static_cast<ObFixedLengthBase*>(vec));
      break;
    case VEC_CONTINUOUS:
      ret = from_buf(buf, pos, size, static_cast<ObContinuousBase*>(vec));
      break;
    case VEC_UNIFORM:
      static_cast<UniformFormat *>(vec)->set_all_null(size);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector format", KR(ret), K(format));
      break;
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::calc_nested_size(ObExpr &expr, ObEvalCtx &ctx, const uint16_t *selector,
                                                     const ObArray<ObLength> &lengths, const int64_t size,
                                                     int64_t &batch_mem_size)
{
  int ret = OB_SUCCESS;
  ObIVector *vec = expr.get_vector(ctx);
  const VectorFormat format = vec->get_format();
  if (is_uniform_format(format) && OB_FAIL(distribute_uniform_nested_batch(expr, ctx, selector, format, size))) {
    SQL_LOG(WARN, "Failed to add batch nested attrs", K(ret), K(format), K(size));
  }
  for (uint32_t i = 0; i < expr.attrs_cnt_ && OB_SUCC(ret); ++i) {
    ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
    if (OB_FAIL(calc_vector_size(vec, selector, UNFIXED_LENGTH, size, batch_mem_size))) {
      LOG_WARN("fail to calc vector size", KR(ret));
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::distribute_uniform_nested_batch(ObExpr &expr, ObEvalCtx &ctx, const uint16_t *selector,
                                                                    const VectorFormat format, const int64_t size)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < expr.attrs_cnt_ && OB_SUCC(ret); ++i) {
    if (OB_FAIL(expr.attrs_[i]->init_vector_for_write(ctx, i == 0 ? VEC_FIXED : format, size))) {
      SQL_LOG(WARN, "Failed to init vector", K(ret), K(i), K(format), K(size));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObArrayExprUtils::batch_dispatch_array_attrs(ctx, expr, 0, size, selector))) {
    SQL_LOG(WARN, "Failed to dispatch nested attrs", K(ret), K(format), K(size));
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::add_nested_batch(ObExpr &expr, ObEvalCtx &ctx, const uint16_t *selector,
                                                     const int64_t size, char *head, int64_t &pos)
{
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < expr.attrs_cnt_ && OB_SUCC(ret); ++i) {
    ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
    if (OB_FAIL(vector_to_buf(vec, selector, UNFIXED_LENGTH, size, head, pos))) {
      LOG_WARN("vector to buf failed", KR(ret));
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::calc_rows_size(ObEvalCtx &ctx,
                                                   const ObExprPtrIArray &exprs,
                                                   const IVectorPtrs &vectors,
                                                   const uint16_t *selector,
                                                   const ObArray<ObLength> &lengths,
                                                   const int64_t size,
                                                   int64_t &batch_mem_size)
{
  int ret = OB_SUCCESS;
  batch_mem_size = get_header_size(vectors.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    const ObIVector *vec = vectors.at(i);
    if (exprs.at(i)->is_nested_expr()) {
      if (OB_FAIL(ColumnBlock::calc_nested_size(*exprs.at(i), ctx, selector, lengths, size, batch_mem_size))) {
        LOG_WARN("calc nested expr size failed", K(ret), K(size));
      }
    } else {
      if (OB_FAIL(calc_vector_size(vec, selector, lengths[i], size, batch_mem_size))) {
        LOG_WARN("fail to calc vector size", KR(ret));
      }
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::calc_rows_size(const IVectorPtrs &vectors,
                                                   const uint16_t *selector,
                                                   const ObArray<ObLength> &lengths,
                                                   const int64_t size,
                                                   int64_t &batch_mem_size)
{
  int ret = OB_SUCCESS;
  batch_mem_size = get_header_size(vectors.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    const ObIVector *vec = vectors.at(i);
    if (OB_FAIL(calc_vector_size(vec, selector, lengths[i], size, batch_mem_size))) {
      LOG_WARN("fail to calc vector size", KR(ret));
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::add_batch(ObEvalCtx &ctx,
                                              const ObExprPtrIArray &exprs,
                                              ShrinkBuffer &buf,
                                              const IVectorPtrs &vectors,
                                              const uint16_t *selector,
                                              const ObArray<ObLength> &lengths,
                                              const int64_t size,
                                              const int64_t batch_mem_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_mem_size > buf.remain())) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("block is not enough", K(ret), K(batch_mem_size), K(buf));
  } else {
    char *head = buf.head();
    *reinterpret_cast<int32_t *>(head) = static_cast<int32_t>(size); // row_count
    int32_t *vec_offsets = reinterpret_cast<int32_t *>(head + sizeof(int32_t));
    int64_t pos = get_header_size(vectors.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
      const ObIVector *vec = vectors.at(i);
      vec_offsets[i] = pos;
      if (exprs.at(i)->is_nested_expr()) {
        if (OB_FAIL(ColumnBlock::add_nested_batch(*exprs.at(i), ctx, selector, size, head, pos))) {
          LOG_WARN("calc nested expr size failed", K(ret), K(size));
        }
      } else {
        if (OB_FAIL(vector_to_buf(vec, selector, lengths[i], size, head, pos))) {
          LOG_WARN("vector to buf failed", KR(ret));
        }
      }
    }
    vec_offsets[vectors.count()] = pos; // last offset, the size of vector
    buf.fast_advance(pos);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(pos != batch_mem_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected memory size", K(ret), K(pos), K(batch_mem_size));
    } else {
      cnt_ += size;
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::add_batch(ShrinkBuffer &buf,
                                              const IVectorPtrs &vectors,
                                              const uint16_t *selector,
                                              const ObArray<ObLength> &lengths,
                                              const int64_t size,
                                              const int64_t batch_mem_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_mem_size > buf.remain())) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("block is not enough", K(ret), K(batch_mem_size), K(buf));
  } else {
    char *head = buf.head();
    *reinterpret_cast<int32_t *>(head) = static_cast<int32_t>(size); // row_count
    int32_t *vec_offsets = reinterpret_cast<int32_t *>(head + sizeof(int32_t));
    int64_t pos = get_header_size(vectors.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
      const ObIVector *vec = vectors.at(i);
      vec_offsets[i] = pos;
      if (OB_FAIL(vector_to_buf(vec, selector, lengths[i], size, head, pos))) {
        LOG_WARN("vector to buf failed", KR(ret));
      }
    }
    vec_offsets[vectors.count()] = pos; // last offset, the size of vector
    buf.fast_advance(pos);
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(pos != batch_mem_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected memory size", K(ret), K(pos), K(batch_mem_size));
    } else {
      cnt_ += size;
    }
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::get_nested_batch(ObExpr &expr, ObEvalCtx &ctx, char *buf, int64_t &pos, const int64_t size) const
{
  int ret = OB_SUCCESS;
  ObIVector *root_vec = expr.get_vector(ctx);
  int64_t pos_save = pos;
  // get null/flag for root vector
  if (root_vec->get_format() == VEC_CONTINUOUS &&
      OB_FAIL(from_buf(buf, pos, size, static_cast<ObBitmapNullVectorBase*>(root_vec)))) {
    LOG_WARN("failed to get null value", K(ret), K(expr));
  } else {
    pos = pos_save;
  }
  for (uint32_t i = 0; i < expr.attrs_cnt_ && OB_SUCC(ret); ++i) {
    ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
    if (OB_FAIL(vector_from_buf(buf, pos ,size, vec))) {
      LOG_WARN("vector from buf failed", KR(ret));
    }
  }

  return ret;
}

int ObTempColumnStore::ColumnBlock::get_next_batch(const ObExprPtrIArray &exprs,
                                                   ObEvalCtx &ctx,const IVectorPtrs &vectors,
                                                   const int32_t start_read_pos,
                                                   int32_t &batch_rows,
                                                   int32_t &batch_pos) const
{
  int ret = OB_SUCCESS;
  char* buf = const_cast<char *>(start_read_pos + payload_);
  const int32_t size = *reinterpret_cast<const int32_t*>(buf);
  const int32_t *vec_offsets = reinterpret_cast<int32_t *>(buf + sizeof(int32_t));
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    ObIVector *vec = vectors.at(i);
    int64_t pos = vec_offsets[i];
    if (NULL == vec || (VEC_UNIFORM_CONST == vec->get_format())) {
      // if vector is null or uniform const, skip read vector
    } else if (exprs.at(i)->is_nested_expr()) {
      if (OB_FAIL(ColumnBlock::get_nested_batch(*exprs.at(i), ctx, buf, pos, size))) {
        LOG_WARN("calc nested expr size failed", K(ret), K(size));
      }
    } else {
      if (OB_FAIL(vector_from_buf(buf, pos ,size, vec))) {
        LOG_WARN("vector from buf failed", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    batch_rows = size;
    batch_pos = vec_offsets[vectors.count()];
  }
  return ret;
}

int ObTempColumnStore::ColumnBlock::get_next_batch(const IVectorPtrs &vectors,
                                                   const int32_t start_read_pos,
                                                   int32_t &batch_rows,
                                                   int32_t &batch_pos) const
{
  int ret = OB_SUCCESS;
  char* buf = const_cast<char *>(payload_ + start_read_pos);
  const int32_t size = *reinterpret_cast<const int32_t*>(buf);
  const int32_t *vec_offsets = reinterpret_cast<int32_t *>(buf + sizeof(int32_t));
  for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
    ObIVector *vec = vectors.at(i);
    int64_t pos = vec_offsets[i];
    if (OB_FAIL(vector_from_buf(buf, pos ,size, vec))) {
      LOG_WARN("vector from buf failed", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    batch_rows = size;
    batch_pos = vec_offsets[vectors.count()];
  }
  return ret;
}

int ObTempColumnStore::Iterator::init(ObTempColumnStore *store)
{
  reset();
  column_store_ = store;
  return BlockReader::init(store);
}

int ObTempColumnStore::Iterator::nested_from_vector(ObExpr &expr, ObEvalCtx &ctx, const int64_t start_pos, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObIVector *root_vec = expr.get_vector(ctx);
  if (root_vec->get_format() != VEC_CONTINUOUS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected format", K(ret), K(root_vec->get_format()));
  } else if (OB_FAIL(from_vector(static_cast<ObContinuousBase*>(root_vec), &expr, ctx, start_pos, size))) {
     LOG_WARN("from vector failed", K(ret), K(root_vec->get_format()));
  } else {
    for (uint32_t i = 0; i < expr.attrs_cnt_ && OB_SUCC(ret); ++i) {
      ObIVector *vec = expr.attrs_[i]->get_vector(ctx);
      const VectorFormat format = vec->get_format();
      switch (format) {
        case VEC_FIXED:
          ret = from_vector(static_cast<ObFixedLengthBase*>(vec), expr.attrs_[i], ctx, start_pos, size);
          break;
        case VEC_CONTINUOUS:
          ret = from_vector(static_cast<ObContinuousBase*>(vec), expr.attrs_[i], ctx, start_pos, size);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected format", K(ret), K(format));
      }
    }
  }
  return ret;
}

int ObTempColumnStore::Iterator::get_next_batch(const ObExprPtrIArray &exprs,
                                                ObEvalCtx &ctx,
                                                const int64_t max_rows,
                                                int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (OB_UNLIKELY(exprs.count() != column_store_->get_col_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count mismatch", K(ret), K(exprs.count()), K(column_store_->get_col_cnt()));
  } else if (OB_UNLIKELY(NULL == cur_blk_ || !cur_blk_->contain(cur_blk_id_))) {
    if (OB_FAIL(next_block())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next block", K(ret));
      }
    }
  }
  int32_t batch_rows = 0;
  int32_t batch_pos = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ensure_read_vectors(exprs, ctx, max_rows))) {
    LOG_WARN("fail to ensure read vectors", K(ret));
  } else if (OB_FAIL(cur_blk_->get_next_batch(exprs, ctx, *vectors_,
                                              read_pos_, batch_rows, batch_pos))) {
    LOG_WARN("fail to get next batch from column block", K(ret));
  } else if (OB_UNLIKELY(has_rest_row_in_batch())) {
    // current block has remaining unread data
    const int64_t begin = batch_rows - rest_row_cnt_;
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
      ObExpr *e = exprs.at(i);
      ObIVector *vec = NULL;
      if (OB_ISNULL(e) || ((is_uniform_format((vec = e->get_vector(ctx))->get_format())))) {
        // skip null input expr and uniform expr
      } else if (e->is_nested_expr()) {
        if (OB_FAIL(nested_from_vector(*e, ctx, begin, batch_rows))) {
          LOG_WARN("failed to get nested expr", K(ret), K(begin), K(batch_rows));
        }
      } else {
        const VectorFormat format = vec->get_format();
        switch (format) {
        case VEC_FIXED:
          ret = from_vector(static_cast<ObFixedLengthBase*>(vec), e, ctx, begin, batch_rows);
          break;
        case VEC_CONTINUOUS:
          ret = from_vector(static_cast<ObContinuousBase*>(vec), e, ctx, begin, batch_rows);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected format", K(ret), K(format));
        }
      }
    }
    if (OB_SUCC(ret)) {
      read_rows = MIN(max_rows, rest_row_cnt_);
      rest_row_cnt_ -= read_rows;
    }
  } else {
    read_rows = MIN(max_rows, batch_rows);
    if (OB_UNLIKELY(read_rows < batch_rows)) {
      rest_row_cnt_ = batch_rows - read_rows;
    }
  }
  if (OB_SUCC(ret)) {
    cur_blk_id_ += read_rows;
    if (0 == rest_row_cnt_) {
      read_pos_ += batch_pos;
    }
    for (int64_t i = 0; i < exprs.count(); i++) {
      if (NULL != exprs.at(i)) {
        exprs.at(i)->set_evaluated_projected(ctx);
      }
    }
  }
  return ret;
}

int ObTempColumnStore::Iterator::get_next_batch(const IVectorPtrs &vectors,
                                                int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (OB_UNLIKELY(NULL == cur_blk_ || !cur_blk_->contain(cur_blk_id_))) {
    if (OB_FAIL(next_block())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next block", K(ret));
      }
    }
  }
  int32_t batch_rows = 0;
  int32_t batch_pos = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cur_blk_->get_next_batch(vectors, read_pos_, batch_rows, batch_pos))) {
    LOG_WARN("fail to get next batch from column block", K(ret));
  } else {
    cur_blk_id_ += batch_rows;
    read_pos_ += batch_pos;
    read_rows = batch_rows;
  }
  return ret;
}

int ObTempColumnStore::Iterator::next_block()
{
  int ret = OB_SUCCESS;
  const Block *read_blk = NULL;
  if (cur_blk_id_ >= get_row_cnt()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_block(cur_blk_id_, read_blk))) {
    LOG_WARN("fail to get block from store", K(ret), K(cur_blk_id_));
  } else {
    cur_blk_ = static_cast<const ColumnBlock*>(read_blk);
    rest_row_cnt_ = 0;
    read_pos_ = 0;
  }
  return ret;
}

int ObTempColumnStore::Iterator::ensure_read_vectors(const ObExprPtrIArray &exprs,
                                                     ObEvalCtx &ctx,
                                                     const int64_t max_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_store_->batch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("try read vector from empty store", K(ret), K(get_row_cnt()));
  } else if (OB_ISNULL(vectors_)) {
    if (column_store_->reuse_vector_array_) {
      vectors_ = &column_store_->batch_ctx_->vectors_;
    } else {
      void *mem = column_store_->allocator_->alloc(sizeof(ObArray<ObIVector *>),
                                                   column_store_->mem_attr_);
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vector array", K(ret));
      } else {
        vectors_ = new(mem)ObArray<ObIVector *>();
        vectors_->set_attr(column_store_->mem_attr_);
        ret = vectors_->prepare_allocate(get_col_cnt());
      }
    }
  }
  for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
    ObExpr *e = exprs.at(i);
    if (OB_ISNULL(e) || e->is_const_expr()) {
      vectors_->at(i) = NULL;
    } else if (OB_FAIL(e->init_vector(ctx, e->get_temp_column_store_res_format(), max_rows))) {
      LOG_WARN("fail to init vector", K(ret));
    } else {
      vectors_->at(i) = e->get_vector(ctx);
    }
  }
  return ret;
}

ObTempColumnStore::ObTempColumnStore(common::ObIAllocator *alloc /* = NULL */)
   : ObTempBlockStore(alloc), cur_blk_(NULL), col_cnt_(0), batch_ctx_(NULL), max_batch_size_(0),
     reuse_vector_array_(true)
{
}

ObTempColumnStore::~ObTempColumnStore()
{
  reset();
}

void ObTempColumnStore::reset()
{
  reset_batch_ctx();
  cur_blk_ = NULL;
  ObTempBlockStore::reset();
}

void ObTempColumnStore::reset_batch_ctx()
{
  if (NULL != batch_ctx_) {
    batch_ctx_->~BatchCtx();
    allocator_->free(batch_ctx_);
    batch_ctx_ = NULL;
  }
}

int ObTempColumnStore::init(const ObExprPtrIArray &exprs,
                            const int64_t max_batch_size,
                            const lib::ObMemAttr &mem_attr,
                            const int64_t mem_limit,
                            const bool enable_dump,
                            const bool reuse_vector_array,
                            const common::ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  mem_attr_ = mem_attr;
  col_cnt_ = exprs.count();
  max_batch_size_ = max_batch_size;
  ObTempBlockStore::set_inner_allocator_attr(mem_attr);
  OZ(ObTempBlockStore::init(mem_limit, enable_dump, mem_attr.tenant_id_, mem_attr.ctx_id_,
                            mem_attr_.label_, compressor_type));
  reuse_vector_array_ = reuse_vector_array;
  inited_ = true;
  return ret;
}

int ObTempColumnStore::init(const IVectorPtrs &vectors,
                            const int64_t max_batch_size,
                            const ObMemAttr &mem_attr,
                            const int64_t mem_limit,
                            const bool enable_dump,
                            const ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  mem_attr_ = mem_attr;
  col_cnt_ = vectors.count();
  max_batch_size_ = max_batch_size;
  ObTempBlockStore::set_inner_allocator_attr(mem_attr);
  OZ(ObTempBlockStore::init(mem_limit, enable_dump, mem_attr.tenant_id_, mem_attr.ctx_id_,
                            mem_attr_.label_, compressor_type));
  OZ(init_batch_ctx(vectors));
  reuse_vector_array_ = false;
  inited_ = true;
  return ret;
}

int ObTempColumnStore::init_vectors(const ObIArray<ObColumnSchemaItem> &col_array,
                                    ObIAllocator &allocator,
                                    IVectorPtrs &vectors)
{
  int ret = OB_SUCCESS;
  vectors.reset();
  if (OB_UNLIKELY(col_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(col_array));
  } else if (OB_FAIL(vectors.prepare_allocate(col_array.count()))) {
    LOG_WARN("fail to prepare allocate vectors", K(ret), K(col_array.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); ++i) {
      const ObColumnSchemaItem &column_schema = col_array.at(i);
      VecValueTypeClass value_tc = get_vec_value_tc(column_schema.col_type_.get_type(),
                                                    column_schema.col_type_.get_scale(),
                                                    column_schema.col_accuracy_.get_precision());
      const bool is_fixed = is_fixed_length_vec(value_tc);
      ObIVector *vector = nullptr;
      if (is_fixed) { // fixed format
        switch (value_tc) {
        #define FIXED_VECTOR_INIT_SWITCH(value_tc)                              \
          case value_tc: {                                                      \
            using VecType = RTVectorType<VEC_FIXED, value_tc>;                  \
            static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                          "vector size exceeds MAX_VECTOR_STRUCT_SIZE");        \
            vector = OB_NEWx(VecType, &allocator, nullptr, nullptr);            \
            break;                                                              \
          }
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTEGER);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_UINTEGER);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_FLOAT);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_DOUBLE);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_FIXED_DOUBLE);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_DATETIME);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_DATE);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIME);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_YEAR);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_UNKNOWN);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_BIT);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TZ);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_TIMESTAMP_TINY);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_YM);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_INTERVAL_DS);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT32);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT64);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT128);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT256);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_DEC_INT512);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATE);
          FIXED_VECTOR_INIT_SWITCH(VEC_TC_MYSQL_DATETIME);
        #undef FIXED_VECTOR_INIT_SWITCH
          default:
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid fixed vector value type class", K(ret), K(i), K(column_schema), K(value_tc));
            break;
        }
      } else { // continuous format
        switch (value_tc) {
        #define CONTINUOUS_VECTOR_INIT_SWITCH(value_tc)                         \
          case value_tc: {                                                      \
            using VecType = RTVectorType<VEC_CONTINUOUS, value_tc>;             \
            static_assert(sizeof(VecType) <= ObIVector::MAX_VECTOR_STRUCT_SIZE, \
                          "vector size exceeds MAX_VECTOR_STRUCT_SIZE");        \
            vector = OB_NEWx(VecType, &allocator, nullptr, nullptr, nullptr);   \
            break;                                                              \
          }
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_NUMBER);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_EXTEND);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_STRING);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ENUM_SET_INNER);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_RAW);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ROWID);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_LOB);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_JSON);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_GEO);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_UDT);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_COLLECTION);
          CONTINUOUS_VECTOR_INIT_SWITCH(VEC_TC_ROARINGBITMAP);
        #undef CONTINUOUS_VECTOR_INIT_SWITCH
          default:
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid continuous vector value type class", K(ret), K(i), K(column_schema), K(value_tc));
            break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(vector)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc vector", KR(ret));
      } else {
        vectors.at(i) = vector;
      }
    }
  }
  return ret;
}

int ObTempColumnStore::init_batch_ctx(const ObExprPtrIArray &exprs)
{
  int ret = OB_SUCCESS;
  const int64_t max_batch_size = max_batch_size_;
  if (OB_UNLIKELY(NULL == batch_ctx_)) {
    const int64_t size = sizeof(*batch_ctx_) + sizeof(*batch_ctx_->selector_) * max_batch_size;
    char *mem = static_cast<char *>(allocator_->alloc(size, mem_attr_));
    if (OB_UNLIKELY(max_batch_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max batch size is not positive when init batch ctx", K(ret), K(max_batch_size));
    } else if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(size), K(col_cnt_), K(max_batch_size));
    } else {
      char *begin = mem;
      batch_ctx_ = new(mem)BatchCtx();
      batch_ctx_->vectors_.set_attr(mem_attr_);
      batch_ctx_->lengths_.set_attr(mem_attr_);
      batch_ctx_->max_batch_size_ = max_batch_size;
      if (OB_FAIL(batch_ctx_->vectors_.prepare_allocate(col_cnt_))) {
        LOG_WARN("fail to prepare allocate vectors", K(ret), K(col_cnt_));
      } else if (OB_FAIL(batch_ctx_->lengths_.prepare_allocate(col_cnt_))) {
        LOG_WARN("fail to prepare allocate lengths", K(ret), K(col_cnt_));
      } else {
        for (int64_t i = 0; i < exprs.count(); ++i) {
          ObExpr *expr = exprs.at(i);
          if (expr->is_fixed_length_data_) {
            batch_ctx_->lengths_.at(i) = expr->get_fixed_length();
          } else if (expr->datum_meta_.type_ == ObNullType && expr->is_batch_result()) {
            batch_ctx_->lengths_.at(i) = NULL_WITH_BATCH_RESULT_LENGTH;
          } else {
            batch_ctx_->lengths_.at(i) = UNFIXED_LENGTH;
          }
        }
        mem += sizeof(*batch_ctx_);
        batch_ctx_->selector_ = reinterpret_cast<typeof(batch_ctx_->selector_)>(mem);
        mem += sizeof(*batch_ctx_->selector_) * max_batch_size;
        if (mem - begin != size) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("size mismatch", K(ret), K(mem - begin), K(size), K(col_cnt_),
                                    K(max_batch_size));
        }
      }
    }
  }

  return ret;
}

int ObTempColumnStore::init_batch_ctx(const IVectorPtrs &vectors)
{
  int ret = OB_SUCCESS;
  const int64_t max_batch_size = max_batch_size_;
  if (OB_LIKELY(NULL == batch_ctx_)) {
    const int64_t size = sizeof(*batch_ctx_) + sizeof(*batch_ctx_->selector_) * max_batch_size;
    char *mem = static_cast<char *>(allocator_->alloc(size, mem_attr_));
    if (OB_UNLIKELY(max_batch_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max batch size is not positive when init batch ctx", K(ret), K(max_batch_size));
    } else if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(size), K(col_cnt_), K(max_batch_size));
    } else {
      char *begin = mem;
      batch_ctx_ = new (mem) BatchCtx();
      batch_ctx_->vectors_.set_attr(mem_attr_);
      batch_ctx_->lengths_.set_attr(mem_attr_);
      batch_ctx_->max_batch_size_ = max_batch_size;
      if (OB_FAIL(batch_ctx_->lengths_.prepare_allocate(col_cnt_))) {
        LOG_WARN("fail to prepare allocate lengths", K(ret), K(col_cnt_));
      } else {
        mem += sizeof(*batch_ctx_);
        batch_ctx_->selector_ = reinterpret_cast<typeof(batch_ctx_->selector_)>(mem);
        mem += sizeof(*batch_ctx_->selector_) * max_batch_size;
        if (mem - begin != size) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("size mismatch", K(ret), K(mem - begin), K(size), K(col_cnt_),
                                    K(max_batch_size));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < vectors.count(); ++i) {
          ObIVector *vector = vectors.at(i);
          const VectorFormat format = vector->get_format();
          switch (format) {
            case VEC_FIXED:
              batch_ctx_->lengths_.at(i) = static_cast<ObFixedLengthBase *>(vector)->get_length();
              break;
            case VEC_CONTINUOUS:
              batch_ctx_->lengths_.at(i) = UNFIXED_LENGTH;
              break;
            default:
              ret = OB_ERR_UNDEFINED;
              LOG_WARN("unexpected vector format", KR(ret), K(i), K(format));
              break;
          }
        }
      }
    }
  }
  return ret;
}

int ObTempColumnStore::add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                                 const ObBatchRows &brs, int64_t &stored_rows_count)
{
  int ret = OB_SUCCESS;
  int16_t size = 0;
  const uint16_t *selector = NULL;
  if (OB_UNLIKELY(exprs.count() != get_col_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count mismatch", K(ret), K(exprs.count()), K(get_col_cnt()));
  } else if (OB_FAIL(init_batch_ctx(exprs))) {
    LOG_WARN("fail to init batch ctx", K(ret));
  } else if (brs.all_rows_active_ || (0 == brs.skip_->accumulate_bit_cnt(brs.size_))) {
    // all skipped, set selector point to null
    size = brs.size_;
  } else {
    for (int64_t i = 0; i < brs.size_; i++) {
      if (brs.skip_->at(i)) {
        continue;
      } else {
        batch_ctx_->selector_[size++] = i;
      }
    }
    selector = batch_ctx_->selector_;
  }
  if (OB_SUCC(ret) && size > 0) {
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
      ObExpr *e = exprs.at(i);
      ObIVector *vec = NULL;
      if (OB_FAIL(e->eval_vector(ctx, brs))) {
        LOG_WARN("evaluate batch failed", K(ret));
      } else {
        vec = e->get_vector(ctx);
        batch_ctx_->vectors_.at(i) = vec;
      }
    }
    int64_t batch_mem_size = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ColumnBlock::calc_rows_size(ctx, exprs, batch_ctx_->vectors_,
                                                   selector,
                                                   batch_ctx_->lengths_,
                                                   size,
                                                   batch_mem_size))) {
      LOG_WARN("fail to calc rows size", K(ret));
    } else if (OB_FAIL(ensure_write_blk(batch_mem_size))) {
      LOG_WARN("ensure write block failed", K(ret));
    } else if (OB_FAIL(cur_blk_->add_batch(ctx, exprs, blk_buf_, batch_ctx_->vectors_, selector, batch_ctx_->lengths_,
                                           size, batch_mem_size))) {
      LOG_WARN("fail to add batch to column store", K(ret));
    } else {
      block_id_cnt_ += size;
      inc_mem_used(batch_mem_size);
    }
  }
  stored_rows_count = size;
  return ret;
}

int ObTempColumnStore::add_batch(const IVectorPtrs &vectors,
                                 const ObBatchRows &brs,
                                 int64_t &stored_rows_count)
{
  int ret = OB_SUCCESS;
  int16_t size = 0;
  const uint16_t *selector = NULL;
  if (OB_UNLIKELY(vectors.count() != get_col_cnt())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column count mismatch", K(ret), K(vectors.count()), K(get_col_cnt()));
  } else if (OB_ISNULL(batch_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch ctx not init", K(ret));
  } else if (brs.all_rows_active_ || (0 == brs.skip_->accumulate_bit_cnt(brs.size_))) {
    // all skipped, set selector point to null
    size = brs.size_;
  } else {
    for (int64_t i = 0; i < brs.size_; i++) {
      if (brs.skip_->at(i)) {
        continue;
      } else {
        batch_ctx_->selector_[size++] = i;
      }
    }
    selector = batch_ctx_->selector_;
  }
  if (OB_SUCC(ret) && size > 0) {
    int64_t batch_mem_size = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ColumnBlock::calc_rows_size(vectors,
                                                   selector,
                                                   batch_ctx_->lengths_,
                                                   size,
                                                   batch_mem_size))) {
      LOG_WARN("fail to calc rows size", K(ret));
    } else if (OB_FAIL(ensure_write_blk(batch_mem_size))) {
      LOG_WARN("ensure write block failed", K(ret));
    } else if (OB_FAIL(cur_blk_->add_batch(blk_buf_,
                                           vectors,
                                           selector,
                                           batch_ctx_->lengths_,
                                           size,
                                           batch_mem_size))) {
      LOG_WARN("fail to add batch to column store", K(ret));
    } else {
      block_id_cnt_ += size;
      inc_mem_used(batch_mem_size);
    }
  }
  stored_rows_count = size;
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
