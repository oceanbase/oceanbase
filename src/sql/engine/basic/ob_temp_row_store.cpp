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

#include "ob_temp_row_store.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/ob_define.h"

namespace oceanbase
{
using namespace common;

namespace sql
{

#define ROW_BLK reinterpret_cast<RowBlock *>(blk_)

template<>
int ObTempRowStoreBase<false>::RowBlock::get_store_row(int64_t &cur_pos, const ObCompactRow *&sr)
{
  int ret = OB_SUCCESS;
  if (cur_pos >= raw_size_) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid index", K(ret), K(cur_pos), K_(cnt));
  } else {
    ObCompactRow *row = reinterpret_cast<ObCompactRow *>(&payload_[cur_pos]);
    cur_pos += row->get_row_size();
    sr = row;
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::RowBlock::add_row(
    ShrinkBuffer &buf,
    const common::ObIArray<ObExpr*> &exprs,
    const RowMeta &row_meta,
    ObEvalCtx &ctx,
    ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  const int64_t batch_idx = ctx.get_batch_idx();
  int64_t row_size = row_meta.get_row_fixed_size();
  int64_t remain_size = buf.remain();
  if (OB_UNLIKELY(row_size > remain_size)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    stored_row = new(buf.head())ObCompactRow();
    stored_row->init(row_meta);
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
      ObExpr *expr = exprs.at(i);
      ObIVector *vec = expr->get_vector(ctx);
      if (OB_FAIL(vec->to_row(row_meta, stored_row, batch_idx, i,
                              remain_size - row_size, expr->is_fixed_length_data_,
                              row_size))) {
        if (OB_BUF_NOT_ENOUGH != ret) {
          LOG_WARN("failed to add row", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(post_add_row(buf, row_size))) {
      LOG_WARN("fill index to buffer tail failed", K(ret));
    }
    ++cnt_;
    stored_row->set_row_size(row_size);
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::RowBlock::add_row(
    ShrinkBuffer &buf, const ObCompactRow *src_row, ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src row is null", K(ret));
  } else {
    stored_row = new(buf.head())ObCompactRow();
    MEMCPY(stored_row, src_row, src_row->get_row_size());
    if (RA) {
      ret = post_add_row(buf, src_row->get_row_size());
    } else {
      buf.fast_advance(src_row->get_row_size());
    }
    ++cnt_;
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::RowBlock::add_batch(
    ShrinkBuffer &buf,
    const IVectorPtrs &vectors,
    const RowMeta &row_meta,
    const uint16_t selector[],
    const int64_t size,
    const uint32_t row_size_arr[],
    int64_t batch_mem_size,
    ObCompactRow **stored_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(batch_mem_size > buf.remain())) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    memset(buf.head(), 0, batch_mem_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < size; i++) {
      stored_rows[i] = reinterpret_cast<ObCompactRow *> (buf.head());
      stored_rows[i]->set_row_size(row_size_arr[i]);
      ret = post_add_row(buf, row_size_arr[i]);
    }
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < vectors.count(); col_idx ++) {
      if (nullptr == vectors.at(col_idx)) {
        ret = vector_to_nulls(row_meta, stored_rows, selector, size, col_idx);
      } else {
        vectors.at(col_idx)->to_rows(row_meta, stored_rows,
                                     selector, size, col_idx);
      }
    }
    if (OB_SUCC(ret)) {
      cnt_ += size;
    }
  }
  return ret;
}

template <>
int32_t ObTempRowStoreBase<true>::RowBlock::get_row_location(const int64_t row_id) const
{
  return *reinterpret_cast<const row_idx_t *>(reinterpret_cast<const char *>(this) + raw_size_
                                              - (row_id - block_id_ + 1) * ROW_INDEX_SIZE);
}

template<bool RA>
int ObTempRowStoreBase<RA>::RowBlock::get_next_batch(ObTempRowStoreBase::ReaderBase &iter,
                                             const int64_t max_rows,
                                             int64_t &read_rows,
                                             const ObCompactRow **stored_rows) const
{
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (RA) {
    iter.read_pos_ = get_row_location(iter.cur_blk_id_);
  }
  for (read_rows = 0; read_rows < max_rows && iter.cur_blk_id_ < end(); ++read_rows) {
    ++iter.cur_blk_id_;
    stored_rows[read_rows] = reinterpret_cast<const ObCompactRow *>(iter.read_pos_ + payload_);
    iter.read_pos_ += stored_rows[read_rows]->get_row_size();
  }
  if (0 == read_rows) {
    ret = OB_ITER_END;
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::RowBlock::calc_row_size(const common::ObIArray<ObExpr*> &exprs,
                                            const RowMeta &row_meta,
                                            ObEvalCtx &ctx,
                                            int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  const int64_t fixed_row_size = row_meta.get_row_fixed_size();
  size += fixed_row_size;
  int64_t batch_idx = ctx.get_batch_idx();
  const bool reordered = row_meta.fixed_expr_reordered();
  for (int64_t col_idx = 0; col_idx < exprs.count(); col_idx++) {
    if (reordered && row_meta.project_idx(col_idx) < row_meta.fixed_cnt_) {
      continue;
    }
    ObIVector *vec = exprs.at(col_idx)->get_vector(ctx);
    VectorFormat format = vec->get_format();
    if (VEC_DISCRETE == format) {
      ObDiscreteBase *disc_vec = static_cast<ObDiscreteBase *>(vec);
      if (!disc_vec->is_null(batch_idx)) {
        ObLength *lens = disc_vec->get_lens();
        size += lens[batch_idx];
      }
    } else if (VEC_CONTINUOUS == format) {
      ObContinuousBase *cont_vec = static_cast<ObContinuousBase*>(vec);
      uint32_t *offsets = cont_vec->get_offsets();
      size += (offsets[batch_idx + 1] - offsets[batch_idx]);
    } else if (is_uniform_format(format)) {
      ObUniformBase *uni_vec = static_cast<ObUniformBase *>(vec);
      ObDatum *datums = uni_vec->get_datums();
      const uint64_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT64_MAX;
      size += datums[batch_idx & idx_mask].len_;
    } else if (VEC_FIXED == format) {
      ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase*>(vec);
      size += fixed_vec->get_length();
    }
    LOG_DEBUG("calc row size", K(col_idx), K(size), K(format));
  }
  return ret;
}

// calc need size for this batch
template<bool RA>
int ObTempRowStoreBase<RA>::RowBlock::calc_rows_size(const IVectorPtrs &vectors,
                                             const RowMeta &row_meta,
                                             const uint16_t selector[],
                                             const int64_t size,
                                             uint32_t row_size_arr[],
                                             const common::ObIArray<int64_t> *dup_length) {
  int ret = OB_SUCCESS;
  const int64_t fixed_row_size = row_meta.get_row_fixed_size();
  const bool reordered = row_meta.fixed_expr_reordered();
  for (int64_t i = 0; i < size; i++) {
    row_size_arr[i] = fixed_row_size;
  }
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < vectors.count(); col_idx++) {
    ObIVector *vec = vectors.at(col_idx);
    if (nullptr == vec) {
      if (OB_ISNULL(dup_length)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get length", K(ret));
      } else {
        for (int i = 0; i < size; i++) {
          row_size_arr[i] += dup_length->at(col_idx);
        }
      }
      continue;
    }
    if (reordered && row_meta.project_idx(col_idx) < row_meta.fixed_cnt_) {
      continue;
    }
    VectorFormat format = vec->get_format();
    if (VEC_DISCRETE == format) {
      ObDiscreteBase *disc_vec = static_cast<ObDiscreteBase *>(vec);
      ObLength *lens = disc_vec->get_lens();
      for (int64_t i = 0; i < size; i++) {
        if (!disc_vec->is_null(selector[i])) {
          row_size_arr[i] += lens[selector[i]];
        }
      }
    } else if (VEC_CONTINUOUS == format) {
      ObContinuousBase *cont_vec = static_cast<ObContinuousBase*>(vec);
      uint32_t *offsets = cont_vec->get_offsets();
      for (int64_t i = 0; i < size; i++) {
        row_size_arr[i] += offsets[selector[i] + 1] - offsets[selector[i]];
      }
    } else if (is_uniform_format(format)) {
      ObUniformBase *uni_vec = static_cast<ObUniformBase *>(vec);
      ObDatum *datums = uni_vec->get_datums();
      const uint16_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT16_MAX;
      for (int64_t i = 0; i < size; i++) {
        if (!datums[selector[i] & idx_mask].is_null()) {
          row_size_arr[i] += datums[selector[i] & idx_mask].len_;
        }
      }
    } else if (VEC_FIXED == format) {
      ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase*>(vec);
      for (int64_t i = 0; i < size; i++) {
        row_size_arr[i] += fixed_vec->get_length();
      }
    }
  }

  return ret;
}

template<>
int ObTempRowStoreBase<true>::RowBlock::get_row(const int64_t row_id, const ObCompactRow *&sr) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!contain(row_id))) {
    ret = OB_INDEX_OUT_OF_RANGE;
    LOG_WARN("invalid index", K(ret), K(row_id), K(*this));
  } else {
    int32_t location = get_row_location(row_id);
    const ObCompactRow *row = reinterpret_cast<const ObCompactRow *>(&payload_[location]);
    sr = row;
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::DtlRowBlock::calc_rows_size(const IVectorPtrs &vectors,
                                                const RowMeta &row_meta,
                                                const ObBatchRows &brs,
                                                uint32_t row_size_arr[]) {
  int ret = OB_SUCCESS;
  const int64_t fixed_row_size = row_meta.get_row_fixed_size();
  const bool reordered = row_meta.fixed_expr_reordered();
  for (int64_t i = 0; i < brs.size_; i++) {
    row_size_arr[i] = fixed_row_size;
  }
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < vectors.count(); col_idx++) {
    ObIVector *vec = vectors.at(col_idx);
    if (reordered && row_meta.project_idx(col_idx) < row_meta.fixed_cnt_) {
      continue;
    }
    VectorFormat format = vec->get_format();
    if (VEC_DISCRETE == format) {
      ObDiscreteBase *disc_vec = static_cast<ObDiscreteBase *>(vec);
      ObLength *lens = disc_vec->get_lens();
      for (int64_t i = 0; i < brs.size_; i++) {
        if (brs.skip_->at(i)) {
          continue;
        }
        if (!disc_vec->is_null(i)) {
          row_size_arr[i] += lens[i];
        }
      }
    } else if (VEC_CONTINUOUS == format) {
      ObContinuousBase *cont_vec = static_cast<ObContinuousBase*>(vec);
      uint32_t *offsets = cont_vec->get_offsets();
      for (int64_t i = 0; i < brs.size_; i++) {
        if (brs.skip_->at(i)) {
          continue;
        }
        row_size_arr[i] += offsets[i + 1] - offsets[i];
      }
    } else if (is_uniform_format(format)) {
      ObUniformBase *uni_vec = static_cast<ObUniformBase *>(vec);
      ObDatum *datums = uni_vec->get_datums();
      const uint16_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT16_MAX;
      for (int64_t i = 0; i < brs.size_; i++) {
        if (brs.skip_->at(i)) {
          continue;
        }
        if (!datums[i & idx_mask].is_null()) {
          row_size_arr[i] += datums[i & idx_mask].len_;
        }
      }
    } else if (VEC_FIXED == format) {
      ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase*>(vec);
      for (int64_t i = 0; i < brs.size_; i++) {
        if (brs.skip_->at(i)) {
          continue;
        }
        row_size_arr[i] += fixed_vec->get_length();
      }
    }
  }

  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::ReaderBase::init(ObTempRowStoreBase *store)
{
  reset();
  row_store_ = store;
  return BlockReader::init(store);
}

template<bool RA>
int ObTempRowStoreBase<RA>::ReaderBase::get_next_batch(const ObExprPtrIArray &exprs,
                                             ObEvalCtx &ctx,
                                             const int64_t max_rows,
                                             int64_t &read_rows,
                                             const ObCompactRow **stored_rows)
{
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (OB_FAIL(row_store_->init_batch_ctx())) {
    LOG_WARN("init batch ctx failed", K(ret));
  } else if (OB_UNLIKELY(NULL == cur_blk_ || !cur_blk_->contain(cur_blk_id_))) {
    if (OB_FAIL(next_block())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next block", K(ret));
      }
    }
  }
  for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
    ObExpr *e = exprs.at(i);
    ObIVector *vec = NULL;
    if (OB_FAIL(e->init_vector_default(ctx, max_rows))) {
      LOG_WARN("fail to init vector", K(ret));
    } else {
      vec = e->get_vector(ctx);
      row_store_->batch_ctx_->vectors_.at(i) = vec;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_next_batch(row_store_->batch_ctx_->vectors_, max_rows, read_rows,
                                    stored_rows))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to get next batch from store", K(ret));
    }
  } else {
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
      exprs.at(i)->set_evaluated_projected(ctx);
    }
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::ReaderBase::get_next_batch(const IVectorPtrs &vectors,
                                             const int64_t max_rows,
                                             int64_t &read_rows,
                                             const ObCompactRow **stored_rows)
{
  int ret = OB_SUCCESS;
  const ObCompactRow **rows = (NULL == stored_rows)
                           ? const_cast<const ObCompactRow**>(row_store_->batch_ctx_->rows_)
                           : stored_rows;
  begin_new_batch();
  for (read_rows = 0; read_rows < max_rows && OB_SUCC(ret); ) {
    int64_t read_rows_in_blk = 0;
    const ObCompactRow **srs = &rows[read_rows];
    if (OB_UNLIKELY(!cur_blk_->contain(cur_blk_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current block is invalid", K(ret));
    } else if (OB_FAIL(cur_blk_->get_next_batch(*this, max_rows - read_rows, read_rows_in_blk,
                                                srs))) {
      LOG_WARN("fail to get batch from block", K(ret));
    } else {
      read_rows += read_rows_in_blk;
      if (read_rows < max_rows && OB_FAIL(next_block())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next block", K(ret));
        }
      }
    }
  }
  // return success if got row
  if (OB_ITER_END == ret && read_rows != 0) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCC(ret) && read_rows > 0) {
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < vectors.count(); col_idx ++) {
      if (VEC_UNIFORM_CONST != vectors.at(col_idx)->get_format()) {
        ret = vectors.at(col_idx)->from_rows(row_store_->row_meta_, rows, read_rows, col_idx);
      }
    }
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::ReaderBase::get_next_batch(const int64_t max_rows,
                                             int64_t &read_rows,
                                             const ObCompactRow **stored_rows) {
  int ret = OB_SUCCESS;
  read_rows = 0;
  if (OB_ISNULL(stored_rows)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("stored rows is null", K(ret));
  } else if (OB_UNLIKELY(NULL == cur_blk_ || !cur_blk_->contain(cur_blk_id_))) {
    if (OB_FAIL(next_block())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("fail to get next block", K(ret));
      }
    }
  }
  begin_new_batch();
  for (read_rows = 0; read_rows < max_rows && OB_SUCC(ret); ) {
    int64_t read_rows_in_blk = 0;
    const ObCompactRow **srs = &stored_rows[read_rows];
    if (OB_UNLIKELY(!cur_blk_->contain(cur_blk_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current block is invalid", K(ret), K(cur_blk_id_), K(*cur_blk_));
    } else if (OB_FAIL(cur_blk_->get_next_batch(*this, max_rows - read_rows, read_rows_in_blk,
                                                srs))) {
      LOG_WARN("fail to get batch from block", K(ret));
    } else {
      read_rows += read_rows_in_blk;
      if (read_rows < max_rows && OB_FAIL(next_block())) {
        if (ret != OB_ITER_END) {
          LOG_WARN("fail to get next block", K(ret));
        }
      }
    }
  }
  // return success if got row
  if (OB_ITER_END == ret && read_rows != 0) {
    ret = OB_SUCCESS;
  }

  return ret;
}

template<>
int ObTempRowStoreBase<false>::Iterator::attach_rows(const ObExprPtrIArray &exprs,
                                          ObEvalCtx &ctx,
                                          const RowMeta &row_meta,
                                          const ObCompactRow **srows,
                                          const int64_t read_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < exprs.count(); col_idx ++) {
    if (OB_FAIL(exprs.at(col_idx)->init_vector_default(ctx, read_rows))) {
      LOG_WARN("fail to init vector", K(ret));
    } else {
      ObIVector *vec = exprs.at(col_idx)->get_vector(ctx);
      if (VEC_UNIFORM_CONST != vec->get_format()) {
        ret = vec->from_rows(row_meta, srows, read_rows, col_idx);
        exprs.at(col_idx)->set_evaluated_projected(ctx);
      }
    }
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::ReaderBase::next_block()
{
  int ret = OB_SUCCESS;
  const Block *read_blk = NULL;
  if (cur_blk_id_ >= get_row_cnt()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_block(cur_blk_id_, read_blk))) {
    LOG_WARN("fail to get block from store", K(ret), K(cur_blk_id_));
  } else {
    LOG_DEBUG("next block", KP(read_blk), K(*read_blk), K(read_blk->checksum()));
    cur_blk_ = static_cast<const RowBlock*>(read_blk);
    row_idx_ = 0;
    read_pos_ = 0;
  }
  return ret;
}

template<>
int ObTempRowStoreBase<true>::RAReader::get_row(const int64_t row_id, const ObCompactRow *&sr)
{
  int ret = OB_SUCCESS;
  cur_blk_id_ = row_id;
  if (OB_FAIL(next_block())) { // get the block contains cur_blk_id_
    LOG_WARN("load block failed", K(ret));
  } else if (OB_FAIL(cur_blk_->get_row(row_id, sr))) {
    LOG_WARN("get row from block failed", K(ret), K(row_id), K(*cur_blk_));
  } else if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL store row returned", K(ret));
  }
  if (ret == OB_ITER_END) {
    ret = OB_INDEX_OUT_OF_RANGE;
  }
  return ret;
}

template<>
int ObTempRowStoreBase<true>::RAReader::get_batch_rows(const ObExprPtrIArray &exprs,
                                             ObEvalCtx &ctx,
                                             const int64_t start_idx,
                                             const int64_t end_idx,
                                             int64_t &read_rows,
                                             const ObCompactRow **stored_rows) {
  int ret = OB_SUCCESS;
  cur_blk_id_ = start_idx;
  ret = get_next_batch(exprs, ctx, end_idx - start_idx, read_rows, stored_rows);
  return ret;
}

template <>
int ObTempRowStoreBase<true>::RAReader::get_batch_rows(const int64_t start_idx,
                                                       const int64_t end_idx, int64_t &read_rows,
                                                       const ObCompactRow **stored_rows)
{
  int ret = OB_SUCCESS;
  cur_blk_id_ = start_idx;
  return get_next_batch(end_idx - start_idx, read_rows, stored_rows);
}

template<bool RA>
ObTempRowStoreBase<RA>::ObTempRowStoreBase(common::ObIAllocator *alloc /* = NULL */)
   : ObTempBlockStore(alloc), col_cnt_(0), batch_ctx_(NULL),
     row_meta_(allocator_), max_batch_size_(0)
{
}

template<bool RA>
ObTempRowStoreBase<RA>::~ObTempRowStoreBase()
{
  destroy();
}

template<bool RA>
void ObTempRowStoreBase<RA>::destroy()
{
  row_meta_.reset();
  reset();
}

template<bool RA>
void ObTempRowStoreBase<RA>::reset()
{
  if (NULL != batch_ctx_) {
    batch_ctx_->~BatchCtx();
    allocator_->free(batch_ctx_);
    batch_ctx_ = NULL;
  }
  ObTempBlockStore::reset();
}

template<bool RA>
int ObTempRowStoreBase<RA>::init(const ObExprPtrIArray &exprs,
                         const int64_t max_batch_size,
                         const lib::ObMemAttr &mem_attr,
                         const int64_t mem_limit,
                         bool enable_dump,
                         uint32_t row_extra_size,
                         const common::ObCompressorType compressor_type,
                         const bool reorder_fixed_expr /*true*/,
                         const bool enable_trunc /*false*/)
{
  int ret = OB_SUCCESS;
  mem_attr_ = mem_attr;
  col_cnt_ = exprs.count();
  max_batch_size_ = max_batch_size;
  ObTempBlockStore::set_inner_allocator_attr(mem_attr);
  OZ(ObTempBlockStore::init(mem_limit, enable_dump, mem_attr.tenant_id_, mem_attr.ctx_id_, mem_attr_.label_,
                            compressor_type, enable_trunc));
  OZ(row_meta_.init(exprs, row_extra_size, reorder_fixed_expr));
  inited_ = true;
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::init(const RowMeta &row_meta,
                         const int64_t max_batch_size,
                         const lib::ObMemAttr &mem_attr,
                         const int64_t mem_limit,
                         bool enable_dump,
                         const common::ObCompressorType compressor_type,
                         const bool enable_trunc /*false*/)
{
  int ret = OB_SUCCESS;
  mem_attr_ = mem_attr;
  col_cnt_ = row_meta.col_cnt_;
  max_batch_size_ = max_batch_size;
  if (!row_meta.fixed_expr_reordered()) {
    row_meta_ = row_meta;
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", K(ret));
  } else if (OB_FAIL(row_meta_.deep_copy(row_meta, allocator_))) {
    LOG_WARN("deep copy row meta failed", K(ret));
  }
  OZ(ObTempBlockStore::init(mem_limit, enable_dump, mem_attr.tenant_id_, mem_attr.ctx_id_, mem_attr_.label_,
                            compressor_type, enable_trunc));
  inited_ = true;
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::init_batch_ctx()
{
  int ret = OB_SUCCESS;
  const int64_t max_batch_size = max_batch_size_;
  if (OB_UNLIKELY(NULL == batch_ctx_)) {
    const int64_t size = sizeof(*batch_ctx_)
        + sizeof(*batch_ctx_->row_size_array_) * max_batch_size
        + sizeof(*batch_ctx_->selector_) * max_batch_size
        + sizeof(*batch_ctx_->rows_) * max_batch_size;
    char *mem = static_cast<char *>(allocator_->alloc(size, mem_attr_));
    if (OB_UNLIKELY(max_batch_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max batch size is not positive when init batch ctx", K(ret), K(max_batch_size));
    } else if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(size), K(col_cnt_), K(max_batch_size));
    } else {
      auto begin = mem;
      batch_ctx_ = new(mem)BatchCtx();
      batch_ctx_->vectors_.set_attr(mem_attr_);
      ret = batch_ctx_->vectors_.prepare_allocate(col_cnt_);
      batch_ctx_->max_batch_size_ = max_batch_size;
      if (OB_SUCC(ret)) {
        mem += sizeof(*batch_ctx_);
        #define SET_BATCH_CTX_FIELD(X, N) \
        batch_ctx_->X = reinterpret_cast<typeof(batch_ctx_->X)>(mem); \
        mem += sizeof(*batch_ctx_->X) * N;
        SET_BATCH_CTX_FIELD(rows_, max_batch_size);
        SET_BATCH_CTX_FIELD(row_size_array_, max_batch_size);
        SET_BATCH_CTX_FIELD(selector_, max_batch_size);
        #undef SET_BATCH_CTX_FIELD
        if (mem - begin != size) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("size mismatch", K(ret), K(mem - begin), K(size), K(col_cnt_), K(max_batch_size));
        }
      }
    }
  }

  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                              const ObBatchRows &brs, int64_t &stored_rows_count,
                              ObCompactRow **stored_rows,
                              const int64_t start_pos /* 0 */)
{
  int ret = OB_SUCCESS;
  int16_t size = 0;
  if (OB_FAIL(init_batch_ctx())) {
    LOG_WARN("fail to init batch ctx", K(ret));
  } else {
    for (int64_t i = start_pos; i < brs.size_; i++) {
      if (brs.skip_->at(i)) {
        continue;
      } else {
        batch_ctx_->selector_[size++] = i;
      }
    }
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
    OZ (add_batch(batch_ctx_->vectors_, batch_ctx_->selector_, size, stored_rows));
  }
  if (OB_SUCC(ret)) {
    stored_rows_count = size;
  }
  return ret;
}

template <bool RA_ACCESS>
int ObTempRowStoreBase<RA_ACCESS>::add_batch(const common::ObIArray<ObExpr *> &exprs,
                                             ObEvalCtx &ctx, const EvalBound &bound,
                                             const ObBitVector &skip, int64_t &stored_rows_count,
                                             ObCompactRow **stored_rows)
{
  int ret = OB_SUCCESS;
  int16_t size = 0;
  if (OB_FAIL(init_batch_ctx())) {
    LOG_WARN("init batch ctx failed", K(ret));
  } else {
    for (int i = bound.start(); i < bound.end(); i++) {
      if (skip.at(i)) {
        continue;
      } else {
        batch_ctx_->selector_[size++] = i;
      }
    }
  }
  if (OB_SUCC(ret) && size > 0) {
    for (int i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      ObExpr *e = exprs.at(i);
      ObIVector *vec = nullptr;
      if (OB_FAIL(e->eval_vector(ctx, skip, bound))) {
        LOG_WARN("eval vector failed", K(ret));
      } else {
        vec = e->get_vector(ctx);
        batch_ctx_->vectors_.at(i) = vec;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(add_batch(batch_ctx_->vectors_, batch_ctx_->selector_, size, stored_rows))) {
      LOG_WARN("add batch rows failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    stored_rows_count = size;
  }
  return ret;
}

template <>
int ObTempRowStoreBase<false>::try_add_batch(const common::ObIArray<ObExpr *> &exprs,
                                             ObEvalCtx *ctx, const int64_t batch_size,
                                             const int64_t memory_limit, bool &batch_added)
{
  int ret = OB_SUCCESS;
  int64_t rows_size = 0;
  CK(is_inited());
  OZ(init_batch_ctx());
  if (OB_SUCC(ret)) {
    for (int64_t i = 0 ; i < batch_size; i ++) {
      batch_ctx_->selector_[i] = i;
    }
    ObEvalCtx::TempAllocGuard alloc_guard(*ctx);
    ObIAllocator &alloc = alloc_guard.get_allocator();
    void *mem = nullptr;
    if (OB_ISNULL(mem = alloc.alloc(ObBitVector::memory_size(batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip", K(ret), K(batch_size));
    } else {
      ObBitVector *skip = to_bit_vector(mem);
      skip->reset(batch_size);
      for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
        ObExpr *e = exprs.at(i);
        ObIVector *vec = NULL;
        if (OB_FAIL(e->eval_vector(*ctx, *skip, batch_size, true/*all_rows_active*/))) {
          LOG_WARN("evaluate batch failed", K(ret));
        } else {
          vec = e->get_vector(*ctx);
          batch_ctx_->vectors_.at(i) = vec;
        }
      } // for end
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(0 == batch_size)) {
    // no rows, do nothing
  } else if (OB_FAIL(RowBlock::calc_rows_size(batch_ctx_->vectors_,
                                              row_meta_,
                                              batch_ctx_->selector_,
                                              batch_size,
                                              batch_ctx_->row_size_array_))) {
    LOG_WARN("fail to calc rows size", K(ret));
  } else {
    for (int64_t i = 0; i < batch_size; i++) {
      rows_size += batch_ctx_->row_size_array_[i];
    }
  }
  if (OB_FAIL(ret)) {
  } else if (rows_size + get_mem_used() > memory_limit) {
    batch_added = false;
  } else {
    int64_t count = 0;
    if (OB_FAIL(add_batch(batch_ctx_->vectors_,
                          batch_ctx_->selector_,
                          batch_size,
                          batch_ctx_->rows_))) {
      LOG_WARN("failed to add batch", K(ret));
    } else {
      batch_added = true;
    }
  }
  LOG_DEBUG("try add batch", K(batch_added), K(memory_limit), K(batch_size), K(rows_size));

  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::add_row(const ObCompactRow *src_row, ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_batch_ctx())) {
    LOG_WARN("init batch ctx failed", K(ret));
  } else if (OB_ISNULL(src_row) || src_row->get_row_size() <= 0) {
  } else if (OB_FAIL(ensure_write_blk(src_row->get_row_size()))) {
    LOG_WARN("ensure write block failed", K(ret), K(src_row->get_row_size()));
  } else if (OB_FAIL(cur_blk()->add_row(blk_buf_, src_row, stored_row))) {
    LOG_WARN("fail to add row", K(ret));
  } else {
    block_id_cnt_ += 1;
    inc_mem_used(src_row->get_row_size());
  }
  return ret;
}


template<>
int ObTempRowStoreBase<false>::try_add_batch(const ObCompactRow **stored_rows,
                                  const int64_t batch_size,
                                  const int64_t memory_limit,
                                  bool &batch_added)
{
  int ret = OB_SUCCESS;
  int64_t rows_size = 0;
  batch_added = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    rows_size += stored_rows[i]->get_row_size();
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (rows_size + get_mem_used() > memory_limit) {
    batch_added = false;
  } else {
    ObCompactRow *res_row = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (OB_FAIL(add_row(stored_rows[i], res_row))) {
        LOG_WARN("add row failed", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      batch_added = true;
    }
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::add_row(const common::ObIArray<ObExpr*> &exprs,
                            ObEvalCtx &ctx,
                            ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  int64_t row_size = 0;
  const int64_t idx_size = RA ? ROW_INDEX_SIZE : 0;
  if (OB_FAIL(init_batch_ctx())) {
    LOG_WARN("init batch ctx failed", K(ret));
  } else if (OB_FAIL(RowBlock::calc_row_size(exprs, row_meta_, ctx, row_size))) {
    LOG_WARN("fail to calc row size", K(ret));
  } else if (OB_FAIL(ensure_write_blk(row_size + idx_size))) {
    LOG_WARN("ensure write block failed", K(ret), K(row_size + idx_size));
  } else if (OB_FAIL(cur_blk()->add_row(blk_buf_, exprs, row_meta_, ctx, stored_row))) {
    LOG_WARN("fail to add row", K(ret));
  } else {
    block_id_cnt_ += 1;
    inc_mem_used(row_size + idx_size);
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::add_row(const common::ObIArray<ObExpr *> &exprs, const int64_t batch_idx,
                            ObEvalCtx &ctx, ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_guard(ctx);
  batch_guard.set_batch_idx(batch_idx);
  return add_row(exprs, ctx, stored_row);
}

template<bool RA>
int ObTempRowStoreBase<RA>::add_batch(const IVectorPtrs &vectors,
                              const uint16_t selector[],
                              const int64_t size,
                              ObCompactRow **stored_rows,
                              const ObIArray<int64_t> *dup_length)
{
  int ret = OB_SUCCESS;
  int64_t batch_mem_size = 0;
  CK(is_inited());
  OZ(init_batch_ctx());
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(0 == size)) {
    // no rows, do nothing
  } else if (OB_FAIL(RowBlock::calc_rows_size(vectors, row_meta_, selector, size,
                                              batch_ctx_->row_size_array_, dup_length))) {
    LOG_WARN("fail to calc rows size", K(ret));
  } else {
    ObCompactRow **rows = (NULL == stored_rows) ? batch_ctx_->rows_ : stored_rows;
    for (int64_t i = 0; i < size; i++) {
      batch_mem_size += batch_ctx_->row_size_array_[i];
    }
    batch_mem_size += size * (RA ? ROW_INDEX_SIZE : 0);
    if (OB_FAIL(ensure_write_blk(batch_mem_size))) {
      LOG_WARN("ensure write block failed", K(ret));
    } else if (OB_FAIL(cur_blk()->add_batch(blk_buf_, vectors, row_meta_, selector, size,
                                            batch_ctx_->row_size_array_, batch_mem_size, rows))) {
      LOG_WARN("fail to add batch", K(ret));
    } else {
      block_id_cnt_ += size;
      inc_mem_used(batch_mem_size);
    }
  }
  return ret;
}

template<bool RA>
int ObTempRowStoreBase<RA>::RowBlock::vector_to_nulls(const sql::RowMeta &row_meta,
                                              sql::ObCompactRow **stored_rows,
                                              const uint16_t *selector, const int64_t size,
                                              const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < size; i++) {
    int64_t row_idx = selector[i];
    stored_rows[i]->set_null(row_meta, col_idx);
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTempRowStoreBase<RA>, template <bool RA>)
{
  int ret = ObTempBlockStore::serialize(buf, buf_len, pos);
  LST_DO_CODE(OB_UNIS_ENCODE,
              col_cnt_,
              row_meta_,
              max_batch_size_);
  return ret;
}


OB_DEF_DESERIALIZE(ObTempRowStoreBase<RA>, template <bool RA>)
{
  int ret = ObTempBlockStore::deserialize(buf, data_len, pos);
  if (OB_SUCC(ret)) {
    mem_attr_.tenant_id_ = get_tenant_id();
    mem_attr_.label_ = "ObTempStoreDE";
    mem_attr_.ctx_id_ = get_mem_ctx_id();
  }
  LST_DO_CODE(OB_UNIS_DECODE,
              col_cnt_,
              row_meta_,
              max_batch_size_);
  // in das, TempRowStore is a member of Result class, but may not used it,
  // in this scene, when deserialize, max_batch_size_ is 0
  if (max_batch_size_ > 0) {
    OZ (init_batch_ctx());
  }

  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTempRowStoreBase<RA>, template <bool RA>)
{
  int64_t len = ObTempBlockStore::get_serialize_size();
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              col_cnt_,
              row_meta_,
              max_batch_size_);
  return len;
}

template class ObTempRowStoreBase<true>;
template class ObTempRowStoreBase<false>;

#undef ROW_BLK
} // end namespace sql
} // end namespace oceanbase
