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

#define USING_LOG_PREFIX SQL_DTL
#include "ob_dtl_vectors_buffer.h"
#include "share/vector/ob_uniform_base.h"
#include "share/vector/ob_discrete_base.h"
#include "share/vector/ob_continuous_base.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace sql {
namespace dtl {


ObDtlVectorsBuffer* ObDtlVectorsBlock::get_buffer()
{
  return static_cast<ObDtlVectorsBuffer*>(
          static_cast<void*>(payload_ + blk_size_ - sizeof(ObDtlVectorsBlock) - sizeof(ObDtlVectorsBuffer)));
}

int32_t ObDtlVectorsBlock::data_size() { return get_buffer()->data_size(); }

int32_t ObDtlVectorsBlock::remain() { return get_buffer()->remain(); }

int ObDtlVectorsBuffer::init(char *buf, const int32_t buf_size)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    data_ = buf;
    cur_pos_ = sizeof(ObDtlVectorsBlock);
    cap_ = buf_size - sizeof(ObDtlVectorsBuffer);
    memset(cols_seg_pos_, 0, sizeof(int32_t) * MAX_COL_CNT);
    memset(cols_seg_start_pos_, 0, sizeof(int32_t) * MAX_COL_CNT);
  }
  return ret;
}

int ObDtlVectorsBuffer::init_vector_buffer(void* mem, const int32_t size, ObDtlVectorsBlock *&block)
{
  int ret = OB_SUCCESS;
  block = new(mem)ObDtlVectorsBlock;
  block->set_block_size(size);
  ObDtlVectorsBuffer* blkbuf = new(block->get_buffer())ObDtlVectorsBuffer;
  if (OB_FAIL(blkbuf->init(static_cast<char *> (mem), size))) {
    LOG_WARN("init shrink buffer failed", K(ret));
  } else if (block->blk_size_ != blkbuf->capacity() + sizeof(ObDtlVectorsBuffer)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to check buffer state", K(ret), K(block->blk_size_), K(blkbuf->capacity()));
  } else if (block != blkbuf->block_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to check buffer state", K(ret), KP(block), KP(blkbuf));
  } else {
    block->rows_ = 0;
  }
  return ret;
}

int ObDtlVectorsBuffer::init_row_meta(const ObExprPtrIArray &exprs)
{
  int ret = OB_SUCCESS;
  ret = meta_.init(exprs, 0, false);
  CK (meta_.col_cnt_ > 0);
  return ret;
}

int ObDtlVectorsBuffer::alloc_segmant(int32_t col_idx, int32_t data_size,
                                      int32_t fixed_len, ObVectorSegment *pre_seg)
{
  int ret = OB_SUCCESS;
  int32_t alloc_size = ObVectorSegment::calc_alloc_size(data_size, fixed_len);
  if (OB_UNLIKELY(cur_pos_ + alloc_size > cap_)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    ObVectorSegment *new_seg = new (data_ + cur_pos_) ObVectorSegment(alloc_size - sizeof(ObVectorSegment));
    new_seg->next_ = nullptr;
    cols_seg_pos_[col_idx] = cur_pos_;
    if (nullptr != pre_seg) {
      pre_seg->next_ = new_seg;
    } else {
      cols_seg_start_pos_[col_idx] = cur_pos_;
    }
    fast_advance(alloc_size);
  }
  return ret;
}

int ObDtlVectorsBuffer::append_row(const common::ObIArray<ObExpr*> &exprs, const int32_t batch_idx, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(meta_.get_var_col_cnt() <= 0)) {
    OZ (init_row_meta(exprs));
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    if (OB_ISNULL(exprs.at(i)) || exprs.count() > MAX_COL_CNT) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid expr", K(ret), K(i), K(exprs.count()));
    } else if (OB_UNLIKELY(0 == cols_seg_pos_[i])) {
      ObIVector *vec = exprs.at(i)->get_vector(ctx);
      if (OB_FAIL(alloc_segmant(i, ObVectorSegment::get_real_data_size(*vec, batch_idx),
                                ObVectorSegment::get_fixed_len(*exprs.at(i), batch_idx), nullptr))) {
        LOG_WARN("failed to alloc new segment", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObVectorSegment *seg = reinterpret_cast<ObVectorSegment *> (data_ + cols_seg_pos_[i]);
      if (OB_FAIL(seg->append_col_in_one_row(*exprs.at(i), exprs.at(i)->is_fixed_length_data_, batch_idx, ctx))) {
        if (OB_BUF_NOT_ENOUGH == ret) {
          ObIVector *vec = exprs.at(i)->get_vector(ctx);
          if (OB_FAIL(alloc_segmant(i, ObVectorSegment::get_real_data_size(*vec, batch_idx),
                                    ObVectorSegment::get_fixed_len(*exprs.at(i), batch_idx), seg))) {
            LOG_WARN("buffer is not enough", K(ret), K(i), K(cur_pos_),
                                             K(ObVectorSegment::get_real_data_size(*vec, batch_idx)),
                                             K(ObVectorSegment::get_fixed_len(*exprs.at(i), batch_idx)));
          } else {
            ObVectorSegment *seg = reinterpret_cast<ObVectorSegment *> (data_ + cols_seg_pos_[i]);
            if (OB_FAIL(seg->append_col_in_one_row(*exprs.at(i), exprs.at(i)->is_fixed_length_data_, batch_idx, ctx))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to append row", K(ret));
            }
          }
        } else {
          LOG_WARN("failed to append col", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    for (int32_t i = 0; i < exprs.count(); ++i) {
      ObVectorSegment *seg = reinterpret_cast<ObVectorSegment *> (data_ + cols_seg_pos_[i]);
      ++seg->seg_rows_;
    }
    ++block_->rows_;
  }
  return ret;
}

void ObDtlVectorsBuffer::calc_new_buffer_size(const common::ObIArray<ObExpr*> *exprs,
                                                 const int32_t batch_idx,
                                                 ObEvalCtx &ctx,
                                                 int64_t &new_block_size)
{
  new_block_size = min_buf_size();
  if (nullptr != exprs){
    for (int32_t i = 0; i < exprs->count(); ++i) {
      ObIVector *vec = exprs->at(i)->get_vector(ctx);
      int32_t real_data_size = ObVectorSegment::get_real_data_size(*vec, batch_idx);
      int32_t fixed_len = ObVectorSegment::get_fixed_len(*exprs->at(i), batch_idx);
      new_block_size += ObVectorSegment::calc_alloc_size(real_data_size, fixed_len);
    }
  }
}

bool ObDtlVectorsBuffer::can_append_row(const common::ObIArray<ObExpr*> &exprs,
                                        const int32_t batch_idx,
                                        ObEvalCtx &ctx,
                                        int64_t &new_block_size) const
{
  bool can_append = true;
  int32_t virtual_cur_pos = cur_pos_;
  new_block_size = min_buf_size();
  if (0 == block_->rows()) {
    //empty block, we are sure can append
  } else {
    //try to hold all column one by one
    for (int32_t i = 0; i < exprs.count(); ++i) {
      ObIVector *vec = exprs.at(i)->get_vector(ctx);
      ObVectorSegment *seg = reinterpret_cast<ObVectorSegment *> (data_ + cols_seg_pos_[i]);
      int32_t real_data_size = ObVectorSegment::get_real_data_size(*vec, batch_idx);
      int32_t fixed_len = ObVectorSegment::get_fixed_len(*exprs.at(i), batch_idx);
      new_block_size += ObVectorSegment::calc_alloc_size(real_data_size, fixed_len);
      if (can_append && !seg->can_append_col(real_data_size, fixed_len)) {
        // try virtual alloc new segment
        virtual_cur_pos += ObVectorSegment::calc_alloc_size(real_data_size, fixed_len);
        if (virtual_cur_pos > cap_) {
          can_append = false;
        }
      }
    }
  }
  return can_append;
}

int ObVectorSegment::append_col_in_one_row(const ObExpr &expr,
                                           const bool is_fixed_data,
                                           const int32_t batch_idx,
                                           ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIVector *vec = expr.get_vector(ctx);
  if (OB_UNLIKELY(VectorFormat::VEC_INVALID == format_)) {
    if (is_fixed_data) {
      init(VEC_FIXED, expr.res_buf_len_);
    } else {
      init(VEC_CONTINUOUS, 0/*CONTINUOUS do not use len*/);
    }
  }
  if (seg_rows_ >= MAX_ROW_CNT) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (VectorFormat::VEC_FIXED == format_) {
    if (OB_LIKELY(data_pos_ + fixed_len_ < cap_)) {
      if (vec->is_null(batch_idx)) {
        nulls_->set(seg_rows_);
      } else {
        memcpy(payload_ + data_pos_, vec->get_payload(batch_idx), fixed_len_);
      }
      data_pos_ += fixed_len_;
    } else {
      ret = OB_BUF_NOT_ENOUGH;
    }
  } else {
    int32_t len = vec->get_length(batch_idx);
    if (vec->is_null(batch_idx)) {
      if (data_pos_ > offset_pos_ - sizeof(uint32_t)) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        nulls_->set(seg_rows_);
        set_offset(data_pos_);
      }
    } else if (data_pos_ + len > offset_pos_ - sizeof(uint32_t)) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      memcpy(payload_ + data_pos_, vec->get_payload(batch_idx), len);
      data_pos_ += len;
      set_offset(data_pos_);
    }
  }
  return ret;
}

void ObVectorSegment::init(const VectorFormat format, const int32_t len)
{
  data_pos_ = 0;
  format_ = format;
  nulls_ = to_bit_vector(payload_ + data_pos_);
  nulls_->reset(MAX_ROW_CNT);
  data_pos_ += ObBitVector::memory_size(MAX_ROW_CNT);
  data_start_pos_ = data_pos_;
  if (VectorFormat::VEC_FIXED == format_) {
    fixed_len_ = len;
  } else {
    offset_pos_ = cap_;
    set_offset(data_pos_);
  }
}

bool ObVectorSegment::can_append_col(int32_t real_data_size, int32_t fixed_len) const
{
  bool can_append = true;
  int32_t need_size = real_data_size + fixed_len;
  if (seg_rows_ >= MAX_ROW_CNT || data_pos_ + need_size > offset_pos_ - sizeof(uint32_t)) {
    can_append = false;
  }
  return can_append;
}

int32_t ObVectorSegment::calc_alloc_size(int32_t data_size, int32_t fixed_len)
{
  int32_t min_seg_size = data_size + ObVectorSegment::get_bit_vector_size()
                          + sizeof(uint32_t) /*for continuous*/
                          + fixed_len
                          + sizeof(ObVectorSegment);
  int32_t alloc_size = std::max(static_cast<int32_t> (ObDtlVectorsBuffer::DEFAULT_BLOCK_SIZE),
                                min_seg_size);
  return alloc_size;
}

int ObDtlVectors::init(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    inited_ = true;
    read_rows_ = 0;
    if (OB_ISNULL(buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get buffer", K(ret));
    } else {
      int32_t pos = 0;
      magic_ = reinterpret_cast<int32_t *> (buf_ + pos);
      *magic_ = ObDtlVectorsBuffer::MAGIC;
      pos += sizeof(int32_t);
      col_cnt_ = reinterpret_cast<int32_t *> (buf_ + pos);
      *col_cnt_ = exprs.count();
      pos += sizeof(int32_t);
      row_cnt_ = reinterpret_cast<int32_t *> (buf_ + pos);
      *row_cnt_ = 0;
      pos += sizeof(int32_t);
      infos_ = reinterpret_cast<VectorInfo *> (buf_ + pos);
      pos += *col_cnt_ * sizeof(VectorInfo);
      // calc max row cnt && divide remain area
      int32_t remain_size = mem_limit_ - pos;
      int32_t row_size = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
        VectorInfo &info = infos_[i];
        info.format_ = VectorFormat::VEC_FIXED;
        ObIVector *vec = exprs.at(i)->get_vector(ctx);
        if (VectorFormat::VEC_FIXED ==  vec->get_format()) {
          info.fixed_len_ = static_cast<ObFixedLengthBase *> (vec)->get_length();
          row_size += info.fixed_len_;
        } else if (VectorFormat::VEC_UNIFORM == vec->get_format()
                   || VectorFormat::VEC_UNIFORM_CONST == vec->get_format()) {
          info.fixed_len_ = exprs.at(i)->res_buf_len_;
          row_size += info.fixed_len_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid fixed expr", K(vec->get_format()), K(ret));
        }
      }
      if (OB_SUCC(ret) && *col_cnt_ > 0) {
        if (row_size != 0){
          row_size_ = row_size;
          row_limit_ = remain_size / row_size;
          if (row_size * row_limit_ + *col_cnt_ * ObBitVector::memory_size(row_limit_) > remain_size) {
            row_limit_ = (remain_size - *col_cnt_ * ObBitVector::memory_size(row_limit_)) / row_size;
          }
          if (row_limit_ <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to calc max row cnt", K(remain_size), K(row_size), K(ret));
          } else {
            for (int64_t i = 0; i < exprs.count(); ++i) {
              VectorInfo &info = infos_[i];
              info.nulls_offset_ = pos;
              ObBitVector *nulls = to_bit_vector(buf_ + pos);
              nulls->reset(row_limit_);
              pos += ObBitVector::memory_size(row_limit_);
              info.data_offset_ = pos;
              pos += row_limit_ * info.fixed_len_;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected row_size", K(row_size), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDtlVectors::init()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    inited_ = true;
    read_rows_ = 0;
    if (OB_ISNULL(buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get buffer", K(ret));
    } else {
      int32_t pos = 0;
      magic_ = reinterpret_cast<int32_t *> (buf_ + pos);
      *magic_ = ObDtlVectorsBuffer::MAGIC;
      pos += sizeof(int32_t);
      col_cnt_ = reinterpret_cast<int32_t *> (buf_ + pos);
      *col_cnt_ = 0;
      pos += sizeof(int32_t);
      row_cnt_ = reinterpret_cast<int32_t *> (buf_ + pos);
      *row_cnt_ = 0;
      pos += sizeof(int32_t);
    }
  }
  return ret;
}

int ObDtlVectors::decode()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get buf", K(ret));
  } else {
    int32_t pos = 0;
    magic_ = reinterpret_cast<int32_t *> (buf_ + pos);
    pos += sizeof(int32_t);
    if (OB_UNLIKELY(ObDtlVectorsBuffer::MAGIC != *magic_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("magic check failed", K(ret), K(*magic_));
    } else if (FALSE_IT(col_cnt_ = reinterpret_cast<int32_t *> (buf_ + pos))) {
    } else if (FALSE_IT(pos += sizeof(int32_t))) {
    } else if (FALSE_IT(row_cnt_ = reinterpret_cast<int32_t *> (buf_ + pos))) {
    } else if (FALSE_IT(pos += sizeof(int32_t))) {
    } else if (OB_UNLIKELY(*col_cnt_ < 0 || *row_cnt_ < 0
                            || *col_cnt_ > ObDtlVectorsBuffer::MAX_COL_CNT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected row cnt or col cnt", K(ret), K(*col_cnt_), K(*row_cnt_));
    } else if (0 == *col_cnt_ || 0 == *row_cnt_) {
      //do nothing
    } else if (FALSE_IT(infos_ = reinterpret_cast<VectorInfo *> (buf_ + pos))) {
    } else if (FALSE_IT(pos += *col_cnt_ * sizeof(VectorInfo))) {
    } else {
      read_rows_ = 0;
      inited_ = true;
    }
  }
  return ret;
}

int ObDtlVectors::append_row(const common::ObIArray<ObExpr*> &exprs, const int32_t batch_idx, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (!inited_ && OB_FAIL(init(exprs, ctx))) {
    LOG_WARN("failed to init vector", K(ret));
  } else if (*col_cnt_ > 0 && *row_cnt_ >= row_limit_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    for (int64_t i = 0; i < exprs.count(); ++i) {
      if (exprs.at(i)->get_vector(ctx)->is_null(batch_idx)) {
        get_nulls(i)->set(*row_cnt_);
      } else if (exprs.at(i)->get_format(ctx) == VEC_UNIFORM
                && nullptr == static_cast<ObUniformBase *>
                                (exprs.at(i)->get_vector(ctx))->get_datums()[batch_idx].ptr_) {
        // TODO : to be remove
      } else {
        memcpy(get_data(i) + infos_[i].fixed_len_ * (*row_cnt_), exprs.at(i)->get_vector(ctx)->get_payload(batch_idx), infos_[i].fixed_len_);
      }
    }
    ++(*row_cnt_);
  }
  return ret;
}

int ObDtlVectors::append_batch(const ObIArray<ObExpr*> &exprs, const ObIArray<ObIVector *> &vectors,
                               const uint16_t selector[], const int64_t size,
                               ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (!inited_ && OB_FAIL(init(exprs, ctx))) {
    LOG_WARN("failed to init vector", K(ret));
  } else if (*col_cnt_ > 0 && *row_cnt_ + size >= row_limit_) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    for (int64_t col_idx = 0; col_idx < exprs.count(); ++col_idx) {
      char *dst_data = get_data(col_idx);
      int64_t fixed_len = infos_[col_idx].fixed_len_;
      int64_t virtual_row_cnt = *row_cnt_;
      switch (exprs.at(col_idx)->get_format(ctx)) {
        case VEC_FIXED : {
          ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *> (vectors.at(col_idx));
          if (0 == fixed_len % 8) {
            for (int64_t i = 0; i < size; ++i) {
              int64_t row_idx = selector[i];
              if (fixed_vec->get_nulls()->at(row_idx)) {
                get_nulls(col_idx)->set(virtual_row_cnt);
              } else {
                int64_t base_offset = fixed_len * (virtual_row_cnt);
                for (int64_t i = 0; i < fixed_len / 8; ++i) {
                  *(reinterpret_cast<int64_t *> (dst_data + sizeof(int64_t) * i + base_offset))
                  = *(reinterpret_cast<int64_t *> (fixed_vec->get_data() + sizeof(int64_t) * i + fixed_len * row_idx));
                }
              }
              ++virtual_row_cnt;
            }
          } else {
            for (int64_t i = 0; i < size; ++i) {
              int64_t row_idx = selector[i];
              if (fixed_vec->get_nulls()->at(row_idx)) {
                get_nulls(col_idx)->set(virtual_row_cnt);
              } else {
                memcpy(dst_data + fixed_len * (virtual_row_cnt),
                      fixed_vec->get_data() + fixed_len * row_idx, fixed_len);
              }
              ++virtual_row_cnt;
            }
          }
          break;
        }
        case VEC_DISCRETE : {
          ObDiscreteBase *discrete_vec = static_cast<ObDiscreteBase *> (vectors.at(col_idx));
          for (int64_t i = 0; i < size; ++i) {
            int64_t row_idx = selector[i];
            if (discrete_vec->get_nulls()->at(row_idx)) {
              get_nulls(col_idx)->set(virtual_row_cnt);
            } else {
              memcpy(dst_data + fixed_len * (virtual_row_cnt),
                     discrete_vec->get_ptrs()[row_idx], fixed_len);
            }
            ++virtual_row_cnt;
          }
          break;
        }
        case VEC_CONTINUOUS : {
          ObContinuousBase *continuous_vec = static_cast<ObContinuousBase *> (vectors.at(col_idx));
          for (int64_t i = 0; i < size; ++i) {
            int64_t row_idx = selector[i];
            if (continuous_vec->get_nulls()->at(row_idx)) {
              get_nulls(col_idx)->set(virtual_row_cnt);
            } else {
              memcpy(dst_data + fixed_len * (virtual_row_cnt),
                    continuous_vec->get_data() + continuous_vec->get_offsets()[row_idx],
                    fixed_len);
            }
            ++virtual_row_cnt;
          }
          break;
        }
        case VEC_UNIFORM : {
          ObUniformBase *uniform_vec = static_cast<ObUniformBase *> (vectors.at(col_idx));
          if (0 == fixed_len % 8) {
            for (int64_t i = 0; i < size; ++i) {
              int64_t row_idx = selector[i];
              ObDatum &cell = uniform_vec->get_datums()[row_idx];
              if (cell.is_null()) {
                get_nulls(col_idx)->set(virtual_row_cnt);
              } else {
                int64_t base_offset = fixed_len * (virtual_row_cnt);
                for (int64_t i = 0; i < fixed_len / 8; ++i) {
                  *(reinterpret_cast<int64_t *> (dst_data + base_offset + sizeof(int64_t) * i))
                  = *(reinterpret_cast<const int64_t *> (cell.ptr_ + sizeof(int64_t) * i));
                }
              }
              ++virtual_row_cnt;
            }
          } else {
            for (int64_t i = 0; i < size; ++i) {
              int64_t row_idx = selector[i];
              ObDatum &cell = uniform_vec->get_datums()[row_idx];
              if (cell.is_null()) {
                get_nulls(col_idx)->set(virtual_row_cnt);
              } else {
                memcpy(dst_data + fixed_len * (virtual_row_cnt), cell.ptr_, fixed_len);
              }
              ++virtual_row_cnt;
            }
          }
          break;
        }
        case VEC_UNIFORM_CONST : {
          ObUniformBase *uniform_vec = static_cast<ObUniformBase *> (vectors.at(col_idx));
          const int64_t row_idx = 0;
          for (int64_t i = 0; i < size; ++i) {
            ObDatum &cell = uniform_vec->get_datums()[row_idx];
            if (cell.is_null()) {
              get_nulls(col_idx)->set(virtual_row_cnt);
            } else {
              memcpy(dst_data + fixed_len * (virtual_row_cnt), cell.ptr_, fixed_len);
            }
            ++virtual_row_cnt;
          }
          break;
        }
        default :
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid format", K(ret), K(col_idx), K(exprs.at(col_idx)->get_format(ctx)));
      }
    }
    *row_cnt_ += size;
  }
  return ret;
}



}  // dtl
}  // sql
}  // oceanbase
