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

#include "sql/engine/basic/ob_compact_row.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/vector/ob_fixed_length_vector.h"

namespace oceanbase
{
namespace sql
{
int RowMeta::init(const ObExprPtrIArray &exprs,
                  const int32_t extra_size,
                  const bool enable_reorder_expr /*ture*/)
{
  int ret = OB_SUCCESS;
  reset();
  col_cnt_ = exprs.count();
  extra_size_ = extra_size;
  fixed_cnt_ = 0;
  fixed_offsets_ = NULL;
  projector_ = NULL;
  nulls_off_ = 0;
  var_offsets_off_ = nulls_off_ + ObTinyBitVector::memory_size(col_cnt_);
  if (enable_reorder_expr) {
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
      ObExpr *expr = exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null", K(ret));
      } else if (expr->is_fixed_length_data_) {
        fixed_cnt_++;
      }
    }
  }
  const int64_t var_col_cnt = get_var_col_cnt();
  extra_off_ = var_offsets_off_;
  if (var_col_cnt > 0) {
    // If there is no variable-length data, there is no need to store the variable-length offset.
    extra_off_ += (var_col_cnt + 1) * sizeof(int32_t);
  }
  fix_data_off_ = extra_off_ + extra_size_;
  if (OB_SUCC(ret) && fixed_cnt_ > 0) {
    ObDataBuffer local_alloc(buf_, MAX_LOCAL_BUF_LEN);
    ObIAllocator *alloc = use_local_allocator() ? &local_alloc : allocator_;
    if (OB_ISNULL(alloc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret), K(lbt()));
    } else if (OB_ISNULL(projector_ =
        static_cast<int32_t *>(alloc->alloc(sizeof(int32_t) * col_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc projector failed", K(ret), K(col_cnt_));
    } else if (OB_ISNULL(fixed_offsets_ =
        static_cast<int32_t *>(alloc->alloc(sizeof(int32_t) * (fixed_cnt_ + 1))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc fixed_offsets_ failed", K(ret), K(col_cnt_));
    } else {
      int64_t project_idx = fixed_cnt_;
      int64_t fixed_idx = 0;
      fixed_offsets_[0] = fix_data_off_;
      for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
        ObExpr *expr = exprs.at(i);
        if (expr->is_fixed_length_data_) {
          fixed_offsets_[fixed_idx + 1] = fixed_offsets_[fixed_idx] + expr->get_fixed_length();
          projector_[i] = fixed_idx++;
        } else {
          projector_[i] = project_idx++;
        }
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  if (OB_SUCC(ret)) {
    // The variable-length data is at the end of the fixed-length data. there is no fixed-length
    // data, it is after fix_data_off_. If it exists, it is the value of the last fixed offset.
    if (fixed_cnt_ > 0) {
      var_data_off_ = fixed_offsets_[fixed_cnt_];
    } else {
      var_data_off_ = fix_data_off_;
    }
  }
  return ret;
}

void RowMeta::reset()
{
  if (!use_local_allocator() && NULL != allocator_) {
    if (NULL != fixed_offsets_) {
      allocator_->free(fixed_offsets_);
    }
    if (NULL != projector_) {
      allocator_->free(projector_);
    }
  }
  fixed_offsets_ = NULL;
  projector_ = NULL;
}

int32_t RowMeta::get_row_fixed_size(const int64_t col_cnt,
                                    const int64_t fixed_payload_len,
                                    const int64_t extra_size,
                                    const bool enable_reorder_expr /*false*/)
{
  // TODO shengle init fixed info
  int32_t row_fixed_size = sizeof(RowHeader) + fixed_payload_len + extra_size;
  row_fixed_size += ObBitVector::memory_size(col_cnt);
  if (!enable_reorder_expr) {
    row_fixed_size += (col_cnt + 1) * sizeof(int32_t);
  }
  return row_fixed_size;
}

int64_t ObCompactRow::calc_max_row_size(const ObExprPtrIArray &exprs, int32_t extra_size)
{
  int64_t res = 0;
  ObArenaAllocator alloc;
  RowMeta tmp_meta(&alloc);
  int ret = OB_SUCCESS;
  if (OB_FAIL(tmp_meta.init(exprs, extra_size))) {
    res += INT32_MAX;
    LOG_WARN("failed to init row meta", K(ret));
  } else {
    res += tmp_meta.fix_data_off_;
    for (int64_t i = 0; i < exprs.count(); ++i) {
      if (T_REF_COLUMN == exprs.at(i)->type_) {
        res += exprs.at(i)->max_length_;
      } else {
        res += INT32_MAX;
      }
    }
  }
  return res;
}

void ObCompactRow::init(const RowMeta &meta)
{
  MEMSET(payload_, 0, meta.fix_data_off_);
}

int ObCompactRow::calc_row_size(const RowMeta &row_meta, const common::ObIArray<ObExpr *> &exprs,
                                const ObBatchRows &brs, ObEvalCtx &ctx, int64_t &size)
{
  int ret = OB_SUCCESS;
  size = row_meta.get_row_fixed_size();
  const bool reordered = row_meta.fixed_expr_reordered();
  const int64_t row_idx = ctx.get_batch_idx();
  for (int64_t col_idx = 0; col_idx < exprs.count() && OB_SUCC(ret); ++col_idx) {
    ObExpr *expr = exprs.at(col_idx);
    // ensure the vector is evaluated
    if (OB_FAIL(expr->eval_vector(ctx, brs))) {
      SQL_ENG_LOG(WARN, "fail to evel vector", K(ret), K(expr));
    } else if (reordered && row_meta.project_idx(col_idx) < row_meta.fixed_cnt_) {
      // continue, the size is computed in `fixed_size`
    } else {
      ObIVector *vec = expr->get_vector(ctx);
      const VectorFormat format = vec->get_format();
      if (VEC_DISCRETE == format) {
        ObDiscreteBase *disc_vec = static_cast<ObDiscreteBase *>(vec);
        if (!disc_vec->is_null(row_idx)) {
          ObLength *lens = disc_vec->get_lens();
          size += lens[row_idx];
        }
      } else if (VEC_CONTINUOUS == format) {
        ObContinuousBase *cont_vec = static_cast<ObContinuousBase *>(vec);
        uint32_t *offsets = cont_vec->get_offsets();
        size += (offsets[row_idx + 1] - offsets[row_idx]);
      } else if (is_uniform_format(format)) {
        ObUniformBase *uni_vec = static_cast<ObUniformBase *>(vec);
        ObDatum *datums = uni_vec->get_datums();
        size += datums[expr->get_datum_idx(ctx)].len_;
      } else if (VEC_FIXED == format) {
        ObFixedLengthBase *fixed_vec = static_cast<ObFixedLengthBase *>(vec);
        size += fixed_vec->get_length();
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(RowMeta)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_);
  if (fixed_expr_reordered()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt_; ++i) {
      OB_UNIS_ENCODE(projector_[i]);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i <= fixed_cnt_; ++i) {
      OB_UNIS_ENCODE(fixed_offsets_[i]);
    }
  }
  return ret;
}


OB_DEF_DESERIALIZE(RowMeta)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_);
  projector_ = NULL;
  if (fixed_expr_reordered()) {
    ObDataBuffer local_alloc(buf_, MAX_LOCAL_BUF_LEN);
    ObIAllocator *alloc = use_local_allocator() ? &local_alloc : allocator_;
    if (OB_ISNULL(alloc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else if (OB_ISNULL(projector_ =
        static_cast<int32_t *>(alloc->alloc(sizeof(int32_t) * col_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc projector failed", K(ret), K(col_cnt_));
    } else if (OB_ISNULL(fixed_offsets_ =
        static_cast<int32_t *>(alloc->alloc(sizeof(int32_t) * (fixed_cnt_ + 1))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc projector failed", K(ret), K(col_cnt_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt_; ++i) {
        OB_UNIS_DECODE(projector_[i]);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i <= fixed_cnt_; ++i) {
        OB_UNIS_DECODE(fixed_offsets_[i]);
      }
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(RowMeta)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
              col_cnt_,
              extra_size_,
              fixed_cnt_,
              nulls_off_,
              var_offsets_off_,
              extra_off_,
              fix_data_off_,
              var_data_off_);
  if (fixed_expr_reordered()) {
    for (int64_t i = 0; i < col_cnt_; ++i) {
      OB_UNIS_ADD_LEN(projector_[i]);
    }
    for (int64_t i = 0; i <= fixed_cnt_; ++i) {
      OB_UNIS_ADD_LEN(fixed_offsets_[i]);
    }
  }
  return len;
}

int RowMeta::deep_copy(const RowMeta &other, common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  reset();
  *this = other;
  fixed_offsets_ = NULL;
  projector_ = NULL;
  allocator_ = allocator;
  if (fixed_expr_reordered()) {
    ObDataBuffer local_alloc(buf_, MAX_LOCAL_BUF_LEN);
    ObIAllocator *alloc = use_local_allocator() ? &local_alloc : allocator_;
    if (OB_ISNULL(alloc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is null", K(ret));
    } else if (OB_ISNULL(projector_ =
        static_cast<int32_t *>(alloc->alloc(sizeof(int32_t) * col_cnt_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc projector failed", K(ret), K(col_cnt_));
    } else if (OB_ISNULL(fixed_offsets_ =
        static_cast<int32_t *>(alloc->alloc(sizeof(int32_t) * (fixed_cnt_ + 1))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc projector failed", K(ret), K(col_cnt_));
    } else {
      MEMCPY(projector_, other.projector_, col_cnt_ * sizeof(int32_t));
      MEMCPY(fixed_offsets_, other.fixed_offsets_, (fixed_cnt_ + 1) * sizeof(int32_t));
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

int64_t ToStrCompactRow::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  J_ARRAY_START();
  ObDatum d;
  for (int col_id = 0; col_id < meta_.col_cnt_; col_id++) {
    d = row_.get_datum(meta_, col_id);
    if (NULL == exprs_) {
      pos += d.to_string(buf + pos, buf_len - pos);
    } else {
      ObExpr *expr = exprs_->at(col_id);
      if (NULL == expr) {
        pos += d.to_string(buf + pos, buf_len - pos);
      } else {
        pos += DATUM2STR(*expr, d).to_string(buf + pos, buf_len - pos);
      }
    }
    if (col_id != meta_.col_cnt_ - 1) {
      J_COMMA();
    }
  }
  if (meta_.extra_size_ > 0) {
    char *extra = static_cast<char *>(row_.get_extra_payload(meta_));
    // print hex value
    BUF_PRINTF(", extra_hex: ");
    if (OB_FAIL(hex_print(extra, meta_.extra_size_, buf, buf_len, pos))) {
      // do nothing
    }
    J_COMMA();
  }
  J_ARRAY_END();
  return pos;
}

} // end namespace sql
} // end namespace oceanbase
