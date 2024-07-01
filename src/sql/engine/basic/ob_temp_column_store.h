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

#ifndef OCEANBASE_BASIC_OB_TEMP_COLUMN_STORE_H_
#define OCEANBASE_BASIC_OB_TEMP_COLUMN_STORE_H_

#include "share/ob_define.h"
#include "sql/engine/basic/ob_temp_block_store.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "share/vector/ob_i_vector.h"
#include "lib/alloc/alloc_struct.h"

namespace oceanbase
{
namespace sql
{

class ObTempColumnStore : public ObTempBlockStore
{
  const static ObLength UNFIXED_LENGTH = -1;
  const static ObLength NULL_WITH_BATCH_RESULT_LENGTH = 0;
public:
  class Iterator;
  /*
   * ColumnBlock provides functions for writing and reading, and does not occupy memory
   * the memory layout is as follows:
   *
   * Vector (FIXED or CONTINUOUS format):
   *    +-----------------------------------------------------------------------------------------+
   *    | fixed_vector (null vector, data) | continuous_vector (null vector, offsets, data) | ... |
   *    +-----------------------------------------------------------------------------------------+
   *
   * Vector Batch, composed of multiple Vectors:
   *    +-------------------------------------------------------------------------------------+
   *    | batch rows count (int32_t) | offsets | vector1 | vector2 | vector3 | ... | vector n |
   *    +-------------------------------------------------------------------------------------+
   *
   * Column Block, composed of multiple Vector Batches:
   *    +------------------------------------------------------+
   *    | vector batch1 | vector batch2 | ... | vector batch n |
   *    +------------------------------------------------------+
   *
   * ColumnBlock will only have two types of output vectors, `FIXED` and `CONTINUOUS`,
   * and can receive any input, including UNIFORM, DISCRETE, FIXED and CONTINUOUS.
   * For the vector format, please refer to the `to_buf` and `from_buf` interfaces below.
   */
  struct ColumnBlock : public Block
  {
    static int calc_rows_size(const IVectorPtrs &vectors,
                              const uint16_t *selector,
                              const ObArray<ObLength> &lengths,
                              const int64_t size,
                              int64_t &batch_mem_size);
    int add_batch(ShrinkBuffer &buf,
                  const IVectorPtrs &vectors,
                  const uint16_t *selector,
                  const ObArray<ObLength> &lengths,
                  const int64_t size,
                  const int64_t batch_mem_size);

    int get_next_batch(const IVectorPtrs &vectors,
                       const ObArray<ObLength> &lengths,
                       const int32_t start_read_pos,
                       int32_t &batch_rows,
                       int32_t &batch_pos) const;
  private:
    inline static int64_t get_header_size(const int64_t vec_cnt)
    {
      // row_count : int32_t
      // offsets   : int32_t[vec_cnt + 1]
      return sizeof(int32_t) * (vec_cnt + 2);
    }
  };

  class Iterator : public ObTempBlockStore::BlockReader
  {
  public:
    friend struct ColumnBlock;
    friend class ObTempColumnStore;
    Iterator() : ObTempBlockStore::BlockReader(), column_store_(NULL), cur_blk_(NULL),
                 cur_blk_id_(0), rest_row_cnt_(0), read_pos_(0), vectors_(NULL) {}
    virtual ~Iterator() {}

    int init(ObTempColumnStore *store);
    inline bool has_next() const { return cur_blk_id_ < get_row_cnt(); }
    inline int64_t get_row_cnt() const { return column_store_->get_row_cnt(); }
    inline int64_t get_col_cnt() const { return column_store_->get_col_cnt(); }
    int get_next_batch(const ObExprPtrIArray &exprs,
                       ObEvalCtx &ctx,
                       const int64_t max_rows,
                       int64_t &read_rows);

    inline bool has_rest_row_in_batch() const { return rest_row_cnt_ > 0; }
    void reset()
    {
      cur_blk_ = NULL;
      cur_blk_id_ = 0;
      rest_row_cnt_ = 0;
      read_pos_ = 0;
      if (NULL != column_store_ && !column_store_->reuse_vector_array_ && NULL != vectors_) {
        vectors_->reset();
        column_store_->allocator_->free(vectors_);
      }
      vectors_ = NULL;
      ObTempBlockStore::BlockReader::reset();
    }

  private:
    int next_block();
    int ensure_read_vectors(const ObExprPtrIArray &exprs, ObEvalCtx &ctx, const int64_t max_rows);

  private:
    ObTempColumnStore *column_store_;
    const ColumnBlock *cur_blk_;
    int64_t cur_blk_id_; // block id(row_id) for iterator, from 0 to row_cnt_
    int32_t rest_row_cnt_; // rest row count unread in current batch
    int32_t read_pos_; // current memory read position in reader block
    ObArray<ObIVector *> *vectors_;
  };

  struct BatchCtx
  {
    ~BatchCtx()
    {
      vectors_.reset();
      lengths_.reset();
      selector_ = nullptr;
    }
    ObArray<ObIVector *> vectors_;
    ObArray<ObLength> lengths_;
    int64_t max_batch_size_;
    uint16_t *selector_;
  };

public:
  explicit ObTempColumnStore(common::ObIAllocator *alloc = NULL);

  virtual ~ObTempColumnStore();

  void reset();

  int init(const ObExprPtrIArray &exprs,
           const int64_t max_batch_size,
           const lib::ObMemAttr &mem_attr,
           const int64_t mem_limit,
           const bool enable_dump,
           const bool reuse_vector_array,
           const common::ObCompressorType compressor_type);

  int init_batch_ctx(const ObExprPtrIArray &exprs);

  int begin(Iterator &it)
  {
    return it.init(this);
  }
  int add_batch(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &ctx,
                const ObBatchRows &brs, int64_t &stored_rows_count);

  inline int64_t get_row_cnt() const { return block_id_cnt_; }
  inline int64_t get_row_cnt_on_disk() const { return dumped_block_id_cnt_; }
  inline int64_t get_row_cnt_in_memory() const { return get_row_cnt() - get_row_cnt_on_disk(); }
  inline int64_t get_col_cnt() const { return col_cnt_; }

private:
  inline int ensure_write_blk(const int64_t mem_size)
  {
    int ret = common::OB_SUCCESS;
    if (NULL == cur_blk_ || mem_size > blk_buf_.remain()) {
      if (OB_FAIL(new_block(mem_size))) {
        SQL_ENG_LOG(WARN, "fail to new block", K(ret), K(mem_size));
      } else {
        cur_blk_ = static_cast<ColumnBlock *>(blk_);
      }
    }
    return ret;
  }
  // Vector format related functions
  // ObBitmapNullVectorBase: flag and nulls bitmap
  inline static int64_t null_bitmap_size(const int64_t size)
  {
    return sizeof(uint16_t) + sql::ObBitVector::memory_size(size);
  }

  /*
   * Write the source vector to the store and convert vector to stored vector type.
   * The maping are as follows:
   *    FIXED        -> FIXED
   *    DISCRETE     -> CONTINUOUS
   *    CONTINUOUS   -> CONTINUOUS
   *    UNIFORM      -> FIXED OR CONTINUOUS
   * There three types of functions: calculation of batch size, data writing and reading
   */
  template<typename VEC>
  inline static int64_t calc_size(const VEC* vec, const uint16_t *selector, const int64_t size);
  template<typename VEC>
  inline static int to_buf(const VEC* vec, const uint16_t *selector,
                           const int64_t size, char *buf, int64_t &pos);
  // Uniform vector source
  template <bool IS_CONST>
  inline static int64_t calc_size(const ObUniformBase* vec,
                                  const uint16_t *selector,
                                  const int64_t size,
                                  const ObLength fixed_len);
  template <bool IS_CONST>
  inline static int to_buf(const ObUniformBase* vec,
                           const uint16_t *selector,
                           const int64_t size,
                           const ObLength fixed_len,
                           char *buf,
                           int64_t &pos);
  template<typename VEC>
  inline static int from_buf(char *buf, int64_t &pos, const int64_t size, VEC *vec)
  {
    // The default is not supported and requires specialized implementation.
    return common::OB_NOT_SUPPORTED;
  }
  template<typename VEC>
  inline static int from_vector(VEC *vec, ObExpr *expr, ObEvalCtx &ctx,
                                const int64_t start_pos, const int64_t size)
  {
    // The default is not supported and requires specialized implementation.
    return common::OB_NOT_SUPPORTED;
  }

private:
  lib::ObMemAttr mem_attr_;
  ColumnBlock *cur_blk_;
  int64_t col_cnt_;
  BatchCtx *batch_ctx_;
  int64_t max_batch_size_;
  bool reuse_vector_array_;
};

// Bitmap null vector
template<>
inline int64_t ObTempColumnStore::calc_size(const ObBitmapNullVectorBase* vec,
                                            const uint16_t *selector,
                                            const int64_t size)
{
  UNUSED(selector);
  return null_bitmap_size(size);
}

template<>
inline int ObTempColumnStore::to_buf(const ObBitmapNullVectorBase* vec,
                                     const uint16_t *selector,
                                     const int64_t size,
                                     char *buf, int64_t &pos)
{
  *reinterpret_cast<uint16_t *>(buf + pos) = vec->get_flag();
  pos += sizeof(uint16_t);
  const sql::ObBitVector *src_nulls = vec->get_nulls();
  sql::ObBitVector *dst_nulls = reinterpret_cast<sql::ObBitVector *>(buf + pos);
  if (NULL == selector) {
    dst_nulls->deep_copy(*src_nulls, size);
  } else {
    dst_nulls->reset(size);
    for (int64_t i = 0; i < size; ++i) {
      if (src_nulls->at(selector[i])) {
        dst_nulls->set(i);
      }
    }
  }
  pos += sql::ObBitVector::memory_size(size);
  return common::OB_SUCCESS;
}

template<>
inline int ObTempColumnStore::from_buf(char *buf, int64_t &pos, const int64_t size,
                                       ObBitmapNullVectorBase *vec)
{
  const uint16_t flag = *reinterpret_cast<const uint16_t *>(buf + pos);
  pos += sizeof(uint16_t);
  sql::ObBitVector *nulls = reinterpret_cast<sql::ObBitVector *>(buf + pos);
  pos += sql::ObBitVector::memory_size(size);
  vec->from(nulls, flag);
  return common::OB_SUCCESS;
}

template<>
inline int ObTempColumnStore::from_vector(ObBitmapNullVectorBase *vec, ObExpr *expr, ObEvalCtx &ctx,
                                          const int64_t start_pos, const int64_t size)
{
  // reuse null vector memory if there is no null data
  if (vec->has_null()) {
    const uint16_t flag = vec->get_flag();
    ObBitVector &nulls = expr->get_nulls(ctx);
    nulls.reset(size - start_pos);
    for (int64_t idx = start_pos; idx < size; ++idx) {
      if (vec->is_null(idx)) {
        nulls.set(idx - start_pos);
      }
    }
    vec->from(&nulls, flag);
  }
  return common::OB_SUCCESS;
}

// Fixed length vector members: null bitmap, length and data array
template<>
inline int64_t ObTempColumnStore::calc_size(const ObFixedLengthBase* vec,
                                            const uint16_t *selector,
                                            const int64_t size)
{
  UNUSED(selector);
  return null_bitmap_size(size) + sizeof(ObLength) + vec->get_length() * size;
}

template<>
inline int ObTempColumnStore::to_buf(const ObFixedLengthBase* vec,
                                     const uint16_t *selector,
                                     const int64_t size,
                                     char *buf, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(to_buf(static_cast<const ObBitmapNullVectorBase*>(vec), selector, size, buf, pos))) {
    SQL_ENG_LOG(WARN, "fail to convert null bitmap to buffer", K(ret));
  } else {
    const ObLength len = vec->get_length();
    *reinterpret_cast<ObLength *>(buf + pos) = len;
    pos += sizeof(ObLength);
    if (NULL == selector) {
      MEMCPY(buf + pos, vec->get_data(), len * size);
    } else {
      // TODO @zuojiao.hzj: opt fixed memcpy
      char *dst = buf + pos;
      for (int64_t i = 0; i < size; ++i) {
        const int64_t idx = selector[i];
        MEMCPY(dst + i * len, vec->get_data() + idx * len, len);
      }
    }
    pos += len * size;
  }
  return ret;
}

template<>
inline int ObTempColumnStore::from_buf(char *buf, int64_t &pos, const int64_t size,
                                       ObFixedLengthBase *vec)
{
  from_buf(buf, pos, size, static_cast<ObBitmapNullVectorBase*>(vec));
  ObLength len = *reinterpret_cast<ObLength *>(buf + pos);
  pos += sizeof(ObLength);
  char *data = buf + pos;
  pos += len * size;
  vec->from(len, data);
  return common::OB_SUCCESS;
}

template<>
inline int ObTempColumnStore::from_vector(ObFixedLengthBase *vec, ObExpr *expr, ObEvalCtx &ctx,
                                          const int64_t start_pos, const int64_t size)
{
  from_vector(static_cast<ObBitmapNullVectorBase*>(vec), expr, ctx, start_pos, size);
  const ObLength len = vec->get_length();
  char *data = vec->get_data() + start_pos * len;
  vec->from(len, data);
  return common::OB_SUCCESS;
}

// Continuous vector members: null bitmap, offset and data array
template<>
inline int64_t ObTempColumnStore::calc_size(const ObContinuousBase* vec,
                                            const uint16_t *selector,
                                            const int64_t size)
{
  int64_t len = null_bitmap_size(size) + sizeof(uint32_t) * (size + 1);
  const uint32_t *offset = vec->get_offsets();
  if (NULL == selector) {
    len += (offset[size] - offset[0]);
  } else {
    for (int64_t i = 0; i < size; ++i) {
      const int64_t idx = selector[i];
      len += (offset[idx + 1] - offset[idx]);
    }
  }
  return len;
}

template<>
inline int ObTempColumnStore::to_buf(const ObContinuousBase* vec,
                                     const uint16_t *selector,
                                     const int64_t size,
                                     char *buf, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(to_buf(static_cast<const ObBitmapNullVectorBase*>(vec), selector, size, buf, pos))) {
    SQL_ENG_LOG(WARN, "fail to convert null bitmap to buffer", K(ret));
  } else {
    const uint32_t *src_offsets = vec->get_offsets();
    uint32_t *offsets = reinterpret_cast<uint32_t *>(buf + pos);
    pos += (size + 1) * sizeof(uint32_t);
    char *data = buf + pos;
    const uint32_t base = src_offsets[0];
    offsets[0] = 0;
    if (NULL == selector) {
      for (int64_t i = 1; i <= size; ++i) {
        offsets[i] = (src_offsets[i] - base);
      }
      MEMCPY(data, vec->get_data() + base, offsets[size]);
    } else {
      for (int64_t i = 0; i < size; ++i) {
        const uint16_t idx = selector[i];
        const uint32_t len = (src_offsets[idx + 1] - src_offsets[idx]);
        MEMCPY(data + offsets[i], vec->get_data() + src_offsets[idx], len);
        offsets[i + 1] = offsets[i] + len;
      }
    }
    pos += offsets[size];
  }
  return ret;
}

template<>
inline int ObTempColumnStore::from_buf(char *buf, int64_t &pos, const int64_t size,
                                       ObContinuousBase *vec)
{
  from_buf(buf, pos, size, static_cast<ObBitmapNullVectorBase*>(vec));
  uint32_t *offsets = reinterpret_cast<uint32_t *>(buf + pos);
  pos += (size + 1) * sizeof(uint32_t);
  char *data = buf + pos;
  pos += offsets[size];
  vec->from(offsets, data);
  return common::OB_SUCCESS;
}

template<>
inline int ObTempColumnStore::from_vector(ObContinuousBase *vec, ObExpr *expr, ObEvalCtx &ctx,
                                          const int64_t start_pos, const int64_t size)
{
  from_vector(static_cast<ObBitmapNullVectorBase*>(vec), expr, ctx, start_pos, size);
  const uint32_t *src_offsets = vec->get_offsets();
  char *data = vec->get_data() + src_offsets[start_pos];
  uint32_t *offsets = expr->get_continuous_vector_offsets(ctx);
  for (int64_t idx = start_pos; idx <= size; ++idx) {
    offsets[idx - start_pos] = (src_offsets[idx] - src_offsets[start_pos]);
  }
  vec->from(offsets, data);
  return common::OB_SUCCESS;
}

// Discrete vector
template<>
inline int64_t ObTempColumnStore::calc_size(const ObDiscreteBase* vec,
                                            const uint16_t *selector,
                                            const int64_t size)
{
  int64_t len = null_bitmap_size(size) + sizeof(uint32_t) * (size + 1);
  if (NULL == selector) {
    for (int64_t i = 0; i < size; ++i) {
      len += vec->is_null(i) ? 0 : vec->get_lens()[i];
    }
  } else {
    for (int64_t i = 0; i < size; ++i) {
      const int64_t idx = selector[i];
      len += vec->is_null(idx) ? 0 : vec->get_lens()[idx];
    }
  }
  return len;
}

// note: the `to_buf` will covert discrete format to continuous
template<>
inline int ObTempColumnStore::to_buf(const ObDiscreteBase* vec,
                                     const uint16_t *selector,
                                     const int64_t size,
                                     char *buf, int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (OB_FAIL(to_buf(static_cast<const ObBitmapNullVectorBase*>(vec), selector, size, buf, pos))) {
    SQL_ENG_LOG(WARN, "fail to convert null bitmap to buffer", K(ret));
  } else {
    uint32_t *offsets = reinterpret_cast<uint32_t *>(buf + pos);
    pos += (size + 1) * sizeof(uint32_t);
    char *data = buf + pos;
    offsets[0] = 0;
    const ObLength *lens = vec->get_lens();
    char **ptrs = vec->get_ptrs();
    if (NULL == selector) {
      for (int i = 0; i < size; ++i) {
        const uint32_t len = vec->is_null(i) ? 0 : lens[i];
        MEMCPY(data + offsets[i], ptrs[i], len);
        offsets[i + 1] = offsets[i] + len;
      }
    } else {
      for (int64_t i = 0; i < size; ++i) {
        const uint16_t idx = selector[i];
        const uint32_t len = vec->is_null(idx) ? 0 : lens[idx];
        MEMCPY(data + offsets[i], ptrs[idx], len);
        offsets[i + 1] = offsets[i] + len;
      }
    }
    pos += offsets[size];
  }
  return ret;
}

// Uniform vector, output Fixed or Continuous format according to the value of `fixed_len`,
// greater than 0 means fixed vector, otherwise continuous
template <bool IS_CONST>
inline int64_t ObTempColumnStore::calc_size(const ObUniformBase* vec,
                                            const uint16_t *selector,
                                            const int64_t size,
                                            const ObLength fixed_len)
{
  int64_t len = 0;
  if (NULL_WITH_BATCH_RESULT_LENGTH == fixed_len) {
  } else {
    len = null_bitmap_size(size);
    if (fixed_len > 0) {
      len += (sizeof(ObLength) + fixed_len * size);
    } else { // continuous format
      len += (sizeof(uint32_t) * (size + 1));
      if (NULL == selector) {
        for (int64_t i = 0; i < size; ++i) {
          if (!vec->get_datums()[IS_CONST ? 0 : i].null_) {
            len += vec->get_datums()[IS_CONST ? 0 : i].len_;
          }
        }
      } else {
        for (int64_t i = 0; i < size; ++i) {
          const int64_t idx = selector[i];
          if (!vec->get_datums()[IS_CONST ? 0 : idx].null_) {
            len += vec->get_datums()[IS_CONST ? 0 : idx].len_;
          }
        }
      }
    }
  }

  return len;
}

template <bool IS_CONST>
inline int ObTempColumnStore::to_buf(const ObUniformBase* vec,
                                     const uint16_t *selector,
                                     const int64_t size,
                                     const ObLength fixed_len,
                                     char *buf,
                                     int64_t &pos)
{
  int ret = common::OB_SUCCESS;
  if (NULL_WITH_BATCH_RESULT_LENGTH == fixed_len) {
    // skip null expr
  } else {
    uint16_t *flag = reinterpret_cast<uint16_t *>(buf + pos);
    sql::ObBitVector *nulls = reinterpret_cast<sql::ObBitVector *>(buf + pos + sizeof(uint16_t));
    bool has_null = false;
    nulls->reset(size);
    pos += null_bitmap_size(size);
    const ObDatum *datums = vec->get_datums();
    if (fixed_len > 0) {
      *reinterpret_cast<ObLength *>(buf + pos) = fixed_len;
      pos += sizeof(ObLength);
      char *data = buf + pos;
      if (NULL == selector) {
        for (int64_t i = 0; i < size; ++i) {
          const ObDatum &datum = datums[IS_CONST ? 0 : i];
          if (datum.null_) {
            nulls->set(i);
            has_null = true;
          } else if (OB_UNLIKELY(fixed_len != datum.len_)) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "fixed length mismatch", K(ret), K(fixed_len), K(datum.len_));
          } else {
            MEMCPY(data + i * fixed_len, datum.ptr_, fixed_len);
          }
        }
      } else {
        for (int64_t i = 0; i < size; ++i) {
          const int64_t idx = selector[i];
          const ObDatum &datum = datums[IS_CONST ? 0 : idx];
          if (datum.null_) {
            nulls->set(i);
            has_null = true;
          } else if (OB_UNLIKELY(fixed_len != datum.len_)) {
            ret = common::OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "fixed length mismatch", K(ret), K(fixed_len), K(datum.len_));
          } else {
            MEMCPY(data + i * fixed_len, datum.ptr_, fixed_len);
          }
        }
      }
      pos += size * fixed_len;
    } else { // continuous format
      uint32_t *offset = reinterpret_cast<uint32_t *>(buf + pos);
      pos += (sizeof(uint32_t) * (size + 1));
      offset[0] = 0;
      char *data = buf + pos;
      if (NULL == selector) {
        for (int64_t i = 0; i < size; ++i) {
          const ObDatum &datum = datums[IS_CONST ? 0 : i];
          if (datum.null_) {
            nulls->set(i);
            offset[i + 1] = offset[i];
            has_null = true;
          } else {
            MEMCPY(data + offset[i], datum.ptr_, datum.len_);
            offset[i + 1] = offset[i] + datum.len_;
          }
        }
      } else {
        for (int64_t i = 0; i < size; ++i) {
          const int64_t idx = selector[i];
          const ObDatum &datum = datums[IS_CONST ? 0 : idx];
          if (datum.null_) {
            nulls->set(i);
            offset[i + 1] = offset[i];
            has_null = true;
          } else {
            MEMCPY(data + offset[i], datum.ptr_, datum.len_);
            offset[i + 1] = offset[i] + datum.len_;
          }
        }
      }
      pos += offset[size];
    }
    *flag = ObBitmapNullVectorBase::get_default_flag(has_null);
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_TEMP_COLUMN_STORE_H_
