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

#ifndef OCEANBASE_SQL_ENGINE_BASIC_OB_COMPACT_ROW_H_
#define OCEANBASE_SQL_ENGINE_BASIC_OB_COMPACT_ROW_H_

#include "share/vector/ob_i_vector.h"
#include "lib/allocator/ob_allocator.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace common {
class ObIAllocator;
}
namespace sql
{
struct RowHeader {
  RowHeader() : row_size_(0) {}

  TO_STRING_KV(K_(row_size));

public:
  static const int64_t OFFSET_LEN = 4;
  uint32_t row_size_;
  union {
    struct {
      //TODO shengle support dynamic offset len, no use now, now only use int32_t
      uint32_t offset_len_    : 3;
      uint32_t has_null_      : 1;
      uint32_t reserved_      : 28;
    };
    uint32_t flag_;
  };
};

struct RowMeta {
  OB_UNIS_VERSION_V(1);
  static const int64_t MAX_LOCAL_BUF_LEN = 128;
public:
  RowMeta(common::ObIAllocator *allocator = nullptr) : allocator_(allocator), col_cnt_(0), extra_size_(0),
              fixed_cnt_(0), fixed_offsets_(NULL), projector_(NULL),
              nulls_off_(0), var_offsets_off_(0), extra_off_(0),
              fix_data_off_(0), var_data_off_(0)
  {
  }
  ~RowMeta() {
    reset();
  }
  int init(const ObExprPtrIArray &exprs, const int32_t extra_size,
           const bool reorder_fixed_expr = true);
  int init (const RowMeta &row_meta);
  void reset();
  int32_t get_row_fixed_size() const { return sizeof(RowHeader) + var_data_off_; }
  int32_t get_var_col_cnt() const { return col_cnt_ - fixed_cnt_; }
  int32_t get_fixed_length(int64_t idx) const
  {
    return fixed_offsets_[idx + 1] - fixed_offsets_[idx];
  }
  inline void set_allocator(common::ObIAllocator *allocator) { allocator_ = allocator; }
  inline int32_t project_idx(int64_t col_idx) const { return projector_[col_idx]; }
  inline bool fixed_expr_reordered() const { return fixed_cnt_ > 0; }
  inline bool is_reordered_fixed_expr(const int64_t col_idx) const
  { return fixed_expr_reordered() && (project_idx(col_idx) < fixed_cnt_); }
  inline int64_t fixed_offsets(const int64_t col_idx) const
  {
    OB_ASSERT (is_reordered_fixed_expr(col_idx));
    return fixed_offsets_[project_idx(col_idx)];
  }
  inline int64_t fixed_length(const int64_t col_idx) const
  {
    OB_ASSERT (is_reordered_fixed_expr(col_idx));
    return get_fixed_length(project_idx(col_idx));
  }
  inline int64_t var_idx(const int64_t col_idx) const
  {
    return fixed_expr_reordered() ? (project_idx(col_idx) - fixed_cnt_) : col_idx;
  }
  //make sure column is fixed reordered
  inline int64_t get_fixed_cell_offset(const int64_t col_idx) const
  {
    return fixed_offsets_[project_idx(col_idx)];
  }
  static int32_t get_row_fixed_size(const int64_t col_cnt,
                                    const int64_t fixed_payload_len,
                                    const int64_t extra_size,
                                    const bool enable_reorder_expr = false);
  int deep_copy(const RowMeta &other, common::ObIAllocator *allocator);

  TO_STRING_KV(K_(col_cnt), K_(extra_size), K_(fixed_cnt), K_(nulls_off), K_(var_offsets_off),
               K_(extra_off), K_(fix_data_off), K_(var_data_off));
private:
  inline bool use_local_allocator() const
  {
    return fixed_cnt_ > 0 && ((col_cnt_ + fixed_cnt_ + 1) * sizeof(int32_t) <= MAX_LOCAL_BUF_LEN);
  }
private:
  char buf_[MAX_LOCAL_BUF_LEN];
public:
  common::ObIAllocator *allocator_;
  int32_t col_cnt_;
  int32_t extra_size_;

  int32_t fixed_cnt_;
  // Fixed-length data offset, based on payload, that is, you can locate the fixed data
  // position through payload + fixed_off.
  int32_t *fixed_offsets_;
  int32_t *projector_;

  // start pos of those offset is payload
  int32_t nulls_off_;
  // Variable-length data offset, based on var_data_off_, is different from fixed-length data.
  // If you need to locate the data, you need to use offset + var_data_off_,
  // and the first offset is initialized to 0
  int32_t var_offsets_off_;
  int32_t extra_off_;
  int32_t fix_data_off_;
  int32_t var_data_off_;
};

/*
* col_cnt
* fixed_offsets
| RowHeader  | NullBitMap  | Offsets     | Extra | Val                              |
|------------|-------------|-------------|-------|--------------------------------- |
| flag       | nulls       | var_offsets | extra | fix_data_buff  |   var_data_buf  |
 */
struct ObCompactRow
{
  ObCompactRow() : header_() {}

  // alloc row buf and init offset_len_
  static int alloc_row(const int16_t offset_len,
                       const int64_t row_size,
                       common::ObIAllocator &alloc,
                       ObCompactRow *&sr);
  static int64_t calc_max_row_size(const ObExprPtrIArray &exprs, int32_t extra_size);

  void init(const RowMeta &meta);

  void set_row_size(const uint32_t size) { header_.row_size_ = size; }

  inline const ObTinyBitVector *nulls() const {
    return reinterpret_cast<const ObTinyBitVector *>(payload_);
  }
  inline ObTinyBitVector *nulls() { return reinterpret_cast<ObTinyBitVector *>(payload_); }

  inline char *payload() { return payload_; }
  inline const char *payload() const { return payload_; }
  inline const int32_t *var_offsets(const RowMeta &meta) const {
    return reinterpret_cast<const int32_t *>(payload_ + meta.var_offsets_off_);
  }
  inline int32_t *var_offsets(const RowMeta &meta) {
    return reinterpret_cast<int32_t *>(payload_ + meta.var_offsets_off_);
  }

  inline const char *var_data(const RowMeta &meta) const { return payload_ + meta.var_data_off_;}
  inline char *var_data(const RowMeta &meta) { return payload_ + meta.var_data_off_; }

  inline int64_t offset(const RowMeta &meta, const int64_t col_idx) {
    int64_t off = 0;
    if (meta.fixed_expr_reordered()) {
      const int32_t idx = meta.project_idx(col_idx);
      if (idx < meta.fixed_cnt_) {
        off = meta.fixed_offsets_[idx];
      } else {
        int64_t var_idx = idx - meta.fixed_cnt_;
        off = meta.var_data_off_ + var_offsets(meta)[var_idx];
      }
    } else {
      off = meta.var_data_off_ + var_offsets(meta)[col_idx];
    }

    return off;
  }

  inline ObLength get_length(const RowMeta &meta, const int64_t col_idx) const {
    ObLength len = 0;
    if (meta.fixed_expr_reordered()) {
      const int32_t idx = meta.project_idx(col_idx);
      if (idx < meta.fixed_cnt_) {
        len = meta.fixed_offsets_[idx + 1] - meta.fixed_offsets_[idx];
      } else {
        int64_t var_idx = idx - meta.fixed_cnt_;
        len = var_offsets(meta)[var_idx + 1] - var_offsets(meta)[var_idx];
      }
    } else {
      len = var_offsets(meta)[col_idx + 1] - var_offsets(meta)[col_idx];
    }

    return len;
  }

  inline void set_null(const RowMeta &meta, const int64_t col_idx) {
    nulls()->set(col_idx);
    if (meta.fixed_expr_reordered()) {
      const int32_t idx = meta.project_idx(col_idx);
      if (idx < meta.fixed_cnt_) {
      } else {
        int32_t *var_offset_arr = var_offsets(meta);
        int64_t var_idx = idx - meta.fixed_cnt_;
        var_offset_arr[var_idx + 1] = var_offset_arr[var_idx];
      }
    } else {
      int32_t *var_offset_arr = var_offsets(meta);
      var_offset_arr[col_idx + 1] = var_offset_arr[col_idx];
    }
  }

  inline bool is_null(const int64_t col_idx) const {
    return nulls()->at(col_idx);
  }

  inline void set_cell_payload(const RowMeta &meta,
                               const int64_t col_idx,
                               const char *payload,
                               const ObLength len) {
    int64_t off = 0;
    if (meta.fixed_expr_reordered()) {
      const int32_t idx = meta.project_idx(col_idx);
      if (idx < meta.fixed_cnt_) {
        off = meta.fixed_offsets_[idx];
      } else {
        int32_t *var_offset_arr = var_offsets(meta);
        int64_t var_idx = idx - meta.fixed_cnt_;
        off = meta.var_data_off_ + var_offset_arr[var_idx];
        var_offset_arr[var_idx + 1] = var_offset_arr[var_idx] + len;
      }
    } else {
      int32_t *var_offset_arr = var_offsets(meta);
      off = meta.var_data_off_ + var_offset_arr[col_idx];
      var_offset_arr[col_idx + 1] = var_offset_arr[col_idx] + len;
    }
    MEMCPY(payload_ + off, payload, len);
  }
  //make sure column is fixed reordered
  inline void set_fixed_cell_payload(const char *payload, const int64_t offset, const ObLength len)
  {
    MEMCPY(payload_ + offset, payload, len);
  }

  inline void get_cell_payload(const RowMeta &meta,
                               const int64_t col_idx,
                               const char *&payload,
                               ObLength &len) const
  {
    if (meta.fixed_expr_reordered()) {
      const int32_t idx = meta.project_idx(col_idx);
      if (idx < meta.fixed_cnt_) {
        payload = payload_ + meta.fixed_offsets_[idx];
        len = meta.get_fixed_length(idx);
      } else {
        const int32_t *var_offset_arr = var_offsets(meta);
        int64_t var_idx = idx - meta.fixed_cnt_;
        payload = var_data(meta) + var_offset_arr[var_idx];
        len = var_offset_arr[var_idx + 1] - var_offset_arr[var_idx];
      }
    } else {
      const int32_t *var_offset_arr = var_offsets(meta);
      payload = var_data(meta) + var_offset_arr[col_idx];
      len = var_offset_arr[col_idx + 1] - var_offset_arr[col_idx];
    }
  }

  inline const char *get_cell_payload(const RowMeta &meta,
                                      const int64_t col_idx) const
  {
    const char *payload = nullptr;
    if (meta.fixed_expr_reordered()) {
      const int32_t idx = meta.project_idx(col_idx);
      if (idx < meta.fixed_cnt_) {
        payload = payload_ + meta.fixed_offsets_[idx];
      } else {
        const int32_t *var_offset_arr = var_offsets(meta);
        int64_t var_idx = idx - meta.fixed_cnt_;
        payload = var_data(meta) + var_offset_arr[var_idx];
      }
    } else {
      const int32_t *var_offset_arr = var_offsets(meta);
      payload = var_data(meta) + var_offset_arr[col_idx];
    }
    return payload;
  }
  //make sure column is fixed reordered
  inline const char *get_fixed_cell_payload(const int64_t offset) const
  {
    return payload_ + offset;
  }

  inline int64_t get_row_size() const {
    return header_.row_size_;
  }

  inline ObDatum get_datum(const RowMeta &meta, const int64_t col_idx) const {
    const char *ptr = NULL;
    ObLength len = 0;
    if (meta.fixed_expr_reordered()) {
      const int32_t idx = meta.project_idx(col_idx);
      if (idx < meta.fixed_cnt_) {
        ptr = payload_ + meta.fixed_offsets_[idx];
        len = meta.get_fixed_length(idx);
      } else {
        const int32_t *var_offset_arr = var_offsets(meta);
        int64_t var_idx = idx - meta.fixed_cnt_;
        ptr = var_data(meta) + var_offset_arr[var_idx];
        len = var_offset_arr[var_idx + 1] - var_offset_arr[var_idx];
      }
    } else {
      const int32_t *var_offset_arr = var_offsets(meta);
      ptr = var_data(meta) + var_offset_arr[col_idx];
      len = var_offset_arr[col_idx + 1] - var_offset_arr[col_idx];
    }

    return ObDatum(ptr, len, is_null(col_idx));
  }

  //virtual function calls in this interface, if performance is considered, try not to use it
  //int to_vector(const RowMeta &row_meta,
  //              const int64_t batch_idx,
  //              common::ObIArray<ObIVector *> &vectors) const;

  //inline int assign(const RowMeta &row_meta, const ObCompactRow *sr) const;

  inline void *get_extra_payload(const RowMeta &row_meta) const
  {
    return static_cast<void *>(const_cast<char *>(payload_ + row_meta.extra_off_));
  }

  inline void *get_extra_payload(const int32_t extra_offset) const
  {
    return static_cast<void *>(const_cast<char *>(payload_ + extra_offset));
  }

  template <typename T>
  inline T &extra_payload(const RowMeta &row_meta) const {
    return *static_cast<T *>(get_extra_payload(row_meta));
  };

  int assign(const ObCompactRow &row)
  {
    int ret = OB_SUCCESS;
    MEMCPY(this, reinterpret_cast<const void *> (&row), row.get_row_size());
    return ret;
  }

  static int calc_row_size(const RowMeta& row_meta,
                           const common::ObIArray<ObExpr*> &exprs,
                           const ObBatchRows &brs,
                           ObEvalCtx &ctx,
                           int64_t &size);
  TO_STRING_KV(K_(header))
protected:
  RowHeader header_;
  char payload_[0];
} __attribute__((packed));

struct ToStrCompactRow
{
  ToStrCompactRow(const RowMeta &meta,
                  const ObCompactRow &row,
                  const ObIArray<ObExpr *> *exprs = NULL)
    : meta_(meta), row_(row), exprs_(exprs)
  {}
  DECLARE_TO_STRING;
private:
  const RowMeta &meta_;
  const ObCompactRow &row_;
  const ObIArray<ObExpr *> *exprs_;
};

typedef ToStrCompactRow CompactRow2STR;
/*
 * Considering that many operator need to temporarily save the previous compact row,
 * `LastCompactRow` is implemented to provide this method.
 * Such as Sort, MergeDistinct, etc.
 * **Note**: It is time-consuming to calculate the size of a single line of vector,
 * please consider whether you must use this structure in performance-aware scenarios.
 */
class LastCompactRow
{
public:
  explicit LastCompactRow(ObIAllocator &alloc)
    : alloc_(alloc), row_meta_(&alloc), compact_row_(nullptr), max_size_(0), reuse_(true),
      is_first_row_(true), pre_alloc_row1_(nullptr), pre_alloc_row2_(nullptr)
  {}
  ~LastCompactRow() { reset(); }
  int save_store_row(const common::ObIArray<ObExpr*> &exprs,
                     const ObBatchRows &brs,
                     ObEvalCtx &ctx,
                     const int32_t extra_size = 0,
                     const bool reorder_fixed_expr = true)
  {
    int ret = OB_SUCCESS;
    int64_t row_size = 0;
    if (OB_UNLIKELY(0 == exprs.count())) {
    } else if (OB_UNLIKELY(extra_size < 0 || extra_size > INT32_MAX)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(ERROR, "invalid extra size", K(ret), K(extra_size));
    } else if (OB_FAIL(init_row_meta(exprs, extra_size, reorder_fixed_expr))) {
      SQL_ENG_LOG(WARN, "fail to init row meta", K(ret));
    } else if (OB_FAIL(ObCompactRow::calc_row_size(row_meta_, exprs, brs, ctx, row_size))) {
      SQL_ENG_LOG(WARN, "failed to calc copy size", K(ret));
    } else if (OB_FAIL(ensure_compact_row_buffer(row_size))) {
      SQL_ENG_LOG(WARN, "fail to ensure compact row buffer", K(ret));
    } else {
      compact_row_->init(row_meta_);
      compact_row_->set_row_size(static_cast<uint32_t>(row_size));
      for (int64_t col_idx = 0; col_idx < exprs.count() && OB_SUCC(ret); ++col_idx) {
        ObIVector *vec = exprs.at(col_idx)->get_vector(ctx);
        vec->to_row(row_meta_, compact_row_, ctx.get_batch_idx(), col_idx);
      }
    }
    return ret;
  }
  int to_expr(const common::ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx) const
  {
    int ret = OB_SUCCESS;
    for (uint32_t i = 0; i < exprs.count(); ++i) {
      if (exprs.at(i)->is_const_expr()) {
        continue;
      } else {
        ObIVector *vec = exprs.at(i)->get_vector(ctx);
        if (compact_row_->is_null(i)) {
          vec->set_null(ctx.get_batch_idx());
        } else {
          const char *payload = NULL;
          ObLength len;
          compact_row_->get_cell_payload(row_meta_, i, payload, len);
          vec->set_payload_shallow(ctx.get_batch_idx(), payload, len);
        }
        exprs.at(i)->set_evaluated_projected(ctx);
      }
    }
    return ret;
  }
  int save_store_row(const ObCompactRow &row)
  {
    int ret = OB_SUCCESS;
    bool reuse = reuse_;
    const int64_t row_size = row.get_row_size();
    if (is_first_row_) {
      // row meta is not inited
      ret = OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "row meta not inited", K(ret));
    } else if (OB_FAIL(ensure_compact_row_buffer(row_size))) {
      SQL_ENG_LOG(WARN, "fail to ensure compact row buffer", K(ret));
    } else {
      MEMCPY(compact_row_, reinterpret_cast<const char *>(&row), row_size);
    }
    return ret;
  }

  void reset()
  {
    compact_row_ = nullptr;
    max_size_ = 0;
    is_first_row_ = true;
    row_meta_.reset();
    if (NULL != pre_alloc_row1_) {
      alloc_.free(pre_alloc_row1_);
      pre_alloc_row1_ = NULL;
    }
    if (NULL != pre_alloc_row2_) {
      alloc_.free(pre_alloc_row2_);
      pre_alloc_row2_ = NULL;
    }
  }
  ObDatum get_datum(const int64_t col_idx) const { return compact_row_->get_datum(row_meta_, col_idx); }

  inline int init_row_meta(const common::ObIArray<ObExpr *> &exprs, const int32_t extra_size,
                           const bool reorder_fixed_expr)
  {
    int ret = OB_SUCCESS;
    if (is_first_row_) {
      ret = row_meta_.init(exprs, extra_size, reorder_fixed_expr);
      is_first_row_ = false;
    }
    return ret;
  }

private:
  inline int ensure_compact_row_buffer(const int64_t row_size) {
    int ret = OB_SUCCESS;
    bool reuse = reuse_;
    reuse = OB_ISNULL(compact_row_) ? false : reuse && (max_size_ >= row_size);
    if (reuse && OB_NOT_NULL(compact_row_)) {
      //switch buffer for write
      if (compact_row_ != pre_alloc_row1_ && compact_row_ != pre_alloc_row2_) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(ERROR, "unexpected status: row is invalid", K(ret),
          K(compact_row_), K(pre_alloc_row1_), K(pre_alloc_row2_));
      } else {
        compact_row_ = (compact_row_ == pre_alloc_row1_ ? pre_alloc_row2_ : pre_alloc_row1_);
      }
    } else {
      //alloc 2 buffer with same length
      max_size_ = (!reuse_ ? row_size : row_size * 2);
      char *buf1 = nullptr;
      char *buf2 = nullptr;
      if (OB_ISNULL(buf1 = reinterpret_cast<char*>(alloc_.alloc(max_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
      } else if (OB_ISNULL(buf2 = reinterpret_cast<char*>(alloc_.alloc(max_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "alloc buf failed", K(ret));
      } else if (OB_ISNULL(pre_alloc_row1_ = new(buf1)ObCompactRow())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "failed to new row", K(ret));
      } else if (OB_ISNULL(pre_alloc_row2_ = new(buf2)ObCompactRow())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(ERROR, "failed to new row", K(ret));
      } else {
        compact_row_ = pre_alloc_row1_;
      }
    }
    return ret;
  }

public:
  TO_STRING_KV(K_(max_size), K_(reuse), KP_(compact_row), K_(is_first_row));
  ObIAllocator &alloc_;
  RowMeta row_meta_;
  ObCompactRow *compact_row_;
  int64_t max_size_;
  bool reuse_;
private:
  bool is_first_row_;
  //To avoid writing memory overwrite, alloc 2 row for alternate writing
  ObCompactRow *pre_alloc_row1_;
  ObCompactRow *pre_alloc_row2_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_SQL_ENGINE_BASIC_OB_COMPACT_ROW_H_
