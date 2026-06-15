/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_OB_DISCRETE_FORMAT_H_
#define OCEANBASE_SHARE_VECTOR_OB_DISCRETE_FORMAT_H_

#include "share/vector/ob_discrete_base.h"
#include "share/vector/ob_uniform_format.h"
#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_continuous_base.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace common
{

class ObDiscreteFormat : public ObDiscreteBase
{
public:
  ObDiscreteFormat(int32_t *lens, char **ptrs, sql::ObBitVector *nulls)
    : ObDiscreteBase(lens, ptrs, nulls)
  {}

  OB_INLINE VectorFormat get_format() const override { return VEC_DISCRETE; }

  OB_INLINE void get_payload(const int64_t idx,
                          const char *&payload,
                          ObLength &length) const override final;
  OB_INLINE void get_payload(const int64_t idx,
                          bool &is_null,
                          const char *&payload,
                          ObLength &length) const override final;
  OB_INLINE const char *get_payload(const int64_t idx) const override final;
  OB_INLINE ObLength get_length(const int64_t idx) const override final
  {
    return lens_[idx];
  };

  OB_INLINE void set_length(const int64_t idx, const ObLength length) override { lens_[idx] = length; };

  OB_INLINE void set_payload(const int64_t idx,
                          const void *payload,
                          const ObLength length) override final;
  OB_INLINE void set_payload_shallow(const int64_t idx, const void *payload,
                                     const ObLength length) override final
  {
    if (OB_UNLIKELY(nulls_->at(idx))) { unset_null(idx); }
    ptrs_[idx] = const_cast<char *>(static_cast<const char *>(payload));
    lens_[idx] = length;
    if (OB_UNLIKELY(is_collection_expr())) {
      set_collection_payload_shallow(idx, payload, length);
    }
  }

  void set_datum(const int64_t idx, const ObDatum &datum) {
    if (datum.is_null()) {
      set_null(idx);
    } else {
      set_payload_shallow(idx, datum.ptr_, datum.len_);
    }
  }
  OB_INLINE int from_rows(const sql::RowMeta &row_meta, const sql::ObCompactRow **stored_rows,
                          const int64_t size, const int64_t col_idx) override final;

  OB_INLINE int from_rows(const sql::RowMeta &row_meta, const sql::ObCompactRow **stored_rows,
                          const uint16_t selector[], const int64_t size,
                          const int64_t col_idx) override final;

  OB_INLINE int from_row(const sql::RowMeta &row_meta, const sql::ObCompactRow *stored_row,
                         const int64_t row_idx, const int64_t col_idx) override final;

  OB_INLINE int to_row(const sql::RowMeta &row_meta, sql::ObCompactRow *stored_row,
                       const uint64_t row_idx, const int64_t col_idx) const override final;
  OB_INLINE int to_row(const sql::RowMeta &row_meta, sql::ObCompactRow *stored_row,
                       const uint64_t row_idx, const int64_t col_idx, const int64_t remain_size,
                       const bool is_fixed_length_data, int64_t &row_size) const override final;
  DEF_VEC_READ_INTERFACES(ObDiscreteFormat);
  DEF_VEC_WRITE_INTERFACES(ObDiscreteFormat);

protected:
  // Implement four pure virtual functions of ObIVector (protected, only called by from_vector_ivector)
  // New naming: from_vector_xxx
  int from_vector_fixed(const ObFixedLengthBase *src_fixed,
                             const sql::ObBitVector *skip,
                             const int64_t start,
                             const int64_t end) override;

  int from_vector_uniform(const ObUniformBase *src_uniform,
                                VectorFormat src_format,
                                const sql::ObBitVector *skip,
                                const int64_t start,
                                const int64_t end) override;

  int from_vector_discrete(const ObDiscreteBase *src_discrete,
                                 const sql::ObBitVector *skip,
                                 const int64_t start,
                                 const int64_t end) override;

  int from_vector_continuous(const ObContinuousBase *src_continuous,
                                   const sql::ObBitVector *skip,
                                   const int64_t start,
                                   const int64_t end) override;

private:
  // DISCRETE format supports copying from uniform, DISCRETE, CONTINUOUS, not from FIXED

private:
  void set_collection_payload_shallow(const int64_t idx, const void *payload, const ObLength length);


  int write_collection_to_row(const sql::RowMeta &row_meta, sql::ObCompactRow *stored_row,
                              const uint64_t row_idx, const int64_t col_idx) const;

  int write_collection_to_row(const sql::RowMeta &row_meta, sql::ObCompactRow *stored_row,
                              const uint64_t row_idx, const int64_t col_idx,
                              const int64_t remain_size, const bool is_fixed_length_data,
                              int64_t &row_size) const;
};

OB_INLINE void ObDiscreteFormat::get_payload(const int64_t idx,
                                          const char *&payload,
                                          ObLength &length) const
{
  payload = ptrs_[idx];
  length = lens_[idx];
}

OB_INLINE void ObDiscreteFormat::get_payload(const int64_t idx,
                                          bool &is_null,
                                          const char *&payload,
                                          ObLength &length) const
{
  is_null = nulls_->at(idx);
  if (!is_null) {
    payload = ptrs_[idx];
    length = lens_[idx];
  }
}

OB_INLINE const char *ObDiscreteFormat::get_payload(const int64_t idx) const
{
  return ptrs_[idx];
}

OB_INLINE void ObDiscreteFormat::set_payload(const int64_t idx,
                                          const void *payload,
                                          const ObLength length)
{
  if (OB_UNLIKELY(nulls_->at(idx))) {
    unset_null(idx);
  }
  MEMCPY(ptrs_[idx], payload, length);
  lens_[idx] = length;
}

OB_INLINE int ObDiscreteFormat::from_rows(const sql::RowMeta &row_meta,
                                          const sql::ObCompactRow **stored_rows, const int64_t size,
                                          const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  const int64_t var_idx = row_meta.var_idx(col_idx);
  if (!has_null() && !is_collection_expr()) {
    for (int64_t i = 0; i < size; i++) {
      if (nullptr == stored_rows[i]) {
        continue;
      }
      if (stored_rows[i]->is_null(col_idx)) {
        set_null(i);
      } else {
        const int32_t *var_offset_arr = stored_rows[i]->var_offsets(row_meta);
        const ObLength len = var_offset_arr[var_idx + 1] - var_offset_arr[var_idx];
        const char *payload = stored_rows[i]->var_data(row_meta) + var_offset_arr[var_idx];
        ptrs_[i] = const_cast<char *>(static_cast<const char *>(payload));
        lens_[i] = len;
      }
    }
  } else {
    for (int64_t i = 0; i < size; i++) {
      if (nullptr == stored_rows[i]) {
        continue;
      }
      if (stored_rows[i]->is_null(col_idx)) {
        set_null(i);
      } else {
        const int32_t *var_offset_arr = stored_rows[i]->var_offsets(row_meta);
        const ObLength len = var_offset_arr[var_idx + 1] - var_offset_arr[var_idx];
        const char *payload = stored_rows[i]->var_data(row_meta) + var_offset_arr[var_idx];
        set_payload_shallow(i, payload, len);
      }
    }
  }
  return ret;
};

OB_INLINE int ObDiscreteFormat::from_rows(const sql::RowMeta &row_meta,
                                          const sql::ObCompactRow **stored_rows,
                                          const uint16_t selector[], const int64_t size,
                                          const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  const int64_t var_idx = row_meta.var_idx(col_idx);
  if (!has_null() && !is_collection_expr()) {
    for (int64_t i = 0; i < size; i++) {
      if (nullptr == stored_rows[i]) {
        continue;
      }
      int64_t row_idx = selector[i];
      if (stored_rows[i]->is_null(col_idx)) {
        set_null(row_idx);
      } else {
        const int32_t *var_offset_arr = stored_rows[i]->var_offsets(row_meta);
        const ObLength len = var_offset_arr[var_idx + 1] - var_offset_arr[var_idx];
        const char *payload = stored_rows[i]->var_data(row_meta) + var_offset_arr[var_idx];
        ptrs_[row_idx] = const_cast<char *>(static_cast<const char *>(payload));
        lens_[row_idx] = len;
      }
    }
  } else {
    for (int64_t i = 0; i < size; i++) {
      if (nullptr == stored_rows[i]) {
        continue;
      }
      int64_t row_idx = selector[i];
      if (stored_rows[i]->is_null(col_idx)) {
        set_null(row_idx);
      } else {
        const int32_t *var_offset_arr = stored_rows[i]->var_offsets(row_meta);
        const ObLength len = var_offset_arr[var_idx + 1] - var_offset_arr[var_idx];
        const char *payload = stored_rows[i]->var_data(row_meta) + var_offset_arr[var_idx];
        set_payload_shallow(row_idx, payload, len);
      }
    }
  }
  return ret;
}

OB_INLINE int ObDiscreteFormat::from_row(const sql::RowMeta &row_meta,
                                         const sql::ObCompactRow *stored_row, const int64_t row_idx,
                                         const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  if (stored_row->is_null(col_idx)) {
    set_null(row_idx);
  } else {
    const char *payload = NULL;
    ObLength len = 0;
    stored_row->get_cell_payload(row_meta, col_idx, payload, len);
    set_payload_shallow(row_idx, payload, len);
  }

  return ret;
}

OB_INLINE int ObDiscreteFormat::to_row(const sql::RowMeta &row_meta, sql::ObCompactRow *stored_row,
                                       const uint64_t row_idx, const int64_t col_idx) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(!is_collection_expr())) {
    if (is_null(row_idx)) {
      stored_row->set_null(row_meta, col_idx);
    } else {
      const char *payload = NULL;
      ObLength len = 0;
      get_payload(row_idx, payload, len);
      stored_row->set_cell_payload(row_meta, col_idx, payload, len);
    }
  } else {
    ret = write_collection_to_row(row_meta, stored_row, row_idx, col_idx);
  }
  return ret;
}

OB_INLINE int ObDiscreteFormat::to_row(const sql::RowMeta &row_meta, sql::ObCompactRow *stored_row,
                                       const uint64_t row_idx, const int64_t col_idx,
                                       const int64_t remain_size,
                                       const bool is_fixed_length_data,
                                       int64_t &row_size) const
{
  int ret = OB_SUCCESS;
  UNUSED(is_fixed_length_data);
  int64_t need_size = 0;
  if (OB_LIKELY(!is_collection_expr())) {
    if (is_null(row_idx)) {
      stored_row->set_null(row_meta, col_idx);
    } else {
      const char *payload = NULL;
      ObLength len = 0;
      get_payload(row_idx, payload, len);
      row_size += len;
      if (len > remain_size) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        stored_row->set_cell_payload(row_meta, col_idx, payload, len);
      }
    }
  } else {
    ret = write_collection_to_row(row_meta, stored_row, row_idx, col_idx, remain_size,
                                  is_fixed_length_data, row_size);
  }
  return ret;
}

OB_INLINE int ObDiscreteFormat::from_vector_fixed(const ObFixedLengthBase *src_fixed,
                                                        const sql::ObBitVector *skip,
                                                        const int64_t start,
                                                        const int64_t end)
{
  UNUSED(src_fixed);
  UNUSED(skip);
  UNUSED(start);
  UNUSED(end);
  // DISCRETE format cannot copy from FIXED
  int ret = OB_NOT_SUPPORTED;
  SQL_ENG_LOG(WARN, "discrete format cannot copy from fixed format", K(ret));
  return ret;
}

OB_INLINE int ObDiscreteFormat::from_vector_uniform(const ObUniformBase *src_uniform,
                                                          VectorFormat src_format,
                                                          const sql::ObBitVector *skip,
                                                          const int64_t start,
                                                          const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_uniform)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "src_uniform is null", K(ret));
  } else if (OB_FAIL(from_vector_bitmap_null(src_uniform, skip, start, end))) {
    SQL_ENG_LOG(WARN, "failed to copy null", K(ret));
  } else {
    const ObDatum *src_datums = src_uniform->get_datums();
    const bool src_is_const = (src_format == VEC_UNIFORM_CONST);
    for (int64_t i = start; i < end; ++i) {
      if (skip != nullptr && skip->at(i)) {
        continue;
      }
      const int64_t src_idx = src_is_const ? 0 : i;
      if (!src_datums[src_idx].is_null()) {
        set_payload_shallow(i, src_datums[src_idx].ptr_, src_datums[src_idx].len_);
      }
    }
  }
  return ret;
}

OB_INLINE int ObDiscreteFormat::from_vector_discrete(const ObDiscreteBase *src_discrete,
                                                           const sql::ObBitVector *skip,
                                                           const int64_t start,
                                                           const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_discrete)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "src_discrete is null", K(ret));
  } else if (OB_FAIL(from_vector_bitmap_null(src_discrete, skip, start, end))) {
    SQL_ENG_LOG(WARN, "failed to copy null", K(ret));
  } else {
    const int64_t size = end - start;
    const ObLength *src_lens = src_discrete->get_lens();
    ObLength *dst_lens = get_lens();
    char **src_ptrs = src_discrete->get_ptrs();
    char **dst_ptrs = get_ptrs();
    if (skip != nullptr) {
      for (int64_t i = start; i < end; ++i) {
        if (!skip->at(i)) {
          dst_lens[i] = src_lens[i];
          dst_ptrs[i] = src_ptrs[i];
        }
      }
    } else {
      MEMCPY(dst_lens + start, src_lens + start, size * sizeof(ObLength));
      MEMCPY(dst_ptrs + start, src_ptrs + start, size * sizeof(char *));
    }
  }
  return ret;
}

OB_INLINE int ObDiscreteFormat::from_vector_continuous(const ObContinuousBase *src_continuous,
                                                              const sql::ObBitVector *skip,
                                                              const int64_t start,
                                                              const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_continuous)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "src_continuous is null", K(ret));
  } else if (OB_FAIL(from_vector_bitmap_null(src_continuous, skip, start, end))) {
    SQL_ENG_LOG(WARN, "failed to copy null", K(ret));
  } else {
    const char *src_data = src_continuous->get_data();
    const uint32_t *src_offsets = src_continuous->get_offsets();
    char **dst_ptrs = get_ptrs();
    ObLength *dst_lens = get_lens();
    for (int64_t i = start; i < end; ++i) {
      if (skip != nullptr && skip->at(i)) {
        continue;
      }
      const sql::ObBitVector *src_nulls = src_continuous->get_nulls();
      if (!src_nulls->at(i)) {
        dst_ptrs[i] = const_cast<char *>(src_data + src_offsets[i]);
        dst_lens[i] = src_offsets[i + 1] - src_offsets[i];
      }
    }
  }
  return ret;
}

}
}
#endif //OCEANBASE_SHARE_VECTOR_OB_DISCRETE_FORMAT_H_
