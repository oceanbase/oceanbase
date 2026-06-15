/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_FORMAT_H_
#define OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_FORMAT_H_

#include "share/vector/ob_continuous_base.h"
#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_uniform_format.h"
#include "share/vector/ob_discrete_base.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace common
{
class ObContinuousFormat : public ObContinuousBase
{
public:
  ObContinuousFormat(uint32_t *offsets, char *data, sql::ObBitVector *nulls)
    : ObContinuousBase(offsets, data, nulls)
  {}

  OB_INLINE VectorFormat get_format() const override {
    return VEC_CONTINUOUS;
  }

  OB_INLINE void get_payload(const int64_t idx,
                          const char *&payload,
                          ObLength &length) const override final;
  OB_INLINE void get_payload(const int64_t idx,
                          bool &is_null,
                          const char *&payload,
                          ObLength &length) const override final;
  OB_INLINE const char *get_payload(const int64_t idx) const override final;
  OB_INLINE ObLength get_length(const int64_t idx) const override final;

  OB_INLINE void set_length(const int64_t idx, const ObLength length) override { /*do nothing*/ };

  OB_INLINE void set_payload(const int64_t idx,
                          const void *payload,
                          const ObLength length) override final;
  OB_INLINE void set_payload_shallow(const int64_t idx,
                                  const void *payload,
                                  const ObLength length) override final {
    set_payload(idx, payload, length);
  };
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
  DEF_VEC_READ_INTERFACES(ObContinuousFormat);
  DEF_VEC_WRITE_INTERFACES(ObContinuousFormat);

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

};

OB_INLINE void ObContinuousFormat::get_payload(const int64_t idx,
                                            const char *&payload,
                                            ObLength &length) const
{
  payload = data_ + offsets_[idx];
  length = get_length(idx);
}

OB_INLINE void ObContinuousFormat::get_payload(const int64_t idx,
                                            bool &is_null,
                                            const char *&payload,
                                            ObLength &length) const
{
  is_null = nulls_->at(idx);
  if (!is_null) {
    payload = data_ + offsets_[idx];
    length = get_length(idx);
  }
}

OB_INLINE const char *ObContinuousFormat::get_payload(const int64_t idx) const
{
  return data_ + offsets_[idx];
}

OB_INLINE ObLength ObContinuousFormat::get_length(const int64_t idx) const
{
  return offsets_[idx + 1] - offsets_[idx];
}

OB_INLINE void ObContinuousFormat::set_payload(const int64_t idx,
                                            const void *payload,
                                            const ObLength length)
{
  if (OB_UNLIKELY(nulls_->at(idx))) {
    unset_null(idx);
  }
  MEMCPY(data_ + offsets_[idx], payload, length);
  offsets_[idx + 1] = offsets_[idx] + length;
}

OB_INLINE int ObContinuousFormat::from_rows(const sql::RowMeta &row_meta,
                                            const sql::ObCompactRow **stored_rows,
                                            const int64_t size, const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  // TODO shengle may can opt by prefect and has_null specialize
  for (int64_t i = 0; i < size; i++) {
    if (nullptr == stored_rows[i]) {
      continue;
    }
    if (stored_rows[i]->is_null(col_idx)) {
      set_null(i);
    } else {
      const char *payload = NULL;
      ObLength len = 0;
      stored_rows[i]->get_cell_payload(row_meta, col_idx, payload, len);
      set_payload_shallow(i, payload, len);
    }
  }

  return ret;
};

OB_INLINE int ObContinuousFormat::from_rows(const sql::RowMeta &row_meta,
                                            const sql::ObCompactRow **stored_rows,
                                            const uint16_t selector[], const int64_t size,
                                            const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < size; i++) {
    if (nullptr == stored_rows[i]) {
      continue;
    }
    int64_t row_idx = selector[i];
    if (stored_rows[i]->is_null(col_idx)) {
      set_null(row_idx);
    } else {
      const char *payload = NULL;
      ObLength len = 0;
      stored_rows[i]->get_cell_payload(row_meta, col_idx, payload, len);
      set_payload_shallow(row_idx, payload, len);
    }
  }
  return ret;
}

OB_INLINE int ObContinuousFormat::from_row(const sql::RowMeta &row_meta,
                                           const sql::ObCompactRow *stored_row,
                                           const int64_t row_idx, const int64_t col_idx)
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

OB_INLINE int ObContinuousFormat::to_row(const sql::RowMeta &row_meta,
                                         sql::ObCompactRow *stored_row, const uint64_t row_idx,
                                         const int64_t col_idx) const
{
  int ret = OB_SUCCESS;
  if (is_null(row_idx)) {
    stored_row->set_null(row_meta, col_idx);
  } else {
    const char *payload = NULL;
    ObLength len = 0;
    get_payload(row_idx, payload, len);
    stored_row->set_cell_payload(row_meta, col_idx, payload, len);
  }
  return ret;
}

OB_INLINE int ObContinuousFormat::to_row(const sql::RowMeta &row_meta,
                                         sql::ObCompactRow *stored_row, const uint64_t row_idx,
                                         const int64_t col_idx, const int64_t remain_size,
                                         const bool is_fixed_length_data,
                                         int64_t &row_size) const
{
  int ret = OB_SUCCESS;
  UNUSED(is_fixed_length_data);
  int64_t need_size = 0;
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
  return ret;
}

OB_INLINE int ObContinuousFormat::from_vector_fixed(const ObFixedLengthBase *src_fixed,
                                                          const sql::ObBitVector *skip,
                                                          const int64_t start,
                                                          const int64_t end)
{
  UNUSED(src_fixed);
  UNUSED(skip);
  UNUSED(start);
  UNUSED(end);
  // CONTINUOUS format cannot copy from FIXED
  int ret = OB_NOT_SUPPORTED;
  SQL_ENG_LOG(WARN, "continuous format cannot copy from fixed format", K(ret));
  return ret;
}

OB_INLINE int ObContinuousFormat::from_vector_uniform(const ObUniformBase *src_uniform,
                                                             VectorFormat src_format,
                                                             const sql::ObBitVector *skip,
                                                             const int64_t start,
                                                             const int64_t end)
{
  UNUSED(src_uniform);
  UNUSED(src_format);
  UNUSED(skip);
  UNUSED(start);
  UNUSED(end);
  // CONTINUOUS format cannot copy from uniform
  int ret = OB_NOT_SUPPORTED;
  SQL_ENG_LOG(WARN, "continuous format cannot copy from uniform format", K(ret));
  return ret;
}

OB_INLINE int ObContinuousFormat::from_vector_discrete(const ObDiscreteBase *src_discrete,
                                                              const sql::ObBitVector *skip,
                                                              const int64_t start,
                                                              const int64_t end)
{
  UNUSED(src_discrete);
  UNUSED(skip);
  UNUSED(start);
  UNUSED(end);
  // CONTINUOUS format cannot copy from DISCRETE
  int ret = OB_NOT_SUPPORTED;
  SQL_ENG_LOG(WARN, "continuous format cannot copy from discrete format", K(ret));
  return ret;
}

OB_INLINE int ObContinuousFormat::from_vector_continuous(const ObContinuousBase *src_continuous,
                                                                 const sql::ObBitVector *skip,
                                                                 const int64_t start,
                                                                 const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_continuous)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "src_continuous is null", K(ret));
  } else if (skip != nullptr) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "continuous format does not support skip", K(ret), K(start), K(end));
  } else if (start != 0 || end != get_max_row_cnt()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "continuous format requires full bound", K(ret), K(start), K(end), "max_row_cnt", get_max_row_cnt());
  } else if (OB_FAIL(from_vector_bitmap_null(src_continuous, skip, start, end))) {
    SQL_ENG_LOG(WARN, "failed to copy null", K(ret));
  } else {
    const uint32_t *src_offsets = src_continuous->get_offsets();
    const char *src_data = src_continuous->get_data();
    uint32_t *dst_offsets = get_offsets();
    const int64_t offsets_size = (end - start + 1) * sizeof(uint32_t);
    MEMCPY(dst_offsets + start, src_offsets + start, offsets_size);
    set_data(const_cast<char *>(src_data));
  }
  return ret;
}

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_FORMAT_H_
