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

#ifndef OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_FORMAT_H_
#define OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_FORMAT_H_

#include "share/vector/ob_continuous_base.h"
#include "sql/engine/basic/ob_compact_row.h"

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

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_CONTINUOUS_FORMAT_H_
