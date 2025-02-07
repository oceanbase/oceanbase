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
#ifndef OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_FORMAT_H_
#define OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_FORMAT_H_

#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_uniform_format.h"
#include "sql/engine/basic/ob_compact_row.h"

namespace oceanbase
{
namespace common
{

template<typename ValueType>
class ObFixedLengthFormat : public ObFixedLengthBase
{
public:
  ObFixedLengthFormat(char *data, sql::ObBitVector *nulls)
    : ObFixedLengthBase(nulls, sizeof(ValueType), data)
  {}

  OB_INLINE VectorFormat get_format() const override final { return VEC_FIXED; }

  OB_INLINE void get_payload(const int64_t idx,
                          const char *&payload,
                          ObLength &length) const override final;
  OB_INLINE void get_payload(const int64_t idx,
                          bool &is_null,
                          const char *&payload,
                          ObLength &length) const override final;
  OB_INLINE const char *get_payload(const int64_t idx) const override final;
  OB_INLINE ObLength get_length(const int64_t idx) const override final { return sizeof(ValueType); }
  OB_INLINE void set_length(const int64_t idx, const ObLength length) override  { /*do nothing*/ }
  OB_INLINE void set_payload(const int64_t idx,
                          const void *payload,
                          const ObLength length) override final {
    OB_ASSERT(length == sizeof(ValueType));
    if (std::is_same<ValueType, char[0]>::value) {
      // do nothing
    } else {
      if (OB_UNLIKELY(nulls_->at(idx))) {
        unset_null(idx);
      }
      (reinterpret_cast<ValueType *>(data_))[idx] = *(static_cast<const ValueType *>(payload));
    }
  }

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
  OB_INLINE int32_t type_size() const { return sizeof(ValueType); }
  DEF_VEC_READ_INTERFACES(ObFixedLengthFormat<ValueType>);
  DEF_VEC_WRITE_INTERFACES(ObFixedLengthFormat<ValueType>);
};

template<typename ValueType>
OB_INLINE void ObFixedLengthFormat<ValueType>::get_payload(const int64_t idx,
                                                        bool &is_null,
                                                        const char *&payload,
                                                        ObLength &length) const
{
  is_null = nulls_->at(idx);
  if (!is_null) {
    payload = reinterpret_cast<const char *>(data_ + sizeof(ValueType) * idx);
    length = type_size();
  }
}

template<typename ValueType>
OB_INLINE void ObFixedLengthFormat<ValueType>::get_payload(
                  const int64_t idx, const char *&payload, ObLength &length) const
{
  payload = reinterpret_cast<const char *>(data_ + sizeof(ValueType) * idx);
  length = type_size();
}

template<typename ValueType>
OB_INLINE const char *ObFixedLengthFormat<ValueType>::get_payload(const int64_t idx) const
{
  return reinterpret_cast<const char *>(data_ + sizeof(ValueType) * idx);
};

template <typename ValueType>
OB_INLINE int ObFixedLengthFormat<ValueType>::from_rows(const sql::RowMeta &row_meta,
                                                        const sql::ObCompactRow **stored_rows,
                                                        const int64_t size, const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  if (row_meta.is_reordered_fixed_expr(col_idx)) {
    const int64_t offset = row_meta.fixed_offsets(col_idx);
    if (!has_null()) {
      for (int64_t i = 0; i < size; i++) {
        if (nullptr == stored_rows[i]) {
          continue;
        }
        if (stored_rows[i]->is_null(col_idx)) {
          set_null(i);
        } else {
          const char *payload = stored_rows[i]->payload() + offset;
          if (!std::is_same<ValueType, char[0]>::value) {
            (reinterpret_cast<ValueType *>(data_))[i] = *(reinterpret_cast<const ValueType *>(payload));
          }
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
          const char *payload = stored_rows[i]->payload() + offset;
          set_payload_shallow(i, payload, sizeof(ValueType));
        }
      }
    }
  } else {
    const int64_t var_idx = row_meta.var_idx(col_idx);
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
}

template <typename ValueType>
OB_INLINE int ObFixedLengthFormat<ValueType>::from_rows(const sql::RowMeta &row_meta,
                                                        const sql::ObCompactRow **stored_rows,
                                                        const uint16_t selector[],
                                                        const int64_t size, const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  if (row_meta.is_reordered_fixed_expr(col_idx)) {
    const int64_t offset = row_meta.fixed_offsets(col_idx);
    if (!has_null()) {
      for (int64_t i = 0; i < size; i++) {
        if (nullptr == stored_rows[i]) {
          continue;
        }
        int64_t row_idx = selector[i];
        if (stored_rows[i]->is_null(col_idx)) {
          set_null(row_idx);
        } else {
          const char *payload = stored_rows[i]->payload() + offset;
          if (!std::is_same<ValueType, char[0]>::value) {
            (reinterpret_cast<ValueType *>(data_))[row_idx] = *(reinterpret_cast<const ValueType *>(payload));
          }
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
          const char *payload = stored_rows[i]->payload() + offset;
          set_payload_shallow(row_idx, payload, sizeof(ValueType));
        }
      }
    }
  } else {
    const int64_t var_idx = row_meta.var_idx(col_idx);
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

template <typename ValueType>
int ObFixedLengthFormat<ValueType>::from_row(const sql::RowMeta &row_meta,
                                             const sql::ObCompactRow *stored_row,
                                             const int64_t row_idx, const int64_t col_idx)
{
  int ret = OB_SUCCESS;
  if (stored_row->is_null(col_idx)) {
    set_null(row_idx);
  } else {
    const char *payload = stored_row->get_cell_payload(row_meta, col_idx);
    set_payload_shallow(row_idx, payload, sizeof(ValueType));
  }

  return ret;
}

template <typename ValueType>
int ObFixedLengthFormat<ValueType>::to_row(const sql::RowMeta &row_meta,
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

template <typename ValueType>
int ObFixedLengthFormat<ValueType>::to_row(const sql::RowMeta &row_meta,
                                           sql::ObCompactRow *stored_row, const uint64_t row_idx,
                                           const int64_t col_idx, const int64_t remain_size,
                                           const bool is_fixed_length_data,
                                           int64_t &row_size) const
{
  int ret = OB_SUCCESS;
  UNUSED(is_fixed_length_data);
  int64_t need_size = 0;
  bool size_calced = row_meta.fixed_expr_reordered();
  if (is_null(row_idx)) {
    need_size = size_calced ? 0 : sizeof(ValueType);
    row_size += need_size;
    if (need_size > remain_size) {
      ret = OB_BUF_NOT_ENOUGH;
    }
    stored_row->set_null(row_meta, col_idx);
  } else {
    const char *payload = NULL;
    ObLength len = 0;
    get_payload(row_idx, payload, len);
    if (!size_calced) {
      row_size += len;
    }
    if (!size_calced && len > remain_size) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      stored_row->set_cell_payload(row_meta, col_idx, payload, len);
    }
  }
  return ret;
}

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_FIXED_LENGTH_FORMAT_H_
