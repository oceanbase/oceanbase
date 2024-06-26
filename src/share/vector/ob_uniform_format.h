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

#ifndef OCEANBASE_SHARE_VECTOR_OB_UNIFORM_FORMAT_H_
#define OCEANBASE_SHARE_VECTOR_OB_UNIFORM_FORMAT_H_

#include "share/vector/ob_uniform_base.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/ob_compact_row.h"

namespace oceanbase
{
namespace sql
{
  struct ObEvalInfo;
}

namespace common
{

template<bool IS_CONST>
class ObUniformFormat : public ObUniformBase
{
public:
  ObUniformFormat(ObDatum *datums, sql::ObEvalInfo *eval_info)
    : ObUniformBase(datums, eval_info)
  {}

  OB_INLINE VectorFormat get_format() const override final {
    return IS_CONST ? VEC_UNIFORM_CONST : VEC_UNIFORM;
  }
  static const VectorFormat FORMAT = IS_CONST ? VEC_UNIFORM_CONST : VEC_UNIFORM;

  OB_INLINE bool has_null() const override final {
    return IS_CONST ? get_datum(0).is_null() : true;
  };
  OB_INLINE void set_has_null() override final { /*do nothing*/ };
  OB_INLINE void reset_has_null() override final { /*do nothing*/ };

  OB_INLINE bool is_batch_ascii() const override final { return false; };
  OB_INLINE void reset_is_batch_ascii() override final { /*do nothing*/ };
  OB_INLINE void set_is_batch_ascii() override final { /*do nothing*/ };
  OB_INLINE void set_has_non_ascii() override final { /*do nothing*/ };

  OB_INLINE bool is_null(const int64_t idx) const override final { return get_datum(idx).is_null(); }
  OB_INLINE void set_null(const int64_t idx) override final {
    get_datum(idx).set_null();
    eval_info_->notnull_ = false;
  };
  OB_INLINE void unset_null(const int64_t idx) override final {
    get_datum(idx).set_none();
  };
  inline void set_all_null(const int64_t size) {
    for (int64_t idx = 0; idx < size; ++idx) {
      get_datum(idx).set_null();
    }
    eval_info_->notnull_ = false;
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

  OB_INLINE void set_length(const int64_t idx, const ObLength length) override;

  OB_INLINE void set_payload(const int64_t idx,
                          const void *payload,
                          const ObLength length) override final {
    MEMCPY(const_cast<char *>(get_payload(idx)), payload, length);
    get_datum(idx).pack_ = length;
  }
  OB_INLINE void set_payload_shallow(const int64_t idx,
                                     const void *payload,
                                     const ObLength length) override final {
    get_datum(idx).ptr_ = static_cast<const char *>(payload);
    get_datum(idx).pack_ = length;
  }

  inline const ObDatum &get_datum(const int64_t idx) const { return datums_[IS_CONST ? 0 : idx]; }
  inline ObDatum &get_datum(const int64_t idx) { return datums_[IS_CONST ? 0 : idx]; }
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

  DEF_VEC_READ_INTERFACES(ObUniformFormat<IS_CONST>);
  DEF_VEC_WRITE_INTERFACES(ObUniformFormat<IS_CONST>);
};

template<bool IS_CONST>
OB_INLINE void ObUniformFormat<IS_CONST>::get_payload(const int64_t idx,
                                                   const char *&payload,
                                                   ObLength &length) const {
  payload = get_datum(idx).ptr_;
  length = get_datum(idx).len_;
}

template<bool IS_CONST>
OB_INLINE void ObUniformFormat<IS_CONST>::get_payload(const int64_t idx,
                                                   bool &is_null,
                                                   const char *&payload,
                                                   ObLength &length) const {
  is_null = get_datum(idx).null_;
  if (!is_null) {
    payload = get_datum(idx).ptr_;
    length = get_datum(idx).len_;
  }
}

template<bool IS_CONST>
OB_INLINE const char *ObUniformFormat<IS_CONST>::get_payload(const int64_t idx) const {
  return const_cast<char *>(get_datum(idx).ptr_);
}

template<bool IS_CONST>
OB_INLINE ObLength ObUniformFormat<IS_CONST>::get_length(const int64_t idx) const {
  return get_datum(idx).len_;
}

template<bool IS_CONST>
OB_INLINE void ObUniformFormat<IS_CONST>::set_length(const int64_t idx, const ObLength length) {
  get_datum(idx).pack_ = length;
}

template <bool IS_CONST>
OB_INLINE int ObUniformFormat<IS_CONST>::from_rows(const sql::RowMeta &row_meta,
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

template <bool IS_CONST>
OB_INLINE int ObUniformFormat<IS_CONST>::from_rows(const sql::RowMeta &row_meta,
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

template <bool IS_CONST>
OB_INLINE int ObUniformFormat<IS_CONST>::from_row(const sql::RowMeta &row_meta,
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

template <bool IS_CONST>
OB_INLINE int ObUniformFormat<IS_CONST>::to_row(const sql::RowMeta &row_meta,
                                                sql::ObCompactRow *stored_row,
                                                const uint64_t row_idx, const int64_t col_idx) const
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

template <bool IS_CONST>
OB_INLINE int ObUniformFormat<IS_CONST>::to_row(const sql::RowMeta &row_meta,
                                                sql::ObCompactRow *stored_row,
                                                const uint64_t row_idx, const int64_t col_idx,
                                                const int64_t remain_size,
                                                const bool is_fixed_length_data,
                                                int64_t &row_size) const
{
  int ret = OB_SUCCESS;
  int64_t need_size = 0;
  bool size_calced = (is_fixed_length_data && row_meta.fixed_expr_reordered());
  if (is_null(row_idx)) {
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
#endif // OCEANBASE_SHARE_VECTOR_OB_UNIFORM_FORMAT_H_
