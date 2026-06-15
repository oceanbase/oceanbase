/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_OB_UNIFORM_FORMAT_H_
#define OCEANBASE_SHARE_VECTOR_OB_UNIFORM_FORMAT_H_

#include "share/vector/ob_uniform_base.h"
#include "share/vector/ob_fixed_length_base.h"
#include "share/vector/ob_discrete_base.h"
#include "share/vector/ob_continuous_base.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "lib/ob_abort.h"

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
    eval_info_->set_notnull(false);
  };
  OB_INLINE void unset_null(const int64_t idx) override final {
    get_datum(idx).set_none();
  };
  inline void set_all_null(const int64_t size) {
    for (int64_t idx = 0; idx < size; ++idx) {
      get_datum(idx).set_null();
    }
    eval_info_->set_notnull(false);
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

  OB_INLINE void set_payload(const int64_t idx, const void *payload,
                             const ObLength length) override final
  {
    MEMCPY(const_cast<char *>(get_payload(idx)), payload, length);
    get_datum(idx).pack_ = length;
  }
  OB_INLINE void set_payload_shallow(const int64_t idx,
                                     const void *payload,
                                     const ObLength length) override final {
    get_datum(idx).ptr_ = static_cast<const char *>(payload);
    get_datum(idx).pack_ = length;
    if (OB_UNLIKELY(is_collection_expr()))  {
      set_collection_payload_shallow(idx, payload, length);
    }
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
  void set_collection_payload_shallow(const int64_t idx, const void *payload,
                                      const ObLength length);
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

template <bool IS_CONST>
OB_INLINE int ObUniformFormat<IS_CONST>::from_rows(const sql::RowMeta &row_meta,
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


template <bool IS_CONST>
int ObUniformFormat<IS_CONST>::from_vector_fixed(const ObFixedLengthBase *src_fixed,
                                                       const sql::ObBitVector *skip,
                                                       const int64_t start,
                                                       const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_fixed)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(from_vector_base(src_fixed))) {
    SQL_ENG_LOG(WARN, "failed to copy base", K(ret));
  } else {
    const sql::ObBitVector *src_nulls = src_fixed->get_nulls();
    const char *src_data = src_fixed->get_data();
    const ObLength src_len = src_fixed->get_length();
    if constexpr (IS_CONST) {
      for (int64_t i = start; i < end; ++i) {
        if (skip != nullptr && skip->at(i)) {
          continue;
        }
        if (src_nulls != nullptr && src_nulls->at(i)) {
          set_null(i);
        } else {
          ObDatum &datum = get_datums()[0];
          datum.ptr_ = src_data + i * src_len;
          datum.pack_ = src_len;
        }
        break;
      }
    } else {
      for (int64_t i = start; i < end; ++i) {
        if (skip != nullptr && skip->at(i)) {
          continue;
        }
        if (src_nulls != nullptr && src_nulls->at(i)) {
          set_null(i);
        } else {
          ObDatum &datum = get_datum(i);
          datum.ptr_ = src_data + i * src_len;
          datum.pack_ = src_len;
        }
      }
    }
  }
  return ret;
}

template <bool IS_CONST>
int ObUniformFormat<IS_CONST>::from_vector_uniform(const ObUniformBase *src_uniform,
                                                          VectorFormat src_format,
                                                          const sql::ObBitVector *skip,
                                                          const int64_t start,
                                                          const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_uniform)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "src_uniform is null", K(ret));
  } else if (OB_FAIL(from_vector_base(src_uniform))) {
    SQL_ENG_LOG(WARN, "failed to copy base", K(ret));
  } else {
    const ObDatum *src_datums = src_uniform->get_datums();
    const bool src_is_const = (src_format == VEC_UNIFORM_CONST);
    ObDatum *dst_datums = get_datums();
    if constexpr (IS_CONST) {
      // Respect skip: only copy when row 0 is not skipped (e.g. IFNULL: do not overwrite with arg1 when arg0 is not null)
      if (skip == nullptr || !skip->at(0)) {
        get_datum(0) = src_datums[0];
      }
    } else {
      // dst is non-const (ObUniformFormat<false>): multi-row copy path
      if (src_is_const) {
        // src const -> dst non-const: broadcast src_datums[0] to all (non-skipped) rows
        if (skip == nullptr) {
          for (int64_t i = start; i < end; ++i) {
            dst_datums[i] = src_datums[0];
          }
        } else {
          for (int64_t i = start; i < end; ++i) {
            if (skip->at(i)) {
              continue;
            }
            get_datum(i) = src_datums[0];
          }
        }
      } else {
        // src non-const -> dst non-const
        if (skip == nullptr) {
          if (dst_datums != src_datums) {
            MEMCPY(dst_datums + start, src_datums + start, (end - start) * sizeof(ObDatum));
          }
        } else {
          for (int64_t i = start; i < end; ++i) {
            if (skip->at(i)) {
              continue;
            }
            get_datum(i) = src_datums[i];
          }
        }
      }
    }
  }
  return ret;
}

template <bool IS_CONST>
int ObUniformFormat<IS_CONST>::from_vector_discrete(const ObDiscreteBase *src_discrete,
                                                           const sql::ObBitVector *skip,
                                                           const int64_t start,
                                                           const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_discrete)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(from_vector_base(src_discrete))) {
    SQL_ENG_LOG(WARN, "failed to copy base", K(ret));
  } else {
    const sql::ObBitVector *src_nulls = src_discrete->get_nulls();
    char **src_ptrs = src_discrete->get_ptrs();
    const ObLength *src_lens = src_discrete->get_lens();
    if constexpr (IS_CONST) {
      if (src_nulls != nullptr && src_nulls->at(start)) {
        set_null(start);
      } else {
        ObDatum &datum = get_datums()[0];
        datum.ptr_ = src_ptrs[start];
        datum.pack_ = src_lens[start];
      }
    } else {
      for (int64_t i = start; i < end; ++i) {
        if (skip != nullptr && skip->at(i)) {
          continue;
        }
        if (src_nulls != nullptr && src_nulls->at(i)) {
          set_null(i);
        } else {
          ObDatum &datum = get_datum(i);
          datum.ptr_ = src_ptrs[i];
          datum.pack_ = src_lens[i];
        }
      }
    }
  }
  return ret;
}

template <bool IS_CONST>
int ObUniformFormat<IS_CONST>::from_vector_continuous(const ObContinuousBase *src_continuous,
                                                             const sql::ObBitVector *skip,
                                                             const int64_t start,
                                                             const int64_t end)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_continuous)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(from_vector_base(src_continuous))) {
    SQL_ENG_LOG(WARN, "failed to copy base", K(ret));
  } else {
    const sql::ObBitVector *src_nulls = src_continuous->get_nulls();
    const char *src_data = src_continuous->get_data();
    const uint32_t *src_offsets = src_continuous->get_offsets();
    if constexpr (IS_CONST) {
      if (src_nulls != nullptr && src_nulls->at(start)) {
        set_null(start);
      } else {
        ObDatum &datum = get_datums()[0];
        datum.ptr_ = src_data + src_offsets[start];
        datum.pack_ = src_offsets[start + 1] - src_offsets[start];
      }
    } else {
      for (int64_t i = start; i < end; ++i) {
        if (skip != nullptr && skip->at(i)) {
          continue;
        }
        if (src_nulls != nullptr && src_nulls->at(i)) {
          set_null(i);
        } else {
          ObDatum &datum = get_datum(i);
          datum.ptr_ = src_data + src_offsets[i];
          datum.pack_ = src_offsets[i + 1] - src_offsets[i];
        }
      }
    }
  }
  return ret;
}

#define DEF_SET_COLLECTION_PAYLOAD(is_const)                                                       \
  template <>                                                                                      \
  void ObUniformFormat<is_const>::set_collection_payload_shallow(                                  \
    const int64_t idx, const void *payload, const ObLength length)                                 \
  {                                                                                                \
    if (sql::ObCollectionExprUtil::is_compact_fmt_cell(payload)) {                                 \
      set_has_compact_collection();                                                                \
    } else {                                                                                       \
    }                                                                                              \
  }

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_OB_UNIFORM_FORMAT_H_