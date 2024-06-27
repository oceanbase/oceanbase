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

#ifndef OCEANBASE_SHARE_VECTOR_OB_I_VECTOR_H_
#define OCEANBASE_SHARE_VECTOR_OB_I_VECTOR_H_

#include "share/datum/ob_datum.h"
#include "share/vector/type_traits.h"
#include "share/vector/static_check_utils.h"
#include "lib/rowid/ob_urowid.h"
#include "common/object/ob_object.h"
#include "sql/engine/ob_bit_vector.h"

namespace oceanbase
{
namespace sql {
  struct ObExpr;
  struct ObCompactRow;
  struct RowMeta;
  struct EvalBound;
}

namespace common
{

#define BATCH_EVAL_HASH_ARGS      const sql::ObExpr &expr,      \
                                  uint64_t *hash_values,        \
                                  const sql::ObBitVector &skip, \
                                  const sql::EvalBound &bound,  \
                                  const uint64_t *seeds,        \
                                  const bool is_batch_seed      \

#define EVAL_HASH_ARGS_FOR_ROW const sql::ObExpr &expr,        \
                                  uint64_t &hash_value,        \
                                  const int64_t batch_idx,     \
                                  const int64_t batch_size,    \
                                  const uint64_t seed

#define VECTOR_ONE_COMPARE_ARGS const sql::ObExpr &expr,    \
                                const int64_t row_idx,      \
                                const bool r_null,          \
                                const char *r_v,            \
                                const ObLength r_len,       \
                                int &cmp_ret

#define VECTOR_NOT_NULL_COMPARE_ARGS const sql::ObExpr &expr,    \
                                const int64_t row_idx1,      \
                                const int64_t row_idx2,      \
                                int &cmp_ret
#define DEF_VEC_READ_INTERFACES(Derived)                                                           \
public:                                                                                            \
  OB_INLINE bool is_false(const int64_t idx) const                                                 \
  {                                                                                                \
    return !derived_this().is_null(idx) && 0 == get_int(idx);                                      \
  }                                                                                                \
  OB_INLINE bool is_true(const int64_t idx) const                                                  \
  {                                                                                                \
    return !derived_this().is_null(idx) && 0 != get_int(idx);                                      \
  }                                                                                                \
  OB_INLINE int8_t get_int8(const uint64_t idx) const                                              \
  {                                                                                                \
    return get<int8_t>(idx);                                                                       \
  }                                                                                                \
  OB_INLINE int8_t get_tinyint(const int64_t idx) const                                            \
  {                                                                                                \
    return get<int8_t>(idx);                                                                       \
  }                                                                                                \
  OB_INLINE int16_t get_smallint(const int64_t idx) const                                          \
  {                                                                                                \
    return get<int16_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int32_t get_mediumint(const int64_t idx) const                                         \
  {                                                                                                \
    return get<int32_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int32_t get_int32(const int64_t idx) const                                             \
  {                                                                                                \
    return get<int32_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int64_t get_int(const int64_t idx) const                                               \
  {                                                                                                \
    return get<int64_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE uint8_t get_uint8(const int64_t idx) const                                             \
  {                                                                                                \
    return get<uint8_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE uint8_t get_utinyint(const int64_t idx) const                                          \
  {                                                                                                \
    return get<uint8_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE uint16_t get_usmallint(const int64_t idx) const                                        \
  {                                                                                                \
    return get<uint16_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE uint32_t get_umediumint(const int64_t idx) const                                       \
  {                                                                                                \
    return get<uint32_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE uint32_t get_uint32(const int64_t idx) const                                           \
  {                                                                                                \
    return get<uint32_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE uint64_t get_uint64(const int64_t idx) const                                           \
  {                                                                                                \
    return get<uint64_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE uint64_t get_uint(const int64_t idx) const                                             \
  {                                                                                                \
    return get<uint64_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE float get_float(const int64_t idx) const                                               \
  {                                                                                                \
    return get<float>(idx);                                                                        \
  }                                                                                                \
  OB_INLINE double get_double(const int64_t idx) const                                             \
  {                                                                                                \
    return get<double>(idx);                                                                       \
  }                                                                                                \
  OB_INLINE float get_ufloat(const int64_t idx) const                                              \
  {                                                                                                \
    return get<float>(idx);                                                                        \
  }                                                                                                \
  OB_INLINE double get_udouble(const int64_t idx) const                                            \
  {                                                                                                \
    return get<double>(idx);                                                                       \
  }                                                                                                \
  OB_INLINE int64_t get_ext(const int64_t idx) const                                               \
  {                                                                                                \
    return get<int64_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int64_t get_unknown(const int64_t idx) const                                           \
  {                                                                                                \
    return get<int64_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE uint64_t get_bit(const int64_t idx) const                                              \
  {                                                                                                \
    return get<uint64_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE bool get_bool(const int64_t idx)                                                       \
  {                                                                                                \
    return 0 != get_int(idx);                                                                      \
  }                                                                                                \
  OB_INLINE uint64_t get_enum(const int64_t idx) const                                             \
  {                                                                                                \
    return get<uint64_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE uint64_t get_set(const int64_t idx) const                                              \
  {                                                                                                \
    return get<uint64_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE uint64_t get_enumset(const int64_t idx) const                                          \
  {                                                                                                \
    return get<uint64_t>(idx);                                                                     \
  }                                                                                                \
  OB_INLINE int64_t get_interval_ym(const int64_t idx) const                                       \
  {                                                                                                \
    return get<int64_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int64_t get_interval_nmonth(const int64_t idx) const                                   \
  {                                                                                                \
    return get<int64_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int64_t get_datetime(const int64_t idx) const                                          \
  {                                                                                                \
    return get<int64_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int64_t get_timestamp(const int64_t idx) const                                         \
  {                                                                                                \
    return get<int64_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int32_t get_date(const int64_t idx) const                                              \
  {                                                                                                \
    return get<int32_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE int64_t get_time(const int64_t idx) const                                              \
  {                                                                                                \
    return get<int32_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE uint8_t get_year(const int64_t idx) const                                              \
  {                                                                                                \
    return get<uint8_t>(idx);                                                                      \
  }                                                                                                \
  OB_INLINE const number::ObCompactNumber &get_number(const int64_t idx) const                     \
  {                                                                                                \
    return *(reinterpret_cast<const number::ObCompactNumber *>(derived_this().get_payload(idx)));  \
  }                                                                                                \
  OB_INLINE const ObIntervalDSValue &get_interval_ds(const int64_t idx) const                      \
  {                                                                                                \
    return *(reinterpret_cast<const ObIntervalDSValue *>(derived_this().get_payload(idx)));        \
  }                                                                                                \
  OB_INLINE const ObOTimestampData &get_otimestamp_tz(const int64_t idx) const                     \
  {                                                                                                \
    return *(reinterpret_cast<const ObOTimestampData *>(derived_this().get_payload(idx)));         \
  }                                                                                                \
  OB_INLINE const ObOTimestampTinyData &get_otimestamp_tiny(const int64_t idx) const                     \
  {                                                                                                \
    return *(reinterpret_cast<const ObOTimestampTinyData *>(derived_this().get_payload(idx)));         \
  }                                                                                                \
  OB_INLINE ObString get_string(const int64_t idx) const                                           \
  {                                                                                                \
    const char *str = NULL;                                                                        \
    ObLength len = 0;                                                                              \
    derived_this().get_payload(idx, str, len);                                                     \
    return ObString(len, str);                                                                     \
  }                                                                                                \
  OB_INLINE int get_enumset_inner(const int64_t idx, ObEnumSetInnerValue &inner_value) const       \
  {                                                                                                \
    int64_t pos = 0;                                                                               \
    const char *payload = NULL;                                                                    \
    ObLength len = 0;                                                                              \
    derived_this().get_payload(idx, payload, len);                                                 \
    return inner_value.deserialize(payload, len, pos);                                             \
  }                                                                                                \
  OB_INLINE ObURowIDData get_urowid(const int64_t idx) const                                       \
  {                                                                                                \
    const char *ptr = NULL;                                                                        \
    ObLength len = 0;                                                                              \
    derived_this().get_payload(idx, ptr, len);                                                     \
    return ObURowIDData(len, reinterpret_cast<const uint8_t *>(ptr));                              \
  }                                                                                                \
  OB_INLINE const ObLobLocator &get_lob_locator(const int64_t idx) const                           \
  {                                                                                                \
    return *(reinterpret_cast<const ObLobLocator *>(derived_this().get_payload(idx)));             \
  }                                                                                                \
  OB_INLINE const ObLobCommon &get_lob_data(const int64_t idx) const                               \
  {                                                                                                \
    return *(reinterpret_cast<const ObLobCommon *>(derived_this().get_payload(idx)));              \
  }                                                                                                \
  OB_INLINE const ObDecimalInt *get_decimal_int(const int64_t idx) const                           \
  {                                                                                                \
    return reinterpret_cast<const ObDecimalInt *>(derived_this().get_payload(idx));                \
  }                                                                                                \
                                                                                                   \
private:                                                                                           \
  const Derived &derived_this() const                                                              \
  {                                                                                                \
    return *static_cast<const Derived *>(this);                                                    \
  }                                                                                                \
  template <typename T>                                                                            \
  OB_INLINE T get(const int64_t idx) const                                                         \
  {                                                                                                \
    static_assert(sizeof(T) <= sizeof(int64_t), "invalid type");                                   \
    return *reinterpret_cast<const T *>(derived_this().get_payload(idx));                          \
  }

#define DEF_VEC_WRITE_INTERFACES(Derived)                                                          \
public:                                                                                            \
  OB_INLINE void set_int(const int64_t idx, const int64_t v)                                       \
  {                                                                                                \
    set<int64_t>(idx, v);                                                                          \
  };                                                                                               \
  OB_INLINE void set_int32(const int64_t idx, const int32_t v)                                     \
  {                                                                                                \
    set<int32_t>(idx, v);                                                                          \
  }                                                                                                \
  OB_INLINE void set_uint(const int64_t idx, const uint64_t v)                                     \
  {                                                                                                \
    set<uint64_t>(idx, v);                                                                         \
  }                                                                                                \
  OB_INLINE void set_uint32(const int64_t idx, const uint32_t v)                                   \
  {                                                                                                \
    set<uint32_t>(idx, v);                                                                         \
  }                                                                                                \
  OB_INLINE void set_bit(const int64_t idx, const uint64_t v)                                      \
  {                                                                                                \
    set<uint64_t>(idx, v);                                                                         \
  }                                                                                                \
  OB_INLINE void set_bool(const int64_t idx, const bool v)                                         \
  {                                                                                                \
    set_int(idx, static_cast<int64_t>(v));                                                         \
  }                                                                                                \
  OB_INLINE void set_true(const int64_t idx)                                                       \
  {                                                                                                \
    set_int(idx, static_cast<int64_t>(true));                                                      \
  }                                                                                                \
  OB_INLINE void set_false(const int64_t idx)                                                      \
  {                                                                                                \
    set_int(idx, static_cast<int64_t>(false));                                                     \
  }                                                                                                \
  OB_INLINE void set_float(const int64_t idx, const float v)                                       \
  {                                                                                                \
    set<float>(idx, v);                                                                            \
  }                                                                                                \
  OB_INLINE void set_double(const int64_t idx, const double v)                                     \
  {                                                                                                \
    set<double>(idx, v);                                                                           \
  }                                                                                                \
  OB_INLINE void set_enum(const int64_t idx, const uint64_t v)                                     \
  {                                                                                                \
    set<uint64_t>(idx, v);                                                                         \
  }                                                                                                \
  OB_INLINE void set_set(const int64_t idx, const uint64_t v)                                      \
  {                                                                                                \
    set<uint64_t>(idx, v);                                                                         \
  }                                                                                                \
  OB_INLINE void set_datetime(const int64_t idx, const int64_t v)                                  \
  {                                                                                                \
    set<int64_t>(idx, v);                                                                          \
  }                                                                                                \
  OB_INLINE void set_timestamp(const int64_t idx, const int64_t v)                                 \
  {                                                                                                \
    set<int64_t>(idx, v);                                                                          \
  }                                                                                                \
  OB_INLINE void set_time(const int64_t idx, const int64_t v)                                      \
  {                                                                                                \
    set_int(idx, v);                                                                               \
  }                                                                                                \
  OB_INLINE void set_date(const int64_t idx, const int32_t v)                                      \
  {                                                                                                \
    set<int32_t>(idx, v);                                                                          \
  }                                                                                                \
  OB_INLINE void set_year(const int64_t idx, const int8_t v)                                       \
  {                                                                                                \
    set<int8_t>(idx, v);                                                                           \
  }                                                                                                \
  OB_INLINE void set_interval_nmonth(const int64_t idx, const int64_t v)                           \
  {                                                                                                \
    set<int64_t>(idx, v);                                                                          \
  }                                                                                                \
  OB_INLINE void set_interval_ym(const int64_t idx, const int64_t v)                               \
  {                                                                                                \
    set<int64_t>(idx, v);                                                                          \
  }                                                                                                \
  OB_INLINE void set_interval_ds(const int64_t idx, const ObIntervalDSValue &v)                    \
  {                                                                                                \
    derived_this().set_payload_shallow(idx, &v, v.get_store_size());                               \
  }                                                                                                \
  OB_INLINE void set_otimestamp_tz(const int64_t idx, const ObOTimestampData &v)                   \
  {                                                                                                \
    *(reinterpret_cast<ObOTimestampData *>(no_cv(derived_this().get_payload(idx)))) = v;           \
  }                                                                                                \
  OB_INLINE void set_otimestamp_tiny(const int64_t idx, const ObOTimestampTinyData &v)             \
  {                                                                                                \
    *(reinterpret_cast<ObOTimestampTinyData *>(no_cv(derived_this().get_payload(idx)))) = v;       \
  }                                                                                                \
  OB_INLINE void set_number(const int64_t idx, const number::ObNumber &num)                        \
  {                                                                                                \
    using CptNumber = number::ObCompactNumber;                                                     \
    CptNumber *cnum = reinterpret_cast<CptNumber *>(no_cv(derived_this().get_payload(idx)));       \
    cnum->desc_ = num.d_;                                                                          \
    const ObLength len = num.d_.len_ * sizeof(*num.get_digits());                                  \
    MEMCPY(&cnum->digits_[0], num.get_digits(), len);                                              \
    derived_this().set_payload_shallow(idx, cnum, len + sizeof(ObNumberDesc));                     \
  }                                                                                                \
  OB_INLINE void set_number(const int64_t idx, const number::ObCompactNumber &cnum)                \
  {                                                                                                \
    ObLength len =                                                                                 \
      static_cast<uint32_t>(sizeof(cnum) + cnum.desc_.len_ * sizeof(cnum.digits_[0]));             \
    derived_this().set_payload(idx, &cnum, len);                                                   \
  }                                                                                                \
  OB_INLINE void set_number_shallow(const int64_t idx, const number::ObCompactNumber &cnum)        \
  {                                                                                                \
    ObLength len =                                                                                 \
      static_cast<uint32_t>(sizeof(cnum) + cnum.desc_.len_ * sizeof(cnum.digits_[0]));             \
    derived_this().set_payload_shallow(idx, &cnum, len);                                           \
  }                                                                                                \
  OB_INLINE void set_string(const int64_t idx, const ObString &v)                                  \
  {                                                                                                \
    derived_this().set_payload_shallow(idx, v.ptr(), v.length());                                  \
  }                                                                                                \
  OB_INLINE void set_string(const int64_t idx, const char *ptr, const uint32_t len)                \
  {                                                                                                \
    derived_this().set_payload_shallow(idx, ptr, len);                                             \
  }                                                                                                \
  OB_INLINE void set_enumset_inner(const int64_t idx, const ObString &v)                           \
  {                                                                                                \
    set_string(idx, v);                                                                            \
  }                                                                                                \
  OB_INLINE void set_enumset_inner(const int64_t idx, const char *ptr, const uint32_t len)         \
  {                                                                                                \
    set_string(idx, ptr, len);                                                                     \
  }                                                                                                \
  OB_INLINE void set_urowid(const int64_t idx, const ObURowIDData &urowid_data)                    \
  {                                                                                                \
    const char *ptr = reinterpret_cast<const char *>(urowid_data.rowid_content_);                  \
    ObLength len = static_cast<uint32_t>(urowid_data.rowid_len_);                                  \
    derived_this().set_payload(idx, ptr, len);                                                     \
  }                                                                                                \
  OB_INLINE void set_urowid(const int64_t idx, const char *ptr, const int64_t size)                \
  {                                                                                                \
    derived_this().set_payload(idx, ptr, static_cast<uint32_t>(size));                             \
  }                                                                                                \
  OB_INLINE void set_lob_locator(const int64_t idx, const ObLobLocator &value)                     \
  {                                                                                                \
    derived_this().set_payload(idx, &value, static_cast<uint32_t>(value.get_total_size()));        \
  }                                                                                                \
  OB_INLINE void set_lob_data(const int64_t idx, const ObLobCommon &value, int64_t length)         \
  {                                                                                                \
    derived_this().set_payload(idx, &value, static_cast<uint32_t>(length));                        \
  }                                                                                                \
  OB_INLINE void set_decimal_int(const int64_t idx, const ObDecimalInt *decint, int32_t len)       \
  {                                                                                                \
    derived_this().set_payload(idx, decint, static_cast<uint32_t>(len));                           \
  }                                                                                                \
                                                                                                   \
private:                                                                                           \
  template <typename T>                                                                            \
  OB_INLINE __attribute__((always_inline)) T *no_cv(const T *ptr) const                            \
  {                                                                                                \
    return const_cast<T *>(ptr);                                                                   \
  }                                                                                                \
  Derived &derived_this()                                                                          \
  {                                                                                                \
    return *static_cast<Derived *>(this);                                                          \
  }                                                                                                \
  template <typename T>                                                                            \
  OB_INLINE void set(const int64_t idx, const T value)                                             \
  {                                                                                                \
    static_assert(sizeof(T) <= sizeof(int64_t), "invalid type");                                   \
    static_cast<Derived *>(this)->set_payload(idx, &value, sizeof(T));                             \
  }

/*
                                               ObIVector
                                                   |
                                              ObVectorBase
                                                   |
                             -------------------------------------------
                             |                                         |
                   ObBitmapNullVectorBase                              |
                             |                                         |
    ----------------------------------------------                     |
    |                        |                   |                     |
ObFixedLengthBase    ObDiscreteBase      ObContinuousBase         ObUniformBase
    |                        |                   |                     |
template<ValueType>                       template<Offset>       template<IS_CONST>
ObFixedLengthFormat   ObDiscreteFormat    ObContinuousFormat     ObUniformFormat
    |                        |                   |                      |
template<ValueType,Op>  template<Op>      template<Offset, Op>   template<IS_CONST, Op>
ObFixedLengthVector   ObDiscreteVector    ObContinuousVector     ObUniformVector
*/
class ObIVector
{
public:
  enum CHARSET_FLAG
  {
    UNKNOWN = 0,
    ASCII = 1,
    NON_ASCII = 2,
  };

public:
  static const int64_t MAX_VECTOR_STRUCT_SIZE = 64;
  virtual VectorFormat get_format() const = 0;

  virtual void get_payload(const int64_t idx,
                           const char *&payload,
                           ObLength &length) const = 0;
  virtual void get_payload(const int64_t idx,
                           bool &is_null,
                           const char *&payload,
                           ObLength &length) const = 0;
  virtual const char *get_payload(const int64_t idx) const = 0;
  virtual ObLength get_length(const int64_t idx) const = 0;

  virtual void set_length(const int64_t idx, const ObLength length) = 0;

  // deep copy payload
  virtual void set_payload(const int64_t idx,
                           const void *payload,
                           const ObLength length) = 0;
  virtual void set_payload_shallow(const int64_t idx,
                                   const void *payload,
                                   const ObLength length) = 0;

  virtual bool has_null() const = 0;
  virtual void set_has_null() = 0;
  virtual void reset_has_null() = 0;
  virtual bool is_batch_ascii() const = 0;
  virtual void reset_is_batch_ascii() = 0;
  virtual void set_is_batch_ascii() = 0;
  virtual void set_has_non_ascii() = 0;
  virtual bool is_null(const int64_t idx) const = 0;
  virtual void set_null(const int64_t idx) = 0;
  virtual void unset_null(const int64_t idx) = 0;
  void set_null(const sql::EvalBound &bound);

  virtual int default_hash(BATCH_EVAL_HASH_ARGS) const = 0;
  virtual int murmur_hash(BATCH_EVAL_HASH_ARGS) const = 0;
  // In vectorization 1.0, hash value (calculated by murmur_hash_v2) of null is inconsistent for different types.
  // For example, null hash value of ObFixedDouble is equal to `seed`, null hash vlaue of int to equal to `murmur_hash_v2(nullptr, 0, seed)`
  // In vectorization 2.0, null hash value (calculated by `murmur_hash_v3`)to equal to `seed` for all types (including null type)
  // We use different interface names (v3 in 2.0, v2 in 1.0) to distinguish hash function behaviors.
  virtual int murmur_hash_v3(BATCH_EVAL_HASH_ARGS) const = 0;
  // used for one row calculation
  virtual int murmur_hash_v3_for_one_row(EVAL_HASH_ARGS_FOR_ROW) const = 0;
  // compare n-th value in vector with other value,
  // the type of other value must be same as this vector
  virtual int null_first_cmp(VECTOR_ONE_COMPARE_ARGS) const = 0;
  virtual int null_last_cmp(VECTOR_ONE_COMPARE_ARGS) const = 0;
  virtual int no_null_cmp(VECTOR_NOT_NULL_COMPARE_ARGS) const = 0;

  // append values to this vector from idx-th column of rows
  virtual int from_rows(const sql::RowMeta &row_meta,
                        const sql::ObCompactRow **stored_rows,
                        const int64_t size,
                        const int64_t col_idx) = 0;

  virtual int from_rows(const sql::RowMeta &row_meta,
                        const sql::ObCompactRow **stored_rows,
                        const uint16_t selector[],
                        const int64_t size,
                        const int64_t col_idx) = 0;

  virtual int from_row(const sql::RowMeta &row_meta,
                       const sql::ObCompactRow *stored_rows,
                       const int64_t row_idx,
                       const int64_t col_idx) = 0;

  // set values from this vector to idx-th column of rows
  virtual void to_rows(const sql::RowMeta &row_meta,
                      sql::ObCompactRow **stored_rows,
                      const uint16_t selector[],
                      const int64_t size,
                      const int64_t col_idx) const = 0;

  virtual void to_rows(const sql::RowMeta &row_meta,
                      sql::ObCompactRow **stored_rows,
                      const int64_t size,
                      const int64_t col_idx) const = 0;

  virtual int to_row(const sql::RowMeta &row_meta,
                     sql::ObCompactRow *stored_row,
                     const uint64_t row_idx,
                     const int64_t col_idx) const = 0;
  virtual int to_row(const sql::RowMeta &row_meta,
                     sql::ObCompactRow *stored_row,
                     const uint64_t row_idx,
                     const int64_t col_idx,
                     const int64_t remain_size,
                     const bool is_fixed_length_data,
                     int64_t &row_size) const = 0;

  virtual int64_t to_string(char *buf, const int64_t buf_len) const
  {
    UNUSED(buf);
    UNUSED(buf_len);
    return 0;
  }
  DEF_VEC_READ_INTERFACES(ObIVector);
  DEF_VEC_WRITE_INTERFACES(ObIVector);
};

using IVectorPtrs = common::ObIArray<ObIVector *>;

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_I_VECTOR_H_
