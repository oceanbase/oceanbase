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

#ifndef OCEANBASE_SHARE_VECTOR_OB_VEC_VISITOR_H_
#define OCEANBASE_SHARE_VECTOR_OB_VEC_VISITOR_H_

#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/vector/ob_fixed_length_vector.h"
#include "share/vector/ob_i_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "share/vector/type_traits.h"

namespace oceanbase
{
namespace common
{
struct FixedLengthVecVisitor {
  FixedLengthVecVisitor(ObFixedLengthBase *vector):
  data_(vector->get_data()), len_(vector->get_length()), nulls_(vector->get_nulls())
  {}
  inline char *get_payload(const int64_t idx) const {
    return data_ + len_ * idx;
  }
  inline bool is_null(const int64_t idx) const { return nulls_->at(idx); }
  inline ObLength get_length(const int) const { return len_; }
  inline void get_payload(const int64_t idx, const char *&payload,
                    ObLength &length) const {
    length = len_;
    payload = data_ + len_ * idx;
  }
  inline void get_payload(const int64_t idx, bool &is_null,
                          const char *&payload, ObLength &length) const {
    is_null = nulls_->at(idx);
    if (!is_null) {
      payload = reinterpret_cast<const char *>(data_ + len_ * idx);
      length = len_;
    }
  }
  inline void set_length(const int64_t idx, const ObLength length) {}
  inline void set_payload(const int64_t idx, const void *payload,
                          const ObLength length) {
    OB_ASSERT(length == len_);
    if (OB_UNLIKELY(nulls_->at(idx))) {
      nulls_->unset(idx);
    }
    MEMCPY(data_ + idx * length, payload, length);
  }
  inline void set_payload_shallow(const int64_t idx, const void *payload,
                                  const ObLength length) {
    set_payload(idx, payload, length);
  }
  inline void set_null(const int64_t idx) {
    nulls_->set(idx);
  }
  TO_STRING_KV(K_(data), K_(len));
  char *data_;
  const ObLength len_;
  sql::ObBitVector *nulls_;
};

template<typename T>
class ObVectorCellVisitor
{
public:
  ObVectorCellVisitor(T &vector, uint16_t idx) : vector_(vector), idx_(idx) {}
protected:
  template <typename ValueType>
  inline __attribute__((always_inline)) ValueType *no_cv(const ValueType *ptr) const { return const_cast<ValueType *>(ptr); }

private:
  #define TYPE_CHECKER_DEF(checker_name, ...)                                  \
  template <typename ValueType>                                                \
  struct checker_name {                                                        \
    static constexpr bool value = exist_type<ValueType, ##__VA_ARGS__>::value; \
  };
  TYPE_CHECKER_DEF(is_native_ctype, bool, int8_t, int16_t, int32_t, int64_t,
                                      uint8_t, uint16_t, uint32_t, uint64_t,
                                      double, float);
public:
  template <typename ValueType>
  inline void set(const ValueType &value)
  {
    static_assert(is_native_ctype<ValueType>::value, "invalid type");
    set_payload(&value, sizeof(ValueType));
  }
  template<typename ValueType>
  inline ValueType get() const
  {
    static_assert(is_native_ctype<ValueType>::value, "invalid type");
    return *(reinterpret_cast<const ValueType *>(get_payload()));
  }
  #undef TYPE_CHECKER_DEF

  inline void set_batch_idx(uint16_t idx)
  {
    idx_ = idx;
  }
  inline const char *get_payload() const
  {
    return vector_.get_payload(idx_);
  }
  inline ObLength get_length() const
  {
    return vector_.get_length(idx_);
  }
  inline bool is_null() const
  {
    return vector_.is_null(idx_);
  }
  // getter methods
  inline int32_t get_int_bytes() const
  {
    return get_length();
  }
  inline int8_t get_int8() const
  {
    return  get<int8_t>();
  }
  inline int8_t get_tinyint() const
  {
    return get<int8_t>();
  }
  inline int16_t get_smallint() const
  {
    return get<int16_t>();
  }
  inline int32_t get_mediumint() const
  {
    return get<int32_t>();
  }
  inline int32_t get_int32() const
  {
    return get<int32_t>();
  }
  inline int64_t get_int() const
  {
    return get<int64_t>();
  }
  inline uint8_t get_uint8() const
  {
    return get<uint8_t>();
  }
  inline uint8_t get_utinyint() const
  {
    return get<uint8_t>();
  }
  inline uint16_t get_usmallint() const
  {
    return get<uint16_t>();
  }
  inline uint32_t get_umediumint() const
  {
    return get<uint32_t>();
  }
  inline uint32_t get_uint32() const
  {
    return get<uint32_t>();
  }
  inline uint64_t get_uint64() const
  {
    return get<uint64_t>();
  }
  inline uint64_t get_uint() const
  {
    return get<uint64_t>();
  }
  inline float get_float() const
  {
    return get<float>();
  }
  inline double get_double() const
  {
    return get<double>();
  }
  inline float get_ufloat() const
  {
    return get<float>();
  }
  inline double get_udouble() const
  {
    return get<double>();
  }
  inline int64_t get_ext() const
  {
    return get<int64_t>();
  }
  inline int64_t get_unknown() const
  {
    return get<int64_t>();
  }
  inline uint64_t get_bit() const
  {
    return get<uint64_t>();
  }
  inline bool get_bool() const
  {
    return 0 != get_int();
  }
  inline uint64_t get_enum() const
  {
    return get<uint64_t>();
  }
  inline uint64_t get_set() const
  {
    return get<uint64_t>();
  }
  inline uint64_t get_enumset() const
  {
    return get<uint64_t>();
  }
  inline int64_t get_interval_ym() const
  {
    return get<int64_t>();
  }
  inline int64_t get_interval_nmonth() const
  {
    return get<int64_t>();
  }
  inline int64_t get_datetime() const
  {
    return get<int64_t>();
  }
  inline int64_t get_timestamp() const
  {
    return get<int64_t>();
  }
  inline int32_t get_date() const
  {
    return get<int32_t>();
  }
  inline int64_t get_time() const
  {
    return get<int32_t>();
  }
  inline uint8_t get_year() const
  {
    return get<uint8_t>();
  }
  inline ObOTimestampData get_otimestamp_tiny() const {
    ObOTimestampData result;
    const ObOTimestampTinyData timestamp_tiny =
        *(reinterpret_cast<const ObOTimestampTinyData *>(get_payload()));
    result.time_ctx_.time_desc_ = timestamp_tiny.desc_;
    result.time_ctx_.tz_desc_ = 0;
    result.time_us_ = timestamp_tiny.time_us_;
    return result;
  }
  inline const number::ObCompactNumber &get_number() const
  {
    return *(reinterpret_cast<const number::ObCompactNumber *>(get_payload()));
  }
  inline const ObIntervalDSValue &get_interval_ds() const
  {
    return *(reinterpret_cast<const ObIntervalDSValue *>(get_payload()));
  }
  inline const ObOTimestampData &get_otimestamp_tz() const
  {
    return *(reinterpret_cast<const ObOTimestampData *>(get_payload()));
  }
  inline const ObLobLocator &get_lob_locator() const
  {
    return *(reinterpret_cast<const ObLobLocator *>(get_payload()));
  }
  inline const ObDecimalInt *get_decimal_int() const
  {
    return reinterpret_cast<const ObDecimalInt *>(get_payload());
  }
  inline ObString get_string() const
  {
    const char *str = NULL;
    ObLength len = 0;
    vector_.get_payload(idx_, str, len);
    return ObString(len, str);
  }
  inline ObURowIDData get_urowid() const
  {
    const char *ptr = NULL;
    ObLength len = 0;
    vector_.get_payload(idx_, ptr, len);
    return ObURowIDData(len, reinterpret_cast<const uint8_t *>(ptr));
  }

  // setter methods
  inline void set_length(const ObLength length)
  {
    vector_.set_length(idx_, length);
  }
  inline void set_null()
  {
    vector_.set_null(idx_);
  }
  inline void set_payload(const void *payload, const ObLength length)
  {
    vector_.set_payload(idx_, payload, length);
  }
  inline void set_payload_shallow(const void *ptr, const ObLength length)
  {
    vector_.set_payload_shallow(idx_, ptr, length);
  }
  inline void set_int(const int64_t v)
  {
    set<int64_t>(v);
  }
  inline void set_int32(const int32_t v)
  {
    set<int32_t>(v);
  }
  inline void set_uint(const uint64_t v)
  {
    set<uint64_t>(v);
  }
  inline void set_uint32(const uint32_t v)
  {
    set<uint32_t>(v);
  }
  inline void set_bit(const uint64_t v)
  {
    set<uint64_t>(v);
  }
  inline void set_bool(const bool v)
  {
    set_int(static_cast<int64_t>(v));
  }
  inline void set_true()
  {
    set_int(static_cast<int64_t>(true));
  }
  inline void set_false()
  {
    set_int(static_cast<int64_t>(false));
  }
  inline void set_float(const float v)
  {
    set<float>(v);
  }
  inline void set_double(const double v)
  {
    set<double>(v);
  }
  inline void set_enum(const uint64_t v)
  {
    set<uint64_t>(v);
  }
  inline void set_set(const uint64_t v)
  {
    set<uint64_t>(v);
  }
  inline void set_interval_nmonth(const int64_t interval_nmonth)
  {
    set<int64_t>(interval_nmonth);
  }
  inline void set_interval_ym(const int64_t interval_nmonth)
  {
    set<int64_t>(interval_nmonth);
  }
  inline void set_interval_ds(const ObIntervalDSValue &v)
  {
    vector_.set_payload_shallow(idx_, &v, v.get_store_size());
  }
  inline void set_datetime(const int64_t v)
  {
    set<int64_t>(v);
  }
  inline void set_timestamp(const int64_t v)
  {
    set<int64_t>(v);
  }
  inline void set_otimestamp_tz(const ObOTimestampData &v)
  {
    *(reinterpret_cast<ObOTimestampData *>(no_cv(get_payload()))) = v;
  }
  inline void set_time(const int64_t v)
  {
    set_int(v);
  }
  inline void set_otimestamp(const ObOTimestampData &v)
  {
    *(reinterpret_cast<ObOTimestampData *>(no_cv(get_payload()))) = v;
  }
  inline void set_date(const int32_t v)
  {
    set<int32_t>(v);
  }
  inline void set_year(const int8_t v)
  {
    set<int8_t>(v);
  }
  // Set number, deep copy all number digits here.
  inline void set_number(const number::ObNumber &num)
  {
    using CptNumber = number::ObCompactNumber;
    CptNumber *cnum = reinterpret_cast<CptNumber *>(no_cv(get_payload()));
    cnum->desc_ = num.d_;
    const ObLength len = num.d_.len_ * sizeof(*num.get_digits());
    MEMCPY(&cnum->digits_[0], num.get_digits(), len);
    set_payload_shallow(cnum, len + sizeof(ObNumberDesc));
  }
  // Set compact number, deep copy all number digits too.
  inline void set_number(const number::ObCompactNumber &cnum)
  {
    ObLength len = static_cast<uint32_t>(sizeof(cnum) + cnum.desc_.len_ * sizeof(cnum.digits_[0]));
    set_payload(&cnum, len);
  }
  inline void set_number_shallow(const number::ObCompactNumber &cnum)
  {
    ObLength len = static_cast<uint32_t>(sizeof(cnum) + cnum.desc_.len_ * sizeof(cnum.digits_[0]));
    set_payload_shallow(&cnum, len);
  }
  inline void set_string(const ObString &v)
  {
    set_payload_shallow(v.ptr(), v.length());
  }
  inline void set_string(const char *ptr, const uint32_t len)
  {
    set_payload_shallow(ptr, len);
  }
  inline void set_enumset_inner(const ObString &v)
  {
    set_string(idx_, v);
  }
  inline void set_enumset_inner(const char *ptr, const uint32_t len)
  {
    set_string(ptr, len);
  }
  inline void set_urowid(const ObURowIDData &urowid_data)
  {
    const char *ptr = reinterpret_cast<const char *>(urowid_data.rowid_content_);
    ObLength len = static_cast<uint32_t>(urowid_data.rowid_len_);
    set_payload(ptr, len);
  }
  inline void set_urowid(const char *ptr, const int64_t size)
  {
    set_payload(ptr, static_cast<uint32_t>(size));
  }
  inline void set_lob_locator(const ObLobLocator &value)
  {
    set_payload(&value, static_cast<uint32_t>(value.get_total_size()));
  }
  inline void set_lob_data(const ObLobCommon &value, int64_t length)
  {
    set_payload(&value, static_cast<uint32_t>(length));
  }
  inline void set_decimal_int(const ObDecimalInt *decint, int32_t len)
  {
    set_payload(decint, static_cast<uint32_t>(len));
  }
  TO_STRING_KV(K_(vector), K_(idx));

private:
  T &vector_;
  uint16_t idx_;
};
} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_SHARE_VECTOR_OB_VEC_VISITOR_H_
