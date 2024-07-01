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

#ifndef OCEANBASE_SHARE_VECTOR_VECTOR_BASIC_OP_H_
#define OCEANBASE_SHARE_VECTOR_VECTOR_BASIC_OP_H_

#include "share/rc/ob_tenant_base.h"
#include "share/ob_lob_access_utils.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_bin.h"
#include "common/object/ob_obj_compare.h"
#include "share/vector/type_traits.h"

namespace oceanbase
{
namespace common
{

#define HASH_ARG_LIST const ObObjMeta &meta,   \
                      const void *data,             \
                      ObLength len,                 \
                      uint64_t seed,                \
                      uint64_t &res

template<VecValueTypeClass value_tc, typename HashMethod, bool hash_v2>
struct VecTCHashCalc {
  inline static int hash(HASH_ARG_LIST) {
    UNUSED(meta);
    res = HashMethod::hash(data, len, seed);
    return OB_SUCCESS;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_FIXED_DOUBLE, HashMethod, hash_v2> {
  inline static int hash(HASH_ARG_LIST) {
    const double *d_val = reinterpret_cast<const double *>(data);
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    int64_t length = ob_fcvt(*d_val, static_cast<int>(meta.get_scale()),
                             sizeof(buf) - 1, buf, NULL);
    res = HashMethod::hash(buf, static_cast<int32_t>(length), seed);
    return OB_SUCCESS;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_TIMESTAMP_TZ, HashMethod, hash_v2> {
  inline static int hash(HASH_ARG_LIST) {
    if (hash_v2) {
      res = HashMethod::hash(data, len, seed);
    } else {
      UNUSED(meta);
      const ObOTimestampData *tmp_data = reinterpret_cast<const ObOTimestampData *>(data);
      uint64_t v = HashMethod::hash(&tmp_data->time_us_, static_cast<int32_t>(sizeof(int64_t)), seed);
      res = HashMethod::hash(&tmp_data->time_ctx_.desc_, sizeof(uint32_t), v);
    }
    return OB_SUCCESS;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_TIMESTAMP_TINY, HashMethod, hash_v2> {
  inline static int hash(HASH_ARG_LIST) {
    if (hash_v2) {
      res = HashMethod::hash(data, len, seed);
    } else {
      UNUSED(meta);
      const ObOTimestampTinyData *tiny_data = reinterpret_cast<const ObOTimestampTinyData *>(data);
      uint64_t v = HashMethod::hash(&tiny_data->time_us_, sizeof(int64_t), seed);
      res = HashMethod::hash(&tiny_data->desc_, sizeof(uint16_t), v);
    }
    return OB_SUCCESS;
  }
};

template <VecValueTypeClass vec_tc, typename HashMethod, bool hash_v2>
struct VecRealTCHashCalc
{
  inline static int hash(HASH_ARG_LIST)
  {
    UNUSED(meta);
    using ValueType = RTCType<vec_tc>;
    const ValueType *v = reinterpret_cast<const ValueType *>(data);
    double v2 = *v;
    if (0.0 == v2) {
      v2 = 0.0;
    } else if (isnan(v2)) {
      v2 = NAN;
    }
    res = HashMethod::hash(&v2, sizeof(v2), seed);
    return OB_SUCCESS;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_FLOAT, HashMethod, hash_v2>
  : VecRealTCHashCalc<VEC_TC_FLOAT, HashMethod, hash_v2>
{};
template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_DOUBLE, HashMethod, hash_v2>
  : VecRealTCHashCalc<VEC_TC_DOUBLE, HashMethod, hash_v2> {};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_INTERVAL_DS, HashMethod, hash_v2> {
  inline static int hash(HASH_ARG_LIST) {
    if (hash_v2) {
      res = HashMethod::hash(data, len, seed);
    } else {
      const ObIntervalDSValue *value = reinterpret_cast<const ObIntervalDSValue *>(data);
      res = seed;
      res = HashMethod::hash(&value->nsecond_, sizeof(value->nsecond_), res);
      res = HashMethod::hash(&value->fractional_second_, sizeof(value->fractional_second_), res);
    }
    return OB_SUCCESS;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_ENUM_SET_INNER, HashMethod, hash_v2>
{
  inline static int hash(HASH_ARG_LIST)
  {
    int ret = OB_SUCCESS;
    if (hash_v2) {
      res = HashMethod::hash(data, len, seed);
    } else {
      res = ObCharset::hash(meta.get_collation_type(), reinterpret_cast<const char *>(data), len,
                            seed, meta.is_calc_end_space(),
                            HashMethod::is_varchar_hash ? HashMethod::hash : NULL);
    }
    return ret;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_INTERVAL_YM, HashMethod, hash_v2> {
  inline static int hash(HASH_ARG_LIST) {
    if (hash_v2) {
      res = HashMethod::hash(data, len, seed);
    } else {
      const ObIntervalYMValue *value = reinterpret_cast<const ObIntervalYMValue *>(data);
      res = HashMethod::hash(&value->nmonth_, sizeof(value->nmonth_), seed);
    }
    return OB_SUCCESS;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_NUMBER, HashMethod, hash_v2> {
  inline static int hash(HASH_ARG_LIST) {
    if (hash_v2) {
      res = HashMethod::hash(data, len, seed);
    } else {
      const number::ObCompactNumber *cnum = reinterpret_cast<const number::ObCompactNumber *>(data);
      res = HashMethod::hash(&cnum->desc_.se_, 1, seed);
      res = HashMethod::hash(cnum->digits_,
                             static_cast<uint64_t>(sizeof(uint32_t) * cnum->desc_.len_), res);
    }

    return OB_SUCCESS;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_LOB, HashMethod, hash_v2>
{
  inline static int lob_locator_get_string(const char *data, int32_t data_len,
                                           ObIAllocator &allocator, ObString &in_data)
  {
    int ret = OB_SUCCESS;
    ObString raw_data(data_len, data);
    ObLobLocatorV2 loc(raw_data, true);
    if (loc.is_lob_locator_v1()) {
      const ObLobLocator *lob_locator_v1 = reinterpret_cast<const ObLobLocator *>(data);
      in_data.assign_ptr(lob_locator_v1->get_payload_ptr(), lob_locator_v1->payload_size_);
    } else if (loc.is_valid()) {
      const ObLobCommon* lob = reinterpret_cast<const ObLobCommon*>(data);
      // fast path for disk inrow lob
      if (data_len != 0 && !lob->is_mem_loc_ && lob->in_row_) {
        in_data.assign_ptr(lob->get_inrow_data_ptr(), static_cast<int32_t>(lob->get_byte_size(data_len)));
      } else {
        ObTextStringIter text_iter(ObLongTextType, CS_TYPE_BINARY, raw_data, true);
        if (OB_FAIL(text_iter.init(0, NULL, &allocator))) {
          COMMON_LOG(WARN, "Lob: str iter init failed ", K(ret), K(text_iter));
        } else if (OB_FAIL(text_iter.get_full_data(in_data))) {
          COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(text_iter));
        }
      }
    } else { // not v1 or v2 lob
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(raw_data));
    }
    return ret;
  }
  inline static int hash(HASH_ARG_LIST)
  {
    int ret = OB_SUCCESS;
    if (hash_v2 && CS_TYPE_UTF8MB4_BIN == meta.get_collation_type()) {
      const ObLobCommon *lob = reinterpret_cast<const ObLobCommon*>(data);
      if (len != 0 && !lob->is_mem_loc_ && lob->in_row_) {
        bool calc_end_space = meta.is_calc_end_space();
        int64_t char_len = lob->get_byte_size(len);
        const char *char_data = lob->get_inrow_data_ptr();
        if (calc_end_space) {
          res = HashMethod::hash(char_data, char_len, seed);
        } else {
          const uchar *key = reinterpret_cast<const uchar *>(char_data);
          const uchar *pos = key;
          int length = char_len;
          key = skip_trailing_space(key, char_len, 0);
          length = (int)(key - pos);
          res = HashMethod::hash((void*)pos, length, seed);
        }
      } else {
        ObString in_data = ObString(len, reinterpret_cast<const char *>(data));
        common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                           MTL_ID());
        // all lob tc can use longtext type for lob iter
        if (OB_FAIL(
              lob_locator_get_string(reinterpret_cast<const char *>(data), len, allocator, in_data))) {
          COMMON_LOG(WARN, "Lob: get string failed", K(ret));
        } else {
          res = ObCharset::hash(meta.get_collation_type(), in_data.ptr(), in_data.length(), seed,
                                meta.is_calc_end_space(),
                                HashMethod::is_varchar_hash ? HashMethod::hash : NULL);
        }
      }
    } else {
      ObString in_data = ObString(len, reinterpret_cast<const char *>(data));
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                         MTL_ID());
      // all lob tc can use longtext type for lob iter
      if (OB_FAIL(
            lob_locator_get_string(reinterpret_cast<const char *>(data), len, allocator, in_data))) {
        COMMON_LOG(WARN, "Lob: get string failed", K(ret));
      } else {
        res = ObCharset::hash(meta.get_collation_type(), in_data.ptr(), in_data.length(), seed,
                              meta.is_calc_end_space(),
                              HashMethod::is_varchar_hash ? HashMethod::hash : NULL);
      }
    }
    return OB_SUCCESS;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_GEO, HashMethod, hash_v2>
{
  inline static int hash(HASH_ARG_LIST)
  {
    int ret = OB_SUCCESS;
    common::ObString wkb;
    res = 0;
    const char *in_str = reinterpret_cast<const char *>(data);
    ObLobLocatorV2 loc(in_str, false);
    if (!loc.is_valid()) {
      COMMON_LOG(WARN, "invalid lob", K(ret));
    } else if (!loc.has_inrow_data()) {
      COMMON_LOG(WARN, "meet outrow lob do calc hash value", K(loc));
    } else if (OB_FAIL(loc.get_inrow_data(wkb))) {
      COMMON_LOG(WARN, "fail to get inrow data", K(ret), K(loc));
    } else {
      res = seed;
      if (wkb.length() > 0) {
        res = ObCharset::hash(CS_TYPE_BINARY, wkb.ptr(), wkb.length(), seed, false,
                              HashMethod::is_varchar_hash ? HashMethod::hash : NULL);
      }
    }
    return ret;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_JSON, HashMethod, hash_v2>
{
  inline static int hash(HASH_ARG_LIST)
  {
    int ret = OB_SUCCESS;
    ObString j_bin_str;
    res = 0;
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                       MTL_ID());
    ObTextStringIter str_iter(ObJsonType, CS_TYPE_BINARY,
                              ObString(len, reinterpret_cast<const char *>(data)),
                              meta.has_lob_header());
    if (OB_FAIL(str_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: str iter init failed", K(ret));
    } else if (OB_FAIL(str_iter.get_full_data(j_bin_str))) {
      COMMON_LOG(WARN, "Lob: str iter get full data failed", K(ret));
    } else {
      ObJsonBinCtx ctx;
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &ctx);
      ObIJsonBase *j_base = &j_bin;
      if (j_bin_str.length() == 0) {
        res = seed;
      } else if (OB_FAIL(j_bin.reset_iter())) {
        COMMON_LOG(WARN, "Lob: fail to reset json bin iter", K(ret), K(j_bin_str));
      } else if (OB_FAIL(j_base->calc_json_hash_value(seed, HashMethod::hash, res))) {
        COMMON_LOG(WARN, "Lob: fail to calc hash", K(ret), K(*j_base));
      }
    }
    return ret;
  }
};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_ROWID, HashMethod, hash_v2>
{
  inline static int hash(HASH_ARG_LIST)
  {
    int ret = OB_SUCCESS;
    if (hash_v2) {
      res = HashMethod::hash(data, len, seed);
    } else {
      res = HashMethod::hash(reinterpret_cast<const char *>(data), len, seed);
    }
    return ret;
  }
};


template<VecValueTypeClass vec_type, typename HashMethod, bool hash_v2,  typename hash_type>
struct VecTCHashCalcWithHashType
{
  inline static int hash(HASH_ARG_LIST)
  {
    if (hash_v2) {
      res = HashMethod::hash(data, len, seed);
    } else {
      using ValueType = RTCType<vec_type>;
      const ValueType *v = reinterpret_cast<const ValueType *>(data);
      hash_type tmp_v = *v;
      res = HashMethod::hash(&tmp_v, sizeof(tmp_v), seed);
    }
    return OB_SUCCESS;
  }
};

template <typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_INTEGER, HashMethod, hash_v2>
  : public VecTCHashCalcWithHashType<VEC_TC_INTEGER, HashMethod, hash_v2, int64_t> {};

template <typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_UINTEGER, HashMethod, hash_v2>
  : public VecTCHashCalcWithHashType<VEC_TC_UINTEGER, HashMethod, hash_v2, uint64_t> {};

template <typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_YEAR, HashMethod, hash_v2>
  : public VecTCHashCalcWithHashType<VEC_TC_UINTEGER, HashMethod, hash_v2, int8_t> {};

template <typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_DATE, HashMethod, hash_v2>
  : public VecTCHashCalcWithHashType<VEC_TC_UINTEGER, HashMethod, hash_v2, int32_t> {};

template <typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_BIT, HashMethod, hash_v2>
  : public VecTCHashCalcWithHashType<VEC_TC_BIT, HashMethod, hash_v2, uint64_t> {};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_TIME, HashMethod, hash_v2>
  : public VecTCHashCalcWithHashType<VEC_TC_TIME, HashMethod, hash_v2, int64_t> {};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_ENUM_SET, HashMethod, hash_v2>
  : public VecTCHashCalcWithHashType<VEC_TC_ENUM_SET, HashMethod, hash_v2, uint64_t> {};


// string data hash

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_STRING, HashMethod, hash_v2> {
  inline static int hash(HASH_ARG_LIST) {
    bool calc_end_space = meta.is_calc_end_space();
    if (hash_v2 && CS_TYPE_UTF8MB4_BIN == meta.get_collation_type()) {
      if (calc_end_space) {
        res = HashMethod::hash(data, len, seed);
      } else {
        const uchar *key = reinterpret_cast<const uchar *>(data);
        const uchar *pos = key;
        int length = len;
        key = skip_trailing_space(key, len, 0);
        length = (int)(key - pos);
        res = HashMethod::hash((void*)pos, length, seed);
      }
    } else {
      res = ObCharset::hash(meta.get_collation_type(), static_cast<const char *>(data), len,
                            seed, calc_end_space,
                            HashMethod::is_varchar_hash ? HashMethod::hash : NULL);
    }
    return OB_SUCCESS;
  }
};

template <typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_RAW, HashMethod, hash_v2>
  : public VecTCHashCalc<VEC_TC_STRING, HashMethod, hash_v2>
{};

template<typename HashMethod, bool hash_v2>
struct VecTCHashCalc<VEC_TC_UDT, HashMethod, hash_v2> {
  inline static int hash(HASH_ARG_LIST) {
    int ret = OB_SUCCESS;
    if (hash_v2 || meta.get_collation_type() != CS_TYPE_BINARY || meta.is_calc_end_space()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      res = ObCharset::hash(meta.get_collation_type(), static_cast<const char *>(data), len, seed,
                            meta.is_calc_end_space(),
                            HashMethod::is_varchar_hash ? HashMethod::hash : NULL);
    }
    return OB_SUCCESS;
  }
};

//***************** VecTCHashCalc def end *******************


//***************** VecTCCmpCalc begin *******************
#define CMP_ARG_LIST const ObObjMeta &l_meta,               \
                     const ObObjMeta &r_meta,               \
                     const void *l_v, const ObLength l_len, \
                     const void *r_v, const ObLength r_len, \
                     int &cmp_ret                           \

template<VecValueTypeClass l_tc, VecValueTypeClass r_tc>
struct VecTCCmpCalc {
  static const constexpr bool defined_ = false;
  inline static int cmp(CMP_ARG_LIST)
  {
    int ret =  OB_NOT_IMPLEMENT;
    OB_ASSERT_MSG(false, "not implemented cmp func");
    return ret;
  }
};

template<VecValueTypeClass l_tc, VecValueTypeClass r_tc>
struct BasicCmpCalc {
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST) {
    UNUSEDx(l_meta, r_meta);
    using LType = RTCType<l_tc>;
    using RType = RTCType<r_tc>;
    cmp_ret = *(reinterpret_cast<const LType*>(l_v)) == *(reinterpret_cast<const RType*>(r_v))
        ? 0
        : (*(reinterpret_cast<const LType*>(l_v)) < *(reinterpret_cast<const RType*>(r_v)) ? -1 : 1);
    return OB_SUCCESS;
  }
};

template <>
struct VecTCCmpCalc<VEC_TC_INTEGER, VEC_TC_INTEGER>
  : public BasicCmpCalc<VEC_TC_INTEGER, VEC_TC_INTEGER> {};

template<>
struct VecTCCmpCalc<VEC_TC_UINTEGER, VEC_TC_UINTEGER>
  : public BasicCmpCalc<VEC_TC_UINTEGER, VEC_TC_UINTEGER> {};

template<>
struct VecTCCmpCalc<VEC_TC_DATE, VEC_TC_DATE>
  : public BasicCmpCalc<VEC_TC_DATE, VEC_TC_DATE> {};

template<>
struct VecTCCmpCalc<VEC_TC_TIME, VEC_TC_TIME>
  : public BasicCmpCalc<VEC_TC_TIME, VEC_TC_TIME> {};

template<>
struct VecTCCmpCalc<VEC_TC_DATETIME, VEC_TC_DATETIME>
  : public BasicCmpCalc<VEC_TC_DATETIME, VEC_TC_DATETIME> {};

template<>
struct VecTCCmpCalc<VEC_TC_YEAR, VEC_TC_YEAR>
  : public BasicCmpCalc<VEC_TC_YEAR, VEC_TC_YEAR> {};

template<>
struct VecTCCmpCalc<VEC_TC_BIT, VEC_TC_BIT>
  : public BasicCmpCalc<VEC_TC_BIT, VEC_TC_BIT> {};


template<>
struct VecTCCmpCalc<VEC_TC_ENUM_SET, VEC_TC_ENUM_SET>
  : public BasicCmpCalc<VEC_TC_ENUM_SET, VEC_TC_ENUM_SET> {};

template<>
struct VecTCCmpCalc<VEC_TC_INTERVAL_YM, VEC_TC_INTERVAL_YM>
  : public BasicCmpCalc<VEC_TC_INTERVAL_YM, VEC_TC_INTERVAL_YM> {};

template<>
struct VecTCCmpCalc<VEC_TC_DEC_INT32, VEC_TC_DEC_INT32>
  : public BasicCmpCalc<VEC_TC_DEC_INT32, VEC_TC_DEC_INT32> {};

template<>
struct VecTCCmpCalc<VEC_TC_DEC_INT64, VEC_TC_DEC_INT64>
  : public BasicCmpCalc<VEC_TC_DEC_INT64, VEC_TC_DEC_INT64> {};

template<>
struct VecTCCmpCalc<VEC_TC_DEC_INT128, VEC_TC_DEC_INT128>
  : public BasicCmpCalc<VEC_TC_DEC_INT128, VEC_TC_DEC_INT128> {};

template<>
struct VecTCCmpCalc<VEC_TC_DEC_INT256, VEC_TC_DEC_INT256>
  : public BasicCmpCalc<VEC_TC_DEC_INT256, VEC_TC_DEC_INT256> {};

template<>
struct VecTCCmpCalc<VEC_TC_DEC_INT512, VEC_TC_DEC_INT512>
  : public BasicCmpCalc<VEC_TC_DEC_INT512, VEC_TC_DEC_INT512> {};
template<>
struct VecTCCmpCalc<VEC_TC_TIMESTAMP_TINY, VEC_TC_TIMESTAMP_TINY>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST) {
    const ObOTimestampTinyData *l_tiny_data = reinterpret_cast<const ObOTimestampTinyData *>(l_v);
    const ObOTimestampTinyData *r_tiny_data = reinterpret_cast<const ObOTimestampTinyData *>(r_v);

    ObOTimestampData l_data, r_data;
    l_data.time_ctx_.time_desc_ = l_tiny_data->desc_;
    l_data.time_ctx_.tz_desc_ = 0;
    l_data.time_us_ = l_tiny_data->time_us_;

    r_data.time_ctx_.time_desc_ = r_tiny_data->desc_;
    r_data.time_ctx_.tz_desc_ = 0;
    r_data.time_us_ = r_tiny_data->time_us_;
    cmp_ret = l_data.compare(r_data);
    return OB_SUCCESS;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_TIMESTAMP_TZ, VEC_TC_TIMESTAMP_TZ>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    const ObOTimestampData *l_data = reinterpret_cast<const ObOTimestampData *>(l_v);
    const ObOTimestampData *r_data = reinterpret_cast<const ObOTimestampData *>(r_v);
    cmp_ret = l_data->compare(*r_data);
    return OB_SUCCESS;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_INTERVAL_DS, VEC_TC_INTERVAL_DS>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    const ObIntervalDSValue *l_data = reinterpret_cast<const ObIntervalDSValue *>(l_v);
    const ObIntervalDSValue *r_data = reinterpret_cast<const ObIntervalDSValue *>(r_v);
    cmp_ret = l_data->compare(*r_data);
    return OB_SUCCESS;
  }
};


template<>
struct VecTCCmpCalc<VEC_TC_NUMBER, VEC_TC_NUMBER>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    const number::ObCompactNumber *l_data = reinterpret_cast<const number::ObCompactNumber *>(l_v);
    const number::ObCompactNumber *r_data = reinterpret_cast<const number::ObCompactNumber *>(r_v);
    cmp_ret = number::ObNumber::compare(l_data->desc_, l_data->digits_,
                                        r_data->desc_, r_data->digits_);
    return OB_SUCCESS;
  }
};

template <>
struct VecTCCmpCalc<VEC_TC_FLOAT, VEC_TC_FLOAT>
{
  static const constexpr bool defined_ = true;
  template<typename T>
  // copy from ObDatumTCCmp<ObFloatTC, ObFloatTC>
  inline static int real_value_cmp(T l, T r, int &cmp_ret)
  {
    cmp_ret = 0;
    // Note: For NaN, we can't use C language compare logic, which is not compatible
    // with oracle rule.
    // Oracle NaN compare rule: NaN is the king (bigger than any number)
    if (isnan(l) || isnan(r)) {
      if (isnan(l) && isnan(r)) {
        cmp_ret = 0;
      } else if (isnan(l)) {
        // l is nan, r is not nan:left always bigger than right
        cmp_ret = 1;
      } else {
        // l is not nan, r is nan, left always less than right
        cmp_ret = -1;
      }
    } else {
      cmp_ret = l == r ? 0 : (l < r ? -1 : 1);
    }
    return OB_SUCCESS;
  }
  inline static int cmp(CMP_ARG_LIST)
  {
    return real_value_cmp<float>(*reinterpret_cast<const float *>(l_v),
                                 *reinterpret_cast<const float *>(r_v), cmp_ret);
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_DOUBLE, VEC_TC_DOUBLE>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    return VecTCCmpCalc<VEC_TC_FLOAT, VEC_TC_FLOAT>::real_value_cmp(
      *reinterpret_cast<const double *>(l_v), *reinterpret_cast<const double *>(r_v), cmp_ret);
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_FIXED_DOUBLE, VEC_TC_FIXED_DOUBLE>
{
  static const constexpr bool defined_ = true;
  // copy from ob_datum_func_func_def.h
  constexpr static double LOG_10[] =
  {
    1e000, 1e001, 1e002, 1e003, 1e004, 1e005, 1e006, 1e007,
    1e008, 1e009, 1e010, 1e011, 1e012, 1e013, 1e014, 1e015,
    1e016, 1e017, 1e018, 1e019, 1e020, 1e021, 1e022, 1e023,
    1e024, 1e025, 1e026, 1e027, 1e028, 1e029, 1e030, 1e031
  };
  inline static int cmp(CMP_ARG_LIST)
  {
    OB_ASSERT(l_meta.get_scale() == r_meta.get_scale());
    OB_ASSERT(l_meta.get_scale() <= OB_MAX_DOUBLE_FLOAT_SCALE);
    cmp_ret = 0;
    const double l = *reinterpret_cast<const double *>(l_v);
    const double r = *reinterpret_cast<const double *>(r_v);
    const double P = 5 / LOG_10[l_meta.get_scale() + 1];
    if (isnan(l) || isnan(r)) {
      if (isnan(l) && isnan(r)) {
        cmp_ret = 0;
      } else if (isnan(l)) {
        // l is nan, r is not nan:left always bigger than right
        cmp_ret = 1;
      } else {
        // l is not nan, r is nan, left always less than right
        cmp_ret = -1;
      }
    } else if (l == r || fabs(l - r) < P) {
      cmp_ret = 0;
    } else {
      cmp_ret = (l < r ? -1: 1);
    }
    return OB_SUCCESS;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_ROWID, VEC_TC_ROWID>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    const ObURowIDData l_data(l_len, reinterpret_cast<const uint8_t *>(l_v));
    const ObURowIDData r_data(r_len, reinterpret_cast<const uint8_t *>(r_v));
    cmp_ret = l_data.compare(r_data);
    return OB_SUCCESS;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_JSON, VEC_TC_JSON>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    OB_ASSERT(l_meta.get_collation_type() == r_meta.get_collation_type());
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    ObString l_data;
    ObString r_data;
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                       MTL_ID());
    common::ObTextStringIter l_instr_iter(ObJsonType, CS_TYPE_BINARY,
                                          ObString(l_len, reinterpret_cast<const char *>(l_v)),
                                          l_meta.has_lob_header());
    common::ObTextStringIter r_instr_iter(ObJsonType, CS_TYPE_BINARY,
                                          ObString(r_len, reinterpret_cast<const char *>(r_v)),
                                          r_meta.has_lob_header());
    if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
       COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret));
    } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
      COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
    } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret));
    } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
      COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
    } else {
      ObJsonBinCtx ctx_l;
      ObJsonBinCtx ctx_r;
      ObJsonBin j_bin_l(l_data.ptr(), l_data.length(), &ctx_l);
      ObJsonBin j_bin_r(r_data.ptr(), r_data.length(), &ctx_r);
      ObIJsonBase *j_base_l = &j_bin_l;
      ObIJsonBase *j_base_r = &j_bin_r;

      if (OB_FAIL(j_bin_l.reset_iter())) {
        COMMON_LOG(WARN, "fail to reset left json bin iter", K(ret), K(l_len));
      } else if (OB_FAIL(j_bin_r.reset_iter())) {
        COMMON_LOG(WARN, "fail to reset right json bin iter", K(ret), K(r_len));
      } else if (OB_FAIL(j_base_l->compare(*j_base_r, cmp_ret))) {
        COMMON_LOG(WARN, "fail to compare json", K(ret), K(*j_base_l), K(*j_base_r));
      }
    }
    return ret;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_GEO, VEC_TC_GEO>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    ObString l_data, r_data;
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                       MTL_ID());
    ObTextStringIter l_instr_iter(ObGeometryType, CS_TYPE_BINARY,
                                  ObString(l_len, reinterpret_cast<const char *>(l_v)),
                                  l_meta.has_lob_header());
    ObTextStringIter r_instr_iter(ObGeometryType, CS_TYPE_BINARY,
                                  ObString(r_len, reinterpret_cast<const char *>(r_v)),
                                  r_meta.has_lob_header());
    if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret));
    } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
      COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_instr_iter));
    } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret));
    } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
      COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(r_instr_iter));
    } else {
      cmp_ret = ObCharset::strcmpsp(CS_TYPE_BINARY, l_data.ptr(), l_data.length(), r_data.ptr(),
                                    r_data.length(), false);
      cmp_ret = (cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0));
    }
    return ret;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_EXTEND, VEC_TC_EXTEND>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    int64_t lv = *reinterpret_cast<const int64_t *>(l_v);
    int64_t rv = *reinterpret_cast<const int64_t *>(r_v);
    cmp_ret = ((ObObj::MIN_OBJECT_VALUE == lv && ObObj::MIN_OBJECT_VALUE == rv)
               || (ObObj::MAX_OBJECT_VALUE == lv && ObObj::MAX_OBJECT_VALUE == rv)) ?
                0 :
                (ObObj::MIN_OBJECT_VALUE == lv ? -1 : 1);
    return OB_SUCCESS;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_UDT, VEC_TC_UDT>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    cmp_ret = 0;
    int ret = OB_SUCCESS;
    bool calc_end_space =
      is_calc_with_end_space(l_meta.get_type(), r_meta.get_type(), lib::is_oracle_mode(),
                             l_meta.get_collation_type(), r_meta.get_collation_type());
    if (l_meta.get_collation_type() != CS_TYPE_BINARY
        || r_meta.get_collation_type() != CS_TYPE_BINARY
        || calc_end_space) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      cmp_ret = ObCharset::strcmpsp(l_meta.get_collation_type(), static_cast<const char *>(l_v),
                                    l_len, static_cast<const char *>(r_v), r_len, calc_end_space);
      cmp_ret = (cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0));
    }
    return ret;
  }
};


// null type comparison

struct VecDummyCmpCalc
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    UNUSEDx(l_meta, r_meta, l_v, r_v, l_len, r_len);
    cmp_ret = 0;
    return OB_SUCCESS;
  }
};

template<VecValueTypeClass r_tc>
struct VecTCCmpCalc<VEC_TC_NULL, r_tc>: public VecDummyCmpCalc {};

template<VecValueTypeClass l_tc>
struct VecTCCmpCalc<l_tc, VEC_TC_NULL>: public VecDummyCmpCalc {};

template<>
struct VecTCCmpCalc<VEC_TC_NULL, VEC_TC_EXTEND>: public VecDummyCmpCalc {};

template<>
struct VecTCCmpCalc<VEC_TC_EXTEND, VEC_TC_NULL>: public VecDummyCmpCalc {};

template<>
struct VecTCCmpCalc<VEC_TC_NULL, VEC_TC_NULL>: public VecDummyCmpCalc {};

// different tc compare
template<>
struct VecTCCmpCalc<VEC_TC_INTEGER, VEC_TC_UINTEGER>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    int64_t lv = *reinterpret_cast<const int64_t *>(l_v);
    uint64_t rv = *reinterpret_cast<const uint64_t *>(r_v);
    cmp_ret = (lv < 0 ? -1 : (lv < rv ? -1 : (lv > rv ? 1 : 0)));
    return OB_SUCCESS;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_UINTEGER, VEC_TC_INTEGER>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    int ret = VecTCCmpCalc<VEC_TC_INTEGER, VEC_TC_UINTEGER>::cmp(r_meta, l_meta, r_v, r_len, l_v,
                                                                 l_len, cmp_ret);
    cmp_ret = -cmp_ret;
    return ret;
  }
};

template <>
struct VecTCCmpCalc<VEC_TC_ENUM_SET, VEC_TC_INTEGER>
  : public VecTCCmpCalc<VEC_TC_UINTEGER, VEC_TC_INTEGER>
{
};

template <>
struct VecTCCmpCalc<VEC_TC_INTEGER, VEC_TC_ENUM_SET>
  : public VecTCCmpCalc<VEC_TC_INTEGER, VEC_TC_UINTEGER>
{
};

template <>
struct VecTCCmpCalc<VEC_TC_ENUM_SET, VEC_TC_UINTEGER>
  : public VecTCCmpCalc<VEC_TC_UINTEGER, VEC_TC_UINTEGER>
{};

template <>
struct VecTCCmpCalc<VEC_TC_UINTEGER, VEC_TC_ENUM_SET>
  : public VecTCCmpCalc<VEC_TC_UINTEGER, VEC_TC_UINTEGER>
{};

template<VecValueTypeClass r_tc>
struct VecTCCmpCalc<VEC_TC_EXTEND, r_tc>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    cmp_ret = (ObObj::MIN_OBJECT_VALUE == *reinterpret_cast<const int64_t *>(l_v)) ? -1 : 1;
    return OB_SUCCESS;
  }
};

template<VecValueTypeClass l_tc>
struct VecTCCmpCalc<l_tc, VEC_TC_EXTEND>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    cmp_ret = (ObObj::MIN_OBJECT_VALUE == *reinterpret_cast<const int64_t *>(r_v)) ? -1 : 1;
    return OB_SUCCESS;
  }
};

// string compare functions
template<>
struct VecTCCmpCalc<VEC_TC_STRING, VEC_TC_STRING>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    OB_ASSERT(l_meta.get_collation_type() == r_meta.get_collation_type());
    bool end_with_space =
      is_calc_with_end_space(l_meta.get_type(), r_meta.get_type(), lib::is_oracle_mode(),
                             l_meta.get_collation_type(), r_meta.get_collation_type());
    cmp_ret =
      ObCharset::strcmpsp(l_meta.get_collation_type(), reinterpret_cast<const char *>(l_v), l_len,
                          reinterpret_cast<const char *>(r_v), r_len, end_with_space);
    cmp_ret = (cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0));
    return OB_SUCCESS;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_RAW, VEC_TC_RAW>: public VecTCCmpCalc<VEC_TC_STRING, VEC_TC_STRING> {};

template<>
struct VecTCCmpCalc<VEC_TC_LOB, VEC_TC_LOB>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    OB_ASSERT(l_meta.get_collation_type() == r_meta.get_collation_type());
    int ret = OB_SUCCESS;
    bool end_with_space =
      is_calc_with_end_space(l_meta.get_type(), r_meta.get_type(), lib::is_oracle_mode(),
                             l_meta.get_collation_type(), r_meta.get_collation_type());
    bool has_lob_header = (l_meta.has_lob_header() || r_meta.has_lob_header());

    if (has_lob_header) {
      const ObLobCommon *rlob = reinterpret_cast<const ObLobCommon *>(r_v);
      const ObLobCommon *llob = reinterpret_cast<const ObLobCommon *>(l_v);
      if (r_len != 0 && !rlob->is_mem_loc_ && rlob->in_row_ &&
          l_len != 0 && !llob->is_mem_loc_ && llob->in_row_) {
        cmp_ret = ObCharset::strcmpsp(l_meta.get_collation_type(),
                  llob->get_inrow_data_ptr(), static_cast<int32_t>(llob->get_byte_size(l_len)),
                  rlob->get_inrow_data_ptr(), static_cast<int32_t>(rlob->get_byte_size(r_len)), end_with_space);
      } else {
        ObString l_data;
        ObString r_data;
        common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                           MTL_ID());
        ObTextStringIter l_instr_iter(ObLongTextType, l_meta.get_collation_type(),
                                      ObString(l_len, reinterpret_cast<const char *>(l_v)), true);
        ObTextStringIter r_instr_iter(ObLongTextType, l_meta.get_collation_type(),
                                      ObString(r_len, reinterpret_cast<const char *>(r_v)), true);
        if (OB_FAIL(l_instr_iter.init(0, NULL, &allocator))) {
          COMMON_LOG(WARN, "Lob: init left lob str iter failed", K(ret), K(l_meta), K(r_meta));
        } else if (OB_FAIL(l_instr_iter.get_full_data(l_data))) {
          COMMON_LOG(WARN, "Lob: get left lob str iter full data failed ", K(ret), K(l_meta),
                     K(r_meta), K(l_instr_iter));
        } else if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
          COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret));
        } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
          COMMON_LOG(WARN, "Lob: get right lob str iter full data failed ", K(ret), K(l_meta),
                     K(r_meta), K(r_instr_iter));
        } else {
          cmp_ret = ObCharset::strcmpsp(l_meta.get_collation_type(), l_data.ptr(), l_data.length(),
                                        r_data.ptr(), r_data.length(), end_with_space);
        }
      }
    } else {
      cmp_ret = ObCharset::strcmpsp(l_meta.get_collation_type(), (const char *)l_v, l_len,
                                    (const char *)r_v, r_len, end_with_space);
    }
    if (OB_SUCC(ret)) {
      // if error occur when reading outrow lobs, the compare result is wrong.
      cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0);
    }
    return ret;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_STRING, VEC_TC_LOB>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    OB_ASSERT(l_meta.get_collation_type() == r_meta.get_collation_type());
    int ret = OB_SUCCESS;
    ObString r_data;
    bool end_with_space =
      is_calc_with_end_space(l_meta.get_type(), r_meta.get_type(), lib::is_oracle_mode(),
                             l_meta.get_collation_type(), r_meta.get_collation_type());
    bool has_lob_header = (l_meta.has_lob_header() || r_meta.has_lob_header());
    if (has_lob_header) {
      common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE,
                                         MTL_ID());
      ObTextStringIter r_instr_iter(ObLongTextType, r_meta.get_collation_type(),
                                    ObString(r_len, reinterpret_cast<const char *>(r_v)), true);
      if (OB_FAIL(r_instr_iter.init(0, NULL, &allocator))) {
        COMMON_LOG(WARN, "Lob: init right lob str iter failed", K(ret), K(r_meta));
      } else if (OB_FAIL(r_instr_iter.get_full_data(r_data))) {
        COMMON_LOG(WARN, "Lob: get right lob str iter full data failed", K(ret), K(r_meta),
                   K(r_instr_iter));
      } else {
        cmp_ret =
          ObCharset::strcmpsp(l_meta.get_collation_type(), reinterpret_cast<const char *>(l_v),
                              l_len, r_data.ptr(), r_data.length(), false);
      }
    } else {
      cmp_ret = ObCharset::strcmpsp(l_meta.get_collation_type(), (const char *)l_v, l_len,
                                    (const char *)r_v, r_len, end_with_space);
    }
    if (OB_SUCC(ret)) { cmp_ret = cmp_ret > 0 ? 1 : (cmp_ret < 0 ? -1 : 0); }
    return ret;
  }
};

template<>
struct VecTCCmpCalc<VEC_TC_LOB, VEC_TC_STRING>
{
  static const constexpr bool defined_ = true;
  inline static int cmp(CMP_ARG_LIST)
  {
    OB_ASSERT(l_meta.get_collation_type() == r_meta.get_collation_type());
    int ret =
      VecTCCmpCalc<VEC_TC_STRING, VEC_TC_LOB>::cmp(r_meta, l_meta, r_v, r_len, l_v, l_len, cmp_ret);
    cmp_ret = -cmp_ret;
    return ret;
  }
};

#undef HASH_ARG_LIST
#undef CMP_ARG_LIST

  //***************** VecTCCmpCalc end *******************

  template <VecValueTypeClass vec_tc>
  struct VectorBasicOp {
  template<typename HashMethod, bool hash_v2>
  inline static int null_hash(uint64_t seed, uint64_t &res) {
    res = seed;
    if (!hash_v2) {
      const int null_type = ObNullType;
      res = HashMethod::hash(&null_type, sizeof(null_type), seed);
    }
    return OB_SUCCESS;
  }

  template <typename HashMethod, bool hash_v2>
  inline static int hash(const ObObjMeta &meta, const void *data, ObLength len, uint64_t seed,
                         uint64_t &res)
  {
    return VecTCHashCalc<vec_tc, HashMethod, hash_v2>::hash(meta, data, len, seed, res);
  }

  inline static int cmp(const ObObjMeta &meta, const void *l_v,
                        ObLength l_len, const void *r_v, ObLength r_len, int &cmp_ret)
  {
    return VecTCCmpCalc<vec_tc, vec_tc>::cmp(meta, meta, l_v, l_len, r_v, r_len, cmp_ret);
  }

  template <typename HashMethod>
  inline static int hash_v2(const ObObjMeta &meta, const void *data, ObLength len, uint64_t seed,
                            uint64_t &res)
  {
    res = HashMethod::hash(data, len, seed);
    return OB_SUCCESS;
  }
};

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_VECTOR_BASIC_OP_H_
