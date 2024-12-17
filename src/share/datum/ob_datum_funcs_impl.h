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

#ifndef OCEANBASE_SHARE_DATUM_FUNCS_UTIL_H_
#define OCEANBASE_SHARE_DATUM_FUNCS_UTIL_H_

#include "share/ob_lob_access_utils.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

template <ObObjType L_T, ObObjType R_T, bool NULL_FIRST>
struct ObNullSafeDatumTypeCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTypeCmp<L_T, R_T>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObObjTypeClass L_TC, ObObjTypeClass R_TC, bool NULL_FIRST>
struct ObNullSafeDatumTCCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTCCmp<L_TC, R_TC>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <bool NULL_FIRST, bool HAS_LOB_HEADER>
struct ObNullSafeDatumJsonCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumJsonCmp<HAS_LOB_HEADER>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <bool NULL_FIRST, bool HAS_LOB_HEADER>
struct ObNullSafeDatumGeoCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumGeoCmp<HAS_LOB_HEADER>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <bool NULL_FIRST, bool HAS_LOB_HEADER>
struct ObNullSafeDatumUDTCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    return datum_cmp::ObDatumUDTCmp<HAS_LOB_HEADER>::cmp(l, r, cmp_ret);
  }
};

template <ObScale SCALE, bool NULL_FIRST>
struct ObNullSafeFixedDoubleCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObFixedDoubleCmp<SCALE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumStrCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumStrCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumTextCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTextCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumTextStringCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumTextStringCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <ObCollationType CS_TYPE, bool WITH_END_SPACE, bool NULL_FIRST>
struct ObNullSafeDatumStringTextCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumStringTextCmp<CS_TYPE, WITH_END_SPACE>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <bool NULL_FIRST, bool HAS_LOB_HEADER>
struct ObNullSafeDatumCollectionCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumCollectionCmp<HAS_LOB_HEADER>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template <bool NULL_FIRST, bool HAS_LOB_HEADER>
struct ObNullSafeDatumRoaringbitmapCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret) {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDatumRoaringbitmapCmp<HAS_LOB_HEADER>::cmp(l, r, cmp_ret);
    }
    return ret;
  }
};

template<ObDecimalIntWideType width1, ObDecimalIntWideType width2, bool NULL_FIRST>
struct ObNullSafeDecintCmp
{
  inline static int cmp(const ObDatum &l, const ObDatum &r, int &cmp_ret)
  {
    int ret = OB_SUCCESS;
    cmp_ret = 0;
    if (OB_UNLIKELY(l.is_null()) && OB_UNLIKELY(r.is_null())) {
      cmp_ret = 0;
    } else if (OB_UNLIKELY(l.is_null())) {
      cmp_ret = NULL_FIRST ? -1 : 1;
    } else if (OB_UNLIKELY(r.is_null())) {
      cmp_ret = NULL_FIRST ? 1 : -1;
    } else {
      ret = datum_cmp::ObDecintCmp<width1, width2>::cmp(l, r, cmp_ret);
    }
    return OB_SUCCESS;
  }
};


OB_INLINE uint64_t datum_varchar_hash(const ObDatum &datum,
                                             const ObCollationType cs_type,
                                             const bool calc_end_space,
                                             const uint64_t seed,
                                             hash_algo algo)
{
  return ObCharset::hash(cs_type, datum.ptr_, datum.len_, seed, calc_end_space, algo);
}

OB_INLINE int datum_lob_locator_get_string(const ObDatum &datum,
                                                   ObIAllocator &allocator,
                                                   ObString& inrow_data)
{
  int ret = OB_SUCCESS;
  ObString raw_data = datum.get_string();
  ObLobLocatorV2 loc(raw_data, true);
  if (loc.is_lob_locator_v1()) {
    const ObLobLocator &lob_locator_v1 = datum.get_lob_locator();
    inrow_data.assign_ptr(lob_locator_v1.get_payload_ptr(), lob_locator_v1.payload_size_);
  } else if (loc.is_valid()) {
    ObTextStringIter text_iter(ObLongTextType, CS_TYPE_BINARY, datum.get_string(), true);
    if (OB_FAIL(text_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: str iter init failed ", K(ret), K(text_iter));
    } else if (OB_FAIL(text_iter.get_full_data(inrow_data))) {
      COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(text_iter));
    }
  } else { // not v1 or v2 lob
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(datum));
  }
  return ret;
}

OB_INLINE int datum_lob_locator_hash(const ObDatum &datum,
                                            const ObCollationType cs_type,
                                            const uint64_t seed,
                                            hash_algo algo,
                                            uint64_t &res)
{
  int ret = OB_SUCCESS;
  if (datum.is_null()) {
    res = seed;
  } else {
    ObString inrow_data = datum.get_string();
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    // all lob tc can use longtext type for lob iter
    if (OB_FAIL(datum_lob_locator_get_string(datum, allocator, inrow_data))) {
      COMMON_LOG(WARN, "Lob: get string failed ", K(ret), K(datum));
    } else {
      res = ObCharset::hash(cs_type, inrow_data.ptr(), inrow_data.length(), seed, algo);
    }
  }
  return ret;
}

template <typename T>
struct DefHashMethod
{
  typedef T HashMethod;
};

template <typename T, bool IS_VEC>
struct VectorIter
{
  explicit VectorIter(T *vec) : vec_(vec) {}
  T &operator[](const int64_t i) const { return IS_VEC ? vec_[i] : vec_[0]; }
private:
  T *vec_;
};

template <typename DatumHashFunc>
struct DefHashFunc
{
  static int null_hash(const uint64_t seed, uint64_t &res)
  {
    const int null_type = ObNullType;
    res = DatumHashFunc::HashMethod::hash(&null_type, sizeof(null_type), seed);
    return OB_SUCCESS;
  }

  static int hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
      ret = null_hash(seed, res);
    } else {
      ret = DatumHashFunc::calc_datum_hash(datum, seed, res);
    }
    return ret;
  }

  OB_INLINE static int hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return DatumHashFunc::calc_datum_hash_v2(datum, seed, res);
  }

  template <typename DATUM_VEC, typename SEED_VEC>
  static void do_hash_batch(uint64_t *hash_values,
                         const DATUM_VEC &datum_vec,
                         const ObBitVector &skip,
                         const int64_t size,
                         const SEED_VEC &seed_vec)
  {
    ObBitVector::flip_foreach(skip, size,
      [&](int64_t idx) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        ret = hash(datum_vec[idx], seed_vec[idx], hash_values[idx]);
        return ret;
      }
    );
  }

  static void hash_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed)
  {
    if (is_batch_datum && !is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    } else if (is_batch_datum && is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else if (!is_batch_datum && is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else {
      do_hash_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    }
  }

  template <typename DATUM_VEC, typename SEED_VEC>
  static void do_hash_v2_batch(uint64_t *hash_values,
                         const DATUM_VEC &datum_vec,
                         const ObBitVector &skip,
                         const int64_t size,
                         const SEED_VEC &seed_vec)
  {
    ObBitVector::flip_foreach(skip, size,
      [&](int64_t idx) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        ret = hash_v2(datum_vec[idx], seed_vec[idx], hash_values[idx]);
        return ret;
      }
    );
  }

  static void hash_v2_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed)
  {
    if (is_batch_datum && !is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    } else if (is_batch_datum && is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else if (!is_batch_datum && is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds));
    } else {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds));
    }
  }
};

template <bool calc_end_space, typename T>
static uint64_t datum_varchar_hash_utf8mb4_bin(
       const char* str_ptr, const int32_t str_len, uint64_t seed)
{
  uint64_t ret = 0;
  if (calc_end_space) {
    ret = T::hash((void*)str_ptr, str_len, seed);
  } else {
    const uchar *key = reinterpret_cast<const uchar *>(str_ptr);
    const uchar *pos = key;
    int length = str_len;
    key = skip_trailing_space(&ob_charset_utf8mb4_bin, key, str_len); // use in utf8mb4 use false false
    length = (int)(key - pos);
    ret = T::hash((void*)pos, length, seed);
  }
  return ret;
}

template <ObCollationType cs_type, bool calc_end_space, typename T, bool is_lob_locator>
struct DatumStrHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    res = datum_varchar_hash(
          datum, cs_type, calc_end_space, seed, T::is_varchar_hash ? T::hash : NULL);
    return OB_SUCCESS;
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    res = datum_varchar_hash(
          datum, cs_type, calc_end_space, seed, T::is_varchar_hash ? T::hash : NULL);
    return OB_SUCCESS;
  }
};

template <bool IS_LOB>
struct DefStrHashFunc
{
  static int null_hash(const uint64_t seed, uint64_t &res, hash_algo hash_al)
  {
    const int null_type = ObNullType;
    res = hash_al(&null_type, sizeof(null_type), seed);
    return OB_SUCCESS;
  }

  static int hash(const ObDatum &datum, const uint64_t seed, uint64_t &res,
                  const ObCollationType cs, const bool calc_end_space, const hash_algo hash_al)
  {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
      ret = null_hash(seed, res, hash_al);
    } else {
      if (IS_LOB) {
        ret = datum_lob_locator_hash(datum, cs, seed, hash_al, res);
      } else {
        res = datum_varchar_hash(datum, cs, calc_end_space, seed, hash_al);
      }
    }
    return ret;
  }

  static int hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res,
                     const ObCollationType cs, const bool calc_end_space, const hash_algo hash_al)
  {
    int ret = OB_SUCCESS;
    if (IS_LOB) {
      ret = datum_lob_locator_hash(datum, cs, seed, hash_al, res);
    } else {
      if (datum.is_null()) {
        res = seed;
      } else {
        res = datum_varchar_hash(datum, cs, calc_end_space, seed, hash_al);
      }
    }
    return ret;
  }

  template <typename DATUM_VEC, typename SEED_VEC>
      static void do_hash_batch(uint64_t *hash_values,
                                const DATUM_VEC &datum_vec,
                                const ObBitVector &skip,
                                const int64_t size,
                                const SEED_VEC &seed_vec,
                                const ObCollationType cs,
                                const bool calc_end_space,
                                const hash_algo hash_al)
  {
    ObBitVector::flip_foreach(skip, size,
      [&](int64_t idx) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        ret = hash(datum_vec[idx], seed_vec[idx], hash_values[idx], cs, calc_end_space, hash_al);
        return ret;
      }
    );
  }

  static void hash_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed,
                         const ObCollationType cs,
                         const bool calc_end_space,
                         const hash_algo hash_al)
  {
    if (is_batch_datum && !is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds), cs, calc_end_space, hash_al);
    } else if (is_batch_datum && is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds), cs, calc_end_space, hash_al);
    } else if (!is_batch_datum && is_batch_seed) {
      do_hash_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds), cs, calc_end_space, hash_al);
    } else {
      do_hash_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds), cs, calc_end_space, hash_al);
    }
  }

  template <typename DATUM_VEC, typename SEED_VEC>
      static void do_hash_v2_batch(uint64_t *hash_values,
                                   const DATUM_VEC &datum_vec,
                                   const ObBitVector &skip,
                                   const int64_t size,
                                   const SEED_VEC &seed_vec,
                                   const ObCollationType cs,
                                   const bool calc_end_space,
                                   const hash_algo hash_al)
  {
    ObBitVector::flip_foreach(skip, size,
      [&](int64_t idx) __attribute__((always_inline)) {
        int ret = OB_SUCCESS;
        ret = hash_v2(datum_vec[idx], seed_vec[idx], hash_values[idx], cs, calc_end_space, hash_al);
        return ret;
      }
    );
  }

  static void hash_v2_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed,
                         const ObCollationType cs,
                         const bool calc_end_space,
                         const hash_algo hash_al)
  {
    if (is_batch_datum && !is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds), cs, calc_end_space, hash_al);
    } else if (is_batch_datum && is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, true>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds), cs, calc_end_space, hash_al);
    } else if (!is_batch_datum && is_batch_seed) {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, true>(seeds), cs, calc_end_space, hash_al);
    } else {
      do_hash_v2_batch(hash_values, VectorIter<const ObDatum, false>(datums), skip, size,
                    VectorIter<const uint64_t, false>(seeds), cs, calc_end_space, hash_al);
    }
  }
};

struct ObStrHashBatchImpl {
  static void OB_NOINLINE hash_batch(uint64_t *hash_values,
                                     ObDatum *datums,
                                     const bool is_batch_datum,
                                     const ObBitVector &skip,
                                     const int64_t size,
                                     const uint64_t *seeds,
                                     const bool is_batch_seed,
                                     const ObCollationType cs,
                                     const bool calc_end_space,
                                     const hash_algo hash_al,
                                     const bool is_lob)
  {
    return is_lob
        ? DefStrHashFunc<true>::hash_batch(hash_values, datums, is_batch_datum, skip, size, seeds,
                                         is_batch_seed, cs, calc_end_space, hash_al)
        : DefStrHashFunc<false>::hash_batch(hash_values, datums, is_batch_datum, skip, size, seeds,
                                          is_batch_seed, cs, calc_end_space, hash_al);
  }

  static void OB_NOINLINE hash_v2_batch(uint64_t *hash_values,
                                        ObDatum *datums,
                                        const bool is_batch_datum,
                                        const ObBitVector &skip,
                                        const int64_t size,
                                        const uint64_t *seeds,
                                        const bool is_batch_seed,
                                        const ObCollationType cs,
                                        const bool calc_end_space,
                                        const hash_algo hash_al,
                                        const  bool is_lob)
  {
    return is_lob
        ? DefStrHashFunc<true>::hash_v2_batch(hash_values, datums, is_batch_datum, skip, size,
                                              seeds, is_batch_seed, cs, calc_end_space, hash_al)
        : DefStrHashFunc<false>::hash_v2_batch(hash_values, datums, is_batch_datum, skip, size,
                                               seeds, is_batch_seed, cs, calc_end_space, hash_al);
  }

};

template <ObCollationType cs, bool calc_end_space, typename T, bool is_lob>
struct StrDatumHashBatchHelper {
  static void hash_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed)
  {
    return ObStrHashBatchImpl::hash_batch(hash_values, datums, is_batch_datum, skip, size,
                                          seeds, is_batch_seed, cs, calc_end_space,
                                          T::is_varchar_hash ? T::hash : NULL, is_lob);
  }
  static void hash_v2_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed)
  {
    return ObStrHashBatchImpl::hash_v2_batch(hash_values, datums, is_batch_datum, skip, size,
                                             seeds, is_batch_seed, cs, calc_end_space,
                                             T::is_varchar_hash ? T::hash : NULL, is_lob);
  }
};

//use DatumStrHashCalculator for CS_TYPE_UTF8MB4_BIN, because CS_TYPE_UTF8MB4_BIN is specializated.
template <bool calc_end_space, typename T, bool is_lob>
struct StrDatumHashBatchHelper<CS_TYPE_UTF8MB4_BIN, calc_end_space, T, is_lob> {
  using HashBatch = DefHashFunc<
      DatumStrHashCalculator<CS_TYPE_UTF8MB4_BIN, calc_end_space, T, is_lob>>;
  static void hash_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed)
  {
    return HashBatch::hash_batch(
        hash_values, datums, is_batch_datum, skip, size, seeds, is_batch_seed);
  }

  static void hash_v2_batch(uint64_t *hash_values,
                         ObDatum *datums,
                         const bool is_batch_datum,
                         const ObBitVector &skip,
                         const int64_t size,
                         const uint64_t *seeds,
                         const bool is_batch_seed)
  {
    return HashBatch::hash_v2_batch(
        hash_values, datums, is_batch_datum, skip, size, seeds, is_batch_seed);
  }
};

template <ObCollationType cs_type, bool calc_end_space, typename T>
struct DatumStrHashCalculator<cs_type, calc_end_space, T, true /* is_lob_locator */>
       : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, cs_type, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, cs_type, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
};

template <bool calc_end_space, typename T>
struct DatumStrHashCalculator<CS_TYPE_UTF8MB4_BIN, calc_end_space, T, false /* is_lob_locator */>
       : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    res = datum_varchar_hash(
          datum, CS_TYPE_UTF8MB4_BIN, calc_end_space, seed, T::is_varchar_hash ? T::hash : NULL);
    return OB_SUCCESS;
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {

    res = datum_varchar_hash_utf8mb4_bin<calc_end_space, T>(datum.ptr_, datum.len_, seed);
    return OB_SUCCESS;
  }
};

template <bool calc_end_space, typename T>
struct DatumStrHashCalculator<CS_TYPE_UTF8MB4_BIN, calc_end_space, T, true /* is_lob_locator */>
       : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    ObString inrow_data = datum.get_string();
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    // all lob tc can use longtext type for lob iter
    if (OB_FAIL(datum_lob_locator_get_string(datum, allocator, inrow_data))) {
      COMMON_LOG(WARN, "Lob: get string failed ", K(ret), K(datum));
    } else {
      res = datum_varchar_hash_utf8mb4_bin<calc_end_space, T>(
            inrow_data.ptr(), inrow_data.length(), seed);
    }
    return ret;
  }
};

template <ObObjType type, typename T>
struct DatumHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return ObjHashCalculator<type, T, ObDatum>::calc_hash_value(datum, seed, res);
  }
  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    res = T::hash(datum.ptr_, datum.len_, seed);
    return OB_SUCCESS;
  }
};

#define DEF_DATUM_TIMESTAMP_HASH_FUNCS(OBJTYPE, TYPE, DESC, VTYPE)              \
  template <typename T>                                                         \
  struct DatumHashCalculator<OBJTYPE, T> : public DefHashMethod<T>              \
  {                                                                             \
    static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)  \
    {                                                                           \
      ObOTimestampData tmp_data = datum.get_##TYPE();                           \
      res = T::hash(&tmp_data.time_us_, static_cast<int32_t>(sizeof(int64_t)), seed);         \
      res = T::hash(&tmp_data.time_ctx_.DESC, static_cast<int32_t>(sizeof(VTYPE)), res);      \
      return OB_SUCCESS;                                                                      \
    }                                                                                         \
    static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)   \
    {                                                                                         \
      res = T::hash(datum.ptr_, datum.len_, seed);                                            \
      return OB_SUCCESS;                                                                      \
    }                                                                                         \
  };

DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampTZType, otimestamp_tz, desc_, uint32_t);
DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampLTZType, otimestamp_tiny, time_desc_, uint16_t);
DEF_DATUM_TIMESTAMP_HASH_FUNCS(ObTimestampNanoType, otimestamp_tiny, time_desc_, uint16_t);

// template specialization to deal with real type and json type
// calc_datum_hash_v2 will fallback to calc_datum_hash
#define DEF_DATUM_SPECIAL_HASH_FUNCS(OBJTYPE)                                               \
  template <typename T>                                                                     \
  struct DatumHashCalculator<OBJTYPE, T> : public DefHashMethod<T>                          \
  {                                                                                         \
    static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)    \
    {                                                                                       \
      return ObjHashCalculator<OBJTYPE, T, ObDatum>::calc_hash_value(datum, seed, res);     \
    }                                                                                       \
    static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res) {         \
      int ret = OB_SUCCESS;                                                                 \
      if (datum.is_null()) {                                                                \
        res = seed;                                                                         \
      } else {                                                                              \
        ret = calc_datum_hash(datum, seed, res);                                            \
      }                                                                                     \
      return ret;                                                                           \
    }                                                                                       \
  };

DEF_DATUM_SPECIAL_HASH_FUNCS(ObFloatType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObUFloatType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObDoubleType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObUDoubleType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObJsonType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObGeometryType);
DEF_DATUM_SPECIAL_HASH_FUNCS(ObRoaringBitmapType);

extern ObDatumCmpFuncType NULLSAFE_JSON_CMP_FUNCS[2][2];
extern ObDatumCmpFuncType NULLSAFE_STR_CMP_FUNCS[CS_TYPE_MAX][2][2];
extern ObDatumCmpFuncType NULLSAFE_TEXT_CMP_FUNCS[CS_TYPE_MAX][2][2];
extern ObDatumCmpFuncType NULLSAFE_TEXT_STR_CMP_FUNCS[CS_TYPE_MAX][2][2];
extern ObDatumCmpFuncType NULLSAFE_STR_TEXT_CMP_FUNCS[CS_TYPE_MAX][2][2];
extern ObDatumCmpFuncType NULLSAFE_TYPE_CMP_FUNCS[ObMaxType][ObMaxType][2];
extern ObDatumCmpFuncType NULLSAFE_TC_CMP_FUNCS[ObMaxTC][ObMaxTC][2];
extern ObDatumCmpFuncType NULLSAFE_GEO_CMP_FUNCS[2][2];
extern ObDatumCmpFuncType FIXED_DOUBLE_CMP_FUNCS[OB_NOT_FIXED_SCALE][2];
extern ObDatumCmpFuncType NULLSAFE_COLLECTION_CMP_FUNCS[2][2];
extern ObDatumCmpFuncType NULLSAFE_ROARINGBITMAP_CMP_FUNCS[2][2];
extern ObDatumCmpFuncType DECINT_CMP_FUNCS[DECIMAL_INT_MAX][DECIMAL_INT_MAX][2];

extern ObExprBasicFuncs EXPR_BASIC_FUNCS[ObMaxType];
extern ObExprBasicFuncs EXPR_BASIC_STR_FUNCS[CS_TYPE_MAX][2][2];
extern ObExprBasicFuncs EXPR_BASIC_JSON_FUNCS[2];
extern ObExprBasicFuncs EXPR_BASIC_GEO_FUNCS[2];
extern ObExprBasicFuncs FIXED_DOUBLE_BASIC_FUNCS[OB_NOT_FIXED_SCALE];
extern ObExprBasicFuncs EXPR_BASIC_UDT_FUNCS[1];
extern ObExprBasicFuncs DECINT_BASIC_FUNCS[DECIMAL_INT_MAX];
extern ObExprBasicFuncs EXPR_BASIC_COLLECTION_FUNCS[2];
extern ObExprBasicFuncs EXPR_BASIC_ROARINGBITMAP_FUNCS[2];

struct DummyIniter
{
  static void init_array() {}
};

template <int X, int Y, bool defined = true>
struct InitTypeCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumTypeCmp<
      static_cast<ObObjType>(X), static_cast<ObObjType>(Y), args...>;
  using Def = datum_cmp::ObDatumTypeCmp<static_cast<ObObjType>(X), static_cast<ObObjType>(Y)>;
  static void init_array()
  {
    NULLSAFE_TYPE_CMP_FUNCS[X][Y][0] = Def::defined_ ? &Cmp<0>::cmp : NULL;
    NULLSAFE_TYPE_CMP_FUNCS[X][Y][1] = Def::defined_ ? &Cmp<1>::cmp : NULL;
  }
};

template<int X, int Y>
struct InitTypeCmpArray<X, Y, false>: public DummyIniter {};

template<int X, int Y>
using TypeCmpIniter = InitTypeCmpArray<X, Y, datum_cmp::ObDatumTypeCmp<static_cast<ObObjType>(X), static_cast<ObObjType>(Y)>::defined_>;

// init type class compare function array
template <int X, int Y, bool defined = true>
struct InitTCCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumTCCmp<
      static_cast<ObObjTypeClass>(X), static_cast<ObObjTypeClass>(Y), args...>;
  using Def = datum_cmp::ObDatumTCCmp<
      static_cast<ObObjTypeClass>(X),
      static_cast<ObObjTypeClass>(Y)>;
  static void init_array()
  {
    NULLSAFE_TC_CMP_FUNCS[X][Y][0] = Def::defined_ ? &Cmp<0>::cmp : NULL;
    NULLSAFE_TC_CMP_FUNCS[X][Y][1] = Def::defined_ ? &Cmp<1>::cmp : NULL;
  }
};

template<int X, int Y>
struct InitTCCmpArray<X, Y, false>: public DummyIniter {};

template<int X, int Y>
using TCCmpIniter = InitTCCmpArray<X, Y, datum_cmp::ObDatumTCCmp<static_cast<ObObjTypeClass>(X),static_cast<ObObjTypeClass>(Y)>::defined_>;

// init basic function array
template <int X>
struct InitBasicFuncArray
{
  template <typename T>
  using Hash = DefHashFunc<DatumHashCalculator<static_cast<ObObjType>(X), T>>;
  template <bool NULL_FIRST>
  using TCCmp = ObNullSafeDatumTCCmp<
      ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_,
      ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_,
      NULL_FIRST>;
  using TCDef = datum_cmp::ObDatumTCCmp<
      ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_,
      ObObjTypeTraits<static_cast<ObObjType>(X)>::tc_>;
  template <bool NULL_FIRST>
  using TypeCmp = ObNullSafeDatumTypeCmp<
      static_cast<ObObjType>(X),
      static_cast<ObObjType>(X),
      NULL_FIRST>;
  using TypeDef = datum_cmp::ObDatumTypeCmp<
      static_cast<ObObjType>(X),
      static_cast<ObObjType>(X)>;
  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_FUNCS;
    basic_funcs[X].default_hash_ = Hash<ObDefaultHash>::hash;
    basic_funcs[X].default_hash_batch_= Hash<ObDefaultHash>::hash_batch;
    basic_funcs[X].murmur_hash_ = Hash<ObMurmurHash>::hash;
    basic_funcs[X].murmur_hash_batch_ = Hash<ObMurmurHash>::hash_batch;
    basic_funcs[X].xx_hash_ = Hash<ObXxHash>::hash;
    basic_funcs[X].xx_hash_batch_ = Hash<ObXxHash>::hash_batch;
    basic_funcs[X].wy_hash_ = Hash<ObWyHash>::hash;
    basic_funcs[X].wy_hash_batch_ = Hash<ObWyHash>::hash_batch;
    basic_funcs[X].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[X].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[X].murmur_hash_v2_ = Hash<ObMurmurHash>::hash_v2;
    basic_funcs[X].murmur_hash_v2_batch_ = Hash<ObMurmurHash>::hash_v2_batch;
  }
};


// init helpers
template<ObCollationType col>
struct CollationDefined
{
  static const bool value_ = datum_cmp::SupportedCollection<col>::defined_;
};

// init basic string function array
template <int X, int Y, bool defined>
struct InitBasicStrFuncArray
{
  static void init_array()
  {
    return;
  }
};

// init basic string function array
template <int X, int Y>
struct InitBasicStrFuncArray<X, Y, true>
{
  template <typename T, bool is_lob_locator>
  using Hash = DefHashFunc<DatumStrHashCalculator<static_cast<ObCollationType>(X),
        static_cast<bool>(Y), T, is_lob_locator>>;
  /*
  template <typename T, bool is_lob_locator>
  using HashBatch = DefHashFunc<DatumStrHashCalculator<static_cast<ObCollationType>(X),
        static_cast<bool>(Y), T, is_lob_locator>>;
        */
  template <typename T, bool is_lob>
  using HashBatch = StrDatumHashBatchHelper<static_cast<ObCollationType>(X), static_cast<bool>(Y), T, is_lob>;

  template <bool null_first>
  using StrCmp = ObNullSafeDatumStrCmp<static_cast<ObCollationType>(X),
                                       static_cast<bool>(Y), null_first>;
  template <bool null_first>
  using TextCmp = ObNullSafeDatumTextCmp<static_cast<ObCollationType>(X),
                                         static_cast<bool>(Y), null_first>;
  using Def = datum_cmp::ObDatumStrCmp<
      static_cast<ObCollationType>(X),
      static_cast<bool>(Y)>;
    using DefText = datum_cmp::ObDatumTextCmp<
      static_cast<ObCollationType>(X),
      static_cast<bool>(Y)>;
  static void init_array()
  {
    if (datum_cmp::SupportedCollection<static_cast<ObCollationType>(X)>::defined_) {
      auto &basic_funcs = EXPR_BASIC_STR_FUNCS;
      basic_funcs[X][Y][0].default_hash_ = Hash<ObDefaultHash, false>::hash;
      basic_funcs[X][Y][0].default_hash_batch_ = HashBatch<ObDefaultHash, false>::hash_batch;
      basic_funcs[X][Y][0].murmur_hash_ = Hash<ObMurmurHash, false>::hash;
      basic_funcs[X][Y][0].murmur_hash_batch_ = HashBatch<ObMurmurHash, false>::hash_batch;
      basic_funcs[X][Y][0].xx_hash_ = Hash<ObXxHash, false>::hash;
      basic_funcs[X][Y][0].xx_hash_batch_ = HashBatch<ObXxHash, false>::hash_batch;
      basic_funcs[X][Y][0].wy_hash_ = Hash<ObWyHash, false>::hash;
      basic_funcs[X][Y][0].wy_hash_batch_ = HashBatch<ObWyHash, false>::hash_batch;
      basic_funcs[X][Y][0].null_first_cmp_ = Def::defined_ ? &StrCmp<1>::cmp : NULL;
      basic_funcs[X][Y][0].null_last_cmp_ = Def::defined_ ? &StrCmp<0>::cmp : NULL;
      basic_funcs[X][Y][0].murmur_hash_v2_ = Hash<ObMurmurHash, false>::hash_v2;
      basic_funcs[X][Y][0].murmur_hash_v2_batch_ = HashBatch<ObMurmurHash, false>::hash_v2_batch;

      basic_funcs[X][Y][1].default_hash_ = Hash<ObDefaultHash, true>::hash;
      basic_funcs[X][Y][1].default_hash_batch_ = HashBatch<ObDefaultHash, true>::hash_batch;
      basic_funcs[X][Y][1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
      basic_funcs[X][Y][1].murmur_hash_batch_ = HashBatch<ObMurmurHash, true>::hash_batch;
      // basic_funcs[X][Y][1].xx_hash_ = Hash<ObXxHash, true>::hash;
      // basic_funcs[X][Y][1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
      // basic_funcs[X][Y][1].wy_hash_ = Hash<ObWyHash, true>::hash;
      // basic_funcs[X][Y][1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;

      // Notice: ObLobType cannot compare, but is_locator = 1 used for other lob types
      basic_funcs[X][Y][1].null_first_cmp_ = DefText::defined_ ? &TextCmp<1>::cmp : NULL;
      basic_funcs[X][Y][1].null_last_cmp_ = DefText::defined_ ? &TextCmp<0>::cmp : NULL;
      basic_funcs[X][Y][1].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
      basic_funcs[X][Y][1].murmur_hash_v2_batch_ = HashBatch<ObMurmurHash, true>::hash_v2_batch;
    }
  }
};

template <int IDX, bool defined>
struct InitStrCmpArray
{
  static void init_array() {}
};

// int string compare function array
template <int IDX>
struct InitStrCmpArray<IDX, true>
{
  template <bool... args>
  using StrCmp = ObNullSafeDatumStrCmp<static_cast<ObCollationType>(IDX), args...>;
  using Def = datum_cmp::ObDatumStrCmp<static_cast<ObCollationType>(IDX), false>;

  template <bool... args>
  using TextCmp = ObNullSafeDatumTextCmp<static_cast<ObCollationType>(IDX), args...>;
  using TextDef = datum_cmp::ObDatumTextCmp<static_cast<ObCollationType>(IDX), false>;

  template <bool... args>
  using TextStringCmp = ObNullSafeDatumTextStringCmp<static_cast<ObCollationType>(IDX), args...>;
  using TextStringDef = datum_cmp::ObDatumTextStringCmp<static_cast<ObCollationType>(IDX), false>;

  template <bool... args>
  using StringTextCmp = ObNullSafeDatumStringTextCmp<static_cast<ObCollationType>(IDX), args...>;
  using StringTextDef = datum_cmp::ObDatumStringTextCmp<static_cast<ObCollationType>(IDX), false>;

  static void init_array()
  {
    auto &funcs = NULLSAFE_STR_CMP_FUNCS;
    funcs[IDX][0][0] = Def::defined_ ? &StrCmp<0, 0>::cmp : NULL;
    funcs[IDX][0][1] = Def::defined_ ? &StrCmp<0, 1>::cmp : NULL;
    funcs[IDX][1][0] = Def::defined_ ? &StrCmp<1, 0>::cmp : NULL;
    funcs[IDX][1][1] = Def::defined_ ? &StrCmp<1, 1>::cmp : NULL;

    // int texts compare function array
    auto &text_funcs = NULLSAFE_TEXT_CMP_FUNCS;
    text_funcs[IDX][0][0] = TextDef::defined_ ? &TextCmp<0, 0>::cmp : NULL;
    text_funcs[IDX][0][1] = TextDef::defined_ ? &TextCmp<0, 1>::cmp : NULL;
    text_funcs[IDX][1][0] = TextDef::defined_ ? &TextCmp<1, 0>::cmp : NULL;
    text_funcs[IDX][1][1] = TextDef::defined_ ? &TextCmp<1, 1>::cmp : NULL;

    // int text compare string function array
    auto &text_string_funcs = NULLSAFE_TEXT_STR_CMP_FUNCS;
    text_string_funcs[IDX][0][0] = TextStringDef::defined_ ? &TextStringCmp<0, 0>::cmp : NULL;
    text_string_funcs[IDX][0][1] = TextStringDef::defined_ ? &TextStringCmp<0, 1>::cmp : NULL;
    text_string_funcs[IDX][1][0] = TextStringDef::defined_ ? &TextStringCmp<1, 0>::cmp : NULL;
    text_string_funcs[IDX][1][1] = TextStringDef::defined_ ? &TextStringCmp<1, 1>::cmp : NULL;

    // int text compare string function array
    auto &string_text_funcs = NULLSAFE_STR_TEXT_CMP_FUNCS;
    string_text_funcs[IDX][0][0] = StringTextDef::defined_ ? &StringTextCmp<0, 0>::cmp : NULL;
    string_text_funcs[IDX][0][1] = StringTextDef::defined_ ? &StringTextCmp<0, 1>::cmp : NULL;
    string_text_funcs[IDX][1][0] = StringTextDef::defined_ ? &StringTextCmp<1, 0>::cmp : NULL;
    string_text_funcs[IDX][1][1] = StringTextDef::defined_ ? &StringTextCmp<1, 1>::cmp : NULL;
  }
};


template<int IDX>
using str_cmp_initer = InitStrCmpArray<IDX, CollationDefined<static_cast<ObCollationType>(IDX)>::value_>;

template<int X, int Y>
using str_basic_initer = InitBasicStrFuncArray<X, Y, CollationDefined<static_cast<ObCollationType>(X)>::value_>;

template <typename T, bool HAS_LOB_HEADER>
struct DatumJsonHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    common::ObString j_bin_str;
    res = 0;
    common::ObArenaAllocator allocator(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTextStringIter str_iter(ObJsonType, CS_TYPE_BINARY, datum.get_string(), HAS_LOB_HEADER);
    if (datum.is_null()) {
      res = seed;
    } else if (OB_FAIL(str_iter.init(0, NULL, &allocator))) {
      COMMON_LOG(WARN, "Lob: str iter init failed ", K(ret), K(str_iter));
    } else if (OB_FAIL(str_iter.get_full_data(j_bin_str))) {
      COMMON_LOG(WARN, "Lob: str iter get full data failed ", K(ret), K(str_iter));
    } else {
      ObJsonBin j_bin(j_bin_str.ptr(), j_bin_str.length(), &allocator);
      ObIJsonBase *j_base = &j_bin;
      if (j_bin_str.length() == 0) {
        res = seed;
      } else if (OB_FAIL(j_bin.reset_iter())) {
        COMMON_LOG(WARN, "Lob: fail to reset json bin iter", K(ret), K(j_bin_str));
      } else if (OB_FAIL(j_base->calc_json_hash_value(seed, T::hash, res))) {
        COMMON_LOG(WARN, "Lob: fail to calc hash", K(ret), K(*j_base));
      }
    }
    return ret;
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res) {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
      res = seed;
    } else {
      ret = calc_datum_hash(datum, seed, res);
    }
    return ret;
  }
};

template<int IDX>
struct InitBasicJsonFuncArray
{
  template <typename T, bool HAS_LOB_HEADER>
  using Hash = DefHashFunc<DatumJsonHashCalculator<T, HAS_LOB_HEADER>>;
  template <bool NULL_FIRST>
  using TCCmp = ObNullSafeDatumTCCmp<ObJsonTC, ObJsonTC, NULL_FIRST>;
  using TCDef = datum_cmp::ObDatumTCCmp<ObJsonTC, ObJsonTC>;
  template <bool NULL_FIRST, bool HAS_LOB_HEADER>
  using TypeCmp = ObNullSafeDatumJsonCmp<NULL_FIRST, HAS_LOB_HEADER>;
  using TypeDef = datum_cmp::ObDatumJsonCmp<false>;

  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_JSON_FUNCS;
    basic_funcs[0].default_hash_ = Hash<ObDefaultHash, false>::hash;
    basic_funcs[0].default_hash_batch_= Hash<ObDefaultHash, false>::hash_batch;
    basic_funcs[0].murmur_hash_ = Hash<ObMurmurHash, false>::hash;
    basic_funcs[0].murmur_hash_batch_ = Hash<ObMurmurHash, false>::hash_batch;
    basic_funcs[0].xx_hash_ = Hash<ObXxHash, false>::hash;
    basic_funcs[0].xx_hash_batch_ = Hash<ObXxHash, false>::hash_batch;
    basic_funcs[0].wy_hash_ = Hash<ObWyHash, false>::hash;
    basic_funcs[0].wy_hash_batch_ = Hash<ObWyHash, false>::hash_batch;
    basic_funcs[0].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 0>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[0].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 0>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[0].murmur_hash_v2_ = Hash<ObMurmurHash, false>::hash_v2;
    basic_funcs[0].murmur_hash_v2_batch_ = Hash<ObMurmurHash, false>::hash_v2_batch;

    basic_funcs[1].default_hash_ = Hash<ObDefaultHash, true>::hash;
    basic_funcs[1].default_hash_batch_= Hash<ObDefaultHash, true>::hash_batch;
    basic_funcs[1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
    basic_funcs[1].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
    // basic_funcs[1].xx_hash_ = Hash<ObXxHash, true>::hash;
    // basic_funcs[1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
    // basic_funcs[1].wy_hash_ = Hash<ObWyHash, true>::hash;
    // basic_funcs[1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
    basic_funcs[1].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 1>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[1].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 1>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[1].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
    basic_funcs[1].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
  }
};

template<int IDX>
struct InitJsonCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumJsonCmp<args...>;
  using Def = datum_cmp::ObDatumJsonCmp<false>;

  static void init_array()
  {
    auto &funcs = NULLSAFE_JSON_CMP_FUNCS;
    funcs[0][0] = Def::defined_ ? &Cmp<0, 0>::cmp : NULL;
    funcs[0][1] = Def::defined_ ? &Cmp<0, 1>::cmp : NULL;
    funcs[1][0] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
    funcs[1][1] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
  }
};

template <typename T, bool HAS_LOB_HEADER>
struct DatumGeoHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
};

template<int IDX>
struct InitBasicGeoFuncArray
{
  template <typename T, bool HAS_LOB_HEADER>
  using Hash = DefHashFunc<DatumGeoHashCalculator<T, HAS_LOB_HEADER>>;
  template <bool NULL_FIRST>
  using TCCmp = ObNullSafeDatumTCCmp<ObGeometryTC, ObGeometryTC, NULL_FIRST>;
  using TCDef = datum_cmp::ObDatumTCCmp<ObGeometryTC, ObGeometryTC>;
  template <bool NULL_FIRST, bool HAS_LOB_HEADER>
  using TypeCmp = ObNullSafeDatumGeoCmp<NULL_FIRST, HAS_LOB_HEADER>;
  using TypeDef = datum_cmp::ObDatumGeoCmp<false>;

  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_GEO_FUNCS;
    basic_funcs[0].default_hash_ = Hash<ObDefaultHash, false>::hash;
    basic_funcs[0].default_hash_batch_= Hash<ObDefaultHash, false>::hash_batch;
    basic_funcs[0].murmur_hash_ = Hash<ObMurmurHash, false>::hash;
    basic_funcs[0].murmur_hash_batch_ = Hash<ObMurmurHash, false>::hash_batch;
    // basic_funcs[0].xx_hash_ = Hash<ObXxHash, false>::hash;
    // basic_funcs[0].xx_hash_batch_ = Hash<ObXxHash, false>::hash_batch;
    // basic_funcs[0].wy_hash_ = Hash<ObWyHash, false>::hash;
    // basic_funcs[0].wy_hash_batch_ = Hash<ObWyHash, false>::hash_batch;
    basic_funcs[0].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 0>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[0].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 0>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[0].murmur_hash_v2_ = Hash<ObMurmurHash, false>::hash_v2;
    basic_funcs[0].murmur_hash_v2_batch_ = Hash<ObMurmurHash, false>::hash_v2_batch;

    basic_funcs[1].default_hash_ = Hash<ObDefaultHash, true>::hash;
    basic_funcs[1].default_hash_batch_= Hash<ObDefaultHash, true>::hash_batch;
    basic_funcs[1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
    basic_funcs[1].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
    basic_funcs[1].xx_hash_ = Hash<ObXxHash, true>::hash;
    basic_funcs[1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
    basic_funcs[1].wy_hash_ = Hash<ObWyHash, true>::hash;
    basic_funcs[1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
    basic_funcs[1].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 1>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[1].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 1>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[1].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
    basic_funcs[1].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
  }
};

template<int IDX>
struct InitGeoCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumGeoCmp<args...>;
  using Def = datum_cmp::ObDatumGeoCmp<false>;

  static void init_array()
  {
    auto &funcs = NULLSAFE_GEO_CMP_FUNCS;
    funcs[0][0] = Def::defined_ ? &Cmp<0, 0>::cmp : NULL;
    funcs[0][1] = Def::defined_ ? &Cmp<0, 1>::cmp : NULL;
    funcs[1][0] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
    funcs[1][1] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
  }
};


template <ObScale SCALE, typename T>
struct DatumFixedDoubleHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    // format fixed double to string first, then calc hash value of the string
    double d_val = datum.get_double();
    char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
    // zero distinguishes positive and negative zeros, formatted as positive zero to calculate
    // hash value
    if (d_val == 0.0) {
      d_val = 0.0;
    }
    int64_t length = ob_fcvt(d_val, static_cast<int>(SCALE), sizeof(buf) - 1, buf, NULL);
    res = T::hash(buf, static_cast<int32_t>(length), seed);
    return OB_SUCCESS;
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    int ret = OB_SUCCESS;
    if (datum.is_null()) {
      res = seed;
    } else {
      ret = calc_datum_hash(datum, seed, res);
    }
    return ret;
  }
};

template <int X>
struct InitFixedDoubleBasicFuncArray
{
  template <typename T>
  using Hash = DefHashFunc<DatumFixedDoubleHashCalculator<static_cast<ObScale>(X), T>>;
  static void init_array()
  {
    auto &basic_funcs = FIXED_DOUBLE_BASIC_FUNCS;
    basic_funcs[X].default_hash_ = Hash<ObDefaultHash>::hash;
    basic_funcs[X].default_hash_batch_= Hash<ObDefaultHash>::hash_batch;
    basic_funcs[X].murmur_hash_ = Hash<ObMurmurHash>::hash;
    basic_funcs[X].murmur_hash_batch_ = Hash<ObMurmurHash>::hash_batch;
    basic_funcs[X].xx_hash_ = Hash<ObXxHash>::hash;
    basic_funcs[X].xx_hash_batch_ = Hash<ObXxHash>::hash_batch;
    basic_funcs[X].wy_hash_ = Hash<ObWyHash>::hash;
    basic_funcs[X].wy_hash_batch_ = Hash<ObWyHash>::hash_batch;
    basic_funcs[X].null_first_cmp_ = ObNullSafeFixedDoubleCmp<static_cast<ObScale>(X), true>::cmp;
    basic_funcs[X].null_last_cmp_ = ObNullSafeFixedDoubleCmp<static_cast<ObScale>(X), false>::cmp;
    basic_funcs[X].murmur_hash_v2_ = Hash<ObMurmurHash>::hash_v2;
    basic_funcs[X].murmur_hash_v2_batch_ = Hash<ObMurmurHash>::hash_v2_batch;
  }
};

template <int X>
struct InitFixedDoubleCmpArray
{
  static void init_array()
  {
    auto &funcs = FIXED_DOUBLE_CMP_FUNCS;
    funcs[X][0] = ObNullSafeFixedDoubleCmp<static_cast<ObScale>(X), false>::cmp;
    funcs[X][1] = ObNullSafeFixedDoubleCmp<static_cast<ObScale>(X), true>::cmp;
  }
};

template <typename T, bool HAS_LOB_HEADER>
struct DatumUDTHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
};

template <int X>
struct InitUDTBasicFuncArray
{
  // only for storage use, udt types are used as null bitmap in storage
  // storage will use murmur_hash_ and null_last_cmp_ maybe, so keep the origin basic func define and others
  // basic func return error code wheh is called: hash func return OB_NOT_SUPPORTED and cmp func return OB_ERR_NO_ORDER_MAP_SQL
  template <typename T>
  using StrHash = DefHashFunc<DatumStrHashCalculator<CS_TYPE_BINARY, false, T, false>>;
  template <typename T, bool HAS_LOB_HEADER>
  using Hash = DefHashFunc<DatumUDTHashCalculator<T, HAS_LOB_HEADER>>;
  template <bool NULL_FIRST, bool HAS_LOB_HEADER>
  using TypeCmp = ObNullSafeDatumUDTCmp<NULL_FIRST, HAS_LOB_HEADER>;
  template <bool null_first>
  using StrCmp = ObNullSafeDatumStrCmp<CS_TYPE_BINARY, false, null_first>;
  using Def = datum_cmp::ObDatumStrCmp<CS_TYPE_BINARY, false>;
  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_UDT_FUNCS;
    basic_funcs[X].default_hash_ = Hash<ObDefaultHash, true>::hash;
    basic_funcs[X].default_hash_batch_= Hash<ObDefaultHash, true>::hash_batch;
    basic_funcs[X].murmur_hash_ = StrHash<ObMurmurHash>::hash;
    basic_funcs[X].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
    basic_funcs[X].xx_hash_ = Hash<ObXxHash, true>::hash;
    basic_funcs[X].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
    basic_funcs[X].wy_hash_ = Hash<ObWyHash, true>::hash;
    basic_funcs[X].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
    basic_funcs[X].null_first_cmp_ = &TypeCmp<1, 1>::cmp;
    basic_funcs[X].null_last_cmp_ = Def::defined_ ? &StrCmp<0>::cmp : NULL;
    basic_funcs[X].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
    basic_funcs[X].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
  }
};

template <typename T, bool HAS_LOB_HEADER>
struct DatumCollectionHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
};

template<int IDX>
struct InitCollectionBasicFuncArray
{
  template <typename T, bool HAS_LOB_HEADER>
  using Hash = DefHashFunc<DatumCollectionHashCalculator<T, HAS_LOB_HEADER>>;
  template <bool NULL_FIRST>
  using TCCmp = ObNullSafeDatumTCCmp<ObCollectionSQLTC, ObCollectionSQLTC, NULL_FIRST>;
  using TCDef = datum_cmp::ObDatumTCCmp<ObCollectionSQLTC, ObCollectionSQLTC>;
  template <bool NULL_FIRST, bool HAS_LOB_HEADER>
  using TypeCmp = ObNullSafeDatumCollectionCmp<NULL_FIRST, HAS_LOB_HEADER>;
  using TypeDef = datum_cmp::ObDatumCollectionCmp<false>;

  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_COLLECTION_FUNCS;
    basic_funcs[0].default_hash_ = Hash<ObDefaultHash, false>::hash;
    basic_funcs[0].default_hash_batch_= Hash<ObDefaultHash, false>::hash_batch;
    basic_funcs[0].murmur_hash_ = Hash<ObMurmurHash, false>::hash;
    basic_funcs[0].murmur_hash_batch_ = Hash<ObMurmurHash, false>::hash_batch;
    basic_funcs[0].xx_hash_ = Hash<ObXxHash, false>::hash;
    basic_funcs[0].xx_hash_batch_ = Hash<ObXxHash, false>::hash_batch;
    basic_funcs[0].wy_hash_ = Hash<ObWyHash, false>::hash;
    basic_funcs[0].wy_hash_batch_ = Hash<ObWyHash, false>::hash_batch;
    basic_funcs[0].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 0>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[0].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 0>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[0].murmur_hash_v2_ = Hash<ObMurmurHash, false>::hash_v2;
    basic_funcs[0].murmur_hash_v2_batch_ = Hash<ObMurmurHash, false>::hash_v2_batch;

    basic_funcs[1].default_hash_ = Hash<ObDefaultHash, true>::hash;
    basic_funcs[1].default_hash_batch_= Hash<ObDefaultHash, true>::hash_batch;
    basic_funcs[1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
    basic_funcs[1].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
    basic_funcs[1].xx_hash_ = Hash<ObXxHash, true>::hash;
    basic_funcs[1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
    basic_funcs[1].wy_hash_ = Hash<ObWyHash, true>::hash;
    basic_funcs[1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
    basic_funcs[1].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 1>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[1].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 1>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[1].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
    basic_funcs[1].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
  }
};

template<int IDX>
struct InitCollectionCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumCollectionCmp<args...>;
  using Def = datum_cmp::ObDatumCollectionCmp<false>;

  static void init_array()
  {
    auto &funcs = NULLSAFE_COLLECTION_CMP_FUNCS;
    funcs[0][0] = Def::defined_ ? &Cmp<0, 0>::cmp : NULL;
    funcs[0][1] = Def::defined_ ? &Cmp<0, 1>::cmp : NULL;
    funcs[1][0] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
    funcs[1][1] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
  }
};

template <typename T, bool HAS_LOB_HEADER>
struct DatumRoaringbitmapHashCalculator : public DefHashMethod<T>
{
  static int calc_datum_hash(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }

  static int calc_datum_hash_v2(const ObDatum &datum, const uint64_t seed, uint64_t &res)
  {
    return datum_lob_locator_hash(datum, CS_TYPE_UTF8MB4_BIN, seed, T::is_varchar_hash ? T::hash : NULL, res);
  }
};

template<int IDX>
struct InitBasicRoaringbitmapFuncArray
{
  template <typename T, bool HAS_LOB_HEADER>
  using Hash = DefHashFunc<DatumRoaringbitmapHashCalculator<T, HAS_LOB_HEADER>>;
  template <bool NULL_FIRST>
  using TCCmp = ObNullSafeDatumTCCmp<ObRoaringBitmapTC, ObRoaringBitmapTC, NULL_FIRST>;
  using TCDef = datum_cmp::ObDatumTCCmp<ObRoaringBitmapTC, ObRoaringBitmapTC>;
  template <bool NULL_FIRST, bool HAS_LOB_HEADER>
  using TypeCmp = ObNullSafeDatumRoaringbitmapCmp<NULL_FIRST, HAS_LOB_HEADER>;
  using TypeDef = datum_cmp::ObDatumRoaringbitmapCmp<false>;

  static void init_array()
  {
    auto &basic_funcs = EXPR_BASIC_ROARINGBITMAP_FUNCS;
    basic_funcs[0].default_hash_ = Hash<ObDefaultHash, false>::hash;
    basic_funcs[0].default_hash_batch_= Hash<ObDefaultHash, false>::hash_batch;
    basic_funcs[0].murmur_hash_ = Hash<ObMurmurHash, false>::hash;
    basic_funcs[0].murmur_hash_batch_ = Hash<ObMurmurHash, false>::hash_batch;
    // basic_funcs[0].xx_hash_ = Hash<ObXxHash, false>::hash;
    // basic_funcs[0].xx_hash_batch_ = Hash<ObXxHash, false>::hash_batch;
    // basic_funcs[0].wy_hash_ = Hash<ObWyHash, false>::hash;
    // basic_funcs[0].wy_hash_batch_ = Hash<ObWyHash, false>::hash_batch;
    basic_funcs[0].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 0>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[0].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 0>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[0].murmur_hash_v2_ = Hash<ObMurmurHash, false>::hash_v2;
    basic_funcs[0].murmur_hash_v2_batch_ = Hash<ObMurmurHash, false>::hash_v2_batch;

    basic_funcs[1].default_hash_ = Hash<ObDefaultHash, true>::hash;
    basic_funcs[1].default_hash_batch_= Hash<ObDefaultHash, true>::hash_batch;
    basic_funcs[1].murmur_hash_ = Hash<ObMurmurHash, true>::hash;
    basic_funcs[1].murmur_hash_batch_ = Hash<ObMurmurHash, true>::hash_batch;
    basic_funcs[1].xx_hash_ = Hash<ObXxHash, true>::hash;
    basic_funcs[1].xx_hash_batch_ = Hash<ObXxHash, true>::hash_batch;
    basic_funcs[1].wy_hash_ = Hash<ObWyHash, true>::hash;
    basic_funcs[1].wy_hash_batch_ = Hash<ObWyHash, true>::hash_batch;
    basic_funcs[1].null_first_cmp_ = TypeDef::defined_
        ? &TypeCmp<1, 1>::cmp
        : TCDef::defined_ ? &TCCmp<1>::cmp : NULL;
    basic_funcs[1].null_last_cmp_ = TypeDef::defined_
        ? &TypeCmp<0, 1>::cmp
        : TCDef::defined_ ? &TCCmp<0>::cmp : NULL;
    basic_funcs[1].murmur_hash_v2_ = Hash<ObMurmurHash, true>::hash_v2;
    basic_funcs[1].murmur_hash_v2_batch_ = Hash<ObMurmurHash, true>::hash_v2_batch;
  }
};

template<int IDX>
struct InitRoaringbitmapCmpArray
{
  template <bool... args>
  using Cmp = ObNullSafeDatumRoaringbitmapCmp<args...>;
  using Def = datum_cmp::ObDatumRoaringbitmapCmp<false>;

  static void init_array()
  {
    auto &funcs = NULLSAFE_ROARINGBITMAP_CMP_FUNCS;
    funcs[0][0] = Def::defined_ ? &Cmp<0, 0>::cmp : NULL;
    funcs[0][1] = Def::defined_ ? &Cmp<0, 1>::cmp : NULL;
    funcs[1][0] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
    funcs[1][1] = Def::defined_ ? &Cmp<1, 1>::cmp : NULL;
  }
};
template<int width>
struct InitDecintBasicFuncArray
{
  template<typename T>
  using Hash = DefHashFunc<DatumHashCalculator<ObDecimalIntType, T>>;

  static void init_array()
  {
    auto &basic_funcs = DECINT_BASIC_FUNCS;
    basic_funcs[width].default_hash_ = Hash<ObDefaultHash>::hash;
    basic_funcs[width].default_hash_batch_= Hash<ObDefaultHash>::hash_batch;
    basic_funcs[width].murmur_hash_ = Hash<ObMurmurHash>::hash;
    basic_funcs[width].murmur_hash_batch_ = Hash<ObMurmurHash>::hash_batch;
    basic_funcs[width].xx_hash_ = Hash<ObXxHash>::hash;
    basic_funcs[width].xx_hash_batch_ = Hash<ObXxHash>::hash_batch;
    basic_funcs[width].wy_hash_ = Hash<ObWyHash>::hash;
    basic_funcs[width].wy_hash_batch_ = Hash<ObWyHash>::hash_batch;
    basic_funcs[width].null_first_cmp_ =
      ObNullSafeDecintCmp<static_cast<ObDecimalIntWideType>(width),
                          static_cast<ObDecimalIntWideType>(width), true>::cmp;
    basic_funcs[width].null_last_cmp_ =
      ObNullSafeDecintCmp<static_cast<ObDecimalIntWideType>(width),
                          static_cast<ObDecimalIntWideType>(width), false>::cmp;
    basic_funcs[width].murmur_hash_v2_ = Hash<ObMurmurHash>::hash_v2;
    basic_funcs[width].murmur_hash_v2_batch_ = Hash<ObMurmurHash>::hash_v2_batch;
  }
};

template<int width1, int width2>
struct InitDecintCmpArray
{
  static void init_array()
  {
    auto &funcs = DECINT_CMP_FUNCS;
    funcs[width1][width2][0] =
      ObNullSafeDecintCmp<static_cast<ObDecimalIntWideType>(width1),
                          static_cast<ObDecimalIntWideType>(width2), false>::cmp;
    funcs[width1][width2][1] = ObNullSafeDecintCmp<static_cast<ObDecimalIntWideType>(width1),
                          static_cast<ObDecimalIntWideType>(width2), true>::cmp;
  }
};

} // end common
} // end oceanbase
#endif // OCEANBASE_SHARE_DATUM_FUNCS_UTIL_H_
