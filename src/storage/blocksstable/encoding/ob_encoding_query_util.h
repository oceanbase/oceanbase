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

#ifndef OCEANBASE_ENCODING_OB_ENCODING_QUERY_UTIL_H_
#define OCEANBASE_ENCODING_OB_ENCODING_QUERY_UTIL_H_

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#include "ob_encoding_util.h"
#include "sql/engine/basic/ob_pushdown_filter.h"


namespace oceanbase
{
namespace blocksstable
{

// Check if current CPU
static inline bool is_avx512_valid()
{
#if defined ( __x86_64__ )
  int a, b, c, d;
  __asm("cpuid" : "=a"(a), "=b"(b), "=c"(c), "=d"(d) : "a"(7), "c"(0) : );
  if ((b & (1 << 31)) == 0) return false;           // AVX512VL invalid
  if ((b & 0x40020000) != 0x40020000) return false; // AVX512BW/AVX512DQ invalid
  return true;
#else
  return false;
#endif
}

static inline bool is_avx2_valid()
{
#if defined ( __x86_64__ )
  int a, b, c, d;
  __asm("cpuid" : "=a"(a), "=b"(b), "=c"(c), "=d"(d) : "a"(7), "c"(0) : );
  if ((b & (1 <<  5)) == 0) return false; // AVX2 invalid
  return true;
#else
  return false;
#endif
}


/**
  * 0: OP := _MM_CMPINT_EQ
  * 1: OP := _MM_CMPINT_LT
  * 2: OP := _MM_CMPINT_LE
  * 3: OP := _MM_CMPINT_FALSE
  * 4: OP := _MM_CMPINT_NE
  * 5: OP := _MM_CMPINT_NLT
  * 6: OP := _MM_CMPINT_NLE
  * 7: OP := _MM_CMPINT_TRUE
  **/
template <int CMP_TYPE>
struct ObCmpTypeToAvxOpMap { constexpr static int value_ = 0; }; // never be used

template <> struct ObCmpTypeToAvxOpMap<sql::WHITE_OP_EQ> { constexpr static int value_ = 0; };
template <> struct ObCmpTypeToAvxOpMap<sql::WHITE_OP_LT> { constexpr static int value_ = 1; };
template <> struct ObCmpTypeToAvxOpMap<sql::WHITE_OP_LE> { constexpr static int value_ = 2; };
template <> struct ObCmpTypeToAvxOpMap<sql::WHITE_OP_GE> { constexpr static int value_ = 5; };
template <> struct ObCmpTypeToAvxOpMap<sql::WHITE_OP_GT> { constexpr static int value_ = 6; };
template <> struct ObCmpTypeToAvxOpMap<sql::WHITE_OP_NE> { constexpr static int value_ = 4; };

template <int TYPE_BYTE>
struct ObEncodingValueLenTagMap { constexpr static int32_t tag_ = -1; };

template <> struct ObEncodingValueLenTagMap<0> { constexpr static int32_t tag_ = 0; };
template <> struct ObEncodingValueLenTagMap<1> { constexpr static int32_t tag_ = 0; };
template <> struct ObEncodingValueLenTagMap<2> { constexpr static int32_t tag_ = 1; };
template <> struct ObEncodingValueLenTagMap<4> { constexpr static int32_t tag_ = 2; };
template <> struct ObEncodingValueLenTagMap<8> { constexpr static int32_t tag_ = 3; };

template <int IS_SIGNED, int TYPE_TAG>
struct ObEncodingTypeInference { typedef char Type; };

template <> struct ObEncodingTypeInference<0, 0> { typedef uint8_t Type; };
template <> struct ObEncodingTypeInference<0, 1> { typedef uint16_t Type; };
template <> struct ObEncodingTypeInference<0, 2> { typedef uint32_t Type; };
template <> struct ObEncodingTypeInference<0, 3> { typedef uint64_t Type; };
template <> struct ObEncodingTypeInference<1, 0> { typedef int8_t Type; };
template <> struct ObEncodingTypeInference<1, 1> { typedef int16_t Type; };
template <> struct ObEncodingTypeInference<1, 2> { typedef int32_t Type; };
template <> struct ObEncodingTypeInference<1, 3> { typedef int64_t Type; };

template <int IS_SIGNED, int TYPE_BYTE>
struct ObEncodingByteLenMap
{
  typedef typename ObEncodingTypeInference<IS_SIGNED, ObEncodingValueLenTagMap<TYPE_BYTE>::tag_>::Type Type;
};

template <typename T, int32_t CMP_TYPE>
OB_INLINE bool value_cmp_t(T l, T r)
{
  bool res = false;
  switch (CMP_TYPE) {
    case sql::WHITE_OP_EQ:
      res = l == r;
      break;
    case sql::WHITE_OP_LE:
      res = l <= r;
      break;
    case sql::WHITE_OP_LT:
      res = l < r;
      break;
    case sql::WHITE_OP_GE:
      res = l >= r;
      break;
    case sql::WHITE_OP_GT:
      res = l > r;
      break;
    case sql::WHITE_OP_NE:
      res = l != r;
      break;
    default:
      res = false;
  }
  return res;
}

OB_INLINE int32_t *get_value_len_tag_map()
{
  static int32_t value_len_tag_map[] = {
    0, // 0
    0, // 1
    1, // 2
    0,
    2, // 4
    0,
    0,
    0,
    3 // 8
  };
  return value_len_tag_map;
}

OB_INLINE int32_t *get_store_class_tag_map()
{
  static int32_t store_class_tag_map[] = {
    -1, // ObExtendSC = 0
    2, // ObIntSC
    2, // ObUIntSC
    0, // ObNumberSC
    1, // ObStringSC
    -1, // ObTextSC
    -1, // ObOTimestampSC
    -1, // ObIntervalSC
    -1, // ObLobSC
    -1, // ObJsonSC
    -1, // ObGeometrySC
    -1 // ObMaxSC
  };
  return store_class_tag_map;
}

OB_INLINE int32_t get_datum_store_len(const common::ObObjDatumMapType &datum_type)
{
  int32_t datum_len = 0;
  switch (datum_type) {
  case common::OBJ_DATUM_1BYTE_DATA: {
    datum_len = sizeof(uint8_t);
    break;
  }
  case common::OBJ_DATUM_4BYTE_DATA: {
    datum_len = sizeof(uint32_t);
    break;
  }
  case common::OBJ_DATUM_8BYTE_DATA: {
    datum_len = sizeof(uint64_t);
    break;
  }
  case common::OBJ_DATUM_4BYTE_LEN_DATA: {
    datum_len = sizeof(uint64_t) + sizeof(uint32_t);
    break;
  }
  case common::OBJ_DATUM_2BYTE_LEN_DATA: {
    datum_len = sizeof(uint64_t) + sizeof(uint16_t);
    break;
  }
  default: {
    datum_len = 0;
  }
  }
  return datum_len;
}


template <int... args>
struct Multiply_T;

template <>
struct Multiply_T<>
{
	constexpr static int value_ = 1;
};

template <int X, int... args>
struct Multiply_T<X, args...>
{
	constexpr static int value_ = X * Multiply_T<args...>::value_;
};


// Multi dimension array initializer
template <template <int ...> class INITER, int ...args>
struct ObNDArrayIniterIndex;

template <template <int ...> class INITER, int ARG_CNT, int IDX, int CNT, int X, int ...args>
struct ObNDArrayIniterIndex<INITER, ARG_CNT, IDX, CNT, X, args...>
{
	static bool apply()
	{
		constexpr static int LEFT = CNT / X;
		return ObNDArrayIniterIndex<INITER, ARG_CNT - 1, IDX % LEFT, LEFT, args..., IDX / LEFT>::apply();
	}
};

template <template <int ...> class INITER, int IDX, int CNT, int X, int ...args>
struct ObNDArrayIniterIndex<INITER, 0, IDX, CNT, X, args...>
{
	static bool apply()
	{
		return INITER<X, args...>()();
	}
};

template <template <int ...> class INITER, int IDX, int CNT, int X, int ...args>
struct ObNDArrayIniterIndex<INITER, -1, IDX, CNT, X, args...> { static bool apply() { return true; }};

template <template <int ...> class INITER, int ARG_CNT, int CNT, int X, int ...args>
struct ObNDArrayIniterIndex<INITER, ARG_CNT, -1, CNT, X, args...> { static bool apply() { return true; }};


template <template <int ...> class INITER, int... args>
struct ObNDArrayIniterLoop;

template <template <int ...> class INITER, int IDX, int CNT, int ...args>
struct ObNDArrayIniterLoop<INITER, IDX, CNT, args...>
{
	static bool apply()
	{
    ObNDArrayIniterLoop<INITER, IDX - 1, CNT, args...>::apply();
    return ObNDArrayIniterIndex<INITER, sizeof...(args), IDX, CNT, args...>::apply();
	}
};

template <template <int ...> class INITER, int CNT, int ...args>
struct ObNDArrayIniterLoop<INITER, -1, CNT, args...>
{
	static bool apply() { return true; }
};


template <template <int ...> class INITER, int ...args>
struct ObNDArrayIniter
{
	static bool apply()
	{
		constexpr static int array_size = Multiply_T<args...>::value_;
		return ObNDArrayIniterLoop<INITER, array_size - 1, array_size, args...>::apply();
	}
};


// Multiple dimension array implementation
// TODO: saitong.zst: combine the MultiDimArray and ObNDArrayIniter to a single template family
template<typename T, uint32_t ... RestDim> struct ObMultiDimArray_T;

template<typename T, uint32_t CurrDim>
struct ObMultiDimArray_T<T, CurrDim>
{
  typedef T ArrType[CurrDim];
  ArrType data_;
  T& operator[](uint32_t idx) { return data_[idx]; }
};

template<typename T, uint32_t CurrDim, uint32_t ... RestDim>
struct ObMultiDimArray_T<T, CurrDim, RestDim...>
{
  typedef typename ObMultiDimArray_T<T, RestDim...>::ArrType OneDimDownArray_T;
  typedef OneDimDownArray_T ArrType[CurrDim];
  ArrType data_;
  OneDimDownArray_T& operator[](uint32_t idx) { return data_[idx]; }
};




} // End of namespace blocksstable
} // End of namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_ENCODING_QUERY_UTIL_H_