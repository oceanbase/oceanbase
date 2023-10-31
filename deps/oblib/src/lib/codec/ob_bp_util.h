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

#ifndef OB_BP_UTIL_
#define OB_BP_UTIL_
#include <stdint.h>
#include <type_traits>
#include <cstddef>
#include <string.h>
#include "lib/utility/ob_macro_utils.h"
#include "ob_sse_to_neon.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

__attribute__((const)) inline uint32_t gccbits(const uint32_t v) {
  return (v == 0) ? 0 : (32 - __builtin_clz(v));
}

__attribute__((const)) inline uint32_t gccbits(const uint8_t v) {
  return gccbits((uint32_t)v);
}

__attribute__((const)) inline uint32_t gccbits(const uint16_t v) {
  return gccbits((uint32_t)v);
}

__attribute__((const)) inline uint32_t gccbits(const uint64_t v) {
  if (v == 0) {
    return 0;
  } else {
    return ((v == 0) ? 0 : (64 - __builtin_clzll(v)));
  }
}

__attribute__((const)) inline bool divisibleby(size_t a, uint32_t x) {
  return ((a % x) == 0);
}

__attribute__((const)) inline uint32_t padn(uint32_t x, uint32_t n) {
  return ((x + n - 1) / n);
}

__attribute__((const)) inline uint32_t pad4(uint32_t x) { return padn(x, 4); }
__attribute__((const)) inline uint32_t pad8(uint32_t x) { return padn(x, 8); }
__attribute__((const)) inline uint32_t pad32(uint32_t x) { return padn(x, 32); }
__attribute__((const)) inline uint32_t pad64(uint32_t x) { return padn(x, 64); }
__attribute__((const)) inline uint32_t pad128(uint32_t x) { return padn(x, 128); }

__attribute__((const)) inline uint32_t popcnt64(uint64_t x) { return __builtin_popcountll(x); }

template <class T>
__attribute__((pure)) uint32_t maxbits2(const T *begin, const T *end) {
  T accumulator = 0;
  const T *k = begin;
  for (; k != end; ++k) {
    accumulator |= *k;
  }
  return gccbits(accumulator);
}

#ifdef __ARM_NEON
#define DO_PREFETCH(addr)
#else
#define DO_PREFETCH(addr) __builtin_prefetch(addr, 0)
#endif

class ObZigZag
{
public:
  template<typename UIntT>
  static OB_INLINE UIntT encode(const UIntT value) {
    using SignedDeltaType = typename std::make_signed<UIntT>::type;
    SignedDeltaType tmp_value = value;
    // negative right shift, high bit is 1
    return (tmp_value << 1) ^ (tmp_value >> ((sizeof(UIntT) * 8) - 1));
  }

  template<typename UIntT>
  static OB_INLINE UIntT decode(const UIntT value) { return (value >> 1) ^ (- (value & 1)); }
};

class ObXor
{
  public:
  template <typename UIntT>
  OB_INLINE static UIntT encode(UIntT &a, UIntT &b) { return a ^ b; }
  template <typename UIntT>
  OB_INLINE static UIntT decode(UIntT &a, UIntT &b) { return a ^ b; }
};

template <typename UIntT>
OB_INLINE UIntT bit_reverse(UIntT x)
{
  static_assert(sizeof(UIntT) == 1
                || sizeof(UIntT) == 2
                || sizeof(UIntT) == 4
                || sizeof(UIntT) == 8,
                "not support");
  return 0;
}

template <>
OB_INLINE uint8_t bit_reverse(uint8_t x) { return __builtin_bitreverse8(x); }
template <>
OB_INLINE uint16_t bit_reverse(uint16_t x) { return __builtin_bitreverse16(x); }
template <>
OB_INLINE uint32_t bit_reverse(uint32_t x) { return __builtin_bitreverse32(x); }
template <>
OB_INLINE uint64_t bit_reverse(uint64_t x) { return __builtin_bitreverse64(x); }

template <typename UIntT>
struct ObDeltaIntType { typedef char Type; };

template <> struct ObDeltaIntType<uint8_t> { typedef uint32_t Type; };
template <> struct ObDeltaIntType<uint16_t> { typedef uint32_t Type; };
template <> struct ObDeltaIntType<uint32_t> { typedef uint32_t Type; };
template <> struct ObDeltaIntType<uint64_t> { typedef uint64_t Type; };


namespace ObBitUtils
{
  // encode slide forward
  OB_INLINE void e_slide(uint64_t &bw, uint32_t &br, char *&op)
  {
     (*(uint64_t *)(op)) = bw;
     op += (br >> 3);
     bw >>= (br & ~7);
     br &= 7;
  }

  // decode slide forward
  OB_INLINE void d_slide(uint64_t &bw, uint32_t &br, const char *&ip, const char *ip_end)
  {
    const char *ip_s = (ip += (br>>3));
    if ((ip_s + sizeof(uint64_t)) <= ip_end) {
      bw = (*(uint64_t *)(ip_s));
    } else {
      char tmp[sizeof(uint64_t)];
      memset(tmp, 0, sizeof(uint64_t));
      memcpy(tmp, ip_s, (ip_end - ip_s));
      bw = (*(uint64_t *)(tmp));
    }
    br &= 7;
  }

  OB_INLINE void align(uint64_t &bw, uint32_t &br, const char *&ip)
  {
    ip += ((br+7) >> 3);
  }

  OB_INLINE uint64_t rs_bw(uint64_t &bw, uint32_t &br) // right shift bw
  {
    return (bw >> br);
  }

  OB_INLINE void r_mv(uint32_t &br, const uint32_t step)
  {
    br += step;
  }

  // BZHI32
  OB_INLINE uint32_t bzhi32(const uint32_t u, const uint32_t b)
  {
    return ((b == 32) ?  0xffffffffu : (u & ((1u << b) - 1)));
  }
  // BZHI64
  OB_INLINE uint64_t bzhi64(const uint64_t u, const uint32_t b)
  {
    return ((b == 64) ?  0xffffffffffffffffull : (u & ((1ull << b) - 1)));
  }

  OB_INLINE uint64_t bzhi_u64(const uint64_t u, const uint64_t b)
  {
    return (u & ((1ull << b) - 1));
  }
  OB_INLINE uint32_t bzhi_u32(const uint32_t u, const uint64_t b)
  {
    return (u & ((1u << b) - 1));
  }

  // BITPEEK32
  OB_INLINE uint32_t peek32(uint64_t &bw, uint32_t &br, const uint32_t b)
  {
    return bzhi32((uint32_t)rs_bw(bw, br), b);
  }
  // BITPEEK64
  OB_INLINE uint64_t peek64(uint64_t &bw, uint32_t &br, const uint32_t b)
  {
    return bzhi64(rs_bw(bw, br), b);
  }

  // BITGET32
  OB_INLINE void get32(uint64_t &bw, uint32_t &br, const uint32_t b, uint32_t &value)
  {
    value = peek32(bw, br, b);
    r_mv(br, b);
  }
  // BITGET64
  OB_INLINE void get64(uint64_t &bw, uint32_t &br, const uint32_t b, uint64_t &value)
  {
    value = peek64(bw, br, b);
    r_mv(br, b);
  }

  // bitpeek57
  OB_INLINE uint64_t peek57(uint64_t &bw, uint32_t &br, const uint32_t b)
  {
    return bzhi_u64(rs_bw(bw, br), b);
  }
  // bitpeek31
  OB_INLINE uint32_t peek31(uint64_t &bw, uint32_t &br, const uint32_t b)
  {
    return bzhi_u32((uint32_t)rs_bw(bw, br), b);
  }

  // bitget57
  OB_INLINE void get57(uint64_t &bw, uint32_t &br, const uint32_t b, uint64_t &value)
  {
    value = peek57(bw, br, b);
    r_mv(br, b);
  }
  // bitget31
  OB_INLINE void get31(uint64_t &bw, uint32_t &br, const uint32_t b, uint32_t &value)
  {
    value = peek31(bw, br, b);
    r_mv(br, b);
  }

  OB_INLINE int flush(uint64_t &bw, uint32_t &br, char *out, uint64_t out_len, uint64_t &out_pos)

  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(out_len - out_pos < sizeof(uint64_t))) {
      ret = OB_BUF_NOT_ENOUGH;
      LIB_LOG(WARN, "buf no enough", K(ret), K(out_len), K(out_pos));
    } else {
      char *op = out + out_pos;
      (*(uint64_t *)(op)) = bw;
      out_pos += ((br+7)>>3);
      bw = 0 ;
      br =0;
    }
    return ret;
  }

  template<typename UIntT>
  OB_INLINE void get(uint64_t &bw, uint32_t &br, const uint32_t b, uint64_t &value, const char *&ip, const char *ip_end);
  // bitget8
  template<>
  OB_INLINE void get<uint8_t>(uint64_t &bw, uint32_t &br, const uint32_t b, uint64_t &value, const char *&ip, const char *ip_end)
  {
    uint32_t tmp_value = 0;
    get31(bw, br, b, tmp_value);
    value = tmp_value;
  }
  // bitget16
  template<>
  OB_INLINE void get<uint16_t>(uint64_t &bw, uint32_t &br, const uint32_t b, uint64_t &value, const char *&ip, const char *ip_end)
  {
    uint32_t tmp_value = 0;
    get31(bw, br, b, tmp_value);
    value = tmp_value;
  }
  // bitget32
  template<>
  OB_INLINE void get<uint32_t>(uint64_t &bw, uint32_t &br, const uint32_t b, uint64_t &value, const char *&ip, const char *ip_end)
  {
    get57(bw, br, b, value);
  }
  // bitget64
  template<>
  OB_INLINE void get<uint64_t>(uint64_t &bw, uint32_t &br, const uint32_t b, uint64_t &value, const char *&ip, const char *ip_end)
  {
    if(b > 45) {
      uint64_t v = 0;
      get57(bw, br, (b - 32), value);
      d_slide(bw, br, ip, ip_end);
      get64(bw, br, 32, v);
      value = ((value << 32) | v);
    } else {
      get57(bw, br, b, value);
    }
  }

  // width: value's valid bit with
  template<typename UIntT>
  OB_INLINE void put(uint64_t &bw, uint32_t &br, const uint32_t width, const uint64_t value, char *&op)
  {
    bw += (value << br);
    br += width;
  }

  template<>
  OB_INLINE void put<uint64_t>(uint64_t &bw, uint32_t &br, const uint32_t width, const uint64_t value, char *&op)
  {
    if (width > 45) {
      put<uint32_t>(bw, br, width - 32, (value >> 32), op);
      e_slide(bw, br, op);
      put<uint32_t>(bw, br, 32, (uint32_t)(value), op);
    } else {
      put<uint32_t>(bw, br, width, value, op);
    };
  }

  OB_INLINE void put64(uint64_t &bw, uint32_t &br, uint32_t width, uint64_t value, char *&op)
  {
    put<uint64_t>(bw, br, width, value, op);
  }
  OB_INLINE void put32(uint64_t &bw, uint32_t &br, uint32_t width, uint64_t value)
  {
    char *op = nullptr;
    put<uint32_t>(bw, br, width, value, op);
  }
};

template<typename UIntT>
OB_INLINE uint32_t get_N0();
template<typename UIntT>
OB_INLINE uint32_t get_N1();
template<typename UIntT>
OB_INLINE uint32_t get_N2();
template<typename UIntT>
OB_INLINE uint32_t get_N3();
template<typename UIntT>
OB_INLINE uint32_t get_N4();


template<>
OB_INLINE uint32_t get_N0<uint8_t>() { return 3; }
template<>
OB_INLINE uint32_t get_N1<uint8_t>() { return 4; }
template<>
OB_INLINE uint32_t get_N2<uint8_t>() { return 3; }
template<>
OB_INLINE uint32_t get_N3<uint8_t>() { return 5; }
template<>
OB_INLINE uint32_t get_N4<uint8_t>() { return 9; } // (1 << (N4 - 1)) > UINT8_MAX


template<>
OB_INLINE uint32_t get_N0<uint16_t>() { return 3; }
template<>
OB_INLINE uint32_t get_N1<uint16_t>() { return 5; }
template<>
OB_INLINE uint32_t get_N2<uint16_t>() { return 6; }
template<>
OB_INLINE uint32_t get_N3<uint16_t>() { return 12; }
template<>
OB_INLINE uint32_t get_N4<uint16_t>() { return 17; } // (1 << (N4 - 1)) > UINT16_MAX


template<>
OB_INLINE uint32_t get_N0<uint32_t>() { return 4; }
template<>
OB_INLINE uint32_t get_N1<uint32_t>() { return 6; }
template<>
OB_INLINE uint32_t get_N2<uint32_t>() { return 6; }
template<>
OB_INLINE uint32_t get_N3<uint32_t>() { return 10; }
template<>
OB_INLINE uint32_t get_N4<uint32_t>() { return 17; }


template<>
OB_INLINE uint32_t get_N0<uint64_t>() { return 4; }
template<>
OB_INLINE uint32_t get_N1<uint64_t>() { return 7; }
template<>
OB_INLINE uint32_t get_N2<uint64_t>() { return 6; }
template<>
OB_INLINE uint32_t get_N3<uint64_t>() { return 12; }
template<>
OB_INLINE uint32_t get_N4<uint64_t>() { return 20; }

} // namespace common
} // namespace oceanbase
#endif // OB_BP_UTIL_
