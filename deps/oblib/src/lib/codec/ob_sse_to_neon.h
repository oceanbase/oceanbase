/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_SSE_TO_NEON_H
#define OB_SSE_TO_NEON_H

#if defined (__ARM_NEON__) && !defined(__ARM_NEON)
#define __ARM_NEON 1
#endif

#if defined(__GNUC__) && (defined(__x86_64__) || defined(__i386__))
/* GCC-compatible compiler, targeting x86/x86-64 */
#include <x86intrin.h>
#elif defined(__GNUC__) && defined(__ARM_NEON)
/* GCC-compatible compiler, targeting ARM with NEON */

#include <arm_neon.h>

// sse instruct to arm neon instruct mapping
#define __m128i uint32x4_t

// arithmetic
#define _mm_sub_epi32(a, b)                  (__m128i)vsubq_u32((uint32x4_t)(a), (uint32x4_t)(b))
#define _mm_add_epi32(a, b)                  vaddq_u32(a, b)

// store
#define _mm_storeu_si128(p, a)               vst1q_u32((uint32_t *)(p), a)
#define _mm_store_si128(p, a)                _mm_storeu_si128(p, a)

// load
#define _mm_loadu_si128(p)                   vld1q_u32((const uint32_t *)(p))
#define _mm_load_si128(p)                    vld1q_u32((const uint32_t *)(p))
#define _mm_lddqu_si128(p)                   vld1q_u32((const uint32_t *)(p))

// others
// emits the Supplemental Streaming SIMD Extensions 3 (SSSE3) instruction palignr to extract a 128-bit byte aligned value.
#define _mm_alignr_epi8(a, b, ralign)        (__m128i)vextq_u8(  (uint8x16_t)(b), (uint8x16_t)(a), ralign)

// insert or extract
// emits the Streaming SIMD Extensions 4 (SSE4) instruction pextrd. This instruction extracts a 32-bit value from a 128 bit parameter.
#define _mm_extract_epi32(a, ndx)            vgetq_lane_u32(a, ndx)

// set
#define _mm_set1_epi32(u)                    vdupq_n_u32(u)
#define _mm_set1_epi16(w)                    (__m128i)vdupq_n_u16(w)

// shift
#define _mm_slli_si128(a, imm)               (__m128i)((imm)<1?(a):((imm)>15?vdupq_n_u8( 0):vextq_u8(vdupq_n_u8(0), (uint8x16_t)(a), 16-(imm)))) // vextq_u8: __constrange(0-15)
#define _mm_slli_epi16(a, count)             (__m128i)vshlq_u16((uint16x8_t)(a), vdupq_n_s16((count)))
#define _mm_srli_epi16(a, count)             (__m128i)vshlq_u16((uint16x8_t)(a), vdupq_n_s16(-(count)))
#define _mm_slli_epi32(a, count)             (__m128i)vshlq_u32((uint32x4_t)(a), vdupq_n_s32((count)))
#define _mm_srli_epi32(a, count)             (__m128i)vshlq_u32((uint32x4_t)(a), vdupq_n_s32(-(count)))

// logical
#define _mm_or_si128(a, b)                   (__m128i)vorrq_u32(  (uint32x4_t)(a), (uint32x4_t)(b))
#define _mm_and_si128(a, b)                  (__m128i)vandq_u32(  (uint32x4_t)(a), (uint32x4_t)(b))


// Shuffles the 4 signed or unsigned 32-bit integers in a as specified by imm.
#define _mm_shuffle_epi32(a, imm)            ({ const uint32x4_t _av =a;\
                                                 uint32x4_t _v = vmovq_n_u32(vgetq_lane_u32(_av, (imm)        & 0x3));\
                                                            _v = vsetq_lane_u32(vgetq_lane_u32(_av, ((imm) >> 2) & 0x3), _v, 1);\
                                                            _v = vsetq_lane_u32(vgetq_lane_u32(_av, ((imm) >> 4) & 0x3), _v, 2);\
                                                            _v = vsetq_lane_u32(vgetq_lane_u32(_av, ((imm) >> 6) & 0x3), _v, 3); _v;\
                                              })

#elif defined(__GNUC__) && defined(__IWMMXT__)
/* GCC-compatible compiler, targeting ARM with WMMX */
#include <mmintrin.h>

#elif defined(__GNUC__) && defined(__loongarch_sx)
/* GCC-compatible compiler, targeting LoongArch with LSX (-mlsx) */
#include <lsxintrin.h>

#define __m128i v4u32

// arithmetic
#define _mm_sub_epi32(a, b)     __lsx_vsub_w((a), (b))
#define _mm_add_epi32(a, b)     __lsx_vadd_w((a), (b))

// store
#define _mm_storeu_si128(p, a)  __lsx_vst((a), (void *)(p), 0)
#define _mm_store_si128(p, a)   __lsx_vst((a), (void *)(p), 0)

// load
#define _mm_loadu_si128(p)      __lsx_vld((const void *)(p), 0)
#define _mm_load_si128(p)       __lsx_vld((const void *)(p), 0)
#define _mm_lddqu_si128(p)      __lsx_vld((const void *)(p), 0)

// alignr: result[i] = b[ralign+i] if ralign+i<16, else a[ralign+i-16]
// equivalent to SSE palignr: concatenate [a(high)|b(low)], extract 16 bytes at byte offset ralign
static __attribute__((always_inline)) __m128i lsx_alignr_bytes(__m128i a, __m128i b, int ralign)
{
    uint8_t idx[16];
    for (int i = 0; i < 16; i++) idx[i] = (uint8_t)(ralign + i);
    __m128i vk = __lsx_vld((const void *)idx, 0);
    return (__m128i)__lsx_vshuf_b((__m128i)a, (__m128i)b, (__m128i)vk);
}
#define _mm_alignr_epi8(a, b, ralign)  lsx_alignr_bytes((a), (b), (ralign))

// extract / set
#define _mm_extract_epi32(a, ndx)      __lsx_vpickve2gr_wu((a), (ndx))
#define _mm_set1_epi32(u)              __lsx_vreplgr2vr_w(u)
#define _mm_set1_epi16(w)              (__m128i)__lsx_vreplgr2vr_h(w)

// byte-level left shift, fill low bytes with zeros
// _mm_slli_si128(a, n): result[i] = a[i-n] if i>=n, else 0
#define _mm_slli_si128(a, imm) \
    (__m128i)((imm) < 1  ? (__m128i)(a) : \
              (imm) > 15 ? (__m128i)__lsx_vreplgr2vr_w(0) : \
              lsx_alignr_bytes((__m128i)(a), (__m128i)__lsx_vreplgr2vr_w(0), 16-(imm)))

// element-level shifts
#define _mm_slli_epi16(a, n)  (__m128i)__lsx_vsll_h((__m128i)(a), (__m128i)__lsx_vreplgr2vr_h(n))
#define _mm_srli_epi16(a, n)  (__m128i)__lsx_vsrl_h((__m128i)(a), (__m128i)__lsx_vreplgr2vr_h(n))
#define _mm_slli_epi32(a, n)  (__m128i)__lsx_vsll_w((__m128i)(a), (__m128i)__lsx_vreplgr2vr_w(n))
#define _mm_srli_epi32(a, n)  (__m128i)__lsx_vsrl_w((__m128i)(a), (__m128i)__lsx_vreplgr2vr_w(n))

// logical
#define _mm_or_si128(a, b)    (__m128i)__lsx_vor_v((__m128i)(a), (__m128i)(b))
#define _mm_and_si128(a, b)   (__m128i)__lsx_vand_v((__m128i)(a), (__m128i)(b))

// shuffle 32-bit elements
#define _mm_shuffle_epi32(a, imm)                                                        \
    ({ const __m128i _av = (a);                                                          \
       __m128i _v = __lsx_vinsgr2vr_w((__m128i)__lsx_vreplgr2vr_w(0),                   \
                       __lsx_vpickve2gr_wu(_av, (imm)        & 0x3), 0);                 \
               _v = __lsx_vinsgr2vr_w(_v, __lsx_vpickve2gr_wu(_av, ((imm)>>2) & 0x3), 1); \
               _v = __lsx_vinsgr2vr_w(_v, __lsx_vpickve2gr_wu(_av, ((imm)>>4) & 0x3), 2); \
               _v = __lsx_vinsgr2vr_w(_v, __lsx_vpickve2gr_wu(_av, ((imm)>>6) & 0x3), 3); \
               _v; })

#endif

#endif //  OB_SSE_TO_NEON_H
