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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_inet.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "common/ob_target_specific.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace sql {
#if defined(__GNUC__) && defined(__x86_64__)
OB_DECLARE_SSE42_SPECIFIC_CODE(
  // 穷举了所有ipv4地址的数字和.的排列可能性，通过simd快速得到输入字符串的bitamp，通过查表法快速确定地址合法性，
  // 并根据排列类型，快速转换。
  // example: "192.168.1.1" => bitmask: 0000101010001000 => patterns:0,3,4,3,8,1,9,1
  // "192.168.1.1"
  // shuffle according to patterns => 0x0902060800010001, 0x0001000100000000
  // maddubs 每两个uint8_t的加权和 => 0x005c004400010001 0x0064006400000000
  // 移位相加 => 0x00c000a800010001
  inline static int str_to_ipv4_sse42(const char *str, int len, bool &is_ip_format_invalid, in_addr *ipv4addr)
  {
    int ret = OB_SUCCESS;
    static const uint8_t patterns_id[256] =
    {
      38,  65,  255, 56,  73,  255, 255, 255, 255, 255, 255, 3,   255, 255, 6,
      255, 255, 9,   255, 27,  255, 12,  30,  255, 255, 255, 255, 15,  255, 33,
      255, 255, 255, 255, 18,  36,  255, 255, 255, 54,  21,  255, 39,  255, 255,
      57,  255, 255, 255, 255, 255, 255, 255, 255, 24,  42,  255, 255, 255, 60,
      255, 255, 255, 255, 255, 255, 255, 255, 45,  255, 255, 63,  255, 255, 255,
      255, 255, 255, 255, 255, 255, 48,  53,  255, 255, 66,  71,  255, 255, 16,
      255, 34,  255, 255, 255, 255, 255, 255, 255, 52,  255, 255, 22,  70,  40,
      255, 255, 58,  51,  255, 255, 69,  255, 255, 255, 255, 255, 255, 255, 255,
      255, 5,   255, 255, 255, 255, 255, 255, 11,  29,  46,  255, 255, 64,  255,
      255, 72,  0,   77,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
      255, 255, 255, 255, 255, 76,  255, 255, 255, 255, 255, 255, 255, 75,  255,
      80,  255, 255, 255, 26,  255, 44,  255, 7,   62,  255, 255, 25,  255, 43,
      13,  31,  61,  255, 255, 255, 255, 255, 255, 255, 255, 255, 2,   19,  37,
      255, 255, 50,  55,  79,  68,  255, 255, 255, 255, 49,  255, 255, 67,  255,
      255, 255, 255, 17,  255, 35,  78,  255, 4,   255, 255, 255, 255, 255, 255,
      10,  23,  28,  41,  255, 255, 59,  255, 255, 255, 8,   255, 255, 255, 255,
      255, 1,   14,  32,  255, 255, 255, 255, 255, 255, 255, 255, 74,  255, 47,
      20,
    };

    static const uint8_t patterns[81][16] =
    {
      {0, 128, 2, 128, 4, 128, 6, 128, 128, 128, 128, 128, 128, 128, 128, 128},
      {0, 128, 2, 128, 4, 128, 7, 6, 128, 128, 128, 128, 128, 128, 128, 6},
      {0, 128, 2, 128, 4, 128, 8, 7, 128, 128, 128, 128, 128, 128, 6, 6},
      {0, 128, 2, 128, 5, 4, 7, 128, 128, 128, 128, 128, 128, 4, 128, 128},
      {0, 128, 2, 128, 5, 4, 8, 7, 128, 128, 128, 128, 128, 4, 128, 7},
      {0, 128, 2, 128, 5, 4, 9, 8, 128, 128, 128, 128, 128, 4, 7, 7},
      {0, 128, 2, 128, 6, 5, 8, 128, 128, 128, 128, 128, 4, 4, 128, 128},
      {0, 128, 2, 128, 6, 5, 9, 8, 128, 128, 128, 128, 4, 4, 128, 8},
      {0, 128, 2, 128, 6, 5, 10, 9, 128, 128, 128, 128, 4, 4, 8, 8},
      {0, 128, 3, 2, 5, 128, 7, 128, 128, 128, 128, 2, 128, 128, 128, 128},
      {0, 128, 3, 2, 5, 128, 8, 7, 128, 128, 128, 2, 128, 128, 128, 7},
      {0, 128, 3, 2, 5, 128, 9, 8, 128, 128, 128, 2, 128, 128, 7, 7},
      {0, 128, 3, 2, 6, 5, 8, 128, 128, 128, 128, 2, 128, 5, 128, 128},
      {0, 128, 3, 2, 6, 5, 9, 8, 128, 128, 128, 2, 128, 5, 128, 8},
      {0, 128, 3, 2, 6, 5, 10, 9, 128, 128, 128, 2, 128, 5, 8, 8},
      {0, 128, 3, 2, 7, 6, 9, 128, 128, 128, 128, 2, 5, 5, 128, 128},
      {0, 128, 3, 2, 7, 6, 10, 9, 128, 128, 128, 2, 5, 5, 128, 9},
      {0, 128, 3, 2, 7, 6, 11, 10, 128, 128, 128, 2, 5, 5, 9, 9},
      {0, 128, 4, 3, 6, 128, 8, 128, 128, 128, 2, 2, 128, 128, 128, 128},
      {0, 128, 4, 3, 6, 128, 9, 8, 128, 128, 2, 2, 128, 128, 128, 8},
      {0, 128, 4, 3, 6, 128, 10, 9, 128, 128, 2, 2, 128, 128, 8, 8},
      {0, 128, 4, 3, 7, 6, 9, 128, 128, 128, 2, 2, 128, 6, 128, 128},
      {0, 128, 4, 3, 7, 6, 10, 9, 128, 128, 2, 2, 128, 6, 128, 9},
      {0, 128, 4, 3, 7, 6, 11, 10, 128, 128, 2, 2, 128, 6, 9, 9},
      {0, 128, 4, 3, 8, 7, 10, 128, 128, 128, 2, 2, 6, 6, 128, 128},
      {0, 128, 4, 3, 8, 7, 11, 10, 128, 128, 2, 2, 6, 6, 128, 10},
      {0, 128, 4, 3, 8, 7, 12, 11, 128, 128, 2, 2, 6, 6, 10, 10},
      {1, 0, 3, 128, 5, 128, 7, 128, 128, 0, 128, 128, 128, 128, 128, 128},
      {1, 0, 3, 128, 5, 128, 8, 7, 128, 0, 128, 128, 128, 128, 128, 7},
      {1, 0, 3, 128, 5, 128, 9, 8, 128, 0, 128, 128, 128, 128, 7, 7},
      {1, 0, 3, 128, 6, 5, 8, 128, 128, 0, 128, 128, 128, 5, 128, 128},
      {1, 0, 3, 128, 6, 5, 9, 8, 128, 0, 128, 128, 128, 5, 128, 8},
      {1, 0, 3, 128, 6, 5, 10, 9, 128, 0, 128, 128, 128, 5, 8, 8},
      {1, 0, 3, 128, 7, 6, 9, 128, 128, 0, 128, 128, 5, 5, 128, 128},
      {1, 0, 3, 128, 7, 6, 10, 9, 128, 0, 128, 128, 5, 5, 128, 9},
      {1, 0, 3, 128, 7, 6, 11, 10, 128, 0, 128, 128, 5, 5, 9, 9},
      {1, 0, 4, 3, 6, 128, 8, 128, 128, 0, 128, 3, 128, 128, 128, 128},
      {1, 0, 4, 3, 6, 128, 9, 8, 128, 0, 128, 3, 128, 128, 128, 8},
      {1, 0, 4, 3, 6, 128, 10, 9, 128, 0, 128, 3, 128, 128, 8, 8},
      {1, 0, 4, 3, 7, 6, 9, 128, 128, 0, 128, 3, 128, 6, 128, 128},
      {1, 0, 4, 3, 7, 6, 10, 9, 128, 0, 128, 3, 128, 6, 128, 9},
      {1, 0, 4, 3, 7, 6, 11, 10, 128, 0, 128, 3, 128, 6, 9, 9},
      {1, 0, 4, 3, 8, 7, 10, 128, 128, 0, 128, 3, 6, 6, 128, 128},
      {1, 0, 4, 3, 8, 7, 11, 10, 128, 0, 128, 3, 6, 6, 128, 10},
      {1, 0, 4, 3, 8, 7, 12, 11, 128, 0, 128, 3, 6, 6, 10, 10},
      {1, 0, 5, 4, 7, 128, 9, 128, 128, 0, 3, 3, 128, 128, 128, 128},
      {1, 0, 5, 4, 7, 128, 10, 9, 128, 0, 3, 3, 128, 128, 128, 9},
      {1, 0, 5, 4, 7, 128, 11, 10, 128, 0, 3, 3, 128, 128, 9, 9},
      {1, 0, 5, 4, 8, 7, 10, 128, 128, 0, 3, 3, 128, 7, 128, 128},
      {1, 0, 5, 4, 8, 7, 11, 10, 128, 0, 3, 3, 128, 7, 128, 10},
      {1, 0, 5, 4, 8, 7, 12, 11, 128, 0, 3, 3, 128, 7, 10, 10},
      {1, 0, 5, 4, 9, 8, 11, 128, 128, 0, 3, 3, 7, 7, 128, 128},
      {1, 0, 5, 4, 9, 8, 12, 11, 128, 0, 3, 3, 7, 7, 128, 11},
      {1, 0, 5, 4, 9, 8, 13, 12, 128, 0, 3, 3, 7, 7, 11, 11},
      {2, 1, 4, 128, 6, 128, 8, 128, 0, 0, 128, 128, 128, 128, 128, 128},
      {2, 1, 4, 128, 6, 128, 9, 8, 0, 0, 128, 128, 128, 128, 128, 8},
      {2, 1, 4, 128, 6, 128, 10, 9, 0, 0, 128, 128, 128, 128, 8, 8},
      {2, 1, 4, 128, 7, 6, 9, 128, 0, 0, 128, 128, 128, 6, 128, 128},
      {2, 1, 4, 128, 7, 6, 10, 9, 0, 0, 128, 128, 128, 6, 128, 9},
      {2, 1, 4, 128, 7, 6, 11, 10, 0, 0, 128, 128, 128, 6, 9, 9},
      {2, 1, 4, 128, 8, 7, 10, 128, 0, 0, 128, 128, 6, 6, 128, 128},
      {2, 1, 4, 128, 8, 7, 11, 10, 0, 0, 128, 128, 6, 6, 128, 10},
      {2, 1, 4, 128, 8, 7, 12, 11, 0, 0, 128, 128, 6, 6, 10, 10},
      {2, 1, 5, 4, 7, 128, 9, 128, 0, 0, 128, 4, 128, 128, 128, 128},
      {2, 1, 5, 4, 7, 128, 10, 9, 0, 0, 128, 4, 128, 128, 128, 9},
      {2, 1, 5, 4, 7, 128, 11, 10, 0, 0, 128, 4, 128, 128, 9, 9},
      {2, 1, 5, 4, 8, 7, 10, 128, 0, 0, 128, 4, 128, 7, 128, 128},
      {2, 1, 5, 4, 8, 7, 11, 10, 0, 0, 128, 4, 128, 7, 128, 10},
      {2, 1, 5, 4, 8, 7, 12, 11, 0, 0, 128, 4, 128, 7, 10, 10},
      {2, 1, 5, 4, 9, 8, 11, 128, 0, 0, 128, 4, 7, 7, 128, 128},
      {2, 1, 5, 4, 9, 8, 12, 11, 0, 0, 128, 4, 7, 7, 128, 11},
      {2, 1, 5, 4, 9, 8, 13, 12, 0, 0, 128, 4, 7, 7, 11, 11},
      {2, 1, 6, 5, 8, 128, 10, 128, 0, 0, 4, 4, 128, 128, 128, 128},
      {2, 1, 6, 5, 8, 128, 11, 10, 0, 0, 4, 4, 128, 128, 128, 10},
      {2, 1, 6, 5, 8, 128, 12, 11, 0, 0, 4, 4, 128, 128, 10, 10},
      {2, 1, 6, 5, 9, 8, 11, 128, 0, 0, 4, 4, 128, 8, 128, 128},
      {2, 1, 6, 5, 9, 8, 12, 11, 0, 0, 4, 4, 128, 8, 128, 11},
      {2, 1, 6, 5, 9, 8, 13, 12, 0, 0, 4, 4, 128, 8, 11, 11},
      {2, 1, 6, 5, 10, 9, 12, 128, 0, 0, 4, 4, 8, 8, 128, 128},
      {2, 1, 6, 5, 10, 9, 13, 12, 0, 0, 4, 4, 8, 8, 128, 12},
      {2, 1, 6, 5, 10, 9, 14, 13, 0, 0, 4, 4, 8, 8, 12, 12},
    };
    if (len < 7 || len > INET_ADDRSTRLEN - 1) {
      is_ip_format_invalid = true;
    } else {
      char aligned_buf[16] __attribute__((aligned(16))) = {0};
      MEMCPY(aligned_buf, str, len);
      __m128i input_128 = _mm_load_si128(reinterpret_cast<const __m128i *>(aligned_buf));
      const __m128i dot = _mm_set1_epi8('.');
      const __m128i t0_cmp = _mm_cmpeq_epi8(input_128, dot);
      uint16_t dotmask = static_cast<uint16_t>(_mm_movemask_epi8(t0_cmp));
      uint16_t mask = static_cast<uint16_t>(1) << len;
      dotmask &= mask - 1;
      dotmask |= mask;
      const uint8_t hashcode = static_cast<uint8_t>((6639 * dotmask) >> 13);
      const uint8_t id = patterns_id[hashcode];

      if (id >= 81) {
        is_ip_format_invalid=true;
      } else {
        const uint8_t *pat = &patterns[id][0];
        const __m128i pattern = _mm_loadu_si128(reinterpret_cast<const __m128i *>(pat));
        __m128i t1 = _mm_shuffle_epi8(input_128, pattern);
        const __m128i ascii0 = _mm_set1_epi8('0');
        __m128i t1b = _mm_blendv_epi8(t1, ascii0, pattern);
        const __m128i t2 = _mm_sub_epi8(t1b, ascii0);
        const __m128i t2z = _mm_add_epi8(t2, _mm_set1_epi8(-128));
        const __m128i c9 = _mm_set1_epi8('9' - '0' - 128);
        const __m128i t2me = _mm_cmpgt_epi8(t2z, c9);
        if (!_mm_test_all_zeros(t2me, t2me)) {
          is_ip_format_invalid=true;
        } else {
          const __m128i weights = _mm_setr_epi8(1, 10, 1, 10, 1, 10, 1, 10, 100, 0, 100, 0, 100, 0, 100, 0);
          const __m128i t3 = _mm_maddubs_epi16(t2, weights);
          const __m128i t4 = _mm_alignr_epi8(t3, t3, 8);
          const __m128i t5 = _mm_add_epi16(t4, t3);
          if (!_mm_testz_si128(t5, _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0, -1, 0, -1, 0, -1, 0, -1, 0))) {
            is_ip_format_invalid=true;
          } else {
            const __m128i t6 = _mm_packus_epi16(t5, t5);
            ipv4addr->s_addr = static_cast<uint32_t>(_mm_cvtsi128_si32(t6));
          }
        }
      }
    }
    return ret;
  }
)
#endif

inline int ObExprInetCommon::str_to_ipv4_nosimd(int len, const char *str, bool &is_ip_format_invalid, in_addr *ipv4addr)
{
  int ret = OB_SUCCESS;
  if (7 > len || INET_ADDRSTRLEN - 1 < len) {
    is_ip_format_invalid = true;
  } else {
    unsigned char byte_addr[4] = {0};
    int dotcnt = 0, numcnt = 0;
    int byte = 0;
    char c;
    for (int i = 0; !is_ip_format_invalid && i < len && *(i + str); ++i) {
      c = *(i + str);
      if ('.' == c) {
        if (255 < byte) {
          is_ip_format_invalid = true;
        } else if (0 == numcnt || 3 < numcnt) {
          is_ip_format_invalid = true;
        } else if (i == len - 1 || !*(i + str + 1)) {
          is_ip_format_invalid = true;
        } else {
          byte_addr[dotcnt] = (unsigned char) byte;
        }
        dotcnt++;
        if (3 < dotcnt) {
          is_ip_format_invalid = true;
        } else {
        }
        dotcnt++;
        byte = 0;
        numcnt = 0;
      } else if (isdigit(c)) {
        byte = byte * 10 + c - '0';
        numcnt++;
      } else {
        is_ip_format_invalid = true;
      }
    }
    if (255 < byte || numcnt > 3) {
      is_ip_format_invalid = true;
    } else if (3 != dotcnt) {
      is_ip_format_invalid = true;
    } else if ('.' == c) { // IP number can't end on '.'
      is_ip_format_invalid = true;
    } else {
      byte_addr[3] = (unsigned char) byte;
    }
    MEMCPY((unsigned char *) ipv4addr, byte_addr, sizeof(in_addr));
  }
  return ret;
}

inline uint8_t ObExprInetCommon::unhex(char c)
{
  static const uint8_t hex_table[256] = {
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 0-15
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 16-31
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 32-47
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 48-63: '0'-'9'
    0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 64-79: 'A'-'F'
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 80-95
    0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 96-111: 'a'-'f'
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 112-127
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 128-143
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 144-159
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 160-175
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 176-191
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 192-207
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 208-223
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // 224-239
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff  // 240-255
  };
  return hex_table[static_cast<uint8_t>(c)];
}

inline int ObExprInetCommon::str_to_ipv6(int len, const char *str, bool &is_ip_format_invalid, in6_addr *ipv6addr)
{
  int ret = OB_SUCCESS;
  if (2 > len || INET6_ADDRSTRLEN - 1 < len) {
    is_ip_format_invalid = true;
  } else if ((str[0] == ':' && str[1] != ':') ||
             (str[len - 1] == ':' && str[len - 2] != ':')) {
    is_ip_format_invalid = true;
  } else {
    uint16_t *ipv6_ptr = (uint16_t *)ipv6addr;
    uint16_t *ipv6_ptr_end = (uint16_t *)ipv6addr + 8;
    const char *str_end = str + len;
    const char *c = str;
    uint16_t *double_colon_ptr = nullptr;
    uint8_t count_hex = 0;
    bool mixed = false;

    for (; !is_ip_format_invalid && c != str_end && *c; ++c) {
      uint8_t hex_val = unhex(*c);
      if (OB_LIKELY(hex_val != 0xff)) {
        if (count_hex < 4) {
          (*ipv6_ptr <<= 4) |= hex_val;
          ++count_hex;
        } else {
          is_ip_format_invalid = true;
        }
      } else if (*c == ':') {
        if (OB_LIKELY(ipv6_ptr - (uint16_t *)ipv6addr < 7) && count_hex > 0) {
          *ipv6_ptr = (*ipv6_ptr >> 8) | (*ipv6_ptr << 8);
          ++ipv6_ptr;
          count_hex = 0;
          if (*(c + 1) == ':') {
            if (double_colon_ptr == nullptr) {
              double_colon_ptr = ipv6_ptr;
              ++c;
            } else {
              is_ip_format_invalid = true;
            }
          }
        } else if (ipv6_ptr == (uint16_t *)ipv6addr && count_hex == 0 && *(c + 1) == ':') {
          *ipv6_ptr = 0;
          ++ipv6_ptr;
          double_colon_ptr = ipv6_ptr;
          ++c;
        } else {
          is_ip_format_invalid = true;
        }
      } else if (*c == '.' && (ipv6_ptr - (uint16_t *)ipv6addr < 7)) {
        ret = str_to_ipv4(len - (c - str) + count_hex, c - count_hex, is_ip_format_invalid, (in_addr *)ipv6_ptr);
        ipv6_ptr += 1;
        mixed = true;
        break;
      } else {
        is_ip_format_invalid = true;
      }
    }
    if (!is_ip_format_invalid && !mixed) {
      *ipv6_ptr = (*ipv6_ptr >> 8) | (*ipv6_ptr << 8);
    }
    if (!is_ip_format_invalid && double_colon_ptr == nullptr && ipv6_ptr != ((uint16_t *)ipv6addr + 7)) {
      is_ip_format_invalid = true;
    } else {
      if (double_colon_ptr != nullptr) {
        int move_len = ((uint16_t *)ipv6addr + 7) - ipv6_ptr;
        int len = ipv6_ptr - double_colon_ptr + 1;
        MEMMOVE(double_colon_ptr + move_len, double_colon_ptr, len << 1);
        MEMSET(double_colon_ptr, 0, move_len << 1);
      }
    }
  }
  return ret;
}

inline int ObExprInetCommon::str_to_ipv4(int len, const char *str, bool &is_ip_format_invalid, in_addr *ipv4addr)
{
  int ret = OB_SUCCESS;
  #if defined(__GNUC__) && defined(__x86_64__)
    if (common::is_arch_supported(ObTargetArch::SSE42)) {
      ret = specific::sse42::str_to_ipv4_sse42(str, len, is_ip_format_invalid, ipv4addr);
    } else {
      ret = ObExprInetCommon::str_to_ipv4_nosimd(len, str, is_ip_format_invalid, ipv4addr);
    }
  #else
    ret = ObExprInetCommon::str_to_ipv4_nosimd(len, str, is_ip_format_invalid, ipv4addr);
  #endif
  return ret;
}

inline int ObExprInetCommon::str_to_ip(int len, const char *str, bool &is_ip_format_invalid, in6_addr *ipv6addr, bool is_ipv6, bool &is_pure_ipv4)
{
  int ret = OB_SUCCESS;
  is_pure_ipv4 = false;
  if (len < 7) {
    ret = str_to_ipv6(len, str, is_ip_format_invalid, ipv6addr);
  } else {
    bool has_colon = str[1] == ':' || str[2] == ':' || str[3] == ':' || str[4] == ':';
    bool has_dot = str[len - 4] == '.' || str[len - 3] == '.' || str[len - 2] == '.';
    if (has_colon) {
      ret = str_to_ipv6(len, str, is_ip_format_invalid, ipv6addr);
    } else if (!is_ipv6 && has_dot) {
      ret = str_to_ipv4(len, str, is_ip_format_invalid, (in_addr *)((uint8_t *)ipv6addr + 12));
      is_pure_ipv4 = true;
    } else {
      is_ip_format_invalid = true;
    }
  }
  return ret;
}



inline int ObExprInetCommon::ipv4_to_str(char *ipv4_binary_ptr,  ObString &ip_str)
{
  static const char *str_table[256] = {
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "30", "31", "32", "33", "34", "35", "36", "37", "38", "39",
    "40", "41", "42", "43", "44", "45", "46", "47", "48", "49",
    "50", "51", "52", "53", "54", "55", "56", "57", "58", "59",
    "60", "61", "62", "63", "64", "65", "66", "67", "68", "69",
    "70", "71", "72", "73", "74", "75", "76", "77", "78", "79",
    "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99",
    "100", "101", "102", "103", "104", "105", "106", "107", "108", "109",
    "110", "111", "112", "113", "114", "115", "116", "117", "118", "119",
    "120", "121", "122", "123", "124", "125", "126", "127", "128", "129",
    "130", "131", "132", "133", "134", "135", "136", "137", "138", "139",
    "140", "141", "142", "143", "144", "145", "146", "147", "148", "149",
    "150", "151", "152", "153", "154", "155", "156", "157", "158", "159",
    "160", "161", "162", "163", "164", "165", "166", "167", "168", "169",
    "170", "171", "172", "173", "174", "175", "176", "177", "178", "179",
    "180", "181", "182", "183", "184", "185", "186", "187", "188", "189",
    "190", "191", "192", "193", "194", "195", "196", "197", "198", "199",
    "200", "201", "202", "203", "204", "205", "206", "207", "208", "209",
    "210", "211", "212", "213", "214", "215", "216", "217", "218", "219",
    "220", "221", "222", "223", "224", "225", "226", "227", "228", "229",
    "230", "231", "232", "233", "234", "235", "236", "237", "238", "239",
    "240", "241", "242", "243", "244", "245", "246", "247", "248", "249",
    "250", "251", "252", "253", "254", "255"
  };

  const uint8_t* bytes = (const uint8_t *)ipv4_binary_ptr;
  char* ip_str_ptr = ip_str.ptr()+ip_str.length();

  #define APPEND_BYTE_STR(byte_val) \
    if ((byte_val) > 99) { \
      *ip_str_ptr++ = str_table[(byte_val)][0]; \
      *ip_str_ptr++ = str_table[(byte_val)][1]; \
      *ip_str_ptr++ = str_table[(byte_val)][2]; \
    } else if ((byte_val) > 9) { \
      *ip_str_ptr++ = str_table[(byte_val)][0]; \
      *ip_str_ptr++ = str_table[(byte_val)][1]; \
    } else { \
      *ip_str_ptr++ = str_table[(byte_val)][0]; \
    }

  APPEND_BYTE_STR(bytes[0]);
  *ip_str_ptr++ = '.';
  APPEND_BYTE_STR(bytes[1]);
  *ip_str_ptr++ = '.';
  APPEND_BYTE_STR(bytes[2]);
  *ip_str_ptr++ = '.';
  APPEND_BYTE_STR(bytes[3]);

  ip_str.set_length(static_cast<ObString::obstr_size_t>(ip_str_ptr - ip_str.ptr()));
  return OB_SUCCESS;
}

inline int ObExprInetCommon::ipv6_to_str(char *ipv6_binary_ptr, ObString &ip_str)
{
  static const char str_table[16] = {
    '0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };
  int gap = -1, gap_len = -1;
  int rg = -1, rg_len = -1;
  const uint16_t *ipv6_ptr = (const uint16_t *) ipv6_binary_ptr;
  //find longest gap
  for (int i = 0; i < 8; ++i) {
    if (0 == *(ipv6_ptr + i)) {
      if (-1 == rg_len) {
        rg_len = 1;
        rg = i;
      } else {
        rg_len++;
      }
    } else {
      if (rg_len > gap_len) {
        gap = rg;
        gap_len = rg_len;
      } else {
      }
      rg_len = -1;
    }
  }
  if (rg_len > gap_len) {
    gap = rg;
    gap_len = rg_len;
  } else {
  }

  char *p = ip_str.ptr();
  for (int i = 0; i < 8; ++i) {
    if (gap == i) {
      if (0 == i) {
        *p = ':';
        ++p;
      } else {
      }
      *p = ':';
      ++p;
      i += gap_len - 1;  //skip gap
    } else {
      // Convert each 16-bit group to hex
      if (*(ipv6_ptr + i) == 0) {
        *p++ = '0';
      } else {
        // Convert to hex, skip leading zeros
        if ((*(ipv6_ptr + i) >> 4 & 0x0f) != 0) {
          *p++ = str_table[*(ipv6_ptr + i) >> 4 & 0x0f];
          *p++ = str_table[*(ipv6_ptr + i) & 0x0f];
          *p++ = str_table[*(ipv6_ptr + i) >> 12];
          *p++ = str_table[*(ipv6_ptr + i) >> 8 & 0x0f];
        } else if ((*(ipv6_ptr + i) & 0x0f) != 0) {
          *p++ = str_table[*(ipv6_ptr + i) & 0x0f];
          *p++ = str_table[*(ipv6_ptr + i) >> 12];
          *p++ = str_table[*(ipv6_ptr + i) >> 8 & 0x0f];
        } else if (*(ipv6_ptr + i) >> 12 != 0) {
          *p++ = str_table[*(ipv6_ptr + i) >> 12];
          *p++ = str_table[*(ipv6_ptr + i) >> 8 & 0x0f];
        } else {
          *p++ = str_table[*(ipv6_ptr + i) >> 8 & 0x0f];
        }
      }
      if (i != 7) {
        *p = ':';
        ++p;
      }
    }
  }
  ip_str.set_length(static_cast<ObString::obstr_size_t>(p - ip_str.ptr()));
  return OB_SUCCESS;
}

inline int ObExprInetCommon::ip_to_str(ObString &ip_binary, bool &is_ip_format_invalid, ObString &ip_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(ip_binary.ptr()) || OB_ISNULL(ip_str.ptr()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ip or result is null", K(ret), K(ip_binary.ptr()), K(ip_str.ptr()));
  } else if (sizeof(in_addr) == ip_binary.length()) {
    ipv4_to_str(ip_binary.ptr(), ip_str);
  } else if (sizeof(in6_addr) == ip_binary.length()) {
    const uint32_t* ip_uint32_ptr = (const uint32_t *)ip_binary.ptr();
    if (ip_uint32_ptr[0] == 0 && ip_uint32_ptr[1] == 0 && ip_uint32_ptr[2] == 0 && (*((uint16_t *)ip_uint32_ptr+6) > 0x0000)) {
      ip_str.write("::", 2);
      ipv4_to_str(ip_binary.ptr() + 12, ip_str);
    } else if (ip_uint32_ptr[0] == 0 && ip_uint32_ptr[1] == 0 && ip_uint32_ptr[2] == 0xFFFF0000) {
      ip_str.write("::ffff:", 7);
      ipv4_to_str(ip_binary.ptr() + 12, ip_str);
    } else {
      ipv6_to_str(ip_binary.ptr(), ip_str);
    }
  } else {
    is_ip_format_invalid = true;
  }
  return ret;
}

ObExprInetAton::ObExprInetAton(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_INETATON, N_INETATON, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprInetAton::~ObExprInetAton()
{}

template <typename T>
int ObExprInetAton::ob_inet_aton(T& result, const ObString& text, bool& is_ip_format_invalid)
{
  int ret = OB_SUCCESS;
  is_ip_format_invalid = false;
  if (text.empty()) {
    is_ip_format_invalid = true;
    LOG_WARN("ip length is zero");
  } else {
    int len = text.length();
    char c;
    int cnt = 0;
    unsigned long value = 0;
    int byte = 0;
    for (int i = 0; !is_ip_format_invalid && i < len; ++i) {
      c = text.ptr()[i];
      if ('0' <= c && '9' >= c) {
        byte = byte * 10 + c - '0';
        if (255 < byte) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid", K(byte), K(text));
        } else {
        }
      } else if ('.' == c) {
        cnt++;
        value = (value << 8) + byte;
        byte = 0;
      } else {
        is_ip_format_invalid = true;
        LOG_WARN("ip format invalid", K(c), K(text));
      }
    }
    if (is_ip_format_invalid) {
    } else if (3 < cnt || '.' == c) { // IP number can't end on '.', '.' count <= 3
      is_ip_format_invalid = true;
      LOG_WARN("ip format invalid", K(text), K(cnt), K(c));
    } else {
      /*
      Attempt to support short forms of IP-addresses.
      Examples:
        127     -> 0.0.0.127
        127.255 -> 127.0.0.255
        127.256 -> NULL
        127.2.1 -> 127.2.0.1
      */
      if (2 == cnt) {
        value <<= 8;
      } else if (1 == cnt) {
        value <<= 16;
      } else {
      }
      value = (value << 8) + byte;
      result.set_int(value);
    }
  }
  return ret;
}

int ObExprInetAton::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (rt_expr.arg_cnt_ != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inet_aton expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of inet_aton expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprInetAton::calc_inet_aton;
  }
  return ret;
}

int ObExprInetAton::calc_inet_aton(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("inet_aton expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    if (text.is_null()) {
      expr_datum.set_null();
    } else {
      ObString m_text = text.get_string();
      bool is_ip_format_invalid = false;
      if (OB_FAIL(ob_inet_aton(expr_datum, m_text, is_ip_format_invalid))) {
        LOG_WARN("fail to execute ob_inet_aton", K(ret));
      } else if (is_ip_format_invalid) {
        uint64_t cast_mode = 0;
        ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
        ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
        ObSQLMode sql_mode = 0;
        if (OB_UNLIKELY(OB_ISNULL(session))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session is NULL", K(ret));
        } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
          LOG_WARN("get sql mode failed", K(ret));
        } else {
          ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                            session->is_ignore_stmt(),
                                            sql_mode,
                                            cast_mode);
          if (CM_IS_WARN_ON_FAIL(cast_mode)) { //support no strict sql_mode
            LOG_USER_WARN(OB_ERR_INCORRECT_STRING_VALUE_FOR_INET, "inet_aton");
            expr_datum.set_null();
          } else {
            ret = OB_ERR_INCORRECT_STRING_VALUE_FOR_INET;
            LOG_USER_ERROR(OB_ERR_INCORRECT_STRING_VALUE_FOR_INET, "inet_aton");
            LOG_WARN("fail to convert ip to int", K(ret), K(m_text));
          }
        }
      }
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInetAton, raw_expr) {
  int ret = OB_SUCCESS;
  if (is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  }
  return ret;
}

ObExprInet6Ntoa::ObExprInet6Ntoa(ObIAllocator& alloc) : ObStringExprOperator(alloc, T_FUN_SYS_INET6NTOA, N_INET6NTOA, 1, VALID_FOR_GENERATED_COL)
{
}

ObExprInet6Ntoa::~ObExprInet6Ntoa()
{}

inline int ObExprInet6Ntoa::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  UNUSED(text);
  type.set_varchar();
  type.set_length(common::MAX_IP_ADDR_LENGTH);
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  const sql::ObSQLSessionInfo *session = type_ctx.get_session();
  if (OB_UNLIKELY(OB_ISNULL(session))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null",K(ret));
  } else {
    type.set_collation_type(get_default_collation_type(type.get_type(), type_ctx));
  }
  return ret;
}

int ObExprInet6Ntoa::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inet6_ntoa expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of inet6_ntoa expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprInet6Ntoa::calc_inet6_ntoa;
    rt_expr.eval_vector_func_ = ObExprInet6Ntoa::calc_inet6_ntoa_vector;
  }
  return ret;
}

int ObExprInet6Ntoa::calc_inet6_ntoa(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("inet6_ntoa expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    if (text.is_null()) {
      expr_datum.set_null();
    } else {
      char * buf = NULL;
      CK(expr.res_buf_len_ >= MAX_IP_ADDR_LENGTH);
      if (OB_FAIL(ret)) {
        LOG_WARN("result buf size greater than MAX_IP_ADDR_LENGTH", K(ret));
      } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, MAX_IP_ADDR_LENGTH))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(MAX_IP_ADDR_LENGTH));
      } else {
        bool is_ip_format_invalid = false;
        ObString num_val;
        ObString ip_str(MAX_IP_ADDR_LENGTH, 0, buf);
        if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, &text, num_val))) {
          LOG_WARN("fail to get string from text", K(ret));
        } else if ((!ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)
                    && !ob_is_binary(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)
                    && !ob_is_blob(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_))
                   || num_val.length() == 0) {
          is_ip_format_invalid = true;
          LOG_WARN("ip format invalid", K(ret), K(text));
        } else if (OB_FAIL(ObExprInetCommon::ip_to_str(num_val, is_ip_format_invalid, ip_str))) {
          LOG_WARN("fail to execute ip_to_str", K(ret));
        } else if (!is_ip_format_invalid) {
          expr_datum.set_string(ip_str);
        }

        if (OB_SUCC(ret) && is_ip_format_invalid) {
          uint64_t cast_mode = 0;
          ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
          ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
          ObSQLMode sql_mode = 0;
          if (OB_UNLIKELY(OB_ISNULL(session))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session is NULL", K(ret));
          } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
            LOG_WARN("get sql mode failed", K(ret));
          } else {
            ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                              session->is_ignore_stmt(),
                                              sql_mode,
                                              cast_mode);
            if (CM_IS_WARN_ON_FAIL(cast_mode)) {
              expr_datum.set_null(); //support no strict sql_mode
            } else {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("ip format invalid", K(ret),K(text));
            }
          }
        }
      }
    }
  }
  return ret;
}


inline int ObExprInet6Ntoa::inet6_ntoa_vector_inner(const ObExpr &expr,
                                                    const ObString &num_val,
                                                    ObString &ip_str,
                                                    bool &is_ip_format_invalid,
                                                    ObEvalCtx &ctx,
                                                    int64_t idx)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  if (num_val.length() == 0) {
    is_ip_format_invalid = true;
  } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, MAX_IP_ADDR_LENGTH, idx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for inet6_ntoa result", K(ret), K(MAX_IP_ADDR_LENGTH));
  } else {
    MEMSET(buf, 0, MAX_IP_ADDR_LENGTH);
    ip_str = ObString(MAX_IP_ADDR_LENGTH, 0, buf);
    ObString num_val_non_const = const_cast<ObString &>(num_val);
    if (OB_FAIL(ObExprInetCommon::ip_to_str(num_val_non_const, is_ip_format_invalid, ip_str))) {
      LOG_WARN("fail to execute ip_to_str", K(ret));
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprInet6Ntoa::inet6_ntoa_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  uint64_t cast_mode = 0;
  ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, session);
  ObSQLMode sql_mode = 0;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode,
                                      cast_mode);
  }

  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();
  const bool is_valid_type = ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)
                            || ob_is_binary(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)
                            || ob_is_blob(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_);

  if (!is_valid_type) {
    for (int64_t idx = bound.start(); idx < bound.end(); ++idx) {
      res_vec->set_null(idx);
    }
  } else if (no_skip_no_null) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx)) {
        continue;
      } else {
        ObString num_val;
        bool is_ip_format_invalid = false;
        ObString ip_str;
        if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, idx, arg_vec, num_val))) {
          LOG_WARN("fail to get string from vector", K(ret));
        } else if (OB_FAIL(inet6_ntoa_vector_inner(expr, num_val, ip_str, is_ip_format_invalid, ctx, idx))) {
          LOG_WARN("fail to execute inet6_ntoa_vector_inner", K(ret));
        } else if (is_ip_format_invalid) {
          if (CM_IS_WARN_ON_FAIL(cast_mode)) {
            res_vec->set_null(idx);
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("ip format invalid", K(ret), K(num_val));
          }
        } else {
          res_vec->set_string(idx, ip_str);
        }
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx) || skip.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
        continue;
      } else {
        ObString num_val;
        bool is_ip_format_invalid = false;
        ObString ip_str;
        if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, idx, arg_vec, num_val))) {
          LOG_WARN("fail to get string from vector", K(ret));
        } else if (OB_FAIL(inet6_ntoa_vector_inner(expr, num_val, ip_str, is_ip_format_invalid, ctx, idx))) {
            LOG_WARN("fail to execute inet6_ntoa_vector_inner", K(ret));
        } else if (is_ip_format_invalid) {
          if (CM_IS_WARN_ON_FAIL(cast_mode)) {
            res_vec->set_null(idx);
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("ip format invalid", K(ret), K(num_val));
          }
        } else {
          res_vec->set_string(idx, ip_str);
        }
      }
    }
  }
  return ret;
}

int ObExprInet6Ntoa::calc_inet6_ntoa_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval inet6_ntoa param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = inet6_ntoa_vector<StrContVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = inet6_ntoa_vector<StrContVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = inet6_ntoa_vector<StrDiscVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = inet6_ntoa_vector<StrDiscVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = inet6_ntoa_vector<StrUniVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = inet6_ntoa_vector<StrUniVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = inet6_ntoa_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInet6Ntoa, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(2);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_COLLATION_CONNECTION);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  }
  return ret;
}

ObExprInet6Aton::ObExprInet6Aton(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_INET6ATON, N_INET6ATON, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprInet6Aton::~ObExprInet6Aton()
{}

int ObExprInet6Aton::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inet6_aton expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of inet6_aton expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprInet6Aton::calc_inet6_aton;
    rt_expr.eval_vector_func_ = ObExprInet6Aton::calc_inet6_aton_vector;
  }
  return ret;
}

int ObExprInet6Aton::calc_inet6_aton(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("inet6_ntoa expr eval param value failed", K(ret));
  } else {
    char * buf = NULL;
    CK(expr.res_buf_len_ >= sizeof(in6_addr));
    if (OB_FAIL(ret)) {
      LOG_WARN("result buf size greater than sizeof(in6_addr)", K(ret));
    } else if (OB_ISNULL(buf = expr.get_str_res_mem(ctx, sizeof(in6_addr)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for lob locator", K(ret), K(sizeof(in6_addr)));
    } else {
      ObDatum& text = expr.locate_param_datum(ctx, 0);
      ObString m_text = text.get_string();
      bool is_ip_format_invalid = false;
      if (text.is_null()) {
        expr_datum.set_null();
      } else {
        ObString str_result(sizeof(in6_addr), 0, buf);
        MEMSET(str_result.ptr(), 0, sizeof(in6_addr));
        if (OB_FAIL(inet6_aton(m_text, is_ip_format_invalid, str_result))) {
          LOG_WARN("fail to execute inet6_aton", K(ret));
        } else if (is_ip_format_invalid) {
          uint64_t cast_mode = 0;
          ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
          ObSolidifiedVarsGetter helper(expr, ctx, ctx.exec_ctx_.get_my_session());
          ObSQLMode sql_mode = 0;
          if (OB_ISNULL(session)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("session is NULL", K(ret));
          } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
            LOG_WARN("get sql mode failed", K(ret));
          } else {
            ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                              session->is_ignore_stmt(),
                                              sql_mode, cast_mode);
            if (CM_IS_WARN_ON_FAIL(cast_mode)) {
              // ignore ret
              LOG_USER_WARN(OB_ERR_INCORRECT_STRING_VALUE_FOR_INET, "inet6_aton");
              expr_datum.set_null(); //support no strict mode
            } else {
              ret = OB_ERR_INCORRECT_STRING_VALUE_FOR_INET;
              LOG_USER_ERROR(OB_ERR_INCORRECT_STRING_VALUE_FOR_INET, "inet6_aton");
              LOG_WARN("ip format invalid", K(ret));
            }
          }
        } else {
          expr_datum.set_string(str_result);
        }
      }
    }
  }
  return ret;
}


inline int ObExprInet6Aton::inet6_aton_vector_inner(const ObExpr &expr,
                                                    const ObString &ip_str,
                                                    ObString &str_result,
                                                    bool &is_ip_format_invalid,
                                                    int64_t idx,
                                                    ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  bool is_pure_ipv4 = false;
  char *result_buf = nullptr;
  if (OB_ISNULL(result_buf = expr.get_str_res_mem(ctx, sizeof(in6_addr), idx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Failed to allocate memory for inet6_aton result", K(ret), K(sizeof(in6_addr)));
  } else {
    MEMSET(result_buf, 0, sizeof(in6_addr));
    if (OB_FAIL(ObExprInetCommon::str_to_ip(ip_str.length(), ip_str.ptr(), is_ip_format_invalid,
                                            (in6_addr *)result_buf, false, is_pure_ipv4))) {
      LOG_WARN("fail to execute str_to_ip", K(ret));
    } else if (is_ip_format_invalid) {
      // pass
    } else if (is_pure_ipv4) {
      str_result = ObString(sizeof(in_addr), (char *)((uint8_t *)result_buf + 12));
    } else {
      str_result = ObString(sizeof(in6_addr), result_buf);
    }
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprInet6Aton::inet6_aton_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);

  uint64_t cast_mode = 0;
  ObSQLSessionInfo* session = ctx.exec_ctx_.get_my_session();
  ObSolidifiedVarsGetter helper(expr, ctx, session);
  ObSQLMode sql_mode = 0;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is NULL", K(ret));
  } else if (OB_FAIL(helper.get_sql_mode(sql_mode))) {
    LOG_WARN("get sql mode failed", K(ret));
  } else {
    ObSQLUtils::get_default_cast_mode(session->get_stmt_type(),
                                      session->is_ignore_stmt(),
                                      sql_mode,
                                      cast_mode);
  }

  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();

  if (no_skip_no_null) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx)) {
        continue;
      } else {
        ObString ip_str = arg_vec->get_string(idx);
        bool is_ip_format_invalid = false;
        ObString str_result;

        if (OB_FAIL(inet6_aton_vector_inner(expr, ip_str, str_result, is_ip_format_invalid, idx, ctx))) {
          LOG_WARN("fail to execute inet6_aton_vector_inner", K(ret));
        } else if (is_ip_format_invalid) {
          if (CM_IS_WARN_ON_FAIL(cast_mode)) {
            res_vec->set_null(idx);
            LOG_USER_WARN(OB_ERR_INCORRECT_STRING_VALUE_FOR_INET, "inet6_aton");
          } else {
            ret = OB_ERR_INCORRECT_STRING_VALUE_FOR_INET;
            LOG_USER_ERROR(OB_ERR_INCORRECT_STRING_VALUE_FOR_INET, "inet6_aton");
            LOG_WARN("ip format invalid", K(ret));
          }
        } else {
          res_vec->set_string(idx, str_result);
        }
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx) || skip.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_null(idx);
        continue;
      } else {
        ObString ip_str = arg_vec->get_string(idx);
        bool is_ip_format_invalid = false;
        ObString str_result;

        if (OB_FAIL(inet6_aton_vector_inner(expr, ip_str, str_result, is_ip_format_invalid, idx, ctx))) {
          LOG_WARN("fail to execute inet6_aton_vector_inner", K(ret));
        } else if (is_ip_format_invalid) {
          if (CM_IS_WARN_ON_FAIL(cast_mode)) {
            res_vec->set_null(idx);
            LOG_USER_WARN(OB_ERR_INCORRECT_STRING_VALUE_FOR_INET, "inet6_aton");
          } else {
            ret = OB_ERR_INCORRECT_STRING_VALUE_FOR_INET;
            LOG_USER_ERROR(OB_ERR_INCORRECT_STRING_VALUE_FOR_INET, "inet6_aton");
            LOG_WARN("ip format invalid", K(ret));
          }
        } else {
          res_vec->set_string(idx, str_result);
        }
      }
    }
  }
  return ret;
}

int ObExprInet6Aton::calc_inet6_aton_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval inet6_aton param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = inet6_aton_vector<StrContVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_DISCRETE == res_format) {
      ret = inet6_aton_vector<StrContVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = inet6_aton_vector<StrDiscVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_DISCRETE == res_format) {
      ret = inet6_aton_vector<StrDiscVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = inet6_aton_vector<StrUniVec, StrUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_DISCRETE == res_format) {
      ret = inet6_aton_vector<StrUniVec, StrDiscVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = inet6_aton_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

int ObExprInet6Aton::inet6_aton(const ObString &ip, bool &is_ip_format_invalid, ObString &str_result)
{
  int ret = OB_SUCCESS;
  bool is_pure_ipv4 = false;
  char *result_buf = str_result.ptr();
  if (OB_UNLIKELY(OB_ISNULL(result_buf))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("result_buf is null", K(ret));
  } else if (OB_FAIL(ObExprInetCommon::str_to_ip(ip.length(), ip.ptr(), is_ip_format_invalid, (in6_addr *)result_buf, false, is_pure_ipv4))) {
    LOG_WARN("fail to execute str_to_ip", K(ret));
  } else if (is_ip_format_invalid) {
    // pass
  } else if (is_pure_ipv4) {
    str_result.assign((char *)((uint8_t *)result_buf + 12), static_cast<int32_t>(sizeof(in_addr)));
  } else {
    str_result.assign(result_buf, static_cast<int32_t>(sizeof(in6_addr)));
  }
  return ret;
}

DEF_SET_LOCAL_SESSION_VARS(ObExprInet6Aton, raw_expr) {
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    SET_LOCAL_SYSVAR_CAPACITY(1);
    EXPR_ADD_LOCAL_SYSVAR(share::SYS_VAR_SQL_MODE);
  }
  return ret;
}

ObExprIsIpv4::ObExprIsIpv4(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4, N_IS_IPV4, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsIpv4::~ObExprIsIpv4()
{}

template <typename T>
int ObExprIsIpv4::is_ipv4(T& result, const ObString& text)
{
  int ret = OB_SUCCESS;
  struct in_addr addr;
  bool is_ip_invalid = false;
  if (OB_UNLIKELY(text.empty())) {
    is_ip_invalid = true;
  } else if (OB_FAIL(ObExprInetCommon::str_to_ipv4(text.length(), text.ptr(), is_ip_invalid, &addr))) {
    LOG_WARN("fail to execute str_to_ipv4", K(ret));
  } else {
    result.set_int(is_ip_invalid ? 0 : 1);
  }
  return ret;
}

template<typename ResVec>
inline int ObExprIsIpv4::is_ipv4_vector_inner(const ObString &str_val, ResVec *res_vec, int64_t idx)
{
  int ret = OB_SUCCESS;
  struct in_addr addr;
  bool is_ip_invalid = false;
  if (OB_UNLIKELY(str_val.empty())) {
    res_vec->set_int(idx, 0ULL);
  } else if (OB_FAIL(ObExprInetCommon::str_to_ipv4(str_val.length(), str_val.ptr(), is_ip_invalid, &addr))) {
    LOG_WARN("fail to execute str_to_ipv4", K(ret));
  } else {
    res_vec->set_int(idx, is_ip_invalid ? 0ULL : 1ULL);
  }
  return ret;
}


template <typename ArgVec, typename ResVec>
int ObExprIsIpv4::is_ipv4_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();

  if (no_skip_no_null) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx)) {
        continue;
      } else {
        ObString text_val = arg_vec->get_string(idx);
        if (OB_FAIL(is_ipv4_vector_inner<ResVec>(text_val, res_vec, idx))) {
          LOG_WARN("fail to execute is_ipv4_vector_inner", K(ret));
        }
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx)||skip.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_int(idx, 0);
        continue;
      } else {
        ObString text_val = arg_vec->get_string(idx);
        if (OB_FAIL(is_ipv4_vector_inner<ResVec>(text_val, res_vec, idx))) {
          LOG_WARN("fail to execute is_ipv4_vector_inner", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprIsIpv4::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4 expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of is_ipv4 expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprIsIpv4::calc_is_ipv4;
    rt_expr.eval_vector_func_ = ObExprIsIpv4::calc_is_ipv4_vector;
  }
  return ret;
}

int ObExprIsIpv4::calc_is_ipv4(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("is_ipv4 expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    ObString m_text = text.get_string();
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else if (OB_FAIL(is_ipv4(expr_datum, m_text))) {
      LOG_WARN("fail to execute is_ipv4", K(ret));
    } else {
    }
  }
  return ret;
}

int ObExprIsIpv4::calc_is_ipv4_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval is_ipv4 param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_vector<StrContVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_vector<StrContVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_vector<StrDiscVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_vector<StrDiscVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_vector<StrUniVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_vector<StrUniVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = is_ipv4_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

ObExprIsIpv4Mapped::ObExprIsIpv4Mapped(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4_MAPPED, N_IS_IPV4_MAPPED, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsIpv4Mapped::~ObExprIsIpv4Mapped()
{}

int ObExprIsIpv4Mapped::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4_mapped expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of is_ipv4_mapped expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprIsIpv4Mapped::calc_is_ipv4_mapped;
    rt_expr.eval_vector_func_ = ObExprIsIpv4Mapped::calc_is_ipv4_mapped_vector;
  }
  return ret;
}

int ObExprIsIpv4Mapped::calc_is_ipv4_mapped(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("is_ipv4_mapped expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    ObString m_text;
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else if (!ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)
               && !ob_is_binary(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)
               && !ob_is_blob(expr.args_[0]->datum_meta_.type_,expr.args_[0]->datum_meta_.cs_type_)) {
      expr_datum.set_int(0);
    } else if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, &text, m_text))) {
      LOG_WARN("fail to get string from text", K(ret));
    } else {
      is_ipv4_mapped(expr_datum, m_text);
    }
  }
  return ret;
}

template <typename T>
inline void ObExprIsIpv4Mapped::is_ipv4_mapped(T& result, const ObString& num_val)
{
  int ipv4_ret = 1;
  if (sizeof(in6_addr) != num_val.length()) { //length of binary ipv6 addr is 16
    ipv4_ret = 0;
  } else {
    ipv4_ret = IN6_IS_ADDR_V4MAPPED((struct in6_addr *) num_val.ptr());
  }
  result.set_int(ipv4_ret);
}

template<typename ResVec>
inline int ObExprIsIpv4Mapped::is_ipv4_mapped_vector_inner(const ObString &num_val, ResVec *res_vec, int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(num_val.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num_val is null", K(ret));
  } else if (sizeof(in6_addr) != num_val.length()) {
    res_vec->set_int(idx, 0);
  } else {
    res_vec->set_int(idx, IN6_IS_ADDR_V4MAPPED((struct in6_addr *) num_val.ptr()));
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprIsIpv4Mapped::is_ipv4_mapped_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();
  const bool is_valid_type = ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)
                             || ob_is_binary(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)
                             || ob_is_blob(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_);
  ObString num_val;
  if (!is_valid_type) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      res_vec->set_int(idx, 0);
    }
  } else if (no_skip_no_null) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx)) {
        continue;
      } else if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, idx, arg_vec, num_val))) {
        LOG_WARN("fail to get string from vector", K(ret));
      } else if (OB_FAIL(is_ipv4_mapped_vector_inner<ResVec>(num_val, res_vec, idx))) {
        LOG_WARN("fail to execute is_ipv4_mapped_vector_inner", K(ret));
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx) || skip.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_int(idx, 0);
        continue;
      } else if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, idx, arg_vec, num_val))) {
        LOG_WARN("fail to get string from vector", K(ret));
      } else if (OB_FAIL(is_ipv4_mapped_vector_inner<ResVec>(num_val, res_vec, idx))) {
        LOG_WARN("fail to execute is_ipv4_mapped_vector_inner", K(ret));
      }
    }
  }
  return ret;
}

int ObExprIsIpv4Mapped::calc_is_ipv4_mapped_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval is_ipv4_mapped param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_mapped_vector<StrContVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_mapped_vector<StrContVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_mapped_vector<StrDiscVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_mapped_vector<StrDiscVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_mapped_vector<StrUniVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_mapped_vector<StrUniVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = is_ipv4_mapped_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

ObExprIsIpv4Compat::ObExprIsIpv4Compat(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV4_COMPAT, N_IS_IPV4_COMPAT, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsIpv4Compat::~ObExprIsIpv4Compat()
{}

int ObExprIsIpv4Compat::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv4_compat expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of is_ipv4_compat expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprIsIpv4Compat::calc_is_ipv4_compat;
    rt_expr.eval_vector_func_ = ObExprIsIpv4Compat::calc_is_ipv4_compat_vector;
  }
  return ret;
}

int ObExprIsIpv4Compat::calc_is_ipv4_compat(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("is_ipv4_compat expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    ObString m_text;
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else if (!ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)
               && !ob_is_binary(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)
               && !ob_is_blob(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)) {
      expr_datum.set_int(0);
    } else if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, &text, m_text))) {
      LOG_WARN("fail to get string from text", K(ret));
    } else {
      is_ipv4_compat(expr_datum, m_text);
    }
  }
  return ret;
}

template <typename T>
void ObExprIsIpv4Compat::is_ipv4_compat(T& result, const ObString& num_val)
{
  int ipv4_ret = 1;
  if (sizeof(in6_addr) != num_val.length()) { //length of binary ipv6 addr is 16
    ipv4_ret = 0;
  } else {
    ipv4_ret = IN6_IS_ADDR_V4COMPAT((struct in6_addr *) num_val.ptr());
  }
  result.set_int(ipv4_ret);
}

template<typename ResVec>
inline int ObExprIsIpv4Compat::is_ipv4_compat_vector_inner(const ObString &num_val, ResVec *res_vec, int64_t idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(num_val.ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num_val is null", K(ret));
  } else if (sizeof(in6_addr) != num_val.length()) {
    res_vec->set_int(idx, 0);
  } else {
    res_vec->set_int(idx, IN6_IS_ADDR_V4COMPAT((struct in6_addr *) num_val.ptr()));
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprIsIpv4Compat::is_ipv4_compat_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();
  const bool is_valid_type = ob_is_varbinary_type(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)
                             || ob_is_binary(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_)
                             || ob_is_blob(expr.args_[0]->datum_meta_.type_, expr.args_[0]->datum_meta_.cs_type_);
  ObString num_val;
  if (!is_valid_type) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      res_vec->set_int(idx, 0);
    }
  } else if (no_skip_no_null) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx)) {
        continue;
      } else if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, idx, arg_vec, num_val))) {
        LOG_WARN("fail to get string from vector", K(ret));
      } else if (OB_FAIL(is_ipv4_compat_vector_inner<ResVec>(num_val, res_vec, idx))) {
        LOG_WARN("fail to execute is_ipv4_compat_vector_inner", K(ret));
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx) || skip.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_int(idx, 0);
        continue;
      } else if (OB_FAIL(ObTextStringHelper::get_string(expr, ctx.tmp_alloc_, 0, idx, arg_vec, num_val))) {
        LOG_WARN("fail to get string from vector", K(ret));
      } else if (OB_FAIL(is_ipv4_compat_vector_inner<ResVec>(num_val, res_vec, idx))) {
        LOG_WARN("fail to execute is_ipv4_compat_vector_inner", K(ret));
      }
    }
  }
  return ret;
}

int ObExprIsIpv4Compat::calc_is_ipv4_compat_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval is_ipv4_compat param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_compat_vector<StrContVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_compat_vector<StrContVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_compat_vector<StrDiscVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_compat_vector<StrDiscVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv4_compat_vector<StrUniVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv4_compat_vector<StrUniVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = is_ipv4_compat_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

ObExprIsIpv6::ObExprIsIpv6(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_IS_IPV6, N_IS_IPV6, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprIsIpv6::~ObExprIsIpv6()
{}

int ObExprIsIpv6::cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const
{
  UNUSED(op_cg_ctx);
  UNUSED(raw_expr);
  int ret = OB_SUCCESS;
  if (1 != rt_expr.arg_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is_ipv6 expr should have one param", K(ret), K(rt_expr.arg_cnt_));
  } else if (OB_UNLIKELY(OB_ISNULL(rt_expr.args_) || OB_ISNULL(rt_expr.args_[0]))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("children of is_ipv6 expr is null", K(ret), K(rt_expr.args_));
  } else {
    rt_expr.eval_func_ = ObExprIsIpv6::calc_is_ipv6;
    rt_expr.eval_vector_func_ = ObExprIsIpv6::calc_is_ipv6_vector;
  }
  return ret;
}

int ObExprIsIpv6::calc_is_ipv6(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.eval_param_value(ctx))) {
    LOG_WARN("is_ipv6 expr eval param value failed", K(ret));
  } else {
    ObDatum& text = expr.locate_param_datum(ctx, 0);
    ObString m_text = text.get_string();
    if (text.is_null()) {
      expr_datum.set_int(0);
    } else if (OB_FAIL(is_ipv6(expr_datum, m_text))) {
      LOG_WARN("fail to execute is_ipv6", K(ret));
    }
  }
  return ret;
}

template <typename T>
int ObExprIsIpv6::is_ipv6(T& result, const ObString& text)
{
  int ret = OB_SUCCESS;
  struct in6_addr addr;
  bool is_ip_invalid = false;
  bool is_pure_ipv4 = false;
  if (OB_UNLIKELY(text.empty())) {
    is_ip_invalid = true;
    result.set_int(0);
  } else if (OB_FAIL(ObExprInetCommon::str_to_ip(text.length(), text.ptr(), is_ip_invalid, &addr, true, is_pure_ipv4))) {
    LOG_WARN("fail to execute str_to_ip", K(ret));
  } else {
    result.set_int(is_ip_invalid ? 0 : 1);
  }
  return ret;
}

template<typename ResVec>
inline int ObExprIsIpv6::is_ipv6_vector_inner(const ObString &str_val, ResVec *res_vec, int64_t idx)
{
  int ret = OB_SUCCESS;
  struct in6_addr addr;
  bool is_ip_invalid = false;
  bool is_pure_ipv4 = false;
  if (OB_UNLIKELY(str_val.empty())) {
    is_ip_invalid = true;
    res_vec->set_int(idx, 0ULL);
  } else if (OB_FAIL(ObExprInetCommon::str_to_ip(str_val.length(), str_val.ptr(), is_ip_invalid, &addr, true, is_pure_ipv4))) {
    LOG_WARN("fail to execute str_to_ip", K(ret));
  } else {
    res_vec->set_int(idx, is_ip_invalid ? 0ULL : 1ULL);
  }
  return ret;
}

template <typename ArgVec, typename ResVec>
int ObExprIsIpv6::is_ipv6_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  ArgVec *arg_vec = static_cast<ArgVec *>(expr.args_[0]->get_vector(ctx));
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  ObBitVector &eval_flags = expr.get_evaluated_flags(ctx);
  bool no_skip_no_null = bound.get_all_rows_active() && !arg_vec->has_null();

  if (no_skip_no_null) {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx)) {
        continue;
      } else {
        ObString text_val = arg_vec->get_string(idx);
        if (OB_FAIL(is_ipv6_vector_inner<ResVec>(text_val, res_vec, idx))) {
          LOG_WARN("fail to execute is_ipv6_vector_inner", K(ret));
        }
      }
    }
  } else {
    for (int64_t idx = bound.start(); OB_SUCC(ret) && idx < bound.end(); ++idx) {
      if (eval_flags.at(idx)||skip.at(idx)) {
        continue;
      } else if (arg_vec->is_null(idx)) {
        res_vec->set_int(idx, 0);
        continue;
      } else {
        ObString text_val = arg_vec->get_string(idx);
        if (OB_FAIL(is_ipv6_vector_inner<ResVec>(text_val, res_vec, idx))) {
          LOG_WARN("fail to execute is_ipv6_vector_inner", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExprIsIpv6::calc_is_ipv6_vector(VECTOR_EVAL_FUNC_ARG_DECL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr.args_[0]->eval_vector(ctx, skip, bound))) {
    LOG_WARN("fail to eval is_ipv6 param", K(ret));
  } else {
    VectorFormat arg_format = expr.args_[0]->get_format(ctx);
    VectorFormat res_format = expr.get_format(ctx);
    if (VEC_CONTINUOUS == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv6_vector<StrContVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_CONTINUOUS == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv6_vector<StrContVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv6_vector<StrDiscVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_DISCRETE == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv6_vector<StrDiscVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_FIXED == res_format) {
      ret = is_ipv6_vector<StrUniVec, IntegerFixedVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else if (VEC_UNIFORM == arg_format && VEC_UNIFORM == res_format) {
      ret = is_ipv6_vector<StrUniVec, IntegerUniVec>(VECTOR_EVAL_FUNC_ARG_LIST);
    } else {
      ret = is_ipv6_vector<ObVectorBase, ObVectorBase>(VECTOR_EVAL_FUNC_ARG_LIST);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
