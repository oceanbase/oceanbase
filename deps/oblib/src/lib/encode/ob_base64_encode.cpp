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

#include "lib/encode/ob_base64_encode.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_target_specific.h"
#if OB_USE_MULTITARGET_CODE
#include <immintrin.h>
#endif
namespace oceanbase
{
namespace common
{

uint8_t ObBase64Encoder::BASE64_VALUES[256];
char ObBase64Encoder::BASE64_CHARS[] =
             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             "abcdefghijklmnopqrstuvwxyz"
             "0123456789+/";

int8_t ObBase64Encoder::FROM_BASE64_TABLE[] =
{
//       0  1  2  3  4  5  6  7  8  9  A  B  C  D  E  F
/*00*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-2,-2,-2,-2,-2,-1,-1,
/*10*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
/*20*/  -2,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,62,-1,-1,-1,63, /*  !"#$%&'()*+,-./ */
/*30*/  52,53,54,55,56,57,58,59,60,61,-1,-1,-1,-1,-1,-1, /* 0123456789:;<=>? */
/*40*/  -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14, /* @ABCDEFGHIJKLMNO */
/*50*/  15,16,17,18,19,20,21,22,23,24,25,-1,-1,-1,-1,-1, /* PQRSTUVWXYZ[\]^_ */
/*60*/  -1,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40, /* `abcdefghijklmno */
/*70*/  41,42,43,44,45,46,47,48,49,50,51,-1,-1,-1,-1,-1, /* pqrstuvwxyz{|}~  */
/*80*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
/*90*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
/*A0*/  -2,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
/*B0*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
/*C0*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
/*D0*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
/*E0*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,
/*F0*/  -1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1,-1
};

template<int N>
struct InitBase64Values
{
  typedef InitBase64Values<N + 1> next_v_;
  static int init()
  {
    ObBase64Encoder::BASE64_VALUES[(uint8_t)(ObBase64Encoder::BASE64_CHARS[N])]
      = (uint8_t)(N);
    if (N >= sizeof(ObBase64Encoder::BASE64_CHARS) - 2) {
      return 1;
    } else {
      return next_v_::init();
    }
  }
};

template<>
struct InitBase64Values<sizeof(ObBase64Encoder::BASE64_CHARS) - 1>
{
  static int init()
  {
    return 1;
  }
};

static int init_ret = InitBase64Values<0>::init();

int ObBase64Encoder::encode(const uint8_t *input, const int64_t input_len,
                            char *output, const int64_t output_len,
                            int64_t &pos, const int16_t wrap)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input) || OB_ISNULL(output) ||
      OB_UNLIKELY(input_len < 0 || output_len < 0) || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument input=%p output=%p, input_len=%ld, pos=%ld",
            input, output, input_len, pos);
  } else {
    uint8_t output_idxes[4] = {0, 0, 0, 0};

    int64_t rounds = input_len / 3;
    if (OB_UNLIKELY(pos + rounds * 4 > output_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      _OB_LOG(WARN, "buffer is not enough pos = %ld, output_len = %ld, rounds = %ld",
                  pos, output_len, rounds);
    }
    const uint8_t *iter_input = input;
    for (int64_t i = 0; OB_SUCC(ret) && i < rounds; i++) {
      output_idxes[0] = ((*iter_input) & 0xfc) >> 2;
      output_idxes[1] = ((*iter_input & 0x03) << 4);
      iter_input++;
      output_idxes[1] += ((*iter_input & 0xf0) >> 4);

      output_idxes[2] = ((*iter_input & 0x0f) << 2);
      iter_input++;
      output_idxes[2] += ((*iter_input & 0xc0) >> 6);
      output_idxes[3] = (*iter_input & 0x3f);

      for (int j = 0; OB_SUCC(ret) && j < 4; j++) {
        output[pos++] = BASE64_CHARS[output_idxes[j]];
      }
      if (wrap > 0) {
        if (((i + 1) % (wrap / 4) == 0) && ((i+1) * 3 != input_len)) {
          output[pos++] = '\n';
        }
      }
      iter_input++;
    } // for end

    int64_t cur_idx = iter_input - input;
    if (cur_idx < input_len) {
      if (input_len == cur_idx + 1) {
        uint8_t idx1 = ((input[cur_idx]) & 0xfc) >> 2;
        uint8_t idx2 = ((input[cur_idx]) & 0x03) << 4;

        if (OB_UNLIKELY(pos + 3 >= output_len)) {
          ret = OB_BUF_NOT_ENOUGH;
          _OB_LOG(WARN, "buffer is not enough pos = %ld, output_len = %ld",
                  pos, output_len);
        } else {
          output[pos++] = BASE64_CHARS[idx1];
          output[pos++] = BASE64_CHARS[idx2];
          output[pos++] = '=';
          output[pos++] = '=';
        }
      } else {
        uint8_t idx1 = ((input[cur_idx]) & 0xfc) >> 2;
        uint8_t idx2 = (((input[cur_idx]) & 0x03) << 4)
                       + (((input[cur_idx + 1]) & 0xf0) >> 4);
        uint8_t idx3 = (((input[cur_idx + 1]) & 0x0f) << 2);

        if (OB_UNLIKELY(pos + 3 >= output_len)) {
          ret = OB_BUF_NOT_ENOUGH;
          _OB_LOG(WARN, "buffer is not enough, pos = %ld, output_len = %ld",
                  pos, output_len);
        } else {
          output[pos++] = BASE64_CHARS[idx1];
          output[pos++] = BASE64_CHARS[idx2];
          output[pos++] = BASE64_CHARS[idx3];
          output[pos++] = '=';
        }
      }
    }
  }
  return ret;
}

int ObBase64Encoder::decode(const char *input, const int64_t input_len,
                            uint8_t *output, const int64_t output_len,
                            int64_t &pos, bool skip_spaces)
{
  int ret = OB_SUCCESS;
  bool all_skipped = false;
  if (OB_ISNULL(input) || OB_UNLIKELY(input_len < 0 || output_len < 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument input=%p, output=%p, input_len=%ld, output_len=%ld, pos=%ld",
            input, output, input_len, output_len, pos);
  } else if (skip_spaces) {
    all_skipped = true;
    for (int64_t i = 0; all_skipped && i < input_len; ++i) {
      if (!ObBase64Encoder::is_base64_skip_space(input[i])) {
        all_skipped = false;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (all_skipped) {
    //return empty string
    pos = 0;
  } else if (OB_ISNULL(output)) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument input=%p, output=%p, input_len=%ld, output_len=%ld, pos=%ld",
            input, output, input_len, output_len, pos);
  } else {
    uint8_t uint8_array_3[3];
    uint8_t uint8_array_4[4];
    int64_t i = 0;
    int64_t rounds = input_len / 4;
    if (OB_UNLIKELY(rounds * 3 + pos > output_len)) {
      ret = OB_BUF_NOT_ENOUGH;
      _OB_LOG(WARN, "buffer not enough, pos=%ld, output_len=%ld, input_len=%ld",
                    pos, output_len, input_len);
    }
    const char *iter_input = input;
    int64_t skipped_spaces = 0;
    for(; OB_SUCC(ret) && iter_input < (input + input_len) && '=' != *iter_input; iter_input++) {
      if (OB_UNLIKELY(!is_base64_char(*iter_input))) {
        if (skip_spaces && is_base64_skip_space(*iter_input)) {
          ++skipped_spaces;
        } else {
          ret = OB_INVALID_ARGUMENT;
          _OB_LOG(WARN, "invalid base64 char, cur_idx=%ld, char=%c",
                      iter_input - input, *iter_input);
        }
      } else {
        uint8_array_4[i++] = (uint8_t)(*iter_input);
        if (4 == i) {
          for (int k = 0; k < 4; k++) {
            uint8_array_4[k] = BASE64_VALUES[uint8_array_4[k]];
          }
          uint8_array_3[0] = (uint8_array_4[0] << 2) + ((uint8_array_4[1] & 0x30) >> 4);
          uint8_array_3[1] = ((uint8_array_4[1] & 0x0f) << 4) + ((uint8_array_4[2] & 0x3c) >> 2);
          uint8_array_3[2] = ((uint8_array_4[2] & 0x03) << 6) + uint8_array_4[3];

          for (int k = 0; k < 3; k++) {
            output[pos++] = (uint8_t)(uint8_array_3[k]);
          }
          i = 0;
        }
      }
    } // for end
    int64_t cur_idx = iter_input - input;
    int64_t cur_valid_len = iter_input - input - skipped_spaces;
    for (const char *iter = iter_input; OB_SUCC(ret) && iter < input + input_len; iter++) {
      // all the rest chars must be '='
      if (skip_spaces && is_base64_skip_space(*iter)) {
        skipped_spaces++;
      } else if (OB_UNLIKELY('=' != *iter)) {
        ret = OB_INVALID_ARGUMENT;
      }
    }// end for
    if (OB_FAIL(ret)) {
    } else if (!skip_spaces) {
      if (OB_UNLIKELY((cur_idx + 3 <= input_len))) {
        // only last char or last two chars can be '='
        ret = OB_INVALID_ARGUMENT;
      }
    } else {
      int64_t valid_len = input_len - skipped_spaces;
      if (valid_len % 4 != 0 || cur_valid_len + 3 <= valid_len) {
        ret = OB_INVALID_ARGUMENT;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (i > 0) {
      for (int k = 0; k < i; k++) {
        uint8_array_4[k] = BASE64_VALUES[uint8_array_4[k]];
      }
      uint8_array_3[0] = (uint8_array_4[0] << 2) + ((uint8_array_4[1] & 0x30) >> 4);
      uint8_array_3[1] = ((uint8_array_4[1] & 0x0f) << 4) + ((uint8_array_4[2] & 0x3c) >> 2);

      if (OB_UNLIKELY(pos + i - 1 >= output_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        if (skip_spaces && (pos + i - 1 >= output_len)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          _OB_LOG(WARN, "buffer not enough, pos=%ld, output_len = %ld, i = %ld", pos, output_len, i);
        }
      } else {
        for (int k = 0; k < i - 1; k++) {
          output[pos++] = (uint8_t)(uint8_array_3[k]);
        }
      }
    }
  }
  return ret;
}

#if OB_USE_MULTITARGET_CODE
OB_DECLARE_AVX512_SPECIFIC_CODE(
/*
This function will check if the input string consists of base64 characters, then decode it.
* The param `is_space_legal` can be used to skip space characters in the input string,
* The param `count_equal` is the number of '=' characters in the input string,
* The return value indicates if the input string is legal base64 string.
*/
int do_decode_with_avx512(const char *input, int64_t input_len,
                            uint8_t *output, const int64_t output_len,
                            int64_t &pos, const uint8_t *BASE64_VALUES,
                            bool is_space_legal)
{
  int ret = OB_SUCCESS;
  bool is_char64_legal = true;
  int64_t count_spaces = 0;
  int64_t count_equal = 0;
  const __m512i lo_a = _mm512_set1_epi8('a');
  const __m512i hi_a = _mm512_set1_epi8('z');
  const __m512i lo_A = _mm512_set1_epi8('A');
  const __m512i hi_A = _mm512_set1_epi8('Z');
  const __m512i lo_0 = _mm512_set1_epi8('0');
  const __m512i hi_0 = _mm512_set1_epi8('9');
  const __m512i plus = _mm512_set1_epi8('+');
  const __m512i slash = _mm512_set1_epi8('/');
  const __m512i equal = _mm512_set1_epi8('=');
  const __m512i spaces[7] = {
    _mm512_set1_epi8(0x09),
    _mm512_set1_epi8(0x0A),
    _mm512_set1_epi8(0x0B),
    _mm512_set1_epi8(0x0C),
    _mm512_set1_epi8(0x0D),
    _mm512_set1_epi8(0x20),
    _mm512_set1_epi8(0xA0),
  };

  // use these variables instead of look-up table BASE64_VALUE
  __m512i offset_upper = _mm512_set1_epi8('A');             // mapping A-Z to 0-25
  __m512i offset_lower = _mm512_set1_epi8('a' - 26);        // mapping a-z to 26-51
  __m512i offset_digit = _mm512_set1_epi8(52 - '0');        // mapping 0-9 to 52-61
  __m512i val_62 = _mm512_set1_epi8(62);                    // mapping '+' to 62
  __m512i val_63 = _mm512_set1_epi8(63);                    // mapping '/' to 63
  __m512i val_zero = _mm512_set1_epi8(0);                   // mapping '=' to 0

  __m512i shuffle_mask = _mm512_set_epi8(                   // this mask is used to reorder 48 values
      0,0,0,0, 14,13,12, 10,9,8, 6,5,4, 2,1,0,
      0,0,0,0, 14,13,12, 10,9,8, 6,5,4, 2,1,0,
      0,0,0,0, 14,13,12, 10,9,8, 6,5,4, 2,1,0,
      0,0,0,0, 14,13,12, 10,9,8, 6,5,4, 2,1,0
  );

  int k = 0; // k notices the number of characters in the current 4 bytes in normal process
  constexpr int BLOCK_SIZE = sizeof(__m512i);
  constexpr int BLOCK_SIZE_4 = BLOCK_SIZE / 4;
  uint8_t uint8_array[BLOCK_SIZE] __attribute__((aligned(BLOCK_SIZE)));
  uint8_t uint8_array_4[4];

  // if input_len is NOT enough for SIMD, use this trick to make use of SIMD
  char temp_input[BLOCK_SIZE];
  if (OB_ISNULL(input) || OB_UNLIKELY(input_len < 0 || output_len < 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(output)) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument input=%p, output=%p, input_len=%ld, output_len=%ld, pos=%ld",
            input, output, input_len, output_len, pos);
  } else if (input_len < BLOCK_SIZE) {
    memset(temp_input, 0x0A, sizeof(temp_input));
    MEMCPY(temp_input, input, input_len);
    input = temp_input;
    input_len = BLOCK_SIZE;
  }

  // codes above try to make use of SIMD to speed up, can be divided into 4 parts
  // 1. check legality
  // 2. translate chars to indexs
  // 3. translate every 4 bytes to 3 bytes
  // 4. reorder and write results
  for (int i = 0; i <= input_len - BLOCK_SIZE && OB_SUCC(ret); i += BLOCK_SIZE) {
    const unsigned char* current_input_ptr = reinterpret_cast<const unsigned char*>(input + i);
    __m512i input_vec = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(current_input_ptr));
    // 1. check if the input string is legal base64 string
    __mmask64 mask_lo = _mm512_cmpge_epi8_mask(input_vec, lo_a);
    __mmask64 mask_hi = _mm512_cmple_epi8_mask(input_vec, hi_a);
    __mmask64 mask_lower = mask_lo & mask_hi;

    mask_lo = _mm512_cmpge_epi8_mask(input_vec, lo_A);
    mask_hi = _mm512_cmple_epi8_mask(input_vec, hi_A);
    __mmask64 mask_upper = mask_lo & mask_hi;

    mask_lo = _mm512_cmpge_epi8_mask(input_vec, lo_0);
    mask_hi = _mm512_cmple_epi8_mask(input_vec, hi_0);
    __mmask64 mask_digit = mask_lo & mask_hi;

    __mmask64 mask_plus = _mm512_cmpeq_epi8_mask(input_vec, plus);
    __mmask64 mask_slash = _mm512_cmpeq_epi8_mask(input_vec, slash);
    __mmask64 mask_equal = _mm512_cmpeq_epi8_mask(input_vec, equal);
    __mmask64 mask_space = 0;
    for (int j = 0; j < sizeof(spaces) / BLOCK_SIZE; ++j) {
      mask_space |= _mm512_cmpeq_epi8_mask(input_vec, spaces[j]);
    }

    __mmask64 valid_mask = 0;
    __mmask64 mask_char64 = 0;
    if (OB_LIKELY(is_char64_legal)) {
      mask_char64 = mask_lower | mask_upper | mask_digit | mask_plus | mask_slash;
      valid_mask |= mask_char64;
    }

    if (OB_UNLIKELY(mask_space != 0)) {
      if (is_space_legal) {
        valid_mask |= mask_space;
        count_spaces += __builtin_popcountll(mask_space);
      }
    }

    if (OB_UNLIKELY(mask_equal != 0)) {
      valid_mask |= mask_equal;
      count_equal += __builtin_popcountll(mask_equal);
      is_char64_legal = false;
      // in addition, check current block, the characters after the first '=' shouldn't be 64 characters
      __mmask64 mask_check_char64 = ~((mask_equal & (-mask_equal)) - 1);
      if (mask_check_char64 & mask_char64) {
        ret = OB_INVALID_ARGUMENT;
      }
    }

    if (OB_UNLIKELY(~valid_mask != 0 || count_equal > 2)) {
      ret = OB_INVALID_ARGUMENT;
    }

    if (OB_SUCC(ret)) {
      // 2. translate every character to its corresponding index in BASE64_VALUES
      __m512i input_vec_idx = input_vec;
      input_vec_idx = _mm512_mask_sub_epi8(input_vec_idx, mask_upper, input_vec_idx, offset_upper);
      input_vec_idx = _mm512_mask_sub_epi8(input_vec_idx, mask_lower, input_vec_idx, offset_lower);
      input_vec_idx = _mm512_mask_add_epi8(input_vec_idx, mask_digit, input_vec_idx, offset_digit);
      input_vec_idx = _mm512_mask_mov_epi8(input_vec_idx, mask_plus, val_62);
      input_vec_idx = _mm512_mask_mov_epi8(input_vec_idx, mask_slash, val_63);
      input_vec_idx = _mm512_mask_mov_epi8(input_vec_idx, mask_equal, val_zero);
      // 3. decode the input, translate every 4 bytes into 3 bytes
      if (mask_space == 0 && k == 0) { // run simd path
        // every time, process 4 bytes, so extract bytes from each 32-bit input
        // both big and little endian are supported.
#if __BYTE_ORDER == __BIG_ENDIAN
        __m512i output0 = _mm512_or_si512(
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0xFF000000)), 22),
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0x30000000)), 28)
        );
        __m512i output1 = _mm512_or_si512(
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0x0F0000)), 12),
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0x3C00)), 10)
        );
        __m512i output2 = _mm512_or_si512(
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0x0300)), 2),
          _mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0xFF))
        );
#else
        __m512i output0 = _mm512_or_si512(
          _mm512_slli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0xFF)), 2),
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0x3000)), 12)
        );
        __m512i output1 = _mm512_or_si512(
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0x0F00)), 4),
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0x3C0000)), 18)
        );
        __m512i output2 = _mm512_or_si512(
          _mm512_srli_epi32(_mm512_and_si512(input_vec_idx, _mm512_set1_epi32(0x030000)), 10),
          _mm512_srli_epi32(input_vec_idx, 24)
        );
#endif
        // 4. reorder the output
        uint8_t temp[BLOCK_SIZE] __attribute__((aligned(BLOCK_SIZE)));
        __m512i output_combined = _mm512_or_si512(_mm512_or_si512(_mm512_slli_epi32(output2, 16), _mm512_slli_epi32(output1, 8)), output0);
        __m512i shuffled = _mm512_shuffle_epi8(output_combined, shuffle_mask);
        _mm512_store_si512((__m512i*)(temp), shuffled);
        if (OB_UNLIKELY(pos + 3 * BLOCK_SIZE_4 > output_len)) {
          ret = OB_BUF_NOT_ENOUGH;
          _OB_LOG(WARN, "output buffer is not enough, pos=%ld, output_len=%ld", pos, output_len);
        } else {
          for (int j = 0; j < 4; ++j) {
            MEMCPY(output + pos +  BLOCK_SIZE_4 * j / 4 * 3, temp +  BLOCK_SIZE_4 * j, 12);
          }
          pos += 3 * BLOCK_SIZE_4;
        }
      } else { // run normal translate path
        _mm512_store_si512((__m512*)uint8_array, input_vec_idx);
        for (int j = 0; j < BLOCK_SIZE; ++j) {
          if (mask_space & (1LL << j)) continue;  // check if the current character is space
          uint8_array_4[k++] = uint8_array[j];
          if (k == 4) {
            if (OB_UNLIKELY(pos + 3 > output_len)) {
              ret = OB_BUF_NOT_ENOUGH;
              _OB_LOG(WARN, "output buffer is not enough, pos=%ld, output_len=%ld", pos, output_len);
            } else {
              output[pos++] = (uint8_array_4[0] << 2) + ((uint8_array_4[1] & 0x30) >> 4);
              output[pos++] = ((uint8_array_4[1] & 0x0f) << 4) + ((uint8_array_4[2] & 0x3c) >> 2);
              output[pos++] = ((uint8_array_4[2] & 0x03) << 6) + uint8_array_4[3];
              k = 0;
            }
          }
        }
      }
    } // end if(OB_SUCC(ret))
  } // end for

  // use normal path to process rest bytes ...
  for (int i = (input_len / BLOCK_SIZE) * BLOCK_SIZE; i < input_len && OB_SUCC(ret); ++i) {
    const char &c = input[i];
    bool is_space = ObBase64Encoder::is_base64_skip_space(c);
    if (OB_LIKELY(is_char64_legal && ObBase64Encoder::is_base64_char(c))) {
      uint8_array_4[k++] = BASE64_VALUES[static_cast<const unsigned char>(c)];
    } else if (OB_UNLIKELY(is_space_legal && is_space)) {
      count_spaces++;
    } else if (OB_UNLIKELY(c == '=')) {
      count_equal++;
      is_char64_legal = false;
      uint8_array_4[k++] = BASE64_VALUES[static_cast<const unsigned char>(c)];
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCC(ret) && k == 4) {
      output[pos++] = (uint8_array_4[0] << 2) + ((uint8_array_4[1] & 0x30) >> 4);
      output[pos++] = ((uint8_array_4[1] & 0x0f) << 4) + ((uint8_array_4[2] & 0x3c) >> 2);
      output[pos++] = ((uint8_array_4[2] & 0x03) << 6) + uint8_array_4[3];
      k = 0;
    }
  }

  // the number of '=' must be less than 2
  // the length of input(remove spaces) must be a multiple of 4
  if (OB_UNLIKELY(count_equal > 2 || (input_len != count_spaces && (input_len - count_spaces) % 4 != 0))) {
    ret = OB_INVALID_ARGUMENT;
  }

  // TODO: 测试一下只有等号的case
  // finally, adjust the result
  // xx==  ->  1 byte
  // xxx=  ->  2 bytes
  // xxxx  ->  3 bytes
  pos -= count_equal;
  return ret;
}
);
#endif

int ObBase64Encoder::decode_with_simd(const char *input,  const int64_t input_len,
                            uint8_t *output, const int64_t output_len,
                            int64_t &pos, bool skip_spaces)
{
  int ret = OB_SUCCESS;
  #if OB_USE_MULTITARGET_CODE
  bool avx512_supported = common::is_arch_supported(ObTargetArch::AVX512);
  if (avx512_supported && OB_FAIL(specific::avx512::do_decode_with_avx512(input, input_len, output, output_len, pos, BASE64_VALUES, skip_spaces))) {
    _OB_LOG(WARN, "input may be an illegal base64 string", K(ret), K(input_len));
  } else if (!avx512_supported && OB_FAIL(decode(input, input_len, output, output_len, pos, skip_spaces))) {
    _OB_LOG(WARN, "input is not legal base64 string", K(ret), K(input_len));
  }
  #else
  if (OB_FAIL(decode(input, input_len, output, output_len, pos, skip_spaces))) {
    _OB_LOG(WARN, "input is not legal base64 string", K(ret), K(input_len));
  }
  #endif
  return ret;
}

}
} // end namespace oceanbase
