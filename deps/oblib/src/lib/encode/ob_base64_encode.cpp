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
#include "lib/ob_errno.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

uint8_t ObBase64Encoder::BASE64_VALUES[256];
char ObBase64Encoder::BASE64_CHARS[] =
             "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
             "abcdefghijklmnopqrstuvwxyz"
             "0123456789+/";

int ObBase64Encoder::FROM_BASE64_TABLE[] =
{
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
  if (OB_ISNULL(input) || OB_UNLIKELY(input_len < 0 || output_len < 0 || pos< 0)) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument input=%p, output=%p, input_len=%ld, output_len=%ld, pos=%ld",
            input, output, input_len, output_len, pos);
  } else if (skip_spaces) {
    all_skipped = true;
    for (int64_t i = 0; all_skipped && i < input_len; ++i) {
      if (!ObBase64Encoder::my_base64_decoder_skip_spaces(input[i])) {
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
    if (OB_UNLIKELY(rounds * 3) + pos > output_len) {
      ret = OB_BUF_NOT_ENOUGH;
      _OB_LOG(WARN, "buffer not enough, pos=%ld, output_len=%ld, input_len=%ld",
                    pos, output_len, input_len);
    }
    const char *iter_input = input;
    int64_t skipped_spaces = 0;
    for(; OB_SUCC(ret) && iter_input < (input + input_len) && '=' != *iter_input; iter_input++) {
      if (OB_UNLIKELY(!is_base64_char(*iter_input))) {
        if (skip_spaces && my_base64_decoder_skip_spaces(*iter_input)) {
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
      if (skip_spaces && my_base64_decoder_skip_spaces(*iter)) {
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
}
} // end namespace oceanbase
