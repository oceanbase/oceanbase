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

#include "lib/encode/ob_uuencode.h"

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{

int ObUUEncoder::encode(const uint8_t* input, const int64_t input_len,
                    char* output, const int64_t output_len, int64_t &pos, int64_t &padding)
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
    padding = 0;
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
        output[pos++] = output_idxes[j] + UU_SPACE;
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
          output[pos++] = idx1 + UU_SPACE;
          output[pos++] = idx2 + UU_SPACE;
          output[pos++] = UU_SPACE;
          output[pos++] = UU_SPACE;
          padding = 2;
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
          output[pos++] = idx1 + UU_SPACE;
          output[pos++] = idx2 + UU_SPACE;
          output[pos++] = idx3 + UU_SPACE;
          output[pos++] = UU_SPACE;
          padding = 1;
        }
      }
    }
  }
  return ret;
}


int ObUUEncoder::decode(const char *input, const int64_t input_len,
                            uint8_t *output, const int64_t output_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input) || OB_ISNULL(output) ||
      OB_UNLIKELY(input_len < 0 || output_len < 0 || pos< 0)) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "invalid argument input=%p, output=%p, input_len=%ld, output_len=%ld, pos=%ld",
            input, output, input_len, output_len, pos);
  } else {
    int64_t remain_len = input_len;
    uint8_t uint8_array_3[3];
    uint8_t uint8_array_4[4];
    int64_t rounds = input_len / 4;
    if (OB_UNLIKELY(rounds * 3) + pos > output_len) {
      ret = OB_BUF_NOT_ENOUGH;
      _OB_LOG(WARN, "buffer not enough, pos=%ld, output_len=%ld, input_len=%ld",
                    pos, output_len, input_len);
    }
    const char *iter_input = input;
    int64_t cur_idx = 0;
    for(; OB_SUCC(ret) && iter_input + 4 <= input + input_len; ++cur_idx) {
      if (4 == cur_idx) {
        for (int i = 0; OB_SUCC(ret) && i < 4 ; ++i) {
          if (OB_UNLIKELY(!is_uuprintable_char(*(iter_input + i)))) {
            ret = OB_INVALID_ARGUMENT;
            _OB_LOG(WARN, "invalid uu decode char, cur_idx=%ld, char=%c",
                        iter_input - input, *(iter_input + i));
          } else {
            uint8_array_4[i] = (uint8_t) (*(iter_input + i)) - UU_SPACE;
          }
        }
        if (OB_SUCC(ret)) {
          uint8_array_3[0] = (uint8_array_4[0] << 2) + ((uint8_array_4[1] & 0x30) >> 4);
          uint8_array_3[1] = ((uint8_array_4[1] & 0x0f) << 4) + ((uint8_array_4[2] & 0x3c) >> 2);
          uint8_array_3[2] = ((uint8_array_4[2] & 0x03) << 6) + uint8_array_4[3];
          for (int k = 0; k < 3; k++) {
            output[pos++] = (uint8_t)(uint8_array_3[k]);
          }
        }
        cur_idx = 0;
        iter_input += 4;
      }
    } // for end
    if (OB_SUCC(ret)) {
      cur_idx = iter_input - input;
      for (int i = 0; cur_idx + i < input_len; ++i) {
        uint8_array_4[i] = input[cur_idx + i] - UU_SPACE;
      }
      if (input_len == cur_idx + 1) {
        //还剩余一个char， 错误
        ret = OB_ERR_UNEXPECTED;
        _OB_LOG(WARN, "bad argument");
      } else if (input_len == cur_idx) {
        //结束
      } else if (input_len == cur_idx + 2) {
        uint8_array_3[0] = (uint8_array_4[0] << 2) + ((uint8_array_4[1] & 0x30) >> 4);
      } else if (input_len == cur_idx + 3) {
        uint8_array_3[0] = (uint8_array_4[0] << 2) + ((uint8_array_4[1] & 0x30) >> 4);
        uint8_array_3[1] = ((uint8_array_4[1] & 0x0f) << 4) + ((uint8_array_4[2] & 0x3c) >> 2);
      }

      if (OB_UNLIKELY(pos + input_len - cur_idx - 1 >= output_len)) {
        ret = OB_BUF_NOT_ENOUGH;
        _OB_LOG(WARN, "buffer not enought, pos=%ld, output_len = %ld, i = %ld", pos, output_len, cur_idx);
      } else {
        for (int k = 0; k < input_len - cur_idx - 1; k++) {
          output[pos++] = (uint8_t)(uint8_array_3[k]);
        }
      }
    }
  }
  return ret;
}

} //namespace common
} //namespace oceanbase