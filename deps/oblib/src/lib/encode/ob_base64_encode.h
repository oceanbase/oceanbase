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

#ifndef OB_BASE64_ENCODE_H_
#define OB_BASE64_ENCODE_H_

#include <cctype>
#include <cstdint>

namespace oceanbase {
namespace common {
class ObBase64Encoder {
private:
  template <int N>
  friend class InitBase64Values;
  static char BASE64_CHARS[];

  static uint8_t BASE64_VALUES[256];

  static inline bool is_base64_char(char c)
  {
    return std::isalnum(c) || c == '+' || c == '/';
  }

public:
  static int64_t needed_encoded_length(const int64_t buf_size)
  {
    return (buf_size / 3) * 4 + (buf_size % 3 == 0 ? 0 : 4);
  }

  static int64_t needed_decoded_length(const int64_t buf_size)
  {
    return (buf_size / 4) * 3;
  }

  static int encode(
      const uint8_t* input, const int64_t input_len, char* output, const int64_t output_len, int64_t& pos);

  static int decode(
      const char* input, const int64_t input_len, uint8_t* output, const int64_t output_len, int64_t& pos);
};
}  // end namespace common
}  // end namespace oceanbase
#endif  // !OB_BASE64_ENCODE_H_