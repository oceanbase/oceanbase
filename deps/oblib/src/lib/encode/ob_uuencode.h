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

#ifndef OB_UUENCODE_H_
#define OB_UUENCODE_H_

#include <cctype>
#include <cstdint>

namespace oceanbase
{
namespace common
{

class ObUUEncoder
{
public:
  static int encode(const uint8_t* input, const int64_t input_len,
                    char* output, const int64_t output_len, int64_t &pos, int64_t &padding);

  static int decode(const char* input, const int64_t input_len,
                    uint8_t* output, const int64_t output_len, int64_t &pos);
  static const uint8_t UU_SPACE = 32;
  static const uint8_t UU_END = 95;
private:
  static bool is_uuprintable_char(const char c) { return (c >=UU_SPACE && c <= UU_END);}
};

} //namespace common
} //namespace oceanbase

#endif //OB_UUENCODE_H_
