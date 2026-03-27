/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
