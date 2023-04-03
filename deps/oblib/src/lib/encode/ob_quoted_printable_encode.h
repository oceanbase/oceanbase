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

#ifndef OB_QUOTED_PRINTABLE_ENCODE_H_
#define OB_QUOTED_PRINTABLE_ENCODE_H_

#include <cctype>
#include <cstdint>

namespace oceanbase
{
namespace common
{

class ObQuotedPrintableEncoder
{
private:
  static bool is_hex(const uint8_t input);
  static bool is_printable(const uint8_t input, bool is_mime_mode = false);
public:
  static char hex[];
  const static uint8_t QP_ESCAPE_CHAR = 61;
  const static uint8_t QP_TAB = 9;
  const static uint8_t QP_SPACE = 32;
  const static uint8_t QP_CR = 13;
  const static uint8_t QP_LF = 10;
  const static uint8_t QP_QUESTIONMARK = 63;
  const static uint8_t QP_UNDERLINE = 95;
  const static int BYTE_PER_LINE = 76;
  const static uint8_t PRINTABLE_START = 33;
  const static uint8_t PRINTABLE_END = 126;
  ObQuotedPrintableEncoder ();
  ~ObQuotedPrintableEncoder();

  static int encode(const uint8_t *input, const int64_t input_len,
                    uint8_t* output, const int64_t output_len, int64_t &pos);

  static int decode(const uint8_t *input, const int64_t input_len,
                    uint8_t *output, const int64_t output_len, int64_t &pos);
  static int encode_for_raw(const uint8_t *input, const int64_t input_len,
                            uint8_t *output, const int64_t output_len, int64_t &pos,
                            const bool is_mime_mode);
  static int decode_for_text(const uint8_t *input, const int64_t input_len,
                            uint8_t *output, const int64_t output_len, int64_t &pos);
};



} //namespace common
} // namespace oceanbase
#endif //OB_QUOTED_PRINTABLE_ENCODE_H_
