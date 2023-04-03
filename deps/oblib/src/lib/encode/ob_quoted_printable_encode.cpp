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

#include "lib/encode/ob_quoted_printable_encode.h"

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
namespace common
{
char ObQuotedPrintableEncoder::hex[] = "0123456789ABCDEF";
static int char_convert_to_hex(uint8_t &out)
{
  int ret = OB_SUCCESS;
  uint8_t input = out;
  if (input >= '0' && input <= '9') {
    out = static_cast<int64_t>(input - '0');
  } else if (input >= 'A' && input <= 'F') {
    out = static_cast<int64_t>(input - 'A' + 10);
  } else {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN, "input shoud be hex input = %x", input);
  }
  return ret;
}
bool ObQuotedPrintableEncoder::is_hex(const uint8_t input)
{
  bool flag = false;
  //'0' ~ '9' || 'A' ~ 'F'
  if ((input >= 48 && input <= 57 ) || (input >= 65 && input <= 70)) {
    flag = true;
  }
  return flag;
}
//todo for mime '?' is not printable
bool ObQuotedPrintableEncoder::is_printable(const uint8_t input, bool is_mime_mode)
{
  bool flag = false;
  if ((input >= PRINTABLE_START && input <= PRINTABLE_END && input != QP_ESCAPE_CHAR)
      || (QP_SPACE == input)) {
    flag = true;
  }
  //对于MIME ENCODE， '?'作为分隔符使用
  //why does Oracle treate '_' as a unprintable char ?
  if (is_mime_mode && (QP_SPACE == input || QP_QUESTIONMARK == input || QP_UNDERLINE == input)) {
    flag = false;
  }
  return flag;
}
int ObQuotedPrintableEncoder::decode(const uint8_t *input, const int64_t input_len,
                                     uint8_t *output, const int64_t output_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input) || OB_ISNULL(output)
     ||(OB_UNLIKELY(input_len < 0 || output_len < 0)
     || pos < 0))
  {
    ret =  OB_INVALID_ARGUMENT;
    _OB_LOG(WARN,
           "invalid argument input = %p, output = %p,input_len = %ld, output_len = %ld, pos = %ld",
            input, output, input_len, output_len, pos);
  } else {
    if (output_len - pos < input_len) {
      ret = OB_BUF_NOT_ENOUGH;
      _OB_LOG(WARN, "buffer is not enough pos = %ld, output_len = %ld, input_len = %ld",
              pos, output_len, input_len);
    } else {
      for (int i = 0; OB_SUCC(ret) && i < input_len; ) {
        if (is_printable(input[i]))
        {
          output[pos++] = input[i++]; //printable char, no change
        } else {
          if (QP_ESCAPE_CHAR == input[i] && i + 2 <= input_len) { //at least 2 char behind '='
            ++i; //ingore escape
            if (is_hex(input[i])) { //for normal not printable char "=hexhex"
              if (is_hex(input[i + 1])) {
                uint8_t high = input[i++];
                uint8_t low = input[i++];
                if (OB_FAIL(char_convert_to_hex(high))) {
                  ret = OB_INVALID_ARGUMENT;
                  _OB_LOG(WARN, "there must be 2 hex_char behind ESCAPE =,  but input = %p", input);
                } else if (OB_FAIL(char_convert_to_hex(low))) {
                  ret = OB_INVALID_ARGUMENT;
                  _OB_LOG(WARN, "there must be 2 hex_char behind ESCAPE =,  but input = %p", input);
                } else {
                  uint8_t new_res = ((high << 4) | low);
                  output[pos++] = new_res;
                }
              } else {
                ret = OB_INVALID_ARGUMENT;
                _OB_LOG(WARN, "there must be 2 hex_char behind ESCAPE =,  but input = %p", input);
              }
            } else { //must "={SPACE/TAB}*CRLF"
              while (OB_SUCC(ret) && input[i] != QP_CR) {
                if (input[i] != QP_TAB || input[i] != QP_SPACE) {
                  ret = OB_INVALID_ARGUMENT;
                  _OB_LOG(WARN, "only SPACE or TAB can exist in soft break, but input = %p", input);
                } else {
                  if (i + 2 <= input_len) {//at least CR LF behind SPACE/TAB
                    ++i; //remove mta extra whitespace
                  } else {
                    ret = OB_INVALID_ARGUMENT;
                    _OB_LOG(WARN, "at least CR LF behind SPACE/TAB, but input = %p", input);
                  }
                }
              }
              ++i; //ingore CR
              if (OB_FAIL(ret)) {
              } else if (QP_LF != input[i]) {
                 ret = OB_INVALID_ARGUMENT;
                 _OB_LOG(WARN, "soft break must be =CRLF, but input = %p", input);
              } else {
                ++i; //ingore LF
              }
            }
          } else {//not printable char must behind '='
            ret = OB_INVALID_ARGUMENT;
            _OB_LOG(WARN, "At least 2 char behind ESCAPE =, but input = %p", input);
          }
        }
      }
    }
  }
  return ret;
}

int ObQuotedPrintableEncoder::encode(const uint8_t *input, const int64_t input_len,
                                     uint8_t *output, const int64_t output_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input) || OB_ISNULL(output)
     || (OB_UNLIKELY(input_len < 0 || output_len < 0)
     || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN,
          "invalid argument input = %p, output = %p,input_len = %ld, output_len = %ld, pos = %ld",
            input, output, input_len, output_len, pos);
  } else {
    /*EXPANSION_FACTOR_QUOTE_PRINTABLE(3) * in_raw.length()
    *+ (EXPANSION_FACTOR_QUOTE_PRINTABLE(3) * in_raw.length()
    *\/ NUM_OF_CHAR_PER_LINE_QUOTED_PRINTABLE(73))
    ** NUM_OF_CHAR_FOR_SOFT_BREAK(3);*/
    if (output_len - pos < input_len * 3 + input_len * 3 / 73 * 3) {
      ret = OB_BUF_NOT_ENOUGH;
      _OB_LOG(WARN, "buffer is not enough pos = %ld, output_len = %ld, input_len = %ld",
              pos, output_len, input_len);
    } else {
      int line_count = 0; // for soft line break
      const uint8_t *iter_input = input;
      for (int64_t i = 0; OB_SUCC(ret) && i < input_len; ) {
        //oracle treate CRLF as a sort break, CR/LF as a unprintable char
        if (i < input_len - 1 && QP_CR == input[i] && QP_LF == input[i + 1]) {
          output[pos++] = QP_CR;
          output[pos++] = QP_LF;
          i += 2;
          continue;
        }
        if (is_printable(input[i])) {
          //subtract 1 for the soft line break
          if (++line_count > BYTE_PER_LINE - 1) {
            output[pos++] = QP_ESCAPE_CHAR;
            output[pos++] = QP_CR;
            output[pos++] = QP_LF;
            line_count = 1; // for new char
          }
          output[pos++] = input[i];
          ++i;
        } else {
          //subtract 3 for the soft line break
          if ((line_count += 3) > BYTE_PER_LINE - 1) {
            output[pos++] = QP_ESCAPE_CHAR;
            output[pos++] = QP_CR;
            output[pos++] = QP_LF;
            line_count = 3; // for new char
          }
          uint8_t not_printable = input[i];
          output[pos++] = QP_ESCAPE_CHAR;
          output[pos++] = static_cast<uint8_t>(hex[(not_printable >> 4)]);
          output[pos++] = static_cast<uint8_t>(hex[(not_printable & 0xF)]);
          ++i;
        }
      }
    }
  }
  return ret;
}
//this func only for encode raw to one line, do not do soft break
int ObQuotedPrintableEncoder::encode_for_raw(const uint8_t *input, const int64_t input_len,
                                     uint8_t *output, const int64_t output_len, int64_t &pos,
                                     const bool is_mime_mode)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(input) || OB_ISNULL(output)
     || (OB_UNLIKELY(input_len < 0 || output_len < 0)
     || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    _OB_LOG(WARN,
          "invalid argument input = %p, output = %p,input_len = %ld, output_len = %ld, pos = %ld",
            input, output, input_len, output_len, pos);
  } else {
    if (output_len - pos < input_len * 3) {
      ret = OB_BUF_NOT_ENOUGH;
      _OB_LOG(WARN, "buffer is not enough pos = %ld, output_len = %ld, input_len = %ld",
              pos, output_len, input_len);
    } else {
      const uint8_t *iter_input = input;
      for (int64_t i = 0; i < input_len; ) {
        //oracle treate CRLF as a sort break, CR/LF as a unprintable char
        if (i < input_len - 1 && QP_CR == input[i] && QP_LF == input[i + 1]) {
          output[pos++] = QP_CR;
          output[pos++] = QP_LF;
          i += 2;
          continue;
        }
        if (is_printable(input[i], is_mime_mode)) {
          output[pos++] = input[i];
          ++i;
        } else {
          uint8_t not_printable = input[i];
          output[pos++] = QP_ESCAPE_CHAR;
          output[pos++] = static_cast<uint8_t>(hex[(not_printable >> 4)]);
          output[pos++] = static_cast<uint8_t>(hex[(not_printable & 0xF)]);
          ++i;
        }
      }
    }
  }
  return ret;
}
//为text encode 设计， 换行可以没有CR
int ObQuotedPrintableEncoder::decode_for_text(const uint8_t *input, const int64_t input_len,
                                     uint8_t *output, const int64_t output_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool is_mime_mode = false;
  if (OB_ISNULL(input) || OB_ISNULL(output)
     ||(OB_UNLIKELY(input_len < 0 || output_len < 0)
     || pos < 0))
  {
    ret =  OB_INVALID_ARGUMENT;
    _OB_LOG(WARN,
           "invalid argument input = %p, output = %p,input_len = %ld, output_len = %ld, pos = %ld",
            input, output, input_len, output_len, pos);
  } else {
    if (output_len - pos < input_len) {
      ret = OB_BUF_NOT_ENOUGH;
      _OB_LOG(WARN, "buffer is not enough pos = %ld, output_len = %ld, input_len = %ld",
              pos, output_len, input_len);
    } else {
      for (int i = 0; OB_SUCC(ret) && i < input_len; ) {
        if (is_printable(input[i], is_mime_mode))
        {
          output[pos++] = input[i++]; //printable char, no change
        } else {
          if (QP_ESCAPE_CHAR == input[i] && i + 2 <= input_len) { //at least 2 char behind '='
            ++i; //ingore escape
            if (is_hex(input[i])) { //for normal not printable char "=hexhex"
              if (is_hex(input[i + 1])) {
                uint8_t high = input[i++];
                uint8_t low = input[i++];
                if (OB_FAIL(char_convert_to_hex(high))) {
                  ret = OB_INVALID_ARGUMENT;
                  _OB_LOG(WARN, "there must be 2 hex_char behind ESCAPE =,  but input = %p", input);
                } else if (OB_FAIL(char_convert_to_hex(low))) {
                  ret = OB_INVALID_ARGUMENT;
                  _OB_LOG(WARN, "there must be 2 hex_char behind ESCAPE =,  but input = %p", input);
                } else {
                  uint8_t new_res = ((high << 4) | low);
                  output[pos++] = new_res;
                }
              } else {
                ret = OB_INVALID_ARGUMENT;
                _OB_LOG(WARN, "there must be 2 hex_char behind ESCAPE =,  but input = %p", input);
              }
            } else { //must "={SPACE/TAB}*CRLF"
              while (OB_SUCC(ret) && (input[i] != QP_CR || input[i] != QP_LF)) {
                if (input[i] != QP_TAB || input[i] != QP_SPACE) {
                  ret = OB_INVALID_ARGUMENT;
                  _OB_LOG(WARN, "only SPACE or TAB can exist in soft break, but input = %p", input);
                } else {
                  if (i + 1 <= input_len) {//at least LF behind SPACE/TAB
                    ++i; //remove mta extra whitespace
                  } else {
                    ret = OB_INVALID_ARGUMENT;
                    _OB_LOG(WARN, "at least CR LF behind SPACE/TAB, but input = %p", input);
                  }
                }
              }
              if (OB_SUCC(ret) && QP_CR == input[i]) {
                ++i; //ingore CR
              }
              if (OB_FAIL(ret)) {
              } else if (QP_LF != input[i]) {
                 ret = OB_INVALID_ARGUMENT;
                 _OB_LOG(WARN, "soft break must be =CRLF, but input = %p", input);
              } else {
                ++i; //ingore LF
              }
            }
          } else if (QP_ESCAPE_CHAR == input[i] && i + 1 == input_len && QP_LF == input[i+1]) {
            i += 2; // escape =LF
          } else {//not printable char must behind '='
            ret = OB_INVALID_ARGUMENT;
            _OB_LOG(WARN, "At least 2 char behind ESCAPE =, but input = %p", input);
          }
        }
      }
    }
  }
  return ret;
}


} //namespace common
} //namespace oceanbase