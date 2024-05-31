/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_utils.h"
#include "lib/ob_define.h"
#include "lib/function/ob_function.h"               // ObFunction
#include <cerrno>

#define APPEND_STR(buf, args...) \
  do {\
    if (OB_SUCC(ret) && OB_FAIL(buf.append(args))) { \
      LOG_ERROR("append str failed", "buf_len", buf.length(), "buf_cap", buf.capacity()); \
    }\
  } while(0)

namespace oceanbase
{
namespace oblogminer
{

// not very safe, str must end with '\0'
int logminer_str2ll(const char *str, char* &endptr, int64_t &num)
{
  int ret = OB_SUCCESS;
  if (nullptr == str) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int origin_errno = errno;
    errno = 0;
    num = strtoll(str, &endptr, 10);
    if (0 != errno || endptr == str) {
      ret = OB_INVALID_ARGUMENT;
    }
    errno = origin_errno;
  }

  return ret;
}

int logminer_str2ll(const char *str, int64_t &num)
{
  char *endptr = nullptr;
  return logminer_str2ll(str, endptr, num);
}

int write_keys(const KeyArray &key_arr, common::ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  int64_t total_size = 0;
  char *tmp_buffer = nullptr;
  const char *keys_delimiter = "/";
  APPEND_STR(buffer, "\"");
  ARRAY_FOREACH_N(key_arr, idx, key_num) {
    const ObString &key = key_arr.at(idx);
    APPEND_STR(buffer, key);
    if (idx != key_num - 1) {
      APPEND_STR(buffer, keys_delimiter);
    }
  }
  APPEND_STR(buffer, "\"");

  return ret;
}

int write_signed_number(const int64_t num, common::ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_NUM_LEN = 30;
  char buf[MAX_NUM_LEN] = {0};
  int len = snprintf(buf, MAX_NUM_LEN, "%ld", num);
  APPEND_STR(buffer, buf, len);
  return ret;
}

int write_unsigned_number(const uint64_t num, common::ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_NUM_LEN = 30;
  char buf[MAX_NUM_LEN] = {0};
  int len = snprintf(buf, MAX_NUM_LEN, "%lu", num);
  APPEND_STR(buffer, buf, len);
  return ret;
}

int write_string_no_escape(const ObString &str, common::ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  APPEND_STR(buffer, "\"");
  APPEND_STR(buffer, str);
  APPEND_STR(buffer, "\"");
  return ret;
}

bool is_number(const char *str)
{
  bool bool_ret = true;
  if (nullptr == str) {
    return false;
  }
  for (int64_t i = 0; true == bool_ret; ++i) {
    if ('\0' == str[i]) {
      break;
    } else if (!isdigit(str[i])) {
      bool_ret = false;
    }
  }
	return bool_ret;
}

const char *empty_str_wrapper(const char *str)
{
  return nullptr == str ? "" : str;
}

const char *record_type_to_str(const RecordType type)
{
  static const char *str = "UNKNOWN";

  switch (type)
  {
    case EDELETE:
      str = "DELETE";
      break;

    case EINSERT:
      str = "INSERT";
      break;

    case EUPDATE:
      str = "UPDATE";
      break;

    case EBEGIN:
      str = "BEGIN";
      break;

    case ECOMMIT:
      str = "COMMIT";
      break;

    case EDDL:
      str = "DDL";
      break;

    default:
      str = "NOT_SUPPORTED";
      break;
  }

  return str;
}

int64_t record_type_to_num(const RecordType type)
{
  int num = 0;
  switch (type)
  {
    case EINSERT:
      num = 1;
      break;

    case EUPDATE:
      num = 2;
      break;

    case EDELETE:
      num = 3;
      break;

    case EDDL:
      num = 4;
      break;

    case EBEGIN:
      num = 5;
      break;

    case ECOMMIT:
      num = 6;
      break;

    default:
      num = 0;
      break;
  }

  return num;

}

bool is_trans_end_record_type(const RecordType type)
{
  bool bret = false;
  switch (type) {
    case EDDL:
    case ECOMMIT:
    case HEARTBEAT:
      bret = true;
      break;
    default:
      bret = false;
      break;
  }
  return bret;
}

int parse_line(const char *key_str,
    const char *buf,
    const int64_t buf_len,
    int64_t &pos,
    int64_t &data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expect_token(buf, buf_len, pos, key_str))) {
    LOG_ERROR("failed to get key_str from buf", K(buf_len), K(pos), K(key_str));
  } else if (OB_FAIL(expect_token(buf, buf_len, pos, "="))) {
    LOG_ERROR("failed to get symbol '='", K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(parse_int(buf, buf_len, pos, data))) {
    LOG_ERROR("failed to parse int from buf", K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(expect_token(buf, buf_len, pos, "\n"))) {
    LOG_ERROR("failed to get token \\n", K(buf_len), K(pos));
  }

  return ret;
}

int parse_int(const char *buf, const int64_t data_len, int64_t &pos, int64_t &data)
{
  int ret = OB_SUCCESS;
  enum class ParseState {
    BLANK_READ,
    SYMBOL_READ,
    DIGIT_READ
  };
  ParseState state = ParseState::BLANK_READ;
  bool positive = true;
  bool done = false;
  int64_t orig_pos = pos;
  data = 0;

  while (pos < data_len && !done && OB_SUCC(ret)) {
    char c = buf[pos];
    switch(state) {
      case ParseState::BLANK_READ: {
        if (isblank(c)) {
        } else if (isdigit(c)) {
          state = ParseState::DIGIT_READ;
          data = todigit(c);
        } else if ('+' == c) {
          state = ParseState::SYMBOL_READ;
          positive = true;
        } else if ('-' == c) {
          state = ParseState::SYMBOL_READ;
          positive = false;
        } else {
          ret = OB_INVALID_DATA;
        }
        break;
      }

      case ParseState::SYMBOL_READ: {
        if (isdigit(c)) {
          state = ParseState::DIGIT_READ;
          if (positive) {
            data = todigit(c);
          } else {
            data = -(todigit(c));
          }
        } else {
          ret = OB_INVALID_DATA;
        }
        break;
      }

      case ParseState::DIGIT_READ: {
        if (isdigit(c)) {
          int digit = todigit(c);
          if (positive) {
            if (data > INT64_MAX/10) {
              ret = OB_SIZE_OVERFLOW;
            } else if (INT64_MAX/10 == data) {
              if (digit > INT64_MAX % 10) {
                ret = OB_SIZE_OVERFLOW;
              } else {
                data = data * 10 + digit;
              }
            } else {
              data = data * 10 + digit;
            }
          } else {
            if (data < INT64_MIN / 10) {
              ret = OB_SIZE_OVERFLOW;
            } else if (data == INT64_MIN/10) {
              if (digit > -((INT64_MIN % 10 + 10) % 10 - 10)) {
                ret = OB_SIZE_OVERFLOW;
              } else {
                data = data * 10 - digit;
              }
            } else {
              data = data * 10 - digit;
            }
          }
        } else {
          done = true;
        }
        break;
      }
    }

    if (!done && OB_SUCC(ret)) {
      pos++;
    }

  }

  if (OB_FAIL(ret)) {
    pos = orig_pos;
  }


  return ret;
}

int parse_line(const char *key_str,
    const char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObIAllocator &alloc,
    char *&value)
{
  int ret = OB_SUCCESS;
  ObString tmp_str;
  if (OB_FAIL(parse_line(key_str, buf, buf_len, pos, tmp_str))) {
    LOG_ERROR("failed to parse line to tmp_str", K(buf_len), K(pos), K(key_str));
  } else if (OB_FAIL(ob_dup_cstring(alloc, tmp_str, value))) {
    LOG_ERROR("failed to dup cstring to value", K(tmp_str));
  }
  return ret;
}

int parse_line(const char *key_str,
    const char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObString &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(expect_token(buf, buf_len, pos, key_str))) {
    LOG_ERROR("failed to get key_str from buf", K(buf_len), K(pos), K(key_str));
  } else if (OB_FAIL(expect_token(buf, buf_len, pos, "="))) {
    LOG_ERROR("failed to get symbol '='", K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(parse_string(buf, buf_len, pos, value))) {
    LOG_ERROR("failed to parse string from buf", K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(expect_token(buf, buf_len, pos, "\n"))) {
    LOG_ERROR("failed to get token \\n", K(buf_len), K(pos));
  }
  return ret;
}

int parse_string(const char *buf, const int64_t data_len, int64_t &pos, ObString &value)
{
  int ret = OB_SUCCESS;
  const char *end_ptr = static_cast<const char*>(memchr(buf + pos, '\n', data_len - pos));
  if (nullptr == end_ptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cannot find the end of line in buf", K(pos), K(data_len));
  } else {
    const int64_t value_len = end_ptr - (buf + pos);
    value.assign_ptr(buf + pos, value_len);
    pos += value_len;
  }
  return ret;
}

int expect_token(const char *buf, const int64_t data_len, int64_t &pos, const char *token)
{
  int ret = OB_SUCCESS;

  int64_t token_len = strlen(token);

  if (pos + token_len > data_len) {
    ret = OB_SIZE_OVERFLOW;
  } else if (0 != memcmp(buf + pos, token, token_len)) {
    ret = OB_INVALID_DATA;
  } else {
    pos += token_len;
  }

  return ret;
}

int deep_copy_cstring(ObIAllocator &alloc, const char *src_str, char *&dst_str)
{
  int ret = OB_SUCCESS;

  if (nullptr != dst_str) {
    ret = OB_ERR_UNEXPECTED;
  } else if (nullptr == src_str) {
    // do nothing
  } else {
    const int64_t src_str_len = strlen(src_str);
    if (OB_ISNULL(dst_str = static_cast<char*>(alloc.alloc(src_str_len + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(dst_str, src_str, src_str_len);
      dst_str[src_str_len] = '\0';
    }
  }

  return ret;
}

int uint_to_bit(const uint64_t bit_uint, ObStringBuffer &bit_str)
{
  int ret = OB_SUCCESS;
  const int bits = 64;
  int i = bits-1;
  if (0 == bit_uint) {
    if (OB_FAIL(bit_str.append("0"))) {
      LOG_ERROR("bit_str append failed", K(bit_str));
    }
  } else {
    // ignore leading zeroes
    while (0 == (bit_uint & (1ull << i)) && 0 <= i) {
      i--;
    }
    while (OB_SUCC(ret) && 0 <= i) {
      if (OB_FAIL(bit_str.append((bit_uint & (1ull << i)) ? "1" : "0"))) {
        LOG_ERROR("bit_str append failed", K(bit_str));
      }
      i--;
    }
  }
  return ret;
}

int todigit(char c)
{
  return c - '0';
}



}
}

#undef APPEND_STR