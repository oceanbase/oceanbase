/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "ob_redis_parser.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{
////////////////////////////// ObRedisParser //////////////////////////////
int ObRedisParser::decode(ObIAllocator &allocator, const ObString &redis_msg, ObString &cmd_name,
                          ObArray<ObString> &args)
{
  int ret = OB_SUCCESS;
  ObRedisDecoder decoder(allocator, redis_msg);
  if (OB_FAIL(decoder.decode())) {
    LOG_WARN("fail to decode redis message", K(ret), K(redis_msg));
  } else {
    ret = decoder.get_result(cmd_name, args);
  }
  return ret;
}

int ObRedisParser::encode_with_flag(const char flag, const ObString &msg, ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(buffer.reserve(ObRedisUtil::FLAG_LEN + msg.length() + ObRedisUtil::CRLF_LEN))) {
    LOG_WARN("fail to reserve memory for string buffer", K(ret), K(msg));
  } else if (OB_FAIL(buffer.append(&flag, ObRedisUtil::FLAG_LEN))) {
    LOG_WARN("fail to append flag", K(ret), K(msg));
  } else if (OB_FAIL(buffer.append(msg))) {
    LOG_WARN("fail to append msg", K(ret), K(msg));
  } else if (OB_FAIL(buffer.append(ObRedisUtil::REDIS_CRLF))) {
    LOG_WARN("fail to append REDIS_CRLF", K(ret), K(msg));
  }
  return ret;
}

// "-" + "msg" + "\r\n"
int ObRedisParser::encode_error(ObIAllocator &allocator, const ObString &err_msg,
                                ObString &encoded_msg)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buffer(&allocator);
  if (OB_FAIL(encode_with_flag(ObRedisUtil::SIMPLE_ERR_FLAG, err_msg, buffer))) {
    LOG_WARN("fail to encode err msg with flag", K(ret), K(err_msg));
  } else if (OB_FAIL(buffer.get_result_string(encoded_msg))) {
    LOG_WARN("fail to get encoded string", K(ret), K(err_msg));
  }
  return ret;
}

// "+" + "msg" + "\r\n"
int ObRedisParser::encode_simple_string(ObIAllocator &allocator, const ObString &simpe_str,
                                        ObString &encoded_msg)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buffer(&allocator);
  if (OB_FAIL(encode_with_flag(ObRedisUtil::SIMPLE_STR_FLAG, simpe_str, buffer))) {
    LOG_WARN("fail to encode simple msg with flag", K(ret), K(simpe_str));
  } else if (OB_FAIL(buffer.get_result_string(encoded_msg))) {
    LOG_WARN("fail to get encoded string", K(ret), K(simpe_str));
  }
  return ret;
}

// ":" + string(integer) + "\r\n"
int ObRedisParser::encode_integer(ObIAllocator &allocator, const int64_t integer,
                                  ObString &encoded_msg)
{
  int ret = OB_SUCCESS;
  ObFastFormatInt ffi(integer);
  ObString int_str(ffi.length(), ffi.ptr());
  ObStringBuffer buffer(&allocator);
  if (OB_FAIL(encode_with_flag(ObRedisUtil::INT_FLAG, int_str, buffer))) {
    LOG_WARN("fail to encode simple msg with flag", K(ret), K(int_str));
  } else if (OB_FAIL(buffer.get_result_string(encoded_msg))) {
    LOG_WARN("fail to get encoded string", K(ret), K(int_str));
  }
  return ret;
}

int ObRedisParser::inner_encode_bulk_string(const ObString &bulk_str, ObStringBuffer &buffer)
{
  int ret = OB_SUCCESS;
  ObFastFormatInt ffi(bulk_str.length());
  ObString int_str(ffi.length(), ffi.ptr());

  if (OB_FAIL(buffer.reserve(ObRedisUtil::FLAG_LEN + bulk_str.length() + int_str.length()
                             + ObRedisUtil::CRLF_LEN * 2))) {
    LOG_WARN("fail to reserve memory for string buffer", K(ret), K(bulk_str), K(int_str));
  } else if (OB_FAIL(encode_with_flag(ObRedisUtil::BULK_FLAG, int_str, buffer))) {
    LOG_WARN("fail to encode simple msg with flag", K(ret), K(int_str));
  } else if (OB_FAIL(buffer.append(bulk_str))) {
    LOG_WARN("fail to append msg", K(ret), K(bulk_str), K(int_str));
  } else if (OB_FAIL(buffer.append(ObRedisUtil::REDIS_CRLF))) {
    LOG_WARN("fail to append REDIS_CRLF", K(ret), K(bulk_str), K(int_str));
  }
  return ret;
}

// "$" + string(length) + "\r\n" + bulk_str + "\r\n"
int ObRedisParser::encode_bulk_string(ObIAllocator &allocator, const ObString &bulk_str,
                                      ObString &encoded_msg)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buffer(&allocator);
  if (OB_FAIL(inner_encode_bulk_string(bulk_str, buffer))) {
    LOG_WARN("fail to encode bulk string", K(ret), K(bulk_str));
  } else if (OB_FAIL(buffer.get_result_string(encoded_msg))) {
    LOG_WARN("fail to get encoded string", K(ret), K(bulk_str));
  }
  return ret;
}

// "*" + string(n) + "\r\n" + bulk_str_1 + ... + bulk_str_n
int ObRedisParser::encode_array(ObIAllocator &allocator, const ObIArray<ObString> &array,
                                ObString &encoded_msg)
{
  int ret = OB_SUCCESS;
  ObFastFormatInt ffi(array.count());
  ObString int_str(ffi.length(), ffi.ptr());
  ObStringBuffer buffer(&allocator);
  if (OB_FAIL(encode_with_flag(ObRedisUtil::ARRAY_FLAG, int_str, buffer))) {
    LOG_WARN("fail to encode simple msg with flag", K(ret), K(int_str));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
    if (OB_FAIL(inner_encode_bulk_string(array.at(i), buffer))) {
      LOG_WARN("fail to get encoded string", K(ret), K(i), K(array.at(i)));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(buffer.get_result_string(encoded_msg))) {
    LOG_WARN("fail to get encoded string", K(ret), K(array.count()));
  }
  return ret;
}

////////////////////////////// ObRedisDecoder //////////////////////////////

int ObRedisDecoder::decode()
{
  int ret = OB_SUCCESS;
  ObString header;
  if (OB_FAIL(read_until_crlf(header))) {
    LOG_WARN("fail to read until REDIS_CRLF", K(ret), K(*this));
  } else if (isalpha(header[0])) {
    // inline command
    cmd_name_ = header;
  } else if (header[0] == ObRedisUtil::ARRAY_FLAG) {
    // array
    if (OB_FAIL(decode_array(header))) {
      LOG_WARN("fail to decode array", K(ret), K(*this));
    }
  } else {
    ret = OB_KV_REDIS_PARSE_ERROR;
    LOG_WARN("invalid header", K(ret), K(*this));
  }
  // At the end of decoding all characters should be read
  if (OB_SUCC(ret) && cur_pos_ != length_) {
    ret = OB_KV_REDIS_PARSE_ERROR;
    LOG_WARN("invalid length of redis array", K(ret), K(*this));
  }
  return ret;
}

// decode array, input header format: *3\r\n
int ObRedisDecoder::decode_array(const ObString &header)
{
  int ret = OB_SUCCESS;
  bool is_valid;
  uint64_t argc = ObFastAtoi<uint64_t>::atoi(
      header.ptr() + ObRedisUtil::FLAG_LEN, header.ptr() + header.length(), is_valid);
  if (!is_valid) {
    ret = OB_KV_REDIS_PARSE_ERROR;
    LOG_WARN("invalid length of redis array", K(ret), K(*this), K(header));
  } else if (argc == 0) {
    // do nothing, return empty args_
  } else if (OB_FAIL(args_.reserve(argc - 1))) {
    LOG_WARN("fail to reserve space for args_", K(ret), K(*this));
  }
  for (uint64_t i = 0; OB_SUCC(ret) && i < argc; ++i) {
    ObString bulk_str;
    if (OB_FAIL(decode_bulk_string(bulk_str))) {
      LOG_WARN("fail to decode bulk string", K(ret), K(*this));
    } else if (i == 0) {
      cmd_name_ = bulk_str;
    } else if (OB_FAIL(args_.push_back(bulk_str))) {
      LOG_WARN("fail to push back string", K(ret), K(*this), K(bulk_str));
    }
  }
  return ret;
}

int ObRedisDecoder::decode_bulk_string(ObString &bulk_str)
{
  int ret = OB_SUCCESS;
  ObString bulk_len_str;
  if (OB_FAIL(read_until_crlf(bulk_len_str))) {
    LOG_WARN("fail to read until REDIS_CRLF", K(ret), K(*this));
  } else if (bulk_len_str[0] != ObRedisUtil::BULK_FLAG) {
    ret = OB_KV_REDIS_PARSE_ERROR;
    LOG_WARN("fail to decode bulk string", K(ret), K(*this), K(bulk_len_str));
  } else {
    bool is_valid;
    int32_t bulk_len = ObFastAtoi<int32_t>::atoi(bulk_len_str.ptr() + ObRedisUtil::FLAG_LEN,
                                                 bulk_len_str.ptr() + bulk_len_str.length(),
                                                 is_valid);
    if (!is_valid || bulk_len < 0) {
      ret = OB_KV_REDIS_PARSE_ERROR;
      LOG_WARN("invalid length of redis array", K(ret), K(*this), K(bulk_len_str));
    } else if (OB_FAIL(ob_sub_str(
                   *allocator_, redis_msg_, cur_pos_, cur_pos_ + bulk_len - 1, bulk_str))) {
      LOG_WARN("fail to do ob_sub_str", K(ret));
    } else {
      cur_pos_ += bulk_len + ObRedisUtil::CRLF_LEN;
    }
  }
  return ret;
}

// return splited containing the data up to not including "\r\n"
int ObRedisDecoder::read_until_crlf(ObString &splited)
{
  int ret = OB_SUCCESS;
  const char *lf_pos = static_cast<const char *>(
      memchr(redis_msg_ + cur_pos_, ObRedisUtil::REDIS_LF, length_ - cur_pos_));
  if (lf_pos == NULL) {
    ret = OB_KV_REDIS_PARSE_ERROR;
    LOG_WARN("fail to find '\\n' in redis command", K(ret), K(*this));
  } else {
    const int64_t header_len = lf_pos - redis_msg_ - cur_pos_ + 1;
    // include "\r\n"
    ObString split_with_crlf(header_len, redis_msg_ + cur_pos_);
    if (header_len < ObRedisUtil::HEADER_LEN
        || split_with_crlf[header_len - 2] != ObRedisUtil::REDIS_CR) {
      ret = OB_KV_REDIS_PARSE_ERROR;
      LOG_USER_ERROR(OB_KV_REDIS_PARSE_ERROR, length_, redis_msg_);
      LOG_WARN("fail to parse redis command, insufficient header length",
               K(ret),
               K(*this),
               K(header_len));
    } else if (OB_FAIL(ob_sub_str(*allocator_,
                                  split_with_crlf,
                                  0,
                                  split_with_crlf.length() - ObRedisUtil::CRLF_LEN - 1,
                                  splited))) {
      LOG_WARN("fail to do ob_sub_str", K(ret), K(split_with_crlf));
    } else {
      cur_pos_ += header_len;
    }
  }
  return ret;
}

int ObRedisDecoder::get_result(ObString &cmd_name, ObArray<ObString> &args)
{
  int ret = common::OB_SUCCESS;
  args = args_;
  // Method to uppercase
  if (!cmd_name_.empty() && OB_FAIL(ob_simple_low_to_up(*allocator_, cmd_name_, cmd_name))) {
    LOG_WARN("fail to case up cmd_name", KR(ret), K(cmd_name_));
  }
  return ret;
}

}  // end namespace table
}  // end namespace oceanbase
