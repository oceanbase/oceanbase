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

#include <gtest/gtest.h>

#include "src/share/table/redis/ob_redis_parser.h"
#include "src/share/ob_errno.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{
class TestRedisParser : public::testing::Test
{
public:
  TestRedisParser() {}
  virtual ~TestRedisParser() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestRedisParser, test_decode_inline) {
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString msg = "Monitor\r\n";
  ObString redis_msg;
  ob_write_string(allocator, msg, redis_msg);
  char cmd_name_buf_[64] = {};
  ObString method;
  method.assign_buffer(cmd_name_buf_, sizeof(cmd_name_buf_));
  ObArray<ObString> args;
  ASSERT_EQ(OB_SUCCESS, ObRedisParser::decode(redis_msg, method, args));
  ASSERT_EQ(0, args.size());
  ObString expected = "monitor";
  ASSERT_EQ(method == expected, true);
}

TEST_F(TestRedisParser, test_decode_array) {
  ObString msg = "*2\r\n$3\r\nGET\r\n$6\r\nfoobar\r\n";
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString redis_msg;
  ob_write_string(allocator, msg, redis_msg);
  char cmd_name_buf_[64] = {};
  ObString method;
  method.assign_buffer(cmd_name_buf_, sizeof(cmd_name_buf_));
  ObArray<ObString> args;
  ASSERT_EQ(OB_SUCCESS, ObRedisParser::decode(redis_msg, method, args));
  ASSERT_EQ(1, args.size());
  ObString expected1 = "get";
  ASSERT_EQ(method == expected1, true);
  ObString expected2 = "foobar";
  ASSERT_EQ(args[0] == expected2, true);
}

TEST_F(TestRedisParser, test_decode_err) {
  char cmd_name_buf_[64] = {};
  ObString method;
  method.assign_buffer(cmd_name_buf_, sizeof(cmd_name_buf_));
  ObArray<ObString> args;
  // invalid length
  ObString msg = "*3\r\n$3\r\nGET\r\n$6\r\nfoobar\r\n";
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString redis_msg;
  ob_write_string(allocator, msg, redis_msg);
  ASSERT_EQ(OB_KV_REDIS_PARSE_ERROR, ObRedisParser::decode(redis_msg, method, args));
  msg = "*1\r\n$3\r\nGET\r\n$6\r\nfoobar\r\n";
  ob_write_string(allocator, msg, redis_msg);
  ASSERT_EQ(OB_KV_REDIS_PARSE_ERROR, ObRedisParser::decode(redis_msg, method, args));

  // invalid end char
  msg = "$2\r\n$3\r\nGET\r\n$6\nfoobar\r\n";
  ob_write_string(allocator, msg, redis_msg);
  ASSERT_EQ(OB_KV_REDIS_PARSE_ERROR, ObRedisParser::decode(redis_msg, method, args));
  msg = "Monitor\n";
  ob_write_string(allocator, msg, redis_msg);
  ASSERT_EQ(OB_KV_REDIS_PARSE_ERROR, ObRedisParser::decode(redis_msg, method, args));

  // invalid header
  msg = "-2\r\n$3\r\nGET\r\n$6\nfoobar\r\n";
  ob_write_string(allocator, msg, redis_msg);
  ASSERT_EQ(OB_KV_REDIS_PARSE_ERROR, ObRedisParser::decode(redis_msg, method, args));
}

TEST_F(TestRedisParser, test_encode_simple_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString str = "OK";
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, ObRedisParser::encode_simple_string(allocator, str, encoded));
  ObString expected = "+OK\r\n";
  ASSERT_EQ(expected == encoded, true);
}

TEST_F(TestRedisParser, test_encode_simple_error)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString str = "ERR";
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, ObRedisParser::encode_error(allocator, str, encoded));
  ObString expected = "-ERR\r\n";
  ASSERT_EQ(expected == encoded, true);
}

TEST_F(TestRedisParser, test_encode_integer)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  int64_t i = 1234568789;
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, ObRedisParser::encode_integer(allocator, i, encoded));
  ObString expected = ":1234568789\r\n";
  ASSERT_EQ(expected == encoded, true);

  i = -1234568789;
  ASSERT_EQ(OB_SUCCESS, ObRedisParser::encode_integer(allocator, i, encoded));
  expected = ":-1234568789\r\n";
  ASSERT_EQ(expected == encoded, true);
}

TEST_F(TestRedisParser, test_encode_bulk_string)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString str = "This is a bulk string";
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, ObRedisParser::encode_bulk_string(allocator, str, encoded));
  ObString expected = "$21\r\nThis is a bulk string\r\n";
  ASSERT_EQ(expected == encoded, true);
}

TEST_F(TestRedisParser, test_encode_array)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObArray<ObString> arr;
  ASSERT_EQ(OB_SUCCESS, arr.push_back("hello world"));
  ASSERT_EQ(OB_SUCCESS, arr.push_back("foobarbaz"));
  ObString encoded;
  ASSERT_EQ(OB_SUCCESS, ObRedisParser::encode_array(allocator, arr, encoded));
  ObString expected = "*2\r\n$11\r\nhello world\r\n$9\r\nfoobarbaz\r\n";
  std::cout << expected.ptr() << std::endl;
  std::cout << encoded.ptr() << std::endl;
  ASSERT_EQ(expected == encoded, true);
}
}
}

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_redis_parser.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
