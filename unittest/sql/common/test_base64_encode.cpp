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
#include "share/ob_errno.h"

#include <gtest/gtest.h>
#include <cstdlib>
#include <iostream>
#include <string>
#include <cstring>
#include "share/backup/ob_archive_piece.h"

namespace oceanbase
{
namespace common
{
class TestBase64Encoder: public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

TEST_F(TestBase64Encoder, str_encode)
{
  const char* test_str = "hello, this is a test";
  const char* expect_str = "aGVsbG8sIHRoaXMgaXMgYSB0ZXN0";
  int64_t encode_buf_len = ObBase64Encoder::needed_encoded_length(strlen(test_str));
  ASSERT_EQ(encode_buf_len, strlen(expect_str));

  char *encoded_buffer = (char*)std::malloc(encode_buf_len);
  int64_t encoded_pos = 0;

  ASSERT_EQ(OB_SUCCESS, ObBase64Encoder::encode((const uint8_t *)test_str, strlen(test_str),
                                                encoded_buffer, encode_buf_len, encoded_pos));
  ASSERT_EQ(0, std::strncmp(encoded_buffer, expect_str, strlen(expect_str)));

  int64_t decoded_buf_len = ObBase64Encoder::needed_decoded_length(encoded_pos);
  uint8_t *decoded_buf = (uint8_t *)std::malloc(decoded_buf_len);
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObBase64Encoder::decode(encoded_buffer, encoded_pos,
                                                decoded_buf,
                                                decoded_buf_len, decode_pos));
  ASSERT_EQ(decode_pos, strlen(test_str));
  ASSERT_EQ(0, std::strncmp((char *)decoded_buf, test_str, strlen(test_str)));
}

TEST_F(TestBase64Encoder, bin_encode)
{
  uint8_t bin_test[] = {254, 253, 253, 251, 100, 102, 103, 178, 179, 180};
  const char* expect_str = "/v39+2RmZ7KztA==";
  int64_t encoded_buf_len = ObBase64Encoder::needed_encoded_length(sizeof(bin_test));
  ASSERT_EQ(16, encoded_buf_len);

  char *encoded_buffer = (char *)std::malloc(encoded_buf_len);
  int64_t encoded_pos = 0;

  ASSERT_EQ(OB_SUCCESS, ObBase64Encoder::encode(bin_test, sizeof(bin_test),
                                                 encoded_buffer, encoded_buf_len, encoded_pos));
  ASSERT_EQ(0, std::strncmp(expect_str, encoded_buffer, strlen(expect_str)));

  int64_t decoded_buf_len = ObBase64Encoder::needed_decoded_length(encoded_pos);
  uint8_t *decoded_buffer = (uint8_t *)std::malloc(decoded_buf_len);
  int64_t decoded_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObBase64Encoder::decode(encoded_buffer, encoded_pos,
                                                decoded_buffer, decoded_buf_len, decoded_pos));
  ASSERT_EQ(sizeof(bin_test), decoded_pos);
  ASSERT_EQ(0, std::memcmp(bin_test, decoded_buffer, decoded_pos));
}
} // end oceanbase common
} // end namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
