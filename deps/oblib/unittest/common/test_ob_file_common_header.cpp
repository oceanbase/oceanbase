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

#include <gtest/gtest.h>
#include "common/ob_file_common_header.h"

namespace oceanbase
{
namespace common
{

class TestObFileCommonHeader : public ::testing::Test
{
public:
  class TestPayload
  {
  public:
    static const int16_t TEST_PAYLOAD_VERSION = 1;
    OB_UNIS_VERSION(TEST_PAYLOAD_VERSION);
  public:
    TestPayload() : a_(0), b_(0) {}
    virtual ~TestPayload() {}
    TO_STRING_KV(K_(a), K_(b));
  public:
    int64_t a_;
    int64_t b_;
  };
  TestObFileCommonHeader() {}
  virtual ~TestObFileCommonHeader() {}
  virtual void SetUp() override {}
  virtual void TearDown() override {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestObFileCommonHeader);
};

OB_SERIALIZE_MEMBER(TestObFileCommonHeader::TestPayload, a_, b_);

TEST(TestObFileCommonHeader, test_header)
{
  // generate one test_payload object
  TestObFileCommonHeader::TestPayload test_payload;
  test_payload.a_ = 1;
  test_payload.b_ = 2;

  // serialize
  ObFileCommonHeader header;
  const int64_t header_length = header.get_serialize_size();
  const int64_t payload_length = test_payload.get_serialize_size();
  const int64_t total_length = header_length + payload_length;
  char buf[total_length];
  int64_t header_pos = 0;
  int64_t data_pos = header_length;
  ASSERT_EQ(OB_SUCCESS, test_payload.serialize(buf, total_length, data_pos));
  ASSERT_EQ(data_pos, total_length);
  header.payload_version_ = TestObFileCommonHeader::TestPayload::TEST_PAYLOAD_VERSION;
  header.payload_length_ = static_cast<int32_t>(payload_length);
  header.payload_zlength_ = header.payload_length_;
  header.payload_checksum_ = ob_crc64(buf + header_length, payload_length);
  header.set_header_checksum();
  ASSERT_EQ(OB_SUCCESS, header.serialize(buf, total_length, header_pos));
  ASSERT_EQ(header_pos, header_length);

  // deserialize
  ObFileCommonHeader de_header;
  TestObFileCommonHeader::TestPayload de_test_payload;
  int64_t de_pos = 0;
  ASSERT_EQ(OB_SUCCESS, de_header.deserialize(buf, total_length, de_pos));
  ASSERT_EQ(OB_SUCCESS, de_header.check_header_checksum());
  ASSERT_EQ(OB_SUCCESS, de_header.check_payload_checksum(buf + de_header.header_length_,
                                                         de_header.payload_length_));
  ASSERT_EQ(OB_SUCCESS, de_test_payload.deserialize(buf, total_length, de_pos));
  ASSERT_EQ(1, de_test_payload.a_);
  ASSERT_EQ(2, de_test_payload.b_);
}

TEST(TestObFileCommonHeader, test_util)
{
  // generate one test_payload object
  TestObFileCommonHeader::TestPayload test_payload;
  test_payload.a_ = 1;
  test_payload.b_ = 2;

  // serialize
  ObFileCommonHeader header;
  const int64_t header_length = header.get_serialize_size();
  const int64_t payload_length = test_payload.get_serialize_size();
  const int64_t total_length = header_length + payload_length;
  char buf[total_length];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObFileCommonHeaderUtil::serialize<TestObFileCommonHeader::TestPayload>(
                                                test_payload,
                                                TestObFileCommonHeader::TestPayload::TEST_PAYLOAD_VERSION,
                                                buf, total_length, pos));
  ASSERT_EQ(pos, total_length);

  // deserialize
  ObFileCommonHeader de_header;
  TestObFileCommonHeader::TestPayload de_test_payload;
  int64_t de_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObFileCommonHeaderUtil::deserialize<TestObFileCommonHeader::TestPayload>(
                                                buf, total_length, de_pos, de_test_payload));
  ASSERT_EQ(de_pos, total_length);
  ASSERT_EQ(1, de_test_payload.a_);
  ASSERT_EQ(2, de_test_payload.b_);
}

} //end namespace common
} //end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f ./test_ob_file_common_header.log*");
  OB_LOGGER.set_file_name("test_ob_file_common_header.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
