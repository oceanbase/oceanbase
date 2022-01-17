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

#include "clog/ob_log_checksum_V2.h"
#include "gtest/gtest.h"
namespace oceanbase {
namespace common {
class ObPartitionKey;
}
namespace clog {
class ObLogChecksum;
}
namespace unittest {
class TestObLogChecksum : public ::testing::Test {
protected:
  void SetUp()
  {
    const uint64_t TABLE_ID = 520;
    const int32_t PARTITION_IDX = 1314;
    const int32_t PARTITION_CNT = 1919;
    partition_key_.init(TABLE_ID, PARTITION_IDX, PARTITION_CNT);
  }
  void TearDown()
  {}

protected:
  common::ObPartitionKey partition_key_;
  clog::ObLogChecksum check_sum_;
};
TEST_F(TestObLogChecksum, init_test)
{
  const int64_t ACCUM_CHECKSUM = 12345;
  EXPECT_EQ(common::OB_SUCCESS, check_sum_.init(1, ACCUM_CHECKSUM, partition_key_));
}

TEST_F(TestObLogChecksum, acquire_accum_checksum_test)
{
  const int64_t DATA_CHECKSUM = 123456;
  int64_t accum_checksum = 0;
  const int64_t ACCUM_CHECKSUM = 12345;
  EXPECT_EQ(common::OB_SUCCESS, check_sum_.init(1, ACCUM_CHECKSUM, partition_key_));
  EXPECT_EQ(common::OB_SUCCESS, check_sum_.acquire_accum_checksum(DATA_CHECKSUM, accum_checksum));
}
TEST_F(TestObLogChecksum, verify_accum_checksum_test)
{
  const int64_t DATA_CHECKSUM = 123456;
  int64_t accum_checksum = 0;
  const int64_t ACCUM_CHECKSUM = 12345;
  EXPECT_EQ(common::OB_SUCCESS, check_sum_.init(1, ACCUM_CHECKSUM, partition_key_));
  EXPECT_EQ(common::OB_SUCCESS, check_sum_.acquire_accum_checksum(DATA_CHECKSUM, accum_checksum));
  EXPECT_EQ(common::OB_SUCCESS, check_sum_.verify_accum_checksum(DATA_CHECKSUM, accum_checksum));
  EXPECT_EQ(common::OB_CHECKSUM_ERROR, check_sum_.verify_accum_checksum(DATA_CHECKSUM, ACCUM_CHECKSUM));
}
TEST_F(TestObLogChecksum, set_get_test)
{
  const int64_t ACCUM_CHECKSUM = 12345;
  EXPECT_EQ(common::OB_SUCCESS, check_sum_.init(1, ACCUM_CHECKSUM, partition_key_));
  check_sum_.set_accum_checksum(ACCUM_CHECKSUM);
  EXPECT_EQ(ACCUM_CHECKSUM, check_sum_.get_verify_checksum());
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  //  OB_LOGGER.set_file_name("test_log_allocator.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
