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

#include "logservice/palf/log_define.h"
#include "logservice/palf/lsn.h"
#include <gtest/gtest.h>

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{

TEST(TestLSN, test_log_pos)
{
  const int64_t BUFSIZE = 1 << 21;
  char buf[BUFSIZE];
  LSN lsn_invalid; lsn_invalid.val_ = LOG_INVALID_LSN_VAL;
  LSN lsn1; lsn1.val_ = 1;
  LSN lsn2; lsn2.val_ = 2;
  LSN lsn3; lsn3.val_ = 2;
  LSN lsn4;
  // Test invalid argument
  EXPECT_FALSE(lsn_invalid.is_valid());

  // Test operator <
  EXPECT_TRUE(lsn1 < lsn2);
  EXPECT_FALSE(lsn2 < lsn3);

  EXPECT_EQ(LSN(1+PALF_BLOCK_SIZE), lsn1+PALF_BLOCK_SIZE);
  EXPECT_EQ(LSN(1), LSN(1+PALF_BLOCK_SIZE) - PALF_BLOCK_SIZE);
  EXPECT_EQ(1, lsn2 - lsn1);
  EXPECT_TRUE(lsn1 != lsn2);
  EXPECT_TRUE(lsn2 == lsn3);

  // Test serialization and deserialization
  int64_t pos = 0;
  EXPECT_EQ(OB_SUCCESS, lsn1.serialize(buf, BUFSIZE, pos));
  EXPECT_EQ(pos, lsn1.get_serialize_size());
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, lsn4.deserialize(buf, BUFSIZE, pos));
  EXPECT_EQ(lsn1, lsn4);
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_pos.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_pos");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
