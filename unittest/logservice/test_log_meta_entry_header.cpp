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

#include "logservice/palf/log_meta_entry_header.h"   // LogMetaEntryHeader
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace palf;

TEST(TestLogMetaEntryHeader, test_log_meta_entry_header)
{
  const int64_t BUFSIZE = 1 << 21;
  LogMetaEntryHeader log_meta_entry_header1;
  char buf[BUFSIZE] = "hello world";
  int64_t buf_len = strlen(buf);
  char buf_ser[BUFSIZE];
  int64_t pos = 0;
  // Test invalid argument
  EXPECT_FALSE(log_meta_entry_header1.is_valid());
  EXPECT_EQ(OB_SUCCESS, log_meta_entry_header1.generate(buf, buf_len));
  EXPECT_TRUE(log_meta_entry_header1.is_valid());

  // Test integrity
  EXPECT_TRUE(log_meta_entry_header1.check_integrity(buf, buf_len));

  // Test serialize and deserialize
  EXPECT_EQ(OB_SUCCESS, log_meta_entry_header1.serialize(buf_ser, BUFSIZE, pos));
  EXPECT_EQ(pos, log_meta_entry_header1.get_serialize_size());
  pos = 0;
  LogMetaEntryHeader log_meta_entry_header2;
  EXPECT_EQ(OB_SUCCESS, log_meta_entry_header2.deserialize(buf_ser, BUFSIZE, pos));
  EXPECT_EQ(log_meta_entry_header1, log_meta_entry_header2);
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_meta_entry_header.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_meta_entry_header");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
