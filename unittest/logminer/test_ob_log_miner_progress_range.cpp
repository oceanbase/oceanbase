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

#include "ob_log_miner_progress_range.h"
#include "gtest/gtest.h"

namespace oceanbase
{
namespace oblogminer
{

TEST(test_log_miner_progress_range, SerializeFunc)
{
  ObLogMinerProgressRange range, range1;
  const int64_t buf_len = 100;
  char buf[buf_len];
  int64_t pos = 0;
  const char *buf1 = "MIN_COMMIT_TS=1\nMAX_COMMIT_TS=2\n";
  EXPECT_EQ(OB_SUCCESS, range.deserialize(buf1, strlen(buf1), pos));
  EXPECT_EQ(range.min_commit_ts_, 1);
  EXPECT_EQ(range.max_commit_ts_, 2);
  pos = 0;
  const char *buf2 = "MIN_COMMIT_TS=aaa\nMAX_COMMIT_TS=2\n";
  EXPECT_EQ(OB_INVALID_DATA, range.deserialize(buf2, strlen(buf2), pos));
  pos = 0;
  const char *buf3 = "MIN_COMMIT_TS:1\nMAX_COMMIT_TS:2\n";
  EXPECT_EQ(OB_INVALID_DATA, range.deserialize(buf3, strlen(buf3), pos));
  pos = 0;
  const char *buf4 = "MIN_COMMIT_TS=1MAX_COMMIT_TS=2\n";
  EXPECT_EQ(OB_INVALID_DATA, range.deserialize(buf4, strlen(buf4), pos));
  pos = 0;
  for (int i = 0; i < 10000; i++) {
    range1.min_commit_ts_ = rand();
    range1.max_commit_ts_ = rand();
    EXPECT_EQ(range1.serialize(buf, buf_len, pos), OB_SUCCESS);
    EXPECT_EQ(pos, range1.get_serialize_size());
    pos = 0;
    EXPECT_EQ(range.deserialize(buf, buf_len, pos), OB_SUCCESS);
    EXPECT_EQ(range.get_serialize_size(), pos);
    EXPECT_EQ(range, range1);
    pos = 0;
  }
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_log_miner_progress_range.log");
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_miner_progress_range.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
