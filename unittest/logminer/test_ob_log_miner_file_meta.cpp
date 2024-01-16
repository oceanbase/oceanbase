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

#include "ob_log_miner_file_meta.h"
#include "gtest/gtest.h"

namespace oceanbase
{
namespace oblogminer
{

TEST(test_log_miner_file_meta, SerializeFunc)
{
  ObLogMinerFileMeta file_meta, file_meta1;
  const int64_t buf_len = 300;
  char buf[buf_len];
  int64_t pos = 0;
  const char *buf1 = "MIN_COMMIT_TS=1\nMAX_COMMIT_TS=2\nDATA_LEN=3\n";
  EXPECT_EQ(OB_SUCCESS, file_meta.deserialize(buf1, strlen(buf1), pos));
  EXPECT_EQ(pos, strlen(buf1));
  EXPECT_EQ(file_meta.range_.min_commit_ts_, 1);
  EXPECT_EQ(file_meta.range_.max_commit_ts_, 2);
  EXPECT_EQ(file_meta.data_length_, 3);
  pos = 0;
  const char *buf2 = "MIN_COMMIT_TS=1a\nMAX_COMMIT_TS=2\nDATA_LEN=3\n";
  EXPECT_EQ(OB_INVALID_DATA, file_meta.deserialize(buf2, strlen(buf2), pos));
  pos = 0;
  const char *buf3 = "MIN_COMMIT_TS=11\nMAX_COMMIT_TS=2\nDATA_LEN=3";
  EXPECT_EQ(OB_SIZE_OVERFLOW, file_meta.deserialize(buf3, strlen(buf3), pos));
  pos = 0;

  for (int i = 0; i < 10000; i++) {
    file_meta1.data_length_ = rand();
    file_meta1.range_.min_commit_ts_ = rand();
    file_meta1.range_.max_commit_ts_ = rand();
    EXPECT_EQ(OB_SUCCESS, file_meta1.serialize(buf, buf_len, pos));
    EXPECT_EQ(pos, file_meta1.get_serialize_size());
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, file_meta.deserialize(buf, buf_len, pos));
    EXPECT_EQ(file_meta, file_meta1);
    pos = 0;
  }
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_log_miner_file_meta.log");
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_miner_file_meta.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
