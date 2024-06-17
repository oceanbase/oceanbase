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

#include "ob_log_miner_analyzer_checkpoint.h"
#include "lib/oblog/ob_log.h"
#include "gtest/gtest.h"

namespace oceanbase
{
namespace oblogminer
{

TEST(test_log_miner_analyzer_ckpt, SerializeFunc)
{
  ObLogMinerCheckpoint ckpt, ckpt1;
  int64_t pos = 0;
  const char *buf1 = "PROGRESS=1\nCUR_FILE_ID=3\nMAX_FILE_ID=2\n";
  char tmp_buf[1000];
  EXPECT_EQ(OB_SUCCESS, ckpt.deserialize(buf1, strlen(buf1), pos));
  EXPECT_EQ(ckpt.progress_, 1);
  EXPECT_EQ(ckpt.cur_file_id_, 3);
  EXPECT_EQ(ckpt.max_file_id_, 2);
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, ckpt.serialize(tmp_buf, 1000, pos));
  tmp_buf[pos+1] = '\0';
  EXPECT_STREQ(buf1, tmp_buf);
  pos = 0;
  const char *buf2 = "PROGRESS=a\nCUR_FILE_ID=3\nMAX_FILE_ID=2\n";
  EXPECT_EQ(OB_INVALID_DATA, ckpt.deserialize(buf2, strlen(buf2), pos));
  pos = 0;
  const char *buf3 = "PROGRESS=1\nCUR_FILE_ID=3MAX_FILE_ID=2\n";
  EXPECT_EQ(OB_INVALID_DATA, ckpt.deserialize(buf3, strlen(buf3), pos));
  pos = 0;

  for (int i = 0; i < 10000; i++) {
    int64_t pos1 = 0;
    pos = 0;
    ckpt.cur_file_id_ = rand();
    ckpt.max_file_id_ = rand();
    ckpt.progress_ = rand();
    EXPECT_EQ(OB_SUCCESS, ckpt.serialize(tmp_buf, 1000, pos));
    EXPECT_EQ(pos, ckpt.get_serialize_size());
    EXPECT_EQ(OB_SUCCESS, ckpt1.deserialize(tmp_buf, pos, pos1));
    EXPECT_EQ(ckpt, ckpt1);
  }
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_log_miner_analyzer_checkpoint.log");
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_miner_analyzer_checkpoint.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
