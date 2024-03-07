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

#define private public
#include "ob_log_miner_file_index.h"
#undef private
#include "gtest/gtest.h"

namespace oceanbase
{
namespace oblogminer
{

static bool operator==(const FileIndex &lhs, const FileIndex &rhs)
{
  bool bret = true;

  if (lhs.index_file_len_ != rhs.index_file_len_) {
    bret = false;
  } else if (lhs.index_array_.count() != rhs.index_array_.count()) {
    bret = false;
  } else {
    for (int64_t i = 0; bret && i < lhs.index_array_.count(); i++) {
      const FileIndexItem *lhs_item = lhs.index_array_.at(i);
      const FileIndexItem *rhs_item = rhs.index_array_.at(i);
      if (*lhs_item == *rhs_item) {
      } else {
        bret = false;
      }
    }
  }

  return bret;
}

TEST(test_ob_log_miner_file_index, FileIndexItem)
{
  FileIndexItem item, item1;
  const int64_t buf_len = 300;
  char buf[buf_len];
  int64_t pos = 0;
  const char *buf1 = "1:2:3\n";
  EXPECT_EQ(OB_SUCCESS, item.deserialize(buf1, strlen(buf1), pos));
  EXPECT_EQ(item.file_id_, 1);
  EXPECT_EQ(item.range_.min_commit_ts_, 2);
  EXPECT_EQ(item.range_.max_commit_ts_, 3);
  pos = 0;
  const char *buf2 = "1:2:3";
  EXPECT_EQ(OB_SIZE_OVERFLOW, item.deserialize(buf2, strlen(buf2), pos));
  pos = 0;
  const char *buf3 = "1:a:3\n";
  EXPECT_EQ(OB_INVALID_DATA, item.deserialize(buf3, strlen(buf3), pos));
  pos = 0;
  const char *buf4 = "1|2|3\n";
  EXPECT_EQ(OB_INVALID_DATA, item.deserialize(buf4, strlen(buf4), pos));
  pos = 0;

  for (int i = 0; i < 10000; i++) {
    item1.file_id_ = rand();
    item1.range_.min_commit_ts_ = rand();
    item1.range_.max_commit_ts_ = rand();
    EXPECT_EQ(OB_SUCCESS, item1.serialize(buf, buf_len, pos));
    EXPECT_EQ(item1.get_serialize_size(), pos);
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, item.deserialize(buf, buf_len, pos));
    EXPECT_EQ(item, item1);
    pos = 0;
  }
}

TEST(test_ob_log_miner_file_index, FileIndex)
{
  FileIndex index2, index1,index;
  FileIndexItem *item = nullptr;
  const int64_t buf_size = 1000;
  char buf[buf_size];
  char buffer[buf_size];
  int64_t pos = 0;
  int64_t position = 0;
  const char *buf1 = "0:2:3\n1:3:3\n2:4:7\n";
  EXPECT_EQ(OB_SUCCESS, index.deserialize(buf1, strlen(buf1), pos));
  EXPECT_EQ(OB_SUCCESS, index1.insert_index_item(0,2,3));
  EXPECT_EQ(OB_SUCCESS, index1.insert_index_item(1,3,3));
  EXPECT_EQ(OB_SUCCESS, index1.insert_index_item(2,4,7));
  EXPECT_EQ(index, index1);
  EXPECT_EQ(OB_ERROR_OUT_OF_RANGE, index.get_index_item(-1, item));
  EXPECT_EQ(OB_SUCCESS, index.get_index_item(0, item));
  EXPECT_EQ(OB_SUCCESS, index.get_index_item(1, item));
  EXPECT_EQ(OB_SUCCESS, index.get_index_item(2, item));
  EXPECT_EQ(OB_ERROR_OUT_OF_RANGE, index.get_index_item(3, item));
  pos = 0;
  index.reset();
  index1.reset();
  index2.reset();
  const char *buf2 = "1:2:3\n4:5:6\n3:4:5\n";
  EXPECT_EQ(OB_SUCCESS, index.deserialize(buf2, strlen(buf2), pos));
  EXPECT_EQ(OB_ERR_UNEXPECTED, index.get_index_item(0, item));
  pos = 0;
  index.reset();
  index1.reset();
  index2.reset();
  const char *buf3 = "0:1:2\n1:2:3\n";
  EXPECT_EQ(OB_SUCCESS, index.deserialize(buf3, strlen(buf3), pos));
  EXPECT_EQ(strlen(buf3), index.get_index_file_len());
  pos = 0;
  index.reset();
  index1.reset();
  index2.reset();
  for(int i = 0; i < 1000; i++) {
    index.reset();
    index1.reset();
    int item_cnt = abs(rand()) % 10 + 1;
    for (int j = 0; j < item_cnt; j++) {
      FileIndexItem item;
      item.reset(j, rand(), rand());
      EXPECT_EQ(OB_SUCCESS, index1.insert_index_item(item));
      EXPECT_EQ(OB_SUCCESS, item.serialize(buffer, buf_size, position));
    }
    EXPECT_EQ(position, index1.get_serialize_size());
    EXPECT_EQ(position, index1.get_index_file_len());
    int64_t data_len = position;
    position = 0;
    EXPECT_EQ(OB_SUCCESS, index2.deserialize(buffer, data_len, position));
    EXPECT_EQ(index1, index2);
    EXPECT_EQ(OB_SUCCESS, index1.serialize(buf, buf_size, pos));
    data_len = pos;
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, index.deserialize(buf, data_len, pos));
    EXPECT_EQ(index, index1);
    pos = 0;
    position = 0;
    index.reset();
    index1.reset();
    index2.reset();
  }
}

}
}

int main(int argc, char **argv)
{
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_log_miner_file_index.log");
  oceanbase::ObLogger &logger = oceanbase::ObLogger::get_logger();
  logger.set_file_name("test_ob_log_miner_file_index.log", true, false);
  logger.set_log_level("DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
