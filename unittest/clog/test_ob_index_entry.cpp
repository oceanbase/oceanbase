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
#include "clog/ob_log_entry.h"

using namespace oceanbase::common;
namespace oceanbase {
using namespace clog;
namespace unittest {
class TestObIndexEntry : public ::testing::Test {
public:
  virtual void SetUp();
  virtual void TearDown()
  {}

  ObIndexEntry& get_index_entry()
  {
    return index_;
  }

private:
  ObIndexEntry index_;

  ObPartitionKey partition_key_;
  uint64_t log_id_;
  int32_t file_id_;
  int32_t offset_;
  int32_t size_;
  int64_t submit_timestamp_;
};

void TestObIndexEntry::SetUp()
{
  uint64_t table_id = 3;
  int32_t partition_idx = 1;
  int32_t partition_count = 512;
  partition_key_.init(table_id, partition_idx, partition_count);
  log_id_ = 8;
  file_id_ = 1;
  offset_ = 1024;
  size_ = 129;
  submit_timestamp_ = ObTimeUtility::current_time();
  int64_t accum_checksum_ = 0;
  const bool batch_committed = false;
  EXPECT_EQ(OB_SUCCESS,
      index_.init(
          partition_key_, log_id_, file_id_, offset_, size_, submit_timestamp_, accum_checksum_, batch_committed));
}

TEST_F(TestObIndexEntry, test_member_function)
{
  ObIndexEntry index1, index2;
  EXPECT_EQ(OB_SUCCESS, index1.shallow_copy(get_index_entry()));
  EXPECT_EQ(OB_SUCCESS, index2.shallow_copy(get_index_entry()));
  EXPECT_TRUE(index1.check_magic_num());
  EXPECT_TRUE(index1 == index2);
  EXPECT_EQ(index1.get_partition_key(), index2.get_partition_key());
  EXPECT_EQ(index1.get_log_id(), index2.get_log_id());
  EXPECT_EQ(index1.get_file_id(), index2.get_file_id());
  EXPECT_EQ(index1.get_offset(), index2.get_offset());
  EXPECT_EQ(index1.get_size(), index2.get_size());
  EXPECT_EQ(index1.get_submit_timestamp(), index2.get_submit_timestamp());

  const int64_t len = 1024;
  int64_t pos = 0;
  char buffer[len];
  ObIndexEntry index3;
  EXPECT_EQ(OB_SUCCESS, index1.serialize(buffer, len, pos));
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, index3.deserialize(buffer, len, pos));
  EXPECT_TRUE(index1 == index3);

  CLOG_LOG(INFO, "ObIndexEntry", K(index1));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_index_entry.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_ob_index_entry");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
