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

#include "common/ob_partition_key.h"

#include <gtest/gtest.h>

using namespace oceanbase::common;
namespace oceanbase {
namespace unittest {
class TestObPartitionKey : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

// check the validation of get_tenant_id method
TEST_F(TestObPartitionKey, test_tenant_id)
{
  const int64_t COUNT = 100;
  const int64_t PARTITION_ID = 100;
  const int64_t PARTITION_COUNT = 100;

  for (int64_t i = 0; i < COUNT; ++i) {
    uint64_t table_id = combine_id(i, PARTITION_ID);
    ObPartitionKey partition(table_id, PARTITION_ID, PARTITION_COUNT);
    EXPECT_EQ(i, partition.get_tenant_id());
  }
}

TEST_F(TestObPartitionKey, test_init)
{
  ObPartitionKey key1;
  ObPartitionKey key2;
  uint64_t tid1 = 299;            // Randomly pick a correct table_id
  uint64_t tid2 = OB_INVALID_ID;  // Invalid table_id
  int pid1 = 23;                  // Randomly pick a correct partition_idx
  int pid2 = -1;                  // Invalid partition_idx
  int cnt1 = 90;                  // Correct count value
  int cnt2 = -1;                  // Invalid partition_cnt

  // test_init
  EXPECT_FALSE(key1.is_valid());
  EXPECT_FALSE(key2.is_valid());
  EXPECT_EQ(OB_SUCCESS, key1.init(tid1, pid1, cnt1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, key2.init(tid1, pid2, cnt1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, key2.init(tid2, pid1, cnt1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, key2.init(tid1, pid1, cnt2));
  EXPECT_TRUE(key1.is_valid());
  EXPECT_FALSE(key2.is_valid());
}

TEST_F(TestObPartitionKey, test_serialize_and_deserialize)
{
  uint64_t tid = 123;
  int pid = 456;
  int cnt = 32;
  int64_t pos = 0;
  int64_t size = 128;
  char buf[size];
  ObPartitionKey key1(tid, pid, cnt);
  ObPartitionKey key2(key1);
  int64_t serialize_size = key1.get_serialize_size();

  CLOG_LOG(INFO, "before serialize", "partition key", to_cstring(key1));

  EXPECT_EQ(OB_INVALID_ARGUMENT, key1.serialize(buf, 0, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, key1.serialize(NULL, size, pos));
  EXPECT_EQ(OB_SIZE_OVERFLOW, key1.serialize(buf, serialize_size - 1, pos));  // buf is too small to fail
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, key1.serialize(buf, serialize_size, pos));

  pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, key1.deserialize(NULL, size, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, key1.deserialize(buf, 0, pos));
  EXPECT_EQ(OB_DESERIALIZE_ERROR, key1.deserialize(buf, serialize_size - 1, pos));  // buf is too small to fail
  pos = 0;
  EXPECT_EQ(OB_SUCCESS, key1.deserialize(buf, serialize_size, pos));

  EXPECT_TRUE(key1 == key2);
  CLOG_LOG(INFO, "after serialize", "partition key", to_cstring(key2));
}

TEST_F(TestObPartitionKey, test_compare)
{
  uint64_t tid1 = 101;
  uint64_t tid2 = 201;
  int pid1 = 199;
  int pid2 = 299;
  int cnt1 = 1;
  int cnt2 = 2;

  ObPartitionKey key1(tid1, pid1, cnt1);
  ObPartitionKey key2;

  key2 = key1;
  EXPECT_EQ(0, key1.compare(key2));
  EXPECT_TRUE(key1 == key2);

  key2.reset();
  EXPECT_EQ(OB_SUCCESS, key2.init(tid2, pid2, cnt2));
  EXPECT_EQ(-1, key1.compare(key2));
  EXPECT_EQ(1, key2.compare(key1));
  EXPECT_FALSE(key1 == key2);

  // Compare the case where table_id is equal
  key2.reset();
  EXPECT_EQ(OB_SUCCESS, key2.init(tid1, pid2, cnt1));
  EXPECT_EQ(-1, key1.compare(key2));
  EXPECT_EQ(1, key2.compare(key1));
  EXPECT_FALSE(key1 == key2);
  EXPECT_FALSE(key1.veq(key2));
  EXPECT_FALSE(key1.ideq(key2));

  // Compare the case where table_id and cnt are equal
  key2.reset();
  EXPECT_EQ(OB_SUCCESS, key2.init(tid1, pid1, cnt2));
  EXPECT_EQ(-1, key1.compare(key2));
  EXPECT_EQ(1, key2.compare(key1));
  EXPECT_FALSE(key1 == key2);
}

TEST_F(TestObPartitionKey, test_hash)
{
  uint64_t tid1 = 199;
  uint64_t tid2 = 201;
  int pid1 = 99;
  int pid2 = 101;
  int cnt1 = 1;
  int cnt2 = 2;
  ObPartitionKey key1;
  ObPartitionKey key2;

  EXPECT_EQ(OB_SUCCESS, key1.init(tid1, pid1, cnt1));
  EXPECT_EQ(OB_SUCCESS, key2.init(tid2, pid2, cnt2));
  EXPECT_NE(key1.hash(), key2.hash());
}

TEST_F(TestObPartitionKey, test_pg)
{
  uint64_t tenant_id = 1;
  uint64_t pure_table_id = 500001;
  uint64_t pure_tablegroup_id = ((uint64_t)0x1 << 39) | 50001;
  int64_t partition_id = 0;
  int64_t partition_cnt = 0;

  ObPartitionKey key1;
  ObPartitionKey key2;
  EXPECT_EQ(OB_INVALID_ARGUMENT, key1.init(combine_id(tenant_id, pure_table_id), partition_id));
  EXPECT_EQ(OB_SUCCESS, key1.init(combine_id(tenant_id, pure_tablegroup_id), partition_id));
  EXPECT_EQ(OB_SUCCESS, key2.init(combine_id(tenant_id, pure_table_id), partition_id, partition_cnt));

  ASSERT_TRUE(key1.is_normal_pg());
  ASSERT_FALSE(key2.is_normal_pg());
  EXPECT_EQ(combine_id(tenant_id, pure_tablegroup_id), key1.get_tablegroup_id());
  EXPECT_EQ(partition_id, key1.get_partition_group_id());
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_partition_key.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest::test_ob_partition_key");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
