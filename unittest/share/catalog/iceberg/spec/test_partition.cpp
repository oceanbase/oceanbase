/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "sql/table_format/iceberg/spec/partition.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace oceanbase::sql;
class TestPartition : public ::testing::Test
{
public:
  TestPartition() = default;
  ~TestPartition() = default;
  ObArenaAllocator allocator;
};

TEST_F(TestPartition, test_is_partition)
{
  iceberg::PartitionSpec partition_spec(allocator);
  ASSERT_TRUE(partition_spec.is_unpartitioned());
  iceberg::PartitionField partition_field1(allocator);
  partition_field1.name = "dt";
  partition_field1.transform.transform_type = iceberg::TransformType::Void;
  ASSERT_EQ(OB_SUCCESS, partition_spec.fields.init(2));
  ASSERT_EQ(OB_SUCCESS, partition_spec.fields.push_back(&partition_field1));
  ASSERT_TRUE(partition_spec.is_unpartitioned());
  iceberg::PartitionField partition_field2(allocator);
  partition_field2.name = "dt";
  partition_field2.transform.transform_type = iceberg::TransformType::Identity;
  ASSERT_EQ(OB_SUCCESS, partition_spec.fields.push_back(&partition_field2));
  ASSERT_TRUE(partition_spec.is_partitioned());
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}