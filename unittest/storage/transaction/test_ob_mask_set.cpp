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

#include "storage/transaction/ob_mask_set.h"
#include <gtest/gtest.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
using namespace common;
namespace unittest {
class TestObMaskSet : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  static const int64_t PARTITION_KEY_COUNT = 16;
  // valid partition parameters
  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
  // invalid partition parameters
  static const int64_t INVALID_TABLE_ID = -1;
  static const int32_t INVALID_PARTITION_ID = -1;
  static const int32_t INVALID_PARTITION_COUNT = -100;
};

//////////////////////basic function test//////////////////////////////////////////
// mask a ObPartitionKey object
TEST_F(TestObMaskSet, mask_object)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  ObPartitionArray array;
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS,
        array.push_back(ObPartitionKey(
            TestObMaskSet::VALID_TABLE_ID, static_cast<int32_t>(i), TestObMaskSet::VALID_PARTITION_COUNT)));
  }
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  EXPECT_EQ(OB_SUCCESS, mask_set.mask(array.at(0)));
  EXPECT_TRUE(mask_set.is_mask(array.at(0)));
  EXPECT_FALSE(mask_set.is_mask(array.at(1)));
}

// test whether all managed objects are marked
TEST_F(TestObMaskSet, is_all_mask)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  ObPartitionArray array;
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS,
        array.push_back(ObPartitionKey(
            TestObMaskSet::VALID_TABLE_ID, static_cast<int32_t>(i), TestObMaskSet::VALID_PARTITION_COUNT)));
  }
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  // mask the first half objects in array, where partition_id in [0, 7]
  for (int64_t i = 0; i < PARTITION_KEY_COUNT / 2; ++i) {
    EXPECT_EQ(OB_SUCCESS, mask_set.mask(array.at(i)));
  }
  EXPECT_FALSE(mask_set.is_all_mask());
  // mask the last half objects in array
  for (int64_t i = PARTITION_KEY_COUNT / 2; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS, mask_set.mask(array.at(i)));
  }
  EXPECT_EQ(true, mask_set.is_all_mask());
}

// test the objects not masked
TEST_F(TestObMaskSet, get_not_mask)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  ObPartitionArray array;
  ObPartitionArray not_mask_array;
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS,
        array.push_back(ObPartitionKey(
            TestObMaskSet::VALID_TABLE_ID, static_cast<int32_t>(i), TestObMaskSet::VALID_PARTITION_COUNT)));
  }
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  // mask the half objects in array, where partition_id in [0, 7]
  for (int64_t i = 0; i < PARTITION_KEY_COUNT / 2; ++i) {
    EXPECT_EQ(OB_SUCCESS, mask_set.mask(array.at(i)));
  }
  EXPECT_FALSE(mask_set.is_all_mask());
  EXPECT_EQ(OB_SUCCESS, mask_set.get_not_mask(not_mask_array));
  EXPECT_EQ(PARTITION_KEY_COUNT / 2, not_mask_array.count());
  for (int64_t i = 0; i < not_mask_array.count(); ++i) {
    EXPECT_EQ(i + PARTITION_KEY_COUNT / 2, not_mask_array.at(i).get_partition_id());
  }
}

// test the reset and clear of an ObMaskSet object
TEST_F(TestObMaskSet, reset_clear)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  ObPartitionArray array;
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS,
        array.push_back(ObPartitionKey(
            TestObMaskSet::VALID_TABLE_ID, static_cast<int32_t>(i), TestObMaskSet::VALID_PARTITION_COUNT)));
  }
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  mask_set.clear_set();
  mask_set.reset();
}

//////////////////////////boundary test/////////////////////////////////////////
// fail to init ObMaskSet
TEST_F(TestObMaskSet, init_failed)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  ObPartitionArray array;
  EXPECT_EQ(OB_INVALID_ARGUMENT, mask_set.init(array));
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    ObPartitionKey partition_key;
    if (0 < i) {
      partition_key = ObPartitionKey(VALID_TABLE_ID, static_cast<int32_t>(i), VALID_PARTITION_COUNT);
    }
    EXPECT_EQ(OB_SUCCESS, array.push_back(partition_key));
  }
  EXPECT_EQ(OB_INVALID_ARGUMENT, mask_set.init(array));
  // modify invalid partition
  EXPECT_EQ(OB_SUCCESS, array.remove(0));
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  EXPECT_EQ(OB_INIT_TWICE, mask_set.init(array));
}

// test the failure of initing mask
TEST_F(TestObMaskSet, mask_failed)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  // create an invalid partition, and execute mask operations
  ObPartitionKey invalid_partition_key(INVALID_TABLE_ID, INVALID_PARTITION_ID, INVALID_PARTITION_COUNT);
  EXPECT_EQ(OB_NOT_INIT, mask_set.mask(invalid_partition_key));

  ObPartitionArray array;
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    array.push_back(ObPartitionKey(VALID_TABLE_ID, static_cast<int32_t>(i), VALID_PARTITION_COUNT));
  }
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mask_set.mask(invalid_partition_key));

  // create a partition not managed by array
  EXPECT_EQ(
      OB_MASK_SET_NO_NODE, mask_set.mask(ObPartitionKey(VALID_TABLE_ID, PARTITION_KEY_COUNT, VALID_PARTITION_COUNT)));
}

// test the failure of executing is_all_mask
TEST_F(TestObMaskSet, is_all_mask_failed)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  EXPECT_FALSE(mask_set.is_all_mask());
  ObPartitionArray array;
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(
        OB_SUCCESS, array.push_back(ObPartitionKey(VALID_TABLE_ID, static_cast<int32_t>(i), VALID_PARTITION_COUNT)));
  }
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS, mask_set.mask(array.at(i)));
  }
  EXPECT_TRUE(mask_set.is_all_mask());
}

// test the failure of executing get_not_mask
TEST_F(TestObMaskSet, get_not_mask_failed)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  ObPartitionArray array;
  EXPECT_EQ(OB_NOT_INIT, mask_set.get_not_mask(array));
}

// after reset, execute mask and is_all_mask
TEST_F(TestObMaskSet, reset_operation)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  ObPartitionArray array;
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(
        OB_SUCCESS, array.push_back(ObPartitionKey(VALID_TABLE_ID, static_cast<int32_t>(i), VALID_PARTITION_COUNT)));
  }
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  mask_set.reset();
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(OB_NOT_INIT, mask_set.mask(array.at(i)));
  }
  EXPECT_FALSE(mask_set.is_all_mask());
}

// clear masked partition
TEST_F(TestObMaskSet, clear_operation)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObMaskSet mask_set;
  ObPartitionArray array;
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(
        OB_SUCCESS, array.push_back(ObPartitionKey(VALID_TABLE_ID, static_cast<int32_t>(i), VALID_PARTITION_COUNT)));
  }
  EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
  for (int64_t i = 0; i < PARTITION_KEY_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS, mask_set.mask(array.at(i)));
  }
  mask_set.clear_set();
  EXPECT_FALSE(mask_set.is_all_mask());
}

TEST_F(TestObMaskSet, performance)
{
  const int64_t max_pkey_count = 100;
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  for (int32_t j = 1; j <= max_pkey_count; ++j) {
    int32_t pkey_count = j;
    ObMaskSet mask_set;
    ObPartitionArray array;
    for (int32_t i = 0; i < pkey_count; ++i) {
      EXPECT_EQ(OB_SUCCESS, array.push_back(ObPartitionKey(VALID_TABLE_ID, i, pkey_count)));
    }
    EXPECT_EQ(OB_SUCCESS, mask_set.init(array));
    int64_t start = ObTimeUtility::current_time();
    for (int32_t i = 0; i < pkey_count; ++i) {
      // mask_set.is_mask(array.at(i));
      mask_set.mask(array.at(i));
      // mask_set.is_all_mask();
    }
    int64_t end = ObTimeUtility::current_time();
    TRANS_LOG(INFO,
        "ob_mask_set satistic",
        "pkey_count",
        array.count(),
        "total_used",
        end - start,
        "avg_used",
        static_cast<double>(end - start) * 1.0 / pkey_count);
  }
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_mask_set.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
