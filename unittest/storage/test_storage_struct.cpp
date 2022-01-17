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
#include <storage/ob_storage_struct.h>
#include <lib/oblog/ob_log_module.h>
#include <stdio.h>

namespace oceanbase {
namespace unittest {
using namespace common;
using namespace storage;

class TestStorageStruct : public ::testing::Test {
public:
  TestStorageStruct();
  virtual ~TestStorageStruct();
};

TestStorageStruct::TestStorageStruct()
{}

TestStorageStruct::~TestStorageStruct()
{}

TEST_F(TestStorageStruct, test_recovery_point)
{
  ObRecoverVec vec;
  ObRecoverPoint point;
  bool changed = false;

  printf("======= TEST 1 ======\n");

  vec.reset();
  ASSERT_EQ(0, vec.count());
  ASSERT_EQ(0, vec.add_recover_point(ObRecoverPoint(1, 2, 0, 0, 0)));
  ASSERT_EQ(0, vec.add_recover_point(ObRecoverPoint(3, 4, 0, 0, 0)));
  ASSERT_EQ(0, vec.add_recover_point(ObRecoverPoint(2, 4, 0, 0, 0)));
  // 1. Test incrementality
  ASSERT_EQ(2, vec.count());

  printf("======= TEST 2 ======\n");

  // 2. Test batch insert
  ASSERT_EQ(0, vec.add_recover_point(ObRecoverPoint(6, 5, 0, 0, 0)));
  ASSERT_EQ(0, vec.add_recover_point(ObRecoverPoint(8, 6, 0, 0, 0)));
  ASSERT_EQ(0, vec.add_recover_point(ObRecoverPoint(7, 8, 0, 0, 0)));
  ASSERT_EQ(0, vec.add_recover_point(ObRecoverPoint(9, 11, 0, 0, 0)));
  ASSERT_EQ(0, vec.add_recover_point(ObRecoverPoint(11, 14, 0, 0, 0)));
  ASSERT_EQ(6, vec.count());

  printf("======= TEST 3 ======\n");

  // 3. Test fetch recover point
  ASSERT_EQ(0, vec.get_lower_bound_point(12, point));
  ASSERT_EQ(11, point.snapshot_version_);
  ASSERT_EQ(14, point.recover_log_id_);
  printf("======= TEST 3 ======\n");
  ASSERT_EQ(0, vec.get_lower_bound_point(9, point));
  ASSERT_EQ(9, point.snapshot_version_);
  ASSERT_EQ(11, point.recover_log_id_);
  ASSERT_EQ(0, vec.get_lower_bound_point(2, point));
  ASSERT_EQ(1, point.snapshot_version_);
  ASSERT_EQ(2, point.recover_log_id_);

  printf("======= TEST 4 ======\n");

  // 4. Test GC
  ASSERT_EQ(0, vec.gc_recover_points(5, 10, changed));
  ASSERT_EQ(0, vec.get_lower_bound_point(9, point));
  ASSERT_EQ(9, point.snapshot_version_);
  ASSERT_EQ(true, changed);
  ASSERT_EQ(11, point.recover_log_id_);
  ASSERT_NE(0, vec.get_lower_bound_point(2, point));
  ASSERT_EQ(4, vec.count());

  printf("======= TEST 5 ======\n");

  // 5. Test recover vector compression
  ASSERT_EQ(0, vec.record_major_recover_point(8, 11, changed));
  ASSERT_EQ(true, changed);
  ASSERT_EQ(3, vec.count());
  ASSERT_EQ(0, vec.get_lower_bound_point(9, point));
  ASSERT_EQ(8, point.snapshot_version_);
  ASSERT_EQ(6, point.recover_log_id_);
  ASSERT_EQ(0, vec.get_lower_bound_point(8, point));
  ASSERT_EQ(8, point.snapshot_version_);
  ASSERT_EQ(6, point.recover_log_id_);
}
}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  int ret = 1;
  oceanbase::common::ObLogger& logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_storage_struct.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
