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
#define private public
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {
using namespace common;
namespace share {
namespace schema {

void construct_part_array(ObTableSchema& table, const ObArray<int64_t>& partition_ids)
{
  int ret = OB_SUCCESS;
  table.reset();
  table.set_part_level(PARTITION_LEVEL_ONE);
  table.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_RANGE);
  table.get_part_option().set_part_num(partition_ids.count());
  table.set_schema_version(1);
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
    ObPartition part;
    part.set_part_id(partition_ids.at(i));
    ret = table.add_partition(part, false);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

TEST(ObPartMgrUnit, basic_test)
{
  int ret = OB_SUCCESS;
  ObTableSchema old_table;
  ObTableSchema new_table;
  int64_t schema_version = 1;
  ObArray<int64_t> add;
  ObArray<int64_t> drop;
  // case 0 : schema versions are the same
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  // case 1 : output is 0 or 1
  ObArray<int64_t> old_part;
  ObArray<int64_t> new_part;
  ret = old_part.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_part.push_back(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  construct_part_array(old_table, old_part);
  construct_part_array(new_table, new_part);
  new_table.set_schema_version(++schema_version);
  old_table.set_schema_version(++schema_version);
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, drop.count());
  ASSERT_EQ(1, add.count());
  ASSERT_EQ(0, drop.at(0));
  ASSERT_EQ(1, add.at(0));

  // case 2 : output is 0 or 1
  old_part.reset();
  new_part.reset();
  ret = old_part.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = new_part.push_back(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int i = 2; i < 10; ++i) {
    ret = old_part.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = new_part.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  construct_part_array(old_table, old_part);
  construct_part_array(new_table, new_part);
  new_table.set_schema_version(++schema_version);
  old_table.set_schema_version(++schema_version);
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, drop.count());
  ASSERT_EQ(1, add.count());
  ASSERT_EQ(0, drop.at(0));
  ASSERT_EQ(1, add.at(0));

  // case 3 : drop count is odd add count is even
  old_part.reset();
  new_part.reset();
  for (int i = 0; i < 10; ++i) {
    ret = old_part.push_back(2 * i);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = new_part.push_back(2 * i + 1);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  construct_part_array(old_table, old_part);
  construct_part_array(new_table, new_part);
  new_table.set_schema_version(++schema_version);
  old_table.set_schema_version(++schema_version);
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(10, drop.count());
  ASSERT_EQ(10, add.count());
  for (int i = 0; i < 10; ++i) {
    ASSERT_EQ(2 * i, drop.at(i));
    ASSERT_EQ(2 * i + 1, add.at(i));
  }

  // case 4 noting
  old_part.reset();
  new_part.reset();
  for (int i = 0; i < 10; ++i) {
    ret = old_part.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = new_part.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  construct_part_array(old_table, old_part);
  construct_part_array(new_table, new_part);
  new_table.set_schema_version(++schema_version);
  old_table.set_schema_version(++schema_version);
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, drop.count());
  ASSERT_EQ(0, add.count());

  // case 5 add outout
  old_part.reset();
  new_part.reset();
  for (int i = 0; i < 10; ++i) {
    ret = old_part.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = new_part.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = new_part.push_back(11);
  ASSERT_EQ(OB_SUCCESS, ret);

  construct_part_array(old_table, old_part);
  construct_part_array(new_table, new_part);
  new_table.set_schema_version(++schema_version);
  old_table.set_schema_version(++schema_version);
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, drop.count());
  ASSERT_EQ(1, add.count());
  ASSERT_EQ(11, add.at(0));

  // case 6 drop output
  old_part.reset();
  new_part.reset();
  for (int i = 0; i < 10; ++i) {
    ret = old_part.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = new_part.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = old_part.push_back(10);
  ASSERT_EQ(OB_SUCCESS, ret);

  construct_part_array(old_table, old_part);
  construct_part_array(new_table, new_part);
  new_table.set_schema_version(++schema_version);
  old_table.set_schema_version(++schema_version);
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, drop.count());
  ASSERT_EQ(0, add.count());
  ASSERT_EQ(10, drop.at(0));

  // case 7  old has no partition ,new has partition
  old_table.reset();
  old_table.set_schema_version(++schema_version);
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, drop.count());
  ASSERT_EQ(9, add.count());
  for (int i = 1; i < 10; ++i) {
    ASSERT_EQ(i, add.at(i - 1));
  }

  // case 8 old has partition ,new has no partition
  new_table.reset();
  new_table.set_schema_version(++schema_version);
  construct_part_array(old_table, old_part);
  ret = ObPartMgrUtils::get_part_diff(old_table, new_table, drop, add);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(10, drop.count());
  ASSERT_EQ(0, add.count());
  for (int i = 1; i < 11; ++i) {
    ASSERT_EQ(i, drop.at(i - 1));
  }
}

TEST(ObPartMgrUnit, get_part_idx)
{
  int ret = OB_SUCCESS;
  ObTableSchema table;
  int64_t part_id = -1;
  int64_t part_idx = -1;
  ret = ObPartMgrUtils::get_partition_idx_by_id(table, part_id, part_idx);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  part_id = 3;
  ret = ObPartMgrUtils::get_partition_idx_by_id(table, part_id, part_idx);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
  part_id = 0;
  ret = ObPartMgrUtils::get_partition_idx_by_id(table, part_id, part_idx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, part_idx);
  ObArray<int64_t> part_ids;
  ret = part_ids.push_back(0);
  ASSERT_EQ(OB_SUCCESS, ret);
  construct_part_array(table, part_ids);
  ret = ObPartMgrUtils::get_partition_idx_by_id(table, part_id, part_idx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, part_idx);

  part_ids.reset();
  ret = part_ids.push_back(100);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = part_ids.push_back(50);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = part_ids.push_back(1000);
  ASSERT_EQ(OB_SUCCESS, ret);
  construct_part_array(table, part_ids);
  part_id = 100;
  ret = ObPartMgrUtils::get_partition_idx_by_id(table, part_id, part_idx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, part_idx);
  part_id = 50;
  ret = ObPartMgrUtils::get_partition_idx_by_id(table, part_id, part_idx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, part_idx);
  part_id = 1000;
  ret = ObPartMgrUtils::get_partition_idx_by_id(table, part_id, part_idx);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, part_idx);
  part_id = 10000;
  ret = ObPartMgrUtils::get_partition_idx_by_id(table, part_id, part_idx);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(-1, part_idx);

  part_ids.reset();
  for (int64_t i = 100; i > -1; --i) {
    ret = part_ids.push_back(i);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  construct_part_array(table, part_ids);
  for (int64_t i = 0; i < 101; ++i) {
    ret = ObPartMgrUtils::get_partition_idx_by_id(table, i, part_idx);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(100 - i, part_idx);
  }
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_part.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
