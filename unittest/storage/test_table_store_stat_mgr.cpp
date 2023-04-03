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
#include "storage/ob_table_store_stat_mgr.h"
namespace oceanbase
{
using namespace common;
using namespace storage;
namespace unittest
{
TEST(TestTableStoreStatMgr, normal)
{
  _OB_LOG(INFO, "sizeof(key)=%ld, sizeof(node)=%ld, sizeof(stat)=%ld",
          sizeof(ObTableStoreStatKey), sizeof(ObTableStoreStatNode), sizeof(ObTableStoreStat));
  ObTableStoreStatMgr::get_instance().destroy();
  int ret = OB_SUCCESS;
  ObTableStoreStat stat;
  stat.table_id_ = 1;
  stat.tablet_id_ = 1;
  stat.row_cache_miss_cnt_ = 101;

  //init test
  ret = ObTableStoreStatMgr::get_instance().report_stat(stat);
  ASSERT_EQ(OB_NOT_INIT, ret);
  ret = ObTableStoreStatMgr::get_instance().init(0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = ObTableStoreStatMgr::get_instance().init(100000000);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = ObTableStoreStatMgr::get_instance().init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTableStoreStatMgr::get_instance().destroy();
  ObTableStoreStatMgr::get_instance().destroy();

  // only 1 node
  ret = ObTableStoreStatMgr::get_instance().init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObTableStoreStatMgr::get_instance().report_stat(stat);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTableStoreStat output;
  ObTableStoreStatIterator iter;
  sleep(2);
  ret = iter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_stat(output);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, output.table_id_);
  ASSERT_EQ(1, output.tablet_id_.id());
  ASSERT_EQ(101, output.row_cache_miss_cnt_);
  ret = iter.get_next_stat(output);
  ASSERT_EQ(OB_ITER_END, ret);

  // report again
  ret = ObTableStoreStatMgr::get_instance().report_stat(stat);
  ASSERT_EQ(OB_SUCCESS, ret);
  iter.reset();
  sleep(2);
  ret = iter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_stat(output);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, output.table_id_);
  ASSERT_EQ(1, output.tablet_id_.id());
  ASSERT_EQ(202, output.row_cache_miss_cnt_);

  // report a new partition
  stat.tablet_id_ = 2;
  stat.row_cache_miss_cnt_ = 44;
  ret = ObTableStoreStatMgr::get_instance().report_stat(stat);
  ASSERT_EQ(OB_SUCCESS, ret);
  iter.reset();
  sleep(2);
  ret = iter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_stat(output);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, output.table_id_);
  ASSERT_EQ(2, output.tablet_id_.id());
  ASSERT_EQ(44, output.row_cache_miss_cnt_);

  ObTableStoreStatMgr::get_instance().destroy();
}

TEST(TestTableStoreStatMgr, lru)
{
  ObTableStoreStatMgr::get_instance().destroy();
  int ret = OB_SUCCESS;
  ObTableStoreStat stat;
  ObTableStoreStat output;
  ObTableStoreStatIterator iter;

  ret = ObTableStoreStatMgr::get_instance().init(2);
  ASSERT_EQ(OB_SUCCESS, ret);

  // report several partitions
  for (int64_t i = 1; i <= 4; i++) {
    stat.table_id_ = i;
    stat.tablet_id_ = i;
    stat.row_cache_miss_cnt_ = 100 + i;
    ret = ObTableStoreStatMgr::get_instance().report_stat(stat);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // only 2 node, so after report 4 stats, stat #1 was evict by #3, stat #2 was evict by #4

  // iter stat
  sleep(2);
  ret = iter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_stat(output);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, output.table_id_);
  ASSERT_EQ(3, output.tablet_id_.id());
  ASSERT_EQ(103, output.row_cache_miss_cnt_);
  ret = iter.get_next_stat(output);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, output.table_id_);
  ASSERT_EQ(4, output.tablet_id_.id());
  ASSERT_EQ(104, output.row_cache_miss_cnt_);
  ObTableStoreStatMgr::get_instance().destroy();
}
}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_table_store_stat_mgr.log*");
  OB_LOGGER.set_file_name("test_table_store_stat_mgr.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
