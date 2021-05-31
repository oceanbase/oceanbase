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
#include "storage/ob_sstable_merge_info_mgr.h"

namespace oceanbase {
using namespace blocksstable;
using namespace common;
using namespace storage;

namespace unittest {

TEST(TestSSTableMergeInfoMgr, normal)
{
  int ret = OB_SUCCESS;
  ObSSTableMergeInfo merge_info;

  // not init
  ret = ObSSTableMergeInfoMgr::get_instance().add_sstable_merge_info(merge_info);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObSSTableMergeInfoMgr::get_instance().init(0);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = ObSSTableMergeInfoMgr::get_instance().init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObSSTableMergeInfoMgr::get_instance().init(1);
  ASSERT_NE(OB_SUCCESS, ret);
  ObSSTableMergeInfoMgr::get_instance().destroy();
  ObSSTableMergeInfoMgr::get_instance().destroy();

  ret = ObSSTableMergeInfoMgr::get_instance().init(1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObSSTableMergeInfoMgr::get_instance().add_sstable_merge_info(merge_info);
  ASSERT_NE(OB_SUCCESS, ret);

  merge_info.table_id_ = 1;
  ret = ObSSTableMergeInfoMgr::get_instance().add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  merge_info.table_id_ = 2;
  ret = ObSSTableMergeInfoMgr::get_instance().add_sstable_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // test iter
  ObSSTableMergeInfoIterator iter;
  ret = iter.open();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_merge_info(merge_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = iter.get_next_merge_info(merge_info);
  ASSERT_EQ(OB_ITER_END, ret);
  ret = iter.get_next_merge_info(merge_info);
  ASSERT_EQ(OB_ITER_END, ret);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_sstable_merge_info_mgr.log*");
  OB_LOGGER.set_file_name("test_sstable_merge_info_mgr.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
