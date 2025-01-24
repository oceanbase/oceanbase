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

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#define private public
#define protected public

#include "share/backup/ob_backup_tablet_reorganize_helper.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace backup
{

void set_tablet_info(const int64_t src_tablet_id, const int64_t dest_tablet_id, ObTabletReorganizeInfo &info)
{
  info.src_tablet_id_ = src_tablet_id;
  info.dest_tablet_id_ = dest_tablet_id;
  info.tenant_id_ = 1002;
  info.ls_id_ = ObLSID(1001);
}

/*
        1
      /   \
     2     3
         /   \
        4     5
*/

TEST(TestBackupReorganizeHelper, test1)
{
  int ret = OB_SUCCESS;
  common::ObTabletID tablet_id(1);
  common::ObArray<ObTabletReorganizeInfo> infos;
  ObTabletReorganizeInfo info;

  set_tablet_info(1, 2, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  set_tablet_info(1, 3, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  set_tablet_info(3, 4, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  set_tablet_info(3, 5, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  common::ObArray<common::ObTabletID> tablet_list;

  ret = ObBackupTabletReorganizeHelper::get_leaf_children_from_history(1002, infos, tablet_id, tablet_list);
  EXPECT_EQ(OB_SUCCESS, ret);
}

/*
        1
      /   \
     2     3
    / \
   4   5
  / \
 6   7
*/

TEST(TestBackupReorganizeHelper, test2)
{
  int ret = OB_SUCCESS;
  common::ObTabletID tablet_id(1);
  ObArray<ObTabletReorganizeInfo> infos;
  ObTabletReorganizeInfo info;

  set_tablet_info(1, 2, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  set_tablet_info(1, 3, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  set_tablet_info(2, 4, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  set_tablet_info(2, 5, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  set_tablet_info(4, 6, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  set_tablet_info(4, 7, info);
  ret = infos.push_back(info);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObArray<ObTabletID> tablet_list;

  ret = ObBackupTabletReorganizeHelper::get_leaf_children_from_history(1002, infos, tablet_id, tablet_list);
  EXPECT_EQ(OB_SUCCESS, ret);
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_backup_tablet_reorganize_helper.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_backup_tablet_reorganize_helper.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
