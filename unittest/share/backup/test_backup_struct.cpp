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

#define USING_LOG_PREFIX SHARE

#include "share/backup/ob_backup_path.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace common;
using namespace share;

TEST(ObBackupDest, disk)
{
  const char* backup_test = "file:///root_backup_dir";
  ObBackupDest dest;
  ASSERT_EQ(OB_SUCCESS, dest.set(backup_test));
  LOG_INFO("dump backup dest", K(dest));
  ASSERT_EQ(0, strcmp(dest.root_path_, "file:///root_backup_dir"));
  ASSERT_EQ(0, strcmp(dest.storage_info_, ""));
}

TEST(ObBackupDest, oss)
{
  const char* backup_test =
      "oss://backup_dir/?host=http://oss-cn-hangzhou-zmf.aliyuncs.com&access_id=111&access_key=222";
  ObBackupDest dest;
  ASSERT_EQ(OB_SUCCESS, dest.set(backup_test));
  LOG_INFO("dump backup dest", K(dest));
  ASSERT_EQ(0, strcmp(dest.root_path_, "oss://backup_dir/"));
  ASSERT_EQ(0, strcmp(dest.storage_info_, "host=http://oss-cn-hangzhou-zmf.aliyuncs.com&access_id=111&access_key=222"));
}

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
