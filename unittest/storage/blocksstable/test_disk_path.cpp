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
#include "storage/blocksstable/ob_disk_path.h"

namespace oceanbase
{
using namespace common;
namespace blocksstable
{

class TestDiskPath : public ::testing::Test
{
public:
  TestDiskPath();
  virtual ~TestDiskPath();
  virtual void SetUp();
  virtual void TearDown();
};

TestDiskPath::TestDiskPath()
{

}

TestDiskPath::~TestDiskPath()
{

}

void TestDiskPath::SetUp()
{

}

void TestDiskPath::TearDown()
{

}

TEST_F(TestDiskPath, Test_used_function)
{
  //only test 1.0 used function:set_appname_datadir and get_block_file_path,
  //set_appname_datadir test
  char appname[OB_MAX_APP_NAME_LENGTH] = "test_appname";
  char datadir[OB_MAX_FILE_NAME_LENGTH] = "./test/data/dir";
  ObDiskPath &disk_path = ObDiskPath::get_instance();

  ASSERT_EQ(OB_INVALID_ARGUMENT, disk_path.set_appname_datadir("",""));
  ASSERT_EQ(OB_SUCCESS, disk_path.set_appname_datadir(appname, datadir));
  ASSERT_EQ(OB_INIT_TWICE, disk_path.set_appname_datadir(appname, datadir));

  memset(datadir, 12, OB_MAX_APP_NAME_LENGTH + 1);
  disk_path.reset();
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, disk_path.set_appname_datadir(datadir, datadir));
  //get_block_file_path test
  char appname1[OB_MAX_APP_NAME_LENGTH] = "test_appname";
  char datadir1[OB_MAX_FILE_NAME_LENGTH] = "./test/data/dir";
  disk_path.reset();
  ASSERT_EQ(OB_SUCCESS, disk_path.set_appname_datadir(appname1, datadir1));
  char path[OB_MAX_FILE_NAME_LENGTH];
  char comp_path[OB_MAX_FILE_NAME_LENGTH];
  int n = snprintf(comp_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s/%s",
      datadir1, BLOCK_SSTBALE_DIR_NAME, BLOCK_SSTBALE_FILE_NAME);
  ASSERT_LT(n, OB_MAX_FILE_NAME_LENGTH);
  ASSERT_GT(n, 0);
  ASSERT_EQ(OB_SUCCESS, disk_path.get_block_file_path(1, path, OB_MAX_APP_NAME_LENGTH));
  ASSERT_EQ(0, strncmp(path, comp_path, OB_MAX_APP_NAME_LENGTH));
  ASSERT_EQ(OB_INVALID_ARGUMENT, disk_path.get_block_file_path(1, path, 0));
  //get_block_file_directory test
  char appname2[OB_MAX_APP_NAME_LENGTH] = "test_appname";
  char datadir2[OB_MAX_FILE_NAME_LENGTH] = "./test/data/dir";
  disk_path.reset();
  ASSERT_EQ(OB_SUCCESS, disk_path.set_appname_datadir(appname2, datadir2));
  char path1[OB_MAX_FILE_NAME_LENGTH];
  char comp_path1[OB_MAX_FILE_NAME_LENGTH];
  int t = snprintf(comp_path1, OB_MAX_FILE_NAME_LENGTH, "%s/sstable", datadir2);
  ASSERT_LT(t, OB_MAX_FILE_NAME_LENGTH);
  ASSERT_GT(t, 0);
  ASSERT_EQ(OB_SUCCESS, disk_path.get_block_file_directory(path1, OB_MAX_APP_NAME_LENGTH));
  ASSERT_EQ(0, strncmp(path1, comp_path1, OB_MAX_APP_NAME_LENGTH));
  ASSERT_EQ(OB_INVALID_ARGUMENT, disk_path.get_block_file_directory(path1, 0));
}

}//blocksstable
}//oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
