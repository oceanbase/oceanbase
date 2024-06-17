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
#include "logservice/arbserver/palf_env_lite_mgr.h"
#undef private

namespace oceanbase
{
using namespace common;
using namespace arbserver;
using namespace palflite;

namespace unittest
{
TEST(TestPalfEnvLiteMgr, test_load_cluster_placeholder)
{
  PalfEnvLiteMgr mgr;
  EXPECT_EQ(OB_SUCCESS, mgr.cluster_meta_info_map_.create(10, "PalfEnvLiteMgr"));
  const char *base_dir = "base_dir";
  {
    const char *str = "cluster_1_clustername_test";
    ClusterMetaInfo meta_info;
    EXPECT_EQ(OB_SUCCESS, mgr.load_cluster_placeholder_(str, base_dir));
    EXPECT_EQ(OB_SUCCESS, mgr.get_cluster_meta_info_(1, meta_info));
    EXPECT_EQ(0, strcmp(meta_info.cluster_name_, "test"));
  }
  {
    const char *str = "cluster_2_clustername__";
    ClusterMetaInfo meta_info;
    EXPECT_EQ(OB_SUCCESS, mgr.load_cluster_placeholder_(str, base_dir));
    EXPECT_EQ(OB_SUCCESS, mgr.get_cluster_meta_info_(2, meta_info));
    EXPECT_EQ(0, strcmp(meta_info.cluster_name_, "_"));
  }
  {
    const char *str = "cluster_3_clustername_";
    ClusterMetaInfo meta_info;
    EXPECT_EQ(OB_SUCCESS, mgr.load_cluster_placeholder_(str, base_dir));
    EXPECT_EQ(OB_SUCCESS, mgr.get_cluster_meta_info_(3, meta_info));
    EXPECT_EQ(0, strcmp(meta_info.cluster_name_, ""));
  }
  {
    const char *str = "cluster_4_clustername__";
    ClusterMetaInfo meta_info;
    EXPECT_EQ(OB_SUCCESS, mgr.load_cluster_placeholder_(str, base_dir));
    EXPECT_EQ(OB_SUCCESS, mgr.get_cluster_meta_info_(4, meta_info));
    EXPECT_EQ(0, strcmp(meta_info.cluster_name_, "_"));
  }
  {
    const char *str = "cluster_a_clustername_";
    ClusterMetaInfo meta_info;
    EXPECT_EQ(OB_ERR_UNEXPECTED, mgr.load_cluster_placeholder_(str, base_dir));
  }
}

TEST(TestPalfEnvLiteMgr, test_create_delete_palf)
{
  PalfEnvLiteMgr mgr;
  std::string base_dir = "create_delete_palf";
  strcpy(mgr.base_dir_, base_dir.c_str());
  string mkdir_cmd = "mkdir " + base_dir;
  string rmdir_cmd = "rmdir " + base_dir;
  system(rmdir_cmd.c_str());
  system(mkdir_cmd.c_str());
  std::string log_dir = "runlin_test";
  EXPECT_EQ(OB_SUCCESS, mgr.check_and_prepare_dir(log_dir.c_str()));
  EXPECT_EQ(OB_SUCCESS, mgr.check_and_prepare_dir(log_dir.c_str()));
  EXPECT_EQ(OB_SUCCESS, mgr.remove_dir(log_dir.c_str()));
  EXPECT_EQ(OB_SUCCESS, mgr.remove_dir_while_exist(log_dir.c_str()));
}

TEST(TestPalfEnvLiteMgr, test_create_block)
{
  DummyBlockPool dbp;
  int dir_fd = -1;
  std::string test_dir = "test_create_block";
  std::string mkdir_cmd = "mkdir -p " + test_dir;
  std::string rmdir_cmd = "rm -rf " + test_dir;
  system(rmdir_cmd.c_str());
  const int64_t block_size = 2 * 1024 * 1024;
  system(mkdir_cmd.c_str());
  dir_fd = ::open(test_dir.c_str(), O_DIRECTORY | O_RDONLY);
  EXPECT_NE(-1, dir_fd);
  std::string block_path = "1";
  EXPECT_EQ(OB_SUCCESS, dbp.create_block_at(dir_fd, block_path.c_str(), block_size));
  EXPECT_EQ(OB_SUCCESS, dbp.create_block_at(dir_fd, block_path.c_str(), block_size));
  EXPECT_EQ(OB_SUCCESS, dbp.remove_block_at(dir_fd, block_path.c_str()));
  // file has not exist, return OB_SUCCESS
  EXPECT_EQ(OB_SUCCESS, dbp.remove_block_at(dir_fd, block_path.c_str()));
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_palf_env_lite_mgr.log*");
  OB_LOGGER.set_file_name("test_palf_env_lite_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_palf_env_lite_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
