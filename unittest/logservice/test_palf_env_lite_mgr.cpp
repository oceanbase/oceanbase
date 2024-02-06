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

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_palf_env_lite_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_palf_env_lite_mgr");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
