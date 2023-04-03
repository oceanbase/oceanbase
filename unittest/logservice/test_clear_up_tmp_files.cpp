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
#include "logservice/palf/lsn.h"
#include "lib/file/file_directory_utils.h"

namespace oceanbase
{
using namespace common;
using namespace palf;

namespace unittest
{

TEST(TestClearUpTmpFiles, clear_up_tmp_fils)
{
  std::string base_dir = "base_dir";
  const std::string tmp_log = base_dir+"/"+"tenant_1/1.tmp/log";
  const std::string tmp_meta = base_dir+"/"+"tenant_1/1.tmp/meat.tmp";
  const std::string mkdir_tmp_log = "mkdir -p " + tmp_log;
  const std::string mkdir_tmp_meta = "mkdir -p " + tmp_meta;

  const std::string tmp_file = base_dir+"/"+"tenant_1/2/log/1.tmp";
  const std::string normal_file = base_dir+"/"+"tenant_1/2/log/2";
  const std::string dir = base_dir + "/" + "tenant_1/2/log";
  const std::string mkdir_normal_dir = "mkdir -p " + dir;
  const std::string touch_tmp_file = "touch " + tmp_file;
  const std::string touch_normal_file = "touch " + normal_file;

  system(mkdir_tmp_log.c_str());
  system(mkdir_tmp_meta.c_str());
  system(mkdir_normal_dir.c_str());
  system(touch_tmp_file.c_str());
  system(touch_normal_file.c_str());
  common::FileDirectoryUtils::delete_tmp_file_or_directory_at(base_dir.c_str());
  bool result = false;
  const std::string tenant_1 = base_dir + "/" + "tenant_1";
  const std::string tenant_1_1 = tenant_1 + "/1.tmp";
  const std::string tenant_1_2 = tenant_1 + "/2";
  const std::string tenant_1_2_log = tenant_1_2 + "/log";
  const std::string tenant_1_2_log_2 = tenant_1_2_log + "/2";
  EXPECT_EQ(OB_SUCCESS, common::FileDirectoryUtils::is_empty_directory(tenant_1.c_str(), result));
  EXPECT_EQ(false, result);
  EXPECT_EQ(OB_SUCCESS, common::FileDirectoryUtils::is_exists(tenant_1_1.c_str(), result));
  EXPECT_EQ(false, result);
  EXPECT_EQ(OB_SUCCESS, common::FileDirectoryUtils::is_empty_directory(tenant_1_2.c_str(), result));
  EXPECT_EQ(false, result);
  EXPECT_EQ(OB_SUCCESS, common::FileDirectoryUtils::is_empty_directory(tenant_1_2_log.c_str(), result));
  EXPECT_EQ(false, result);
  EXPECT_EQ(OB_SUCCESS, common::FileDirectoryUtils::is_exists(tenant_1_2_log_2.c_str(), result));
  EXPECT_EQ(true, result);
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_clear_up_tmp_files.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_clear_up_tmp_files");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
