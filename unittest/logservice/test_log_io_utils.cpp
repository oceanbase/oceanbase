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
#include "lib/ob_define.h"
#include "logservice/palf/log_io_utils.h"

namespace oceanbase
{
namespace palf
{
using namespace common;
using namespace palf;

TEST(TestLogIOUtils, test_rename)
{
  EXPECT_EQ(OB_INVALID_ARGUMENT, renameat_with_retry(-1, NULL, -1, NULL));
  EXPECT_EQ(OB_INVALID_ARGUMENT, rename_with_retry(NULL, NULL));

  const char *src_name = "src_file";
  const char *dest_name = "src_file.tmp";
  const char *curr_dir_name  = get_current_dir_name();
  int dir_fd = 0;
  system("rm -rf src_file*");
  EXPECT_LE(0, dir_fd = ::open(curr_dir_name, O_DIRECTORY | O_RDONLY));
  EXPECT_EQ(false, check_rename_success(src_name, dest_name));
  EXPECT_EQ(false, check_renameat_success(dir_fd, src_name, dir_fd, dest_name));
  const char *touch_dest_cmd = "touch src_file.tmp";
  const char *touch_src_cmd = "touch src_file";
  system(touch_dest_cmd);
  EXPECT_EQ(true, check_rename_success(src_name, dest_name));
  EXPECT_EQ(true, check_renameat_success(dir_fd, src_name, dir_fd, dest_name));
  system(touch_src_cmd);
  EXPECT_EQ(false, check_rename_success(src_name, dest_name));
  EXPECT_EQ(false, check_renameat_success(dir_fd, src_name, dir_fd, dest_name));
  EXPECT_EQ(OB_SUCCESS, rename_with_retry(src_name, dest_name));
  system(touch_src_cmd);
  EXPECT_EQ(OB_SUCCESS, rename_with_retry(src_name, dest_name));
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_io_utils.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_io_utils");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
