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
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include <gtest/gtest.h>
#include <regex>

namespace oceanbase
{
namespace unittest
{
using namespace common;

TEST(TestLogMeta, test_log_meta)
{
  const char *str1 = "log/tenant_1001/1001/log";
  const char *str2 = "log/tenant_10011/1001/log";
  const char *str3 = "log/tenant_0111/1001/log";
  std::regex e(".*/tenant_[1][0-9]{0,3}/[1][0-9]{0,3}/log");
  bool is_matched = false;
  int ret = OB_SUCCESS;
  EXPECT_EQ(true, std::regex_match(str1, e));
  EXPECT_EQ(false, std::regex_match(str2, e));
  EXPECT_EQ(false, std::regex_match(str3, e));
}

}
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_dir_match.log", true);
  OB_LOGGER.set_log_level("INFO");
  PALF_LOG(INFO, "begin unittest::test_log_dir_match");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
