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

#define USING_LOG_PREFIX RS

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#define private public

namespace oceanbase
{
namespace share
{
class TestStandaloneLaunch : public ::testing::Test
{
};
TEST_F(TestStandaloneLaunch, standalone_macro)
{
#ifdef OB_BUILD_STANDALONE
#ifndef OB_ENABLE_STANDALONE_LAUNCH
  ASSERT_TRUE(false);
#endif
#elif OB_BUILD_DESKTOP
#ifndef OB_ENABLE_STANDALONE_LAUNCH
  ASSERT_TRUE(false);
#endif
#else
#ifdef OB_ENABLE_STANDALONE_LAUNCH
  ASSERT_TRUE(false);
#endif
#endif
}
}
}

int main(int argc, char **argv)
{
  system("rm -rf test_standalone_launch.log");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
