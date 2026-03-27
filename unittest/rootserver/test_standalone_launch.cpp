/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
