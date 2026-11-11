/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "gtest/gtest.h"
#include "rootserver/standby/ob_protection_mode_utils.h"

namespace oceanbase
{
namespace rootserver
{

using namespace share;

// Verify the shared mapping from synchronous protection modes to their steady levels.
TEST(TestSemiSyncVerifyProtectionLevel, mapSyncModeToSteadyProtectionLevel)
{
  ObProtectionLevel target_level;

  ASSERT_EQ(OB_SUCCESS, standby::ObProtectionModeUtils::get_sync_protection_level(
      ObProtectionMode(ObProtectionMode::MAXIMUM_PROTECTION_MODE), target_level));
  EXPECT_EQ(ObProtectionLevel::MAXIMUM_PROTECTION_LEVEL, target_level.value());

  ASSERT_EQ(OB_SUCCESS, standby::ObProtectionModeUtils::get_sync_protection_level(
      ObProtectionMode(ObProtectionMode::MAXIMUM_AVAILABILITY_MODE), target_level));
  EXPECT_EQ(ObProtectionLevel::MAXIMUM_AVAILABILITY_LEVEL, target_level.value());

  EXPECT_EQ(OB_INVALID_ARGUMENT, standby::ObProtectionModeUtils::get_sync_protection_level(
      ObProtectionMode(ObProtectionMode::MAXIMUM_PERFORMANCE_MODE), target_level));
}

// Verify that semi-sync config values use the standard OceanBase boolean syntax.
TEST(TestSemiSyncVerifyProtectionLevel, parseEnableStandbySemiSyncConfigBool)
{
  bool value = false;
  EXPECT_EQ(OB_SUCCESS, standby::ObProtectionModeUtils::parse_bool_config_value(
      ObString::make_string("true"), value));
  EXPECT_TRUE(value);

  EXPECT_EQ(OB_SUCCESS, standby::ObProtectionModeUtils::parse_bool_config_value(
      ObString::make_string("False"), value));
  EXPECT_FALSE(value);

  EXPECT_EQ(OB_SUCCESS, standby::ObProtectionModeUtils::parse_bool_config_value(
      ObString::make_string("on"), value));
  EXPECT_TRUE(value);

  EXPECT_EQ(OB_SUCCESS, standby::ObProtectionModeUtils::parse_bool_config_value(
      ObString::make_string("0"), value));
  EXPECT_FALSE(value);

  EXPECT_EQ(OB_INVALID_CONFIG, standby::ObProtectionModeUtils::parse_bool_config_value(
      ObString::make_string("not_bool"), value));

  char long_value[common::OB_MAX_CONFIG_VALUE_LEN + 2] = {0};
  MEMSET(long_value, 't', common::OB_MAX_CONFIG_VALUE_LEN + 1);
  EXPECT_EQ(OB_INVALID_CONFIG, standby::ObProtectionModeUtils::parse_bool_config_value(
      ObString(common::OB_MAX_CONFIG_VALUE_LEN + 1, long_value), value));
}

} // namespace rootserver
} // namespace oceanbase

// Run the focused semi-sync Verify unit suite.
int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
