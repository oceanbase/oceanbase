/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "observer/omt/ob_tenant_config.h"

namespace oceanbase
{
namespace unittest
{

TEST(TestArbitrationTimeoutConfig, value_boundary)
{
  omt::ObTenantConfig tenant_config(1001);

  EXPECT_STREQ("5s", tenant_config.arbitration_timeout.str());

  ASSERT_TRUE(tenant_config.arbitration_timeout.set_value("1s"));
  EXPECT_TRUE(tenant_config.arbitration_timeout.check());
  EXPECT_EQ(1000L * 1000L, tenant_config.arbitration_timeout.get_value());

  ASSERT_TRUE(tenant_config.arbitration_timeout.set_value("999ms"));
  EXPECT_FALSE(tenant_config.arbitration_timeout.check());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
