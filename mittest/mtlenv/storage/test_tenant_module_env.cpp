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
#include "../mock_tenant_module_env.h"

namespace oceanbase
{
using namespace storage;

class TestTenantModuleEnv : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
  void SetUp()
  {
    ASSERT_TRUE(MockTenantModuleEnv::get_instance().is_inited());
  }
};

TEST_F(TestTenantModuleEnv, basic)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  //
  // MTL(ObLSSerivce*)
  // do ...
  //
}

TEST_F(TestTenantModuleEnv, basic2)
{
  EXPECT_EQ(OB_SYS_TENANT_ID, MTL_ID());
  //
  // MTL(ObLSSerivce*)
  // do ...
  //
}

} // end oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_tenant_module_env.log");
  OB_LOGGER.set_file_name("test_tenant_module_env.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
