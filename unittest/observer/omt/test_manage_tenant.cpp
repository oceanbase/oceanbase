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
#include "all_mock.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/ob_server_struct.h"
#include "storage/mock_ob_tenant_storage.h"
#include "share/io/ob_io_manager.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::observer;

#define OMT_UNITTEST

class TestManageTenant
    : public ::testing::Test,
      public ObMultiTenant
{
public:
  TestManageTenant()
      : ObMultiTenant(procor_)
  {
    all_mock_init();
    tenant_storage_.init();
  }

  virtual void SetUp()
  {
    ObIOManager::get_instance().init();
  }

  virtual void TearDown()
  {
    ObIOManager::get_instance().destroy();
  }

protected:
  ObFakeWorkerProcessor procor_;
  ObGlobalContext gctx_;
  MockObTenantStorageAgent tenant_storage_;
};

TEST_F(TestManageTenant, AddDelete)
{
  EXPECT_EQ(OB_SUCCESS, add_tenant(1, 1, 1));
  EXPECT_EQ(OB_SUCCESS, add_tenant(2, 1, 1));

  EXPECT_NE(OB_SUCCESS, add_tenant(1, 3, 3));

  EXPECT_EQ(OB_SUCCESS, modify_tenant(1, 5, 5));

  EXPECT_EQ(-4006, del_tenant(2));
  EXPECT_EQ(-4006, del_tenant(1));
  EXPECT_EQ(OB_SUCCESS, add_tenant(1, 1, 1));
  EXPECT_EQ(OB_SUCCESS, add_tenant(2, 1, 1));
  EXPECT_NE(-1, del_tenant(3));
  EXPECT_EQ(2, get_tenant_list().size());

  ObTenant *tenant = NULL;
  EXPECT_EQ(OB_SUCCESS, get_tenant(1, tenant));
  EXPECT_TRUE(tenant);

  tenant = NULL;
  EXPECT_NE(OB_SUCCESS, get_tenant(3, tenant));
  EXPECT_FALSE(tenant);

  destroy();
  tenant = NULL;
  EXPECT_NE(OB_SUCCESS, get_tenant(1, tenant));
  EXPECT_FALSE(tenant);

  EXPECT_EQ(OB_SUCCESS, add_tenant(OB_SERVER_TENANT_ID, 3, 3));
  EXPECT_EQ(OB_SUCCESS, add_tenant(OB_SYS_TENANT_ID, 3, 3));
  EXPECT_EQ(OB_SUCCESS, add_tenant(OB_USER_TENANT_ID, 3, 3));
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_file_name("test_manage_tenant.log", true);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}
