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
#define USING_LOG_PREFIX STORAGETEST

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class TestDeviceConfigMgr : public ::testing::Test
{
public:
  TestDeviceConfigMgr() {}
  virtual ~TestDeviceConfigMgr() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
};

void TestDeviceConfigMgr::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestDeviceConfigMgr::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
    LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestDeviceConfigMgr::SetUp()
{
}

void TestDeviceConfigMgr::TearDown()
{
}

TEST_F(TestDeviceConfigMgr, test_get_device_config_str)
{
  const int64_t buf_len = 4096;
  char buf1[buf_len];
  MEMSET(buf1, 0, buf_len);
  char buf2[buf_len];
  MEMSET(buf2, 0, buf_len);

  ObDeviceConfig device_config;
  ASSERT_EQ(OB_SUCCESS, OB_DEVICE_CONF_MGR.get_device_config(ObStorageUsedType::USED_TYPE_DATA, device_config));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, databuff_printf(buf1, buf_len, pos, "%s?%s", device_config.path_, device_config.endpoint_));

  ASSERT_EQ(OB_SUCCESS, OB_DEVICE_CONF_MGR.get_device_config_str(ObStorageUsedType::USED_TYPE_DATA, buf2, buf_len));

  ASSERT_EQ(0, MEMCMP(buf1, buf2, buf_len));
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_device_config_mgr.log*");
  OB_LOGGER.set_file_name("test_device_config_mgr.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
