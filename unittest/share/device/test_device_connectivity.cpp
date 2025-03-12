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
#include "share/object_storage/ob_device_connectivity.h"
#include "share/object_storage/ob_object_storage_struct.h"

#define private public
#undef private

namespace oceanbase {
namespace unittest {

using namespace oceanbase::common;
using namespace oceanbase::share;

class TestDeviceConnectivity: public ::testing::Test
{
public:
  TestDeviceConnectivity() {}
  virtual ~TestDeviceConnectivity() {}
  virtual void SetUp()
  {
  }
  virtual void TearDown() {}
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestDeviceConnectivity);
};

TEST_F(TestDeviceConnectivity, DISABLED_test_device_connectivity)
{
  const char *path = "oss://cloudstorageb1/test";
  const char *endpoint = "host=oss-cn-hangzhou.aliyuncs.com";
  const char *encrypt_authorization = "access_id=xxx&access_key=xxx";
  const char *extension = "";
  ObBackupDest storage_dest;
  ASSERT_EQ(OB_SUCCESS, storage_dest.set(path, endpoint, encrypt_authorization, extension));
  ObDeviceConnectivityCheckManager conn_check_mgr;
  ASSERT_EQ(OB_SUCCESS, conn_check_mgr.check_device_connectivity(storage_dest));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_device_connectivity.log");
  OB_LOGGER.set_file_name("test_device_connectivity.log");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
