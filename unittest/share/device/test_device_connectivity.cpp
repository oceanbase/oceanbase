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
#include "lib/ob_errno.h"
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

TEST_F(TestDeviceConnectivity, test_change_checksum_type)
{
  ObStorageDestAttr dest_attr;
  dest_attr.reset();
  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_NO_CHECKSUM_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "checksum_type=no_checksum"));

  dest_attr.reset();
  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_MD5_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "checksum_type=md5"));
  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_CRC32_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "checksum_type=crc32"));

  dest_attr.reset();
  ASSERT_EQ(OB_SUCCESS, databuff_printf(dest_attr.extension_, OB_MAX_BACKUP_EXTENSION_LENGTH, "%s", "delete_mode=xxx&s3_region=xxx"));

  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_MD5_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "delete_mode=xxx&s3_region=xxx&checksum_type=md5"));

  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_CRC32_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "delete_mode=xxx&s3_region=xxx&checksum_type=crc32"));

  ASSERT_EQ(OB_SUCCESS, databuff_printf(dest_attr.extension_, OB_MAX_BACKUP_EXTENSION_LENGTH, "%s", "delete_mode=xxx&checksum_type=md5&s3_region=xxx"));
  ASSERT_EQ(OB_SUCCESS, dest_attr.change_checksum_type(ObStorageChecksumType::OB_CRC32_ALGO));
  ASSERT_EQ(0, strcmp(dest_attr.extension_, "delete_mode=xxx&s3_region=xxx&checksum_type=crc32"));
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
