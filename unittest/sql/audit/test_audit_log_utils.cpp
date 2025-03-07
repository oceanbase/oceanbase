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

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#define private public
#include "sql/audit/ob_audit_log_utils.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_device_manager.h"
#undef private

namespace oceanbase
{
namespace sql
{
using namespace oceanbase::common;
using namespace oceanbase::share;

class TestAuditLogUtils : public ::testing::Test
{
public:
  TestAuditLogUtils() {}
  virtual ~TestAuditLogUtils() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void TestAuditLogUtils::SetUpTestCase()
{
  const int64_t test_memory = 6 * 1024 * 1024;
  ObTenantBase *tenant_base = new ObTenantBase(OB_SYS_TENANT_ID);
  auto malloc = ObMallocAllocator::get_instance();
  if (NULL == malloc->get_tenant_ctx_allocator(OB_SYS_TENANT_ID, 0)) {
    malloc->create_and_add_tenant_allocator(OB_SYS_TENANT_ID);
  }
  tenant_base->init();
  ObTenantEnv::set_tenant(tenant_base);
  ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init(test_memory));
  ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_base->set(io_service);
  ObTenantEnv::set_tenant(tenant_base);
}

void TestAuditLogUtils::TearDownTestCase()
{
  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
}

TEST_F(TestAuditLogUtils, parse_filter_definition)
{
  const char *valid_definitions[] = {
    "{}",
    "{\"filter\": {\"log\":true}}",
    "{\"filter\": {\"log\":false}}",
    "{\"filter\": {\"log\":true,\"class\":[]}}",
    "{\"filter\": {\"log\":true,\"class\":[{\"name\":\"connection\"}]}}",
    "{\"filter\": {\"log\":true,\"class\":[{\"name\":\"connection\"},{\"name\":\"general\"},{\"name\":\"table_access\"}]}}",
    "{\"filter\": {\"log\":false,\"class\":[{\"name\":\"connection\"},{\"name\":\"general\"},{\"name\":\"table_access\"}]}}"
  };
  const int valid_class[] = {0, 7, 0, 0, 1, 7, 0};
  const char *invalid_definitions[] = {
    "1",
    "{\"filter\": 1}",
    "{\"filter\": aaa}",
    "{\"filter\": {\"log\":\"a\"}}",
    "{\"filter\": {\"unknown\": true}}",
    "{\"filter\": {\"log\":true,\"class\":[1,2]}}",
    "{\"filter\": {\"log\":true,\"class\":[{\"name\":\"connection\"},{\"name\":\"unknown\"}]}}"
  };
  int valid_num = sizeof(valid_definitions) / sizeof (valid_definitions[0]);
  int valid_count_num = sizeof(valid_class) / sizeof (valid_class[0]);
  int invalid_num = sizeof(invalid_definitions) / sizeof (invalid_definitions[0]);
  bool is_valid = false;
  uint64_t audit_class = 0;
  ASSERT_EQ(valid_num, valid_count_num);
  for (int i = 0; i < valid_num; ++i) {
    is_valid = true;
    audit_class = 0;
    EXPECT_EQ(OB_SUCCESS, sql::ObAuditLogUtils::parse_filter_definition(valid_definitions[i], is_valid, audit_class));
    EXPECT_TRUE(is_valid);
    EXPECT_EQ(valid_class[i], audit_class);
    LOG_INFO("parse result", K(i), K(valid_definitions[i]), K(is_valid), K(audit_class));
  }

  for (int i = 0; i < invalid_num; ++i) {
    is_valid = true;
    audit_class = 0;
    EXPECT_EQ(OB_SUCCESS, sql::ObAuditLogUtils::parse_filter_definition(invalid_definitions[i], is_valid, audit_class));
    EXPECT_FALSE(is_valid);
    EXPECT_EQ(0, audit_class);
    LOG_INFO("parse result", K(i), K(invalid_definitions[i]), K(is_valid), K(audit_class));
  }
}

TEST_F(TestAuditLogUtils, compress_and_upload_log)
{
  sql::ObAuditLogger logger;
  logger.log_cfg_.audit_log_path_ = "file://audit/1234/567";
  const char *dir_path = "1004/ip:port";
  system("rm -rf audit");
  databuff_printf(logger.log_file_.dir_path_, sizeof(logger.log_file_.dir_path_), "%s", dir_path);
  EXPECT_EQ(OB_SUCCESS, logger.compress_and_upload_log("./test_audit_log_utils", "123", true));
  logger.log_cfg_.audit_log_compression_ = ObCompressorType::ZSTD_COMPRESSOR;
  EXPECT_EQ(OB_SUCCESS, logger.compress_and_upload_log("./test_audit_log_utils", "456", true));
}

TEST_F(TestAuditLogUtils, prune_log)
{
  sql::ObAuditLogger logger;
  system("rm -rf audit");
  system("fallocate -l 10K ./audit_file.test");
  logger.log_cfg_.audit_log_path_ = "file://audit/1234/567";
  const char *dir_path = "1004/ip:port";
  int64_t expire_time = 1711947601000000;
  databuff_printf(logger.log_file_.dir_path_, sizeof(logger.log_file_.dir_path_), "%s", dir_path);
  EXPECT_EQ(OB_SUCCESS, logger.compress_and_upload_log("./audit_file.test", "20240401120000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.compress_and_upload_log("./audit_file.test", "20240401130000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.compress_and_upload_log("./audit_file.test", "20240401140000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.compress_and_upload_log("./audit_file.test", "20240401150000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.compress_and_upload_log("./audit_file.test", "20240401160000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.compress_and_upload_log("./audit_file.test", "20240401170000000", true));
  logger.log_cfg_.audit_log_max_size_ = 3 * 10 * 1024 * 1024 - 1;
  EXPECT_EQ(OB_SUCCESS, logger.prune_log(expire_time));
}

} // end namespace sql
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_audit_log_utils.log*");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_audit_log_utils.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
