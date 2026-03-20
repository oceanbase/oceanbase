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
#define protected public
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/audit/ob_audit_log_utils.h"
#include "lib/file/ob_file.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_device_manager.h"
#undef private
#undef protected

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
  ObTenantIOManager *io_service = NULL;
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
    "{\"filter\": {\"log\":false,\"class\":[{\"name\":\"connection\"},{\"name\":\"general\"},{\"name\":\"table_access\"}]}}",
    "{\"filter\":{\"log\":true,\"class\":[{\"name\":\"table_access\",\"event\":[{\"name\":\"read\",\"log\":{\"or\":[{\"and\":[{\"field\":{\"name\":\"table_name\",\"value\":\"t1\"}},{\"field\":{\"name\":\"table_database\",\"value\":\"test1\"}}]},{\"and\":[{\"field\":{\"name\":\"table_name\",\"value\":\"t2\"}},{\"field\":{\"name\":\"table_database\",\"value\":\"test2\"}}]}]}}]}]}}",
    "{\"filter\":{\"log\":true,\"class\":[{\"name\":\"connection\",\"event\":[{\"name\":\"connect\",\"log\":{\"or\":[{\"and\":[{\"field\":{\"name\":\"user\",\"value\":\"d1\"}},{\"field\":{\"name\":\"client_ip\",\"value\":\"127.0.0.1\"}}]},{\"and\":[{\"field\":{\"name\":\"user\",\"value\":\"d2\"}},{\"field\":{\"name\":\"client_ip\",\"value\":\"127.0.0.2\"}}]}]}}]}]}}"
  };
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
  int invalid_num = sizeof(invalid_definitions) / sizeof (invalid_definitions[0]);
  bool is_valid = false;
  ObArenaAllocator allocator;
  ObAuditLogFilter filter;
  filter.set_allocator(&allocator);
  for (int i = 0; i < valid_num; ++i) {
    is_valid = true;
    filter.reset();
    EXPECT_EQ(OB_SUCCESS, sql::ObAuditLogUtils::parse_filter_definition(valid_definitions[i], is_valid, filter, allocator));
    EXPECT_TRUE(is_valid);
    LOG_INFO("parse result", K(i), K(valid_definitions[i]), K(is_valid));
  }
  for (int i = 0; i < invalid_num; ++i) {
    is_valid = true;
    filter.reset();
    EXPECT_EQ(OB_SUCCESS, sql::ObAuditLogUtils::parse_filter_definition(invalid_definitions[i], is_valid, filter, allocator));
    EXPECT_FALSE(is_valid);
    LOG_INFO("parse result", K(i), K(invalid_definitions[i]), K(is_valid));
  }
}

TEST_F(TestAuditLogUtils, parser_filter_table_access_field)
{
  ObString definition = "{\"filter\":{\"log\":true,\"class\":[{\"name\":\"table_access\",\"event\":[{\"name\":\"insert\",\"log\":{\"or\":[{\"and\":[{\"field\":{\"name\":\"table_name\",\"value\":\"t1\"}},{\"field\":{\"name\":\"table_database\",\"value\":\"test1\"}}]},{\"and\":[{\"field\":{\"name\":\"table_name\",\"value\":\"t2\"}},{\"field\":{\"name\":\"table_database\",\"value\":\"test2\"}}]}]}}]}]}}";
  bool is_valid = true;
  ObArenaAllocator allocator;
  bool is_enable = false;
  ObAuditLogFilterInfo *audit_filter;
  audit_filter = OB_NEWx(ObAuditLogFilterInfo, (&allocator));
  ASSERT_NE(nullptr, audit_filter);
  audit_filter->reset();
  audit_filter->get_filter().set_allocator(&allocator);
  EXPECT_EQ(OB_SUCCESS, sql::ObAuditLogUtils::parse_filter_definition(definition, is_valid, audit_filter->get_filter(), allocator));
  EXPECT_TRUE(is_valid);
  ObAuditDMLFieldParam param1, param2, param3;
  param1.event_type_ = static_cast<uint64_t>(ObAuditLogEventDMLType::READ);
  param1.dml_infos_.push_back({"test1", "t1", static_cast<uint64_t>(ObAuditLogEventDMLType::READ)});
  param2.event_type_ = static_cast<uint64_t>(ObAuditLogEventDMLType::INSERT);
  param2.dml_infos_.push_back({"test2", "t2", static_cast<uint64_t>(ObAuditLogEventDMLType::INSERT)});
  param3.event_type_ = static_cast<uint64_t>(ObAuditLogEventDMLType::INSERT);
  param3.dml_infos_.push_back({"test1", "t2", static_cast<uint64_t>(ObAuditLogEventDMLType::INSERT)});
  ASSERT_EQ(OB_SUCCESS, audit_filter->check_is_enable(param2, is_enable));
  ASSERT_TRUE(is_enable);
  ObIKVCacheValue *kv;
  char *buf = NULL;
  int64_t buf_size = audit_filter->size();
  ASSERT_NE(nullptr, buf = static_cast<char *>(allocator.alloc(buf_size)));
  ASSERT_EQ(OB_SUCCESS, audit_filter->deep_copy(buf, buf_size, kv));
  ObAuditLogFilterInfo *filter_info = dynamic_cast<ObAuditLogFilterInfo *>(kv);
  ASSERT_NE(nullptr, filter_info);
  ASSERT_EQ(OB_SUCCESS, filter_info->check_is_enable(param2, is_enable));
  ASSERT_TRUE(is_enable);
  OB_DELETEx(ObAuditLogFilterInfo, &allocator, audit_filter);
  ObAuditLogFilter &filter = filter_info->filter_;
  ASSERT_EQ(1, filter.classes_.count());
  ObAuditLogClass *cls = filter.classes_.at(0);
  ASSERT_NE(nullptr, cls);
  ASSERT_EQ(1, cls->events_.count());
  ObAuditLogEvent *event = cls->events_.at(0);
  ASSERT_NE(nullptr, event);
  ObAuditLogNode *field_root = event->field_;
  ASSERT_NE(nullptr, field_root);
  ObAuditLogOpNode *or_node = dynamic_cast<ObAuditLogOpNode*>(field_root);
  ASSERT_NE(nullptr, or_node);
  ASSERT_EQ(ObAuditLogOpNode::OR_OP, or_node->op_type_);
  ASSERT_EQ(2, or_node->children_.count());
  ObAuditLogOpNode *and_node1 = dynamic_cast<ObAuditLogOpNode*>(or_node->children_.at(0));
  ObAuditLogOpNode *and_node2 = dynamic_cast<ObAuditLogOpNode*>(or_node->children_.at(1));
  ASSERT_NE(nullptr, and_node1);
  ASSERT_NE(nullptr, and_node2);
  ASSERT_EQ(ObAuditLogOpNode::AND_OP, and_node1->op_type_);
  ASSERT_EQ(2, and_node1->children_.count());
  ASSERT_EQ(2, and_node2->children_.count());
  ObAuditLogFieldNode *field1 = dynamic_cast<ObAuditLogFieldNode*>(and_node1->children_.at(0));
  ObAuditLogFieldNode *field2 = dynamic_cast<ObAuditLogFieldNode*>(and_node1->children_.at(1));
  ASSERT_NE(nullptr, field1);
  ASSERT_NE(nullptr, field2);
  EXPECT_EQ(field1->field_type_, static_cast<uint64_t>(ObAuditLogDMLFieldType::TABLE_NAME));
  EXPECT_EQ(field1->value_.str_val_, ObString::make_string("t1"));
  EXPECT_EQ(field2->field_type_, static_cast<uint64_t>(ObAuditLogDMLFieldType::TABLE_DATABASE));
  EXPECT_EQ(field2->value_.str_val_, ObString::make_string("test1"));
}

TEST_F(TestAuditLogUtils, complex_test)
{
  ObString definition1 = "{\"filter\":{\"log\":true,\"class\":[{\"name\":\"table_access\",\"event\":[{\"name\":\"insert\"},{\"name\":\"delete\"}]}]}}";
  bool is_valid = true;
  ObArenaAllocator allocator;
  ObAuditLogFilter filter;
  filter.set_allocator(&allocator);
  EXPECT_EQ(OB_SUCCESS, sql::ObAuditLogUtils::parse_filter_definition(definition1, is_valid, filter, allocator));
  EXPECT_TRUE(is_valid);
  ASSERT_EQ(1, filter.classes_.count());
  ObAuditLogClass *cls = filter.classes_.at(0);
  ASSERT_NE(nullptr, cls);
  ASSERT_EQ(ObAuditLogClassType::TABLE_ACCESS, cls->class_type_);
  ASSERT_EQ(2, cls->events_.count());
  EXPECT_EQ(static_cast<uint64_t>(ObAuditLogEventDMLType::INSERT), cls->events_.at(0)->event_type_);
  EXPECT_EQ(static_cast<uint64_t>(ObAuditLogEventDMLType::DELETE), cls->events_.at(1)->event_type_);
}

TEST_F(TestAuditLogUtils, general_field)
{
  ObArenaAllocator allocator;
  ObStmt *alter_table_stmt = OB_NEWx(ObAlterTableStmt, (&allocator));
  ASSERT_NE(nullptr, alter_table_stmt);
  ObAuditGeneralFieldParam param;
  EXPECT_EQ(OB_SUCCESS, ObAuditLogUtils::fill_audit_general_obj_name(0, alter_table_stmt, NULL, param));
  EXPECT_EQ(0, param.general_sql_command_.case_compare(ObString::make_string("ALTER_TABLE")));
  ObStmt *set_config_stmt = OB_NEWx(ObSetConfigStmt, (&allocator));
  ASSERT_NE(nullptr, set_config_stmt);
  EXPECT_EQ(OB_SUCCESS, ObAuditLogUtils::fill_audit_general_obj_name(0, set_config_stmt, NULL, param));
  EXPECT_EQ(0, param.general_sql_command_.case_compare(ObString::make_string("ALTER_SYSTEM_SET_PARAMETER")));
}

TEST_F(TestAuditLogUtils, parser_filter_conn_field)
{
  ObString definition = "{\"filter\":{\"log\":true,\"class\":[{\"name\":\"connection\",\"event\":[{\"name\":\"connect\",\"log\":{\"and\":[{\"or\":[{\"field\":{\"name\":\"client_ip\",\"value\":\"127.0.0.2\"}},{\"field\":{\"name\":\"user\",\"value\":\"admin\"}}]},{\"or\":[{\"and\":[{\"field\":{\"name\":\"connection_id\",\"value\":10}},{\"field\":{\"name\":\"user\",\"value\":\"u1\"}}]},{\"and\":[{\"field\":{\"name\":\"connection_id\",\"value\":20}},{\"field\":{\"name\":\"client_ip\",\"value\":\"127.0.0.3\"}}]}]}]}}]}]}}";
  bool is_valid = true;
  ObArenaAllocator allocator;
  ObAuditLogFilterInfo *audit_filter;
  audit_filter = OB_NEWx(ObAuditLogFilterInfo, (&allocator));
  ASSERT_NE(nullptr, audit_filter);
  audit_filter->reset();
  audit_filter->get_filter().set_allocator(&allocator);
  EXPECT_EQ(OB_SUCCESS, sql::ObAuditLogUtils::parse_filter_definition(definition, is_valid, audit_filter->get_filter(), allocator));
  EXPECT_TRUE(is_valid);
  bool is_enable = false;
  ObAuditConnFieldParam conn_param;
  conn_param.event_type_ = static_cast<uint64_t>(ObAuditLogEventConnType::CONNECT);
  conn_param.connection_id_ = 10;
  conn_param.user_name_ = ObString::make_string("u2");
  EXPECT_EQ(OB_SUCCESS, audit_filter->check_is_enable(conn_param, is_enable));
  EXPECT_FALSE(is_enable);
  conn_param.user_name_ = ObString::make_string("admin");
  conn_param.connection_id_ = 20;
  conn_param.client_ip_ = "127.0.0.3";
  EXPECT_EQ(OB_SUCCESS, audit_filter->check_is_enable(conn_param, is_enable));
  EXPECT_TRUE(is_enable);
  is_enable = false;
  conn_param.user_name_ = ObString::make_string("u1");
  conn_param.client_ip_ = "127.0.0.2";
  EXPECT_EQ(OB_SUCCESS, audit_filter->check_is_enable(conn_param, is_enable));
  EXPECT_FALSE(is_enable);
  conn_param.connection_id_ = 10;
  EXPECT_EQ(OB_SUCCESS, audit_filter->check_is_enable(conn_param, is_enable));
  EXPECT_TRUE(is_enable);
  OB_DELETEx(ObAuditLogFilterInfo, &allocator, audit_filter);
}

TEST_F(TestAuditLogUtils, archive_log_file)
{
  sql::ObAuditLogger logger;
  logger.log_cfg_.audit_log_path_ = "file://audit/1234/567";
  const char *dir_path = "1004/ip:port";
  system("rm -rf audit ./audit_file.test");
  system("dd if=/dev/urandom of=audit_file.test bs=1M count=1");
  databuff_printf(logger.log_file_.dir_path_, sizeof(logger.log_file_.dir_path_), "%s", dir_path);
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "123", true));
  logger.log_cfg_.audit_log_compression_ = ObCompressorType::ZSTD_COMPRESSOR;
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "456", true));
}

TEST_F(TestAuditLogUtils, prune_log)
{
  sql::ObAuditLogger logger;
  system("rm -rf audit ./audit_file.test");
  system("dd if=/dev/urandom of=audit_file.test bs=1M count=1");
  logger.log_cfg_.audit_log_path_ = "file://audit/1234/567";
  const char *dir_path = "1004/ip:port";
  int64_t expire_time = 1711947601000000;
  databuff_printf(logger.log_file_.dir_path_, sizeof(logger.log_file_.dir_path_), "%s", dir_path);
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401120000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401130000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401140000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401150000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401160000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401170000000", true));
  logger.log_cfg_.audit_log_max_size_ = 3 * 10 * 1024 * 1024 - 1;
  EXPECT_EQ(OB_SUCCESS, logger.prune_log(expire_time));
}

TEST_F(TestAuditLogUtils, compress_and_encrypt_log)
{
  sql::ObAuditLogger logger;
  ObArenaAllocator allocator;
  int64_t pwd_id = 4321;
  const char *password = "1234567890";
  int64_t pwd_len = strlen(password);
  logger.init();
  system("rm -rf audit ./audit_file.test");
  system("dd if=/dev/urandom of=audit_file.test bs=1M count=1");
  logger.log_cfg_.audit_log_path_ = "file://audit/1234/567";
  const char *dir_path = "1004/ip:port";
  int64_t expire_time = 1711947601000000;
  databuff_printf(logger.log_file_.dir_path_, sizeof(logger.log_file_.dir_path_), "%s", dir_path);
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401120000000", true));
  logger.log_cfg_.audit_log_encryption_ = ObCipherOpMode::ob_aes_256_cbc;
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401130000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.set_audit_log_password(pwd_id, password, pwd_len));
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401140000000", true));
  logger.log_cfg_.audit_log_encryption_ = ObCipherOpMode::ob_invalid_mode;
  logger.log_cfg_.audit_log_compression_ = ObCompressorType::ZSTD_COMPRESSOR;
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401150000000", true));
  logger.log_cfg_.audit_log_encryption_ = ObCipherOpMode::ob_aes_256_cbc;
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401160000000", true));
  EXPECT_EQ(OB_SUCCESS, logger.archive_log_file("./audit_file.test", "20240401170000000", true));
  ASSERT_EQ(0, system("openssl enc -d -aes-256-cbc -pass pass:1234567890 -md sha256 -salt -in audit/1234/567/1004/ip:port/audit.log.20240401140000000.enc.4321 -out audit/1234/567/1004/ip:port/audit.log.20240401140000000"));
  ASSERT_EQ(0, system("openssl enc -d -aes-256-cbc -pass pass:1234567890 -md sha256 -salt -in audit/1234/567/1004/ip:port/audit.log.20240401160000000.zst.enc.4321 -out audit/1234/567/1004/ip:port/audit.log.20240401160000000.zst"));
  ASSERT_EQ(0, system("cmp audit/1234/567/1004/ip:port/audit.log.20240401140000000 ./audit_file.test"));
  ASSERT_EQ(0, system("cmp audit/1234/567/1004/ip:port/audit.log.20240401150000000.zst audit/1234/567/1004/ip:port/audit.log.20240401160000000.zst"));
  // unzstd is not available in test core-test environment
  // ASSERT_EQ(0, system("unzstd audit/1234/567/1004/ip:port/audit.log.20240401160000000.zst"));
  // EXPECT_EQ(0, system("cmp audit/1234/567/1004/ip:port/audit.log.20240401160000000 ./audit_file.test"));
}

} // end namespace sql
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_audit_log_utils.log* audit");
  oceanbase::common::ObLogger::get_logger().set_file_name("test_audit_log_utils.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
