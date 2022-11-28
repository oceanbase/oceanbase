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

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include "lib/restore/oss/include/oss_c_sdk/aos_http_io.h"
#include "lib/restore/oss/include/oss_c_sdk/aos_define.h"
#include "vsclient.h"
#include "lib/utility/ob_test_util.h"
#include "share/restore/ob_restore_uri_parser.h"
#include "share/restore/ob_restore_base_reader.h"
#include "lib/restore/ob_storage.h"
#include "share/restore/ob_oss_resource_reader.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::restore;
class TestRestoreReader: public ::testing::Test
{
public:
  TestRestoreReader();
  virtual ~TestRestoreReader();
  virtual void SetUp();
  virtual void TearDown();
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRestoreReader);
protected:
  // function members
protected:
  // data members
};

TestRestoreReader::TestRestoreReader()
{
  int ret = OB_SUCCESS;
  int aos_ret = AOSE_OK;
  if(AOSE_OK != (aos_ret = aos_http_io_initialize(NULL, 0))) {
    OB_LOG(WARN, "fail to init aos", K(aos_ret));
  }
  VIPSrv_Result *result = NULL;
  VIPSrv_Config config;
  config.jmenv_dom = "jmenv.tbsite.net";
  config.cache_dir = getenv("HOME");
  if(NULL == (result = vipsrv_global_init(&config))) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to init vipserver", K(ret));
  } else if(!result->is_success) {
    ret = OB_OSS_ERROR;
    OB_LOG(WARN, "fail to init vipserver", K(ret));
  }

  if(NULL != result) {
    vipsrv_result_deref(result);
  }
}

TestRestoreReader::~TestRestoreReader()
{
  vipsrv_global_cleanup();
  aos_http_io_deinitialize();
}

void TestRestoreReader::SetUp()
{
  const int64_t MAX_BUF_SIZE = 2048;
  char write_buf[MAX_BUF_SIZE];
  char uri[OB_MAX_URI_LENGTH];
  int n = 0;
  ObBackupIoAdapter util;
  const char *storage_info = "host=oss-cn-hangzhou-zmf.aliyuncs.com&access_id=LTAIwCjHu9OYhWyx&access_key=RHyQfn2TnnOyY9wsedthx1nx9EVkH3";

  //resource unit
  n = snprintf(write_buf, MAX_BUF_SIZE, "CREATE RESOURCE UNIT small_unit MAX_CPU 18.000000, MEMORY_SIZE 21474836480;");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/resource_unit_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //resource pool
  n = snprintf(write_buf, MAX_BUF_SIZE, "CREATE RESOURCE POOL pool1 UNIT small_unit, UNIT_NUM 1, ZONE_LIST ('zone1');");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/resource_pool_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //create tenant
  n = snprintf(write_buf, MAX_BUF_SIZE, "CREATE TENANT tt1 charset=utf8mb4, replica_num=5, zone_list('zone1'), primary_zone='zone1', "
      "locality='FULL{1}@zone1', resource_pool_list('pool1');");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/create_tenant_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //create database
  n = snprintf(write_buf, MAX_BUF_SIZE, "CREATE DATABASE IF NOT EXISTS `obtrade` DEFAULT CHARACTER SET = utf8mb4 REPLICA_NUM = 5;");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/create_database_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //system variable
  n = snprintf(write_buf, MAX_BUF_SIZE, "set global wait_timeout  = 28800;");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/system_variable_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //create_user_definition
  n = snprintf(write_buf, MAX_BUF_SIZE, "create user monitor IDENTIFIED BY PASSWORD '*1975d095ac033caf4e1bf94f7202a9bbfeeb66f1';");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/create_user_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //user_privilege_definition
  n = snprintf(write_buf, MAX_BUF_SIZE, "GRANT ALL PRIVILEGES ON *.* TO 'admin' WITH GRANT OPTION;");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/user_privilege_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //crate table
  n = snprintf(write_buf, MAX_BUF_SIZE, " CREATE TABLE obtrade.business_activity_000 ("
      "`TX_ID` varchar(256) NOT NULL id 16,"
      "`STATE` varchar(2) NOT NULL id 17,"
      "`ACCOUNT_TRANS_STATE` varchar(2) NOT NULL id 18,"
      "`GMT_CREATE` datetime NOT NULL id 19,"
      "`GMT_MODIFIED` datetime NOT NULL id 20,"
      "`PROPAGATION` varchar(2) DEFAULT NULL id 21,"
      "`CONTEXT` varchar(4096) DEFAULT NULL id 22,"
      "PRIMARY KEY (`TX_ID`),"
      "KEY `BUSINESS_ACTIVITY_GMT_IND` (`GMT_MODIFIED`) STORING (`STATE`) BLOCK_SIZE 16384  index_table_id = 1101710651081555"
      ") DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 5 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE "
      " TABLEGROUP = 'obtrade_tablegroup'  table_id = 1101710651081554 ;");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/435656765867/ob_trade_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //create_tablegroup_definition
  n = snprintf(write_buf, MAX_BUF_SIZE, "CREATE TABLEGROUP tg1;");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/create_tablegroup_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //backup_info
  n = snprintf(write_buf, MAX_BUF_SIZE, "0_127_1496650316422256_0");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/tenant_id/1001/backup_info");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //data_table_ids
  n = snprintf(write_buf, MAX_BUF_SIZE, "435656765867:ob_trade");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/data_table_ids_list");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //create index
  n = snprintf(write_buf, MAX_BUF_SIZE, "CREATE UNIQUE INDEX __idx_1099511677787_haipeng_unique_index on oceanbase.haipeng ("
      "`c1` int(11) NOT NULL id 16,"
      "`c2` int(11) DEFAULT NULL id 17,"
      "`c3` int(11) DEFAULT NULL id 18"
      ") BLOCK_SIZE 16384  index_table_id = 1099511677791 data_table_id = 1099511677787 STORING (`c2`);");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/435656765867/__idx_1099511677787_haipeng_unique_index_definition");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
  //index_table_ids
  n = snprintf(write_buf, MAX_BUF_SIZE, "435656765867:1099511677787:__idx_1099511677787_haipeng_unique_index");
  snprintf(uri, OB_MAX_URI_LENGTH, "oss://071092/unittest/127/1001/index_table_ids_list");
  EXPECT_EQ(OB_SUCCESS, util.write_single_file(uri, storage_info,  write_buf, n));
}

void TestRestoreReader::TearDown()
{
}

TEST_F(TestRestoreReader, test_basic)
{
  // The following only applies to offline testing. Regression testing on farm will always report errors and there is no runtime environment.

  ObRestoreArgs arg;
  const char *uri_001 = "oss://071092/unittest/1001?timestamp=1496650316422256&host=oss-cn-hangzhou-zmf.aliyuncs.com&access_id=LTAIwCjHu9OYhWyx&access_key=RHyQfn2TnnOyY9wsedthx1nx9EVkH3&restore_user=a&restore_pass=b";
  EXPECT_EQ(OB_SUCCESS, ObRestoreURIParser::parse(ObString(uri_001), arg));
  LOG_INFO("arg info", K(arg));

  ObRestoreBaseReader reader(arg);
  ObArray<ObString> stmts;
  ASSERT_EQ(OB_SUCCESS, reader.init(ObString(uri_001)));
  ASSERT_EQ(OB_SUCCESS, reader.get_create_unit_stmts(stmts));
  LOG_INFO("restore test", K(arg), K(stmts));
  ASSERT_TRUE(stmts.count() > 0);

  ASSERT_EQ(OB_SUCCESS, reader.get_create_pool_stmts(stmts));
  LOG_INFO("restore test", K(arg), K(stmts));
  ASSERT_TRUE(stmts.count() > 0);

  ObString stmt;
  ASSERT_EQ(OB_SUCCESS, reader.get_create_tenant_stmt(stmt));
  LOG_INFO("restore test", K(arg), K(stmt));

  ASSERT_EQ(OB_SUCCESS, reader.get_create_tablegroup_stmts(stmts));
  LOG_INFO("restore test", K(arg), K(stmts));
  ASSERT_TRUE(stmts.count() > 0);

  ASSERT_EQ(OB_SUCCESS, reader.get_create_database_stmts(stmts));
  LOG_INFO("restore test", K(arg), K(stmts));
  ASSERT_TRUE(stmts.count() > 0);

  ASSERT_EQ(OB_SUCCESS, reader.get_create_data_table_stmts(stmts));
  LOG_INFO("restore test", K(arg), K(stmts));
  ASSERT_TRUE(stmts.count() > 0);

  ASSERT_EQ(OB_SUCCESS, reader.get_create_index_table_stmts(stmts));
  LOG_INFO("restore test", K(arg), K(stmts));
  ASSERT_TRUE(stmts.count() > 0);

}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
    OB_LOGGER.set_mod_log_levels("ALL.*:INFO,LIB.MYSQLC:ERROR");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
