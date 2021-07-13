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

#define protected public
#include "storage/transaction/ob_location_adapter.h"
#include "../mockcontainer/mock_ob_location_cache.h"
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
using namespace transaction;
namespace unittest {
class TestObLocationAdapter : public ::testing::Test {
public:
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

public:
  // valid partition parameters
  static const int64_t VALID_TABLE_ID = 1;
  static const int32_t VALID_PARTITION_ID = 1;
  static const int32_t VALID_PARTITION_COUNT = 100;
  // invalid partition parameters
  static const int64_t INVALID_TABLE_ID = -1;
  static const int32_t INVALID_PARTITION_ID = -1;
  static const int32_t INVALID_PARTITION_COUNT = -100;

  // port for ObAddr
  static const int32_t PORT = 8080;
  // ipv4 for ObAddr
  static const ObAddr::VER IP_TYPE = ObAddr::IPV4;
  // ip address for test
  static const char* LOCAL_IP;
};
const char* TestObLocationAdapter::LOCAL_IP = "127.0.0.1";

//////////////////////basic function test//////////////////////////////////////////
// test the init and detroy of ObLocationAdapter
TEST_F(TestObLocationAdapter, init_destroy)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLocationAdapter location_adapter;
  MockObLocationCache* location_cache = new MockObLocationCache();
  EXPECT_TRUE(NULL != location_cache);
  ObMultiVersionSchemaService* schema_service = new ObMultiVersionSchemaService();
  EXPECT_TRUE(NULL != schema_service);
  EXPECT_EQ(OB_SUCCESS, location_cache->init());
  EXPECT_EQ(OB_SUCCESS, location_adapter.init(location_cache, schema_service));
  location_adapter.destroy();
  delete location_cache;
  location_cache = NULL;
  delete schema_service;
  schema_service = NULL;
}

// get ObServer according to a partition
TEST_F(TestObLocationAdapter, get_strong_leader)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // init location_adapter
  ObLocationAdapter location_adapter;
  MockObLocationCache* location_cache = new MockObLocationCache();
  EXPECT_TRUE(NULL != location_cache);
  ObMultiVersionSchemaService* schema_service = new ObMultiVersionSchemaService();
  EXPECT_TRUE(NULL != schema_service);
  EXPECT_EQ(OB_SUCCESS, location_cache->init());
  EXPECT_EQ(OB_SUCCESS, location_adapter.init(location_cache, schema_service));
  ObPartitionKey partition_key(TestObLocationAdapter::VALID_TABLE_ID,
      TestObLocationAdapter::VALID_PARTITION_ID,
      TestObLocationAdapter::VALID_PARTITION_COUNT);

  // add partition and the corresponding ob_server into location cache
  ObAddr partition_leader(TestObLocationAdapter::IP_TYPE, TestObLocationAdapter::LOCAL_IP, TestObLocationAdapter::PORT);
  // there is no leader of the corresponding partition in location cache
  ObAddr leader;
  EXPECT_EQ(OB_ERR_UNEXPECTED, location_adapter.get_strong_leader(partition_key, leader));
  EXPECT_EQ(OB_SUCCESS, location_cache->add(partition_key, partition_leader));
  EXPECT_EQ(OB_SUCCESS, location_adapter.nonblock_get_strong_leader(partition_key, leader));
  EXPECT_EQ(leader, partition_leader);
  leader.reset();
  EXPECT_EQ(OB_SUCCESS, location_adapter.get_strong_leader(partition_key, leader));
  EXPECT_EQ(leader, partition_leader);

  delete location_cache;
  location_cache = NULL;
  delete schema_service;
  schema_service = NULL;
}
///////////////////////////////boundary test//////////////////////////////////////
// test the error cases of init of ObLocationAdapter: repeated init, NULL location_cache
TEST_F(TestObLocationAdapter, repeat_init)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLocationAdapter location_adapter;
  MockObLocationCache* location_cache = NULL;
  ObMultiVersionSchemaService* schema_service = new ObMultiVersionSchemaService();
  EXPECT_TRUE(NULL != schema_service);
  EXPECT_EQ(OB_INVALID_ARGUMENT, location_adapter.init(location_cache, schema_service));
  location_adapter.destroy();

  location_cache = new MockObLocationCache();
  EXPECT_TRUE(NULL != location_cache);
  EXPECT_EQ(OB_SUCCESS, location_adapter.init(location_cache, schema_service));

  EXPECT_EQ(OB_INIT_TWICE, location_adapter.init(location_cache, schema_service));
  location_adapter.destroy();
  delete location_cache;
  location_cache = NULL;
  delete schema_service;
  schema_service = NULL;
}
// due to the execution of nonblock_get_strong_leader without init, partition is invalid
TEST_F(TestObLocationAdapter, get_strong_leader_invalid_args)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  ObLocationAdapter location_adapter;
  ObPartitionKey partition_key(TestObLocationAdapter::INVALID_TABLE_ID,
      TestObLocationAdapter::INVALID_PARTITION_ID,
      TestObLocationAdapter::INVALID_PARTITION_COUNT);
  ObAddr partition_leader(TestObLocationAdapter::IP_TYPE, TestObLocationAdapter::LOCAL_IP, TestObLocationAdapter::PORT);
  EXPECT_EQ(OB_NOT_INIT, location_adapter.nonblock_get_strong_leader(partition_key, partition_leader));
  EXPECT_EQ(OB_NOT_INIT, location_adapter.get_strong_leader(partition_key, partition_leader));

  // init of location_adapter
  MockObLocationCache* location_cache = new MockObLocationCache();
  EXPECT_TRUE(NULL != location_cache);
  ObMultiVersionSchemaService* schema_service = new ObMultiVersionSchemaService();
  EXPECT_TRUE(NULL != schema_service);
  EXPECT_EQ(OB_SUCCESS, location_cache->init());
  EXPECT_EQ(OB_SUCCESS, location_adapter.init(location_cache, schema_service));
  EXPECT_EQ(OB_INVALID_ARGUMENT, location_adapter.nonblock_get_strong_leader(partition_key, partition_leader));
  EXPECT_EQ(OB_INVALID_ARGUMENT, location_adapter.get_strong_leader(partition_key, partition_leader));

  location_adapter.destroy();
  delete location_cache;
  location_cache = NULL;
  delete schema_service;
  schema_service = NULL;
}

TEST_F(TestObLocationAdapter, nonblock_renew)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  // init of location_adapter
  ObLocationAdapter location_adapter;
  ObPartitionKey partition(TestObLocationAdapter::INVALID_TABLE_ID,
      TestObLocationAdapter::INVALID_PARTITION_ID,
      TestObLocationAdapter::INVALID_PARTITION_COUNT);
  const int64_t expire_renew_time = INT64_MAX;
  EXPECT_EQ(OB_NOT_INIT, location_adapter.nonblock_renew(partition, expire_renew_time));

  MockObLocationCache* location_cache = new MockObLocationCache();
  EXPECT_TRUE(NULL != location_cache);
  ObMultiVersionSchemaService* schema_service = new ObMultiVersionSchemaService();
  EXPECT_TRUE(NULL != schema_service);
  EXPECT_EQ(OB_SUCCESS, location_cache->init());
  EXPECT_EQ(OB_SUCCESS, location_adapter.init(location_cache, schema_service));
  EXPECT_EQ(OB_INVALID_ARGUMENT, location_adapter.nonblock_renew(partition, true));

  ObPartitionKey partition_key(TestObLocationAdapter::VALID_TABLE_ID,
      TestObLocationAdapter::VALID_PARTITION_ID,
      TestObLocationAdapter::VALID_PARTITION_COUNT);
  EXPECT_EQ(OB_SUCCESS, location_adapter.nonblock_renew(partition_key, true));

  delete location_cache;
  location_cache = NULL;
  delete schema_service;
  schema_service = NULL;
}
}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_location_adapter.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  if (OB_SUCCESS != (ret = ObClockGenerator::init())) {
    TRANS_LOG(WARN, "init ObClockGenerator error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
