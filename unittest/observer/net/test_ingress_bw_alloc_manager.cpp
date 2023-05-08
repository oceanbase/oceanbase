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

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#include "observer/net/ob_ingress_bw_alloc_service.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;

namespace rootserver
{
class TestEndpointIngressService : public testing::Test
{
public:
  TestEndpointIngressService()
  {}
  virtual ~TestEndpointIngressService()
  {}
  virtual void SetUp(){};
  virtual void TearDown()
  {}
  virtual void TestBody()
  {}
};

TEST_F(TestEndpointIngressService, ingress_service)
{
  int ret = OB_SUCCESS;
  ObNetEndpointIngressManager ingress_manager_;
  ret = ingress_manager_.init();
  ASSERT_EQ(ret, OB_SUCCESS);

  // register_endpoint
  ObAddr addr1(1, 1);
  ObNetEndpointKey key1;
  key1.addr_ = addr1;
  int64_t time = ObTimeUtility::current_time();

  ret = ingress_manager_.register_endpoint(key1, time);
  ASSERT_EQ(ret, OB_SUCCESS);

  // register_endpoint second time
  ret = ingress_manager_.register_endpoint(key1, time);
  ASSERT_EQ(ret, OB_SUCCESS);

  // get endpoint value
  ObNetEndpointValue *value1 = nullptr;
  ret = ingress_manager_.ingress_plan_map_.get_refactored(key1, value1);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(key1.addr_, addr1);
  ASSERT_EQ(value1->expire_time_, time);

  // set endpoint applied bandwidth
  value1->predicted_bw_ = 10;

  // update ingress plan
  ObAddr addr2(2, 2);
  ObNetEndpointKey key2;
  ObNetEndpointValue *value2 = nullptr;
  key2.addr_ = addr2;
  ret = ingress_manager_.register_endpoint(key2, time);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ingress_manager_.ingress_plan_map_.get_refactored(key2, value2);
  ASSERT_EQ(ret, OB_SUCCESS);
  value1->predicted_bw_ = 10;
  value2->predicted_bw_ = 10;

  ObAddr addr3(3, 3);
  ObNetEndpointKey key3;
  ObNetEndpointValue *value3 = nullptr;
  key3.addr_ = addr3;
  ret = ingress_manager_.register_endpoint(key3, time);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ingress_manager_.ingress_plan_map_.get_refactored(key3, value3);
  ASSERT_EQ(ret, OB_SUCCESS);
  value3->predicted_bw_ = 20;

  ingress_manager_.total_bw_limit_ = 100;

  ObNetEndpointKVArray update_kvs;

  ret = ingress_manager_.update_ingress_plan(update_kvs);
  ASSERT_EQ(ret, OB_SUCCESS);

  update_kvs.push_back(ObNetEndpointKeyValue(key2, value2));
  ret = ingress_manager_.update_ingress_plan(update_kvs);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(value2->assigned_bw_, 100);

  update_kvs.push_back(ObNetEndpointKeyValue(key3, value3));
  ret = ingress_manager_.update_ingress_plan(update_kvs);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(value2->assigned_bw_, 45);
  ASSERT_EQ(value3->assigned_bw_, 55);

  ingress_manager_.total_bw_limit_ = 30;
  ret = ingress_manager_.update_ingress_plan(update_kvs);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(value2->assigned_bw_, 10);
  ASSERT_EQ(value3->assigned_bw_, 20);

  update_kvs.push_back(ObNetEndpointKeyValue(key1, value1));
  value1->predicted_bw_ = 2;
  ingress_manager_.total_bw_limit_ = 20;
  ret = ingress_manager_.update_ingress_plan(update_kvs);
  ASSERT_EQ(ret, OB_SUCCESS);

  ASSERT_EQ(value1->assigned_bw_, 2);
  ASSERT_EQ(value2->assigned_bw_, 9);
  ASSERT_EQ(value3->assigned_bw_, 9);

  ObAddr addr4(4, 4);
  ObNetEndpointKey key4;
  ObNetEndpointValue *value4 = nullptr;
  key4.addr_ = addr4;
  ret = ingress_manager_.register_endpoint(key4, time);
  ASSERT_EQ(ret, OB_SUCCESS);
  ret = ingress_manager_.ingress_plan_map_.get_refactored(key4, value4);
  ASSERT_EQ(ret, OB_SUCCESS);
  value4->predicted_bw_ = 100;
  update_kvs.push_back(ObNetEndpointKeyValue(key4, value4));

  ret = ingress_manager_.update_ingress_plan(update_kvs);
  ASSERT_EQ(ret, OB_SUCCESS);
  ASSERT_EQ(value1->assigned_bw_, 2);
  ASSERT_EQ(value2->assigned_bw_, 6);
  ASSERT_EQ(value3->assigned_bw_, 6);
  ASSERT_EQ(value3->assigned_bw_, 6);

  ingress_manager_.destroy();
}
}  // namespace rootserver
}  // namespace oceanbase
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
#undef private