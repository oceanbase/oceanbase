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
#include <gmock/gmock.h>
#include "share/arbitration_service/ob_arbitration_service_table_operator.h"
#include "share/arbitration_service/ob_arbitration_service_info.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"



namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;

class TestArbitrationServiceTableOperator : public unittest::ObSimpleClusterTestBase
{
public:
  TestArbitrationServiceTableOperator() : unittest::ObSimpleClusterTestBase("test_arbitration_service_table_operator") {}
protected:
  ObArbitrationServiceTableOperator arb_service_table_operator_;
};

TEST_F(TestArbitrationServiceTableOperator, test_type)
{
  int ret = OB_SUCCESS;
  ObString arbitration_service_key = "default";
  ObString arbitration_service = "127.0.0.1:1234";
  ObString previous_arbitration_service = "";
  ObArbitrationServiceType addr_type(ObArbitrationServiceType::ArbitrationServiceType::ADDR);
  ObArbitrationServiceType url_type(ObArbitrationServiceType::ArbitrationServiceType::URL);
  ObArbitrationServiceType invalid_type(ObArbitrationServiceType::ArbitrationServiceType::MAX_TYPE);
  ObArbitrationServiceInfo arbitration_service_info;

  arbitration_service_info.reset();
  ret = arbitration_service_info.init(arbitration_service_key, arbitration_service,
                                      previous_arbitration_service, addr_type);
  ASSERT_EQ(OB_SUCCESS, ret);

  arbitration_service_info.reset();
  ret = arbitration_service_info.init(arbitration_service_key, arbitration_service,
                                      previous_arbitration_service, url_type);
  ASSERT_EQ(OB_SUCCESS, ret);

  arbitration_service_info.reset();
  ret = arbitration_service_info.init(arbitration_service_key, arbitration_service,
                                      previous_arbitration_service, invalid_type);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  ObString mixed_addr = "Addr";
  ObString mixed_url = "Url";
  ObArbitrationServiceType addr_type_in_mixed_case;
  ret = addr_type_in_mixed_case.parse_from_string(mixed_addr);
  ASSERT_EQ(OB_SUCCESS, ret);
  arbitration_service_info.reset();
  ret = arbitration_service_info.init(arbitration_service_key, arbitration_service,
                                      previous_arbitration_service, addr_type_in_mixed_case);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObArbitrationServiceType url_type_in_mixed_case;
  ret = url_type_in_mixed_case.parse_from_string(mixed_url);
  ASSERT_EQ(OB_SUCCESS, ret);
  arbitration_service_info.reset();
  ret = arbitration_service_info.init(arbitration_service_key, arbitration_service,
                                      previous_arbitration_service, url_type_in_mixed_case);
  ASSERT_EQ(OB_SUCCESS, ret);

  bool equal = false;
  equal = url_type_in_mixed_case == url_type;
  ASSERT_EQ(true, equal);

  equal = addr_type_in_mixed_case == addr_type;
  ASSERT_EQ(true, equal);
}


TEST_F(TestArbitrationServiceTableOperator, test_single)
{
  int ret = OB_SUCCESS;
  ObString arbitration_service_key = "default";
  ObString arbitration_service = "127.0.0.1:1234";
  ObString previous_arbitration_service = "";
  ObArbitrationServiceType type(ObArbitrationServiceType::ArbitrationServiceType::ADDR);
  ObString invalid_arbitration_service_key = "";
  ObString not_exist_arbitration_service_key = "not";
  ObArbitrationServiceType invalid_type(ObArbitrationServiceType::ArbitrationServiceType::MAX_TYPE);
  bool lock_line = true;

  //create initial members
  common::ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  ObArbitrationServiceInfo invalid_arbitration_service_info;
  ObArbitrationServiceInfo arb_service_info_to_get;
  ObArbitrationServiceInfo arb_service_info;
  bool equal = false;

  // 1. test init with invalid argument
  //    1.1 invalid key
  invalid_arbitration_service_info.reset();
  ret = invalid_arbitration_service_info.init(invalid_arbitration_service_key,
                              arbitration_service, previous_arbitration_service, type);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  //    1.2 invalid type
  invalid_arbitration_service_info.reset();
  ret = invalid_arbitration_service_info.init(arbitration_service_key,
                              arbitration_service, previous_arbitration_service, invalid_type);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // 2. test assign and is_equal
  //    2.1 assign a invalid info
  ret = arb_service_info.init(arbitration_service_key,
                              arbitration_service, previous_arbitration_service, type);
  invalid_arbitration_service_info.reset();
  ret = arb_service_info_to_get.assign(invalid_arbitration_service_info);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  //    2.2 assign a valid info
  ret = arb_service_info_to_get.assign(arb_service_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  equal = arb_service_info_to_get.is_equal(arb_service_info);
  ASSERT_EQ(true, equal);
  arb_service_info_to_get.reset();

  // 3. test expected get
  //    3.1 get from a empty table
  ret = arb_service_table_operator_.get(sql_proxy, arbitration_service_key, lock_line, arb_service_info_to_get);
  ASSERT_EQ(OB_ARBITRATION_SERVICE_NOT_EXIST, ret);
  //    3.2 get a not exist key
  ret = arb_service_table_operator_.get(sql_proxy, not_exist_arbitration_service_key, lock_line, arb_service_info_to_get);
  ASSERT_EQ(OB_ARBITRATION_SERVICE_NOT_EXIST, ret);
  //    3.3 get a invalid key
  ret = arb_service_table_operator_.get(sql_proxy, invalid_arbitration_service_key, lock_line, arb_service_info_to_get);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // 4. test expected remove
  //    4.1 remove from a empty table
  ret = arb_service_table_operator_.remove(sql_proxy, arbitration_service_key);
  ASSERT_EQ(OB_ARBITRATION_SERVICE_NOT_EXIST, ret);
  //    4.2 remove a invalid key
  ret = arb_service_table_operator_.remove(sql_proxy, invalid_arbitration_service_key);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  //    4.3 remove a not exist key
  ret = arb_service_table_operator_.remove(sql_proxy, not_exist_arbitration_service_key);
  ASSERT_EQ(OB_ARBITRATION_SERVICE_NOT_EXIST, ret);

  // 5. test expected update
  //    5.1 update a not exist info
  ret = arb_service_table_operator_.update(sql_proxy, arb_service_info);
  ASSERT_EQ(OB_ARBITRATION_SERVICE_NOT_EXIST, ret);
  //    5.2 update a invalid info
  invalid_arbitration_service_info.reset();
  ret = arb_service_table_operator_.update(sql_proxy, invalid_arbitration_service_info);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // 6. test insert/update/get
  //    6.1 insert a invalid info
  ret = arb_service_table_operator_.insert(sql_proxy, invalid_arbitration_service_info);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  //    6.2 insert a valid info without previous_arbitration_serivce
  ret = arb_service_table_operator_.insert(sql_proxy, arb_service_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  arb_service_info_to_get.reset();
  ret = arb_service_table_operator_.get(sql_proxy, arbitration_service_key, lock_line, arb_service_info_to_get);
  ASSERT_EQ(OB_SUCCESS, ret);
  equal = arb_service_info_to_get.is_equal(arb_service_info);
  ASSERT_EQ(true, equal);
  //    6.3 insert a already exist one again
  ret = arb_service_table_operator_.insert(sql_proxy, arb_service_info);
  ASSERT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, ret);
  //    6.4 test update a valid info with previous_arbitration_service
  arb_service_info.reset();
  ObString valid_previous_arbitration_service = "127.0.0.1:5678";
  ret = arb_service_info.init(arbitration_service_key,
                              arbitration_service, valid_previous_arbitration_service, type);
  ret = arb_service_table_operator_.update(sql_proxy, arb_service_info);
  ASSERT_EQ(OB_SUCCESS, ret);

  // 7. test remove
  ret = arb_service_table_operator_.remove(sql_proxy, arbitration_service_key);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = arb_service_table_operator_.get(sql_proxy, arbitration_service_key, lock_line, arb_service_info_to_get);
  ASSERT_EQ(OB_ARBITRATION_SERVICE_NOT_EXIST, ret);
}
} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
