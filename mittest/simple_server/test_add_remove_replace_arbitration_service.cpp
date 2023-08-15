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

class TestAddRemoveReplaceArbitrationService : public unittest::ObSimpleClusterTestBase
{
public:
  TestAddRemoveReplaceArbitrationService() : unittest::ObSimpleClusterTestBase("test_add_remove_replace_arbitration_service") {}
protected:
  ObArbitrationServiceTableOperator arb_service_table_operator_;
};

TEST_F(TestAddRemoveReplaceArbitrationService, test_add_remove_replace)
{
  int ret = OB_SUCCESS;
  // 0. prepare initial members
  common::ObMySQLProxy &sql_proxy = get_curr_observer().get_mysql_proxy();
  ObArbitrationServiceInfo arbitration_service_info;
  ObString arbitration_service_key("default");
  bool lock_line = true;
  bool is_equal = false;
  int64_t affected_rows = 0;
  ObSqlString sql;
  ObString almost_max_length_arbitration_service;
  int almost_max_length = OB_MAX_ARBITRATION_SERVICE_LENGTH - 1;
  ObString max_length_arbitration_service;
  int max_length = OB_MAX_ARBITRATION_SERVICE_LENGTH;
  ObString over_length_arbitration_service;
  int over_max_length = OB_MAX_ARBITRATION_SERVICE_LENGTH + 1;
  ObMySQLProxy::MySQLResult res1;
  ObMySQLProxy::MySQLResult res2;
  ObMySQLProxy::MySQLResult res3;

  {
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select repeat('1', %d) as str1", almost_max_length));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1, sql.ptr()));
    sqlclient::ObMySQLResult *result1 = res1.get_result();
    ASSERT_NE(nullptr, result1);
    ASSERT_EQ(OB_SUCCESS, result1->next());
    ASSERT_EQ(OB_SUCCESS, result1->get_varchar("str1", almost_max_length_arbitration_service));

    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select repeat('1', %d) as str2", max_length));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res2, sql.ptr()));
    sqlclient::ObMySQLResult *result2 = res2.get_result();
    ASSERT_NE(nullptr, result2);
    ASSERT_EQ(OB_SUCCESS, result2->next());
    ASSERT_EQ(OB_SUCCESS, result2->get_varchar("str2", max_length_arbitration_service));

    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("select repeat('1', %d) as str3", over_max_length));
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res3, sql.ptr()));
    sqlclient::ObMySQLResult *result3 = res3.get_result();
    ASSERT_NE(nullptr, result3);
    ASSERT_EQ(OB_SUCCESS, result3->next());
    ASSERT_EQ(OB_SUCCESS, result3->get_varchar("str3", over_length_arbitration_service));
  }

  // 1. test invalid argument
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system add arbitration service '127.0.0.1'"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system add arbitration service 'invalid'"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system add arbitration service '%.*s'",
                                        over_max_length, over_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system add arbitration service '%.*s'",
                                        max_length, max_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system add arbitration service '%.*s'",
                                        almost_max_length, almost_max_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system remove arbitration service '127.0.0.1'"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system remove arbitration service 'invalid'"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system remove arbitration service '%.*s'",
                                        over_max_length, over_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system remove arbitration service '%.*s'",
                                        max_length, max_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system remove arbitration service '%.*s'",
                                        almost_max_length, almost_max_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system replace arbitration service '127.0.0.1' with '127.0.0.1:2222'"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system replace arbitration service 'invalid' with 'invalid'"));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system replace arbitration service '%.*s' with 'invalid'",
                                        over_max_length, over_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system replace arbitration service 'invalid' with '%.*s'",
                                        over_max_length, over_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system replace arbitration service '%.*s' with 'invalid'",
                                        max_length, max_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system replace arbitration service 'invalid' with '%.*s'",
                                        max_length, max_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system replace arbitration service '%.*s' with 'invalid'",
                                        almost_max_length, almost_max_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("alter system replace arbitration service 'invalid' with '%.*s'",
                                        almost_max_length, almost_max_length_arbitration_service.ptr()));
  ASSERT_EQ(OB_INVALID_ARGUMENT, sql_proxy.write(sql.ptr(), affected_rows));


  // 2. remove or replace a not exist service
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system remove arbitration service '127.0.0.1:1111'"));
  ASSERT_EQ(OB_ARBITRATION_SERVICE_NOT_EXIST, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system replace arbitration service '127.0.0.1:1111' with '127.0.0.1:2222'"));
  ASSERT_EQ(OB_ARBITRATION_SERVICE_NOT_EXIST, sql_proxy.write(sql.ptr(), affected_rows));


  // 3. add a arbitration service
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system add arbitration service '127.0.0.1:1111'"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ret = arb_service_table_operator_.get(sql_proxy, arbitration_service_key, lock_line, arbitration_service_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  is_equal = arbitration_service_info.get_arbitration_service_string() == ObString::make_string("127.0.0.1:1111")
           && arbitration_service_info.get_previous_arbitration_service_string() == ObString::make_string("");
  ASSERT_EQ(true, is_equal);


  // 4. add a same arbitration service again
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system add arbitration service '127.0.0.1:1111'"));
  ASSERT_EQ(OB_ARBITRATION_SERVICE_ALREADY_EXIST, sql_proxy.write(sql.ptr(), affected_rows));


  // 5. replace to self
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system replace arbitration service '127.0.0.1:1111' with '127.0.0.1:1111'"));
  ASSERT_EQ(OB_OP_NOT_ALLOW, sql_proxy.write(sql.ptr(), affected_rows));

  // 6. replace a arbitration service
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system replace arbitration service '127.0.0.1:1111' with '127.0.0.1:2222'"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ret = arb_service_table_operator_.get(sql_proxy, arbitration_service_key, lock_line, arbitration_service_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  is_equal = arbitration_service_info.get_arbitration_service_string() == ObString::make_string("127.0.0.1:2222")
           && arbitration_service_info.get_previous_arbitration_service_string() == ObString::make_string("127.0.0.1:1111");
  ASSERT_EQ(true, is_equal);

  // 7. replace a service already in replacing
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system replace arbitration service '127.0.0.1:1111' with '127.0.0.1:3333'"));
  ASSERT_EQ(OB_OP_NOT_ALLOW, sql_proxy.write(sql.ptr(), affected_rows));

  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system replace arbitration service '127.0.0.1:3333' with '127.0.0.1:4444'"));
  ASSERT_EQ(OB_OP_NOT_ALLOW, sql_proxy.write(sql.ptr(), affected_rows));


  // 8. rollback replacing
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system replace arbitration service '127.0.0.1:2222' with '127.0.0.1:1111'"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));


  // 9. remove a service
  ASSERT_EQ(OB_SUCCESS, sql.assign("alter system remove arbitration service '127.0.0.1:1111'"));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));
  ret = arb_service_table_operator_.get(sql_proxy, arbitration_service_key, lock_line, arbitration_service_info);
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
