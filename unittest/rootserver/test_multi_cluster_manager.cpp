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
#include "rootserver/ob_multi_cluster_manager.h"
#include "share/ob_web_service_root_addr.h"
#include "share/ob_multi_cluster_proxy.h"

namespace oceanbase {
using namespace common;
using namespace share;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace rootserver{
//multiclustermanage
class TestMultiClusterManager_load_Test : public testing::Test
{
public:
  TestMultiClusterManager_load_Test(): cluster_mgr_() {}
  virtual ~TestMultiClusterManager_load_Test(){}
  virtual void SetUp() {};
  virtual void TearDown() {}
  virtual void TestBody() {}
private:
  ObMultiClusterManager cluster_mgr_;
}
;

TEST_F(TestMultiClusterManager_load_Test, jsontocluster)
{
  int ret = OB_SUCCESS;
  ObClusterAddr cluster_addr;
  cluster_addr.cluster_id_ = 1;
  cluster_addr.cluster_role_ = common::STANDBY_CLUSTER;
  cluster_addr.cluster_name_.assign("test");
  ObRootAddr rs;
  rs.server_.set_ip_addr("127.0.0.1", 9988);
  rs.sql_port_ = 1;
  ret = cluster_addr.addr_list_.push_back(rs);
  ASSERT_EQ(OB_SUCCESS, ret);
  rs.server_.set_ip_addr("127.0.0.2", 9988);
  rs.role_ = LEADER;
  rs.sql_port_ = 1111;
  ret = cluster_addr.addr_list_.push_back(rs);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObRootAddr readonly_rs;
  readonly_rs.server_.set_ip_addr("127.0.0.3", 9988);
  readonly_rs.sql_port_ = 1;
  ret = cluster_addr.readonly_addr_list_.push_back(readonly_rs);
  ASSERT_EQ(OB_SUCCESS, ret);
  readonly_rs.server_.set_ip_addr("127.0.0.4", 9988);
  readonly_rs.sql_port_ = 1111;
  ret = cluster_addr.readonly_addr_list_.push_back(readonly_rs);
  ASSERT_EQ(OB_SUCCESS, ret);
  common::ObSqlString test_result;
  ret = ObMultiClusterProxy::cluster_addr_to_json_str(cluster_addr, test_result);
  ASSERT_EQ(OB_SUCCESS, ret);
  common::ObString str_value;
  str_value.assign(test_result.ptr(), static_cast<int32_t>(test_result.length()));
  ObClusterAddr output;
  ret = ObMultiClusterProxy::json_str_to_cluster_addr(str_value, output);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(cluster_addr.addr_list_.count(), output.addr_list_.count());
  ASSERT_EQ(cluster_addr.readonly_addr_list_.count(), output.readonly_addr_list_.count());
  LOG_WARN("output cluster_addr", K(ret), K(cluster_addr), K(output), K(str_value));
  cluster_addr.reset();
  cluster_addr.cluster_id_ = 1;
  cluster_addr.cluster_role_ = common::STANDBY_CLUSTER;
  cluster_addr.cluster_name_.assign("test");
  ret = ObMultiClusterProxy::cluster_addr_to_json_str(cluster_addr, test_result);
  ASSERT_EQ(OB_SUCCESS, ret);
  str_value.assign(test_result.ptr(), static_cast<int32_t>(test_result.length()));
  ret = ObMultiClusterProxy::json_str_to_cluster_addr(str_value, output);
  ASSERT_EQ(cluster_addr.addr_list_.count(), output.addr_list_.count());
  ASSERT_EQ(cluster_addr.readonly_addr_list_.count(), output.readonly_addr_list_.count());
  LOG_WARN("output cluster_addr", K(ret), K(cluster_addr), K(output), K(str_value));
}
}
}
int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
