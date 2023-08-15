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

class TestArbitrationServiceRpc : public unittest::ObSimpleClusterTestBase
{
public:
  TestArbitrationServiceRpc() : unittest::ObSimpleClusterTestBase("test_arbitration_service_rpc") {}
};

TEST_F(TestArbitrationServiceRpc, test_argument)
{
  int ret = OB_SUCCESS;

  uint64_t tenant_id = 1001;
  share::ObLSID ls_id(1);
  share::ObLSID invalid_ls_id(1001);
  common::ObAddr dst_server(ObAddr::VER::IPV4, "127.0.0.1", 1080);
  common::ObMember invalid_member;
  int64_t timestamp_for_arb_member = ObTimeUtility::current_time();
  common::ObMember arb_member(dst_server, timestamp_for_arb_member);
  int64_t timeout_us = 180 * 1000 * 1000; //180s
  int64_t invalid_timeout_us = OB_INVALID_TIMESTAMP;

  ObAddArbArg add_arg;
  ret = add_arg.init(tenant_id, invalid_ls_id, arb_member, timeout_us);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = add_arg.init(tenant_id, ls_id, invalid_member, timeout_us);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = add_arg.init(tenant_id, ls_id, arb_member, invalid_timeout_us);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = add_arg.init(tenant_id, ls_id, arb_member, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObRemoveArbArg remove_arg;
  ret = remove_arg.init(tenant_id, invalid_ls_id, arb_member, timeout_us);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = remove_arg.init(tenant_id, ls_id, invalid_member, timeout_us);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = remove_arg.init(tenant_id, ls_id, arb_member, invalid_timeout_us);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = remove_arg.init(tenant_id, ls_id, arb_member, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCreateArbArg create_arg;
  share::ObTenantRole tenant_role(ObTenantRole::PRIMARY_TENANT);
  share::ObTenantRole invalid_tenant_role(ObTenantRole::INVALID_TENANT);
  ret = create_arg.init(tenant_id, invalid_ls_id, tenant_role);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = create_arg.init(tenant_id, ls_id, invalid_tenant_role);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = create_arg.init(tenant_id, ls_id, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDeleteArbArg delete_arg;
  ret = delete_arg.init(tenant_id, invalid_ls_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = delete_arg.init(tenant_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
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
