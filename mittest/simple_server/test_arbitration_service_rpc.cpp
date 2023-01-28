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

TEST_F(TestArbitrationServiceRpc, test_simple)
{
  int ret = OB_SUCCESS;
  obrpc::ObNetClient client;
  obrpc::ObSrvRpcProxy srv_proxy;

  uint64_t tenant_id = 1001;
  share::ObLSID ls_id(1);
  common::ObAddr dst_server = GCTX.self_addr();
  int64_t timestamp_for_arb_member = ObTimeUtility::current_time();
  common::ObMember arb_member(dst_server, timestamp_for_arb_member);
  int64_t timeout_us = 180 * 1000 * 1000; //180s

  ret = client.init();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = client.get_proxy(srv_proxy);
  ASSERT_EQ(OB_SUCCESS, ret);

  srv_proxy.set_server(dst_server);
  srv_proxy.set_timeout(timeout_us);

  ObAddArbArg add_arg;
  ObAddArbResult add_result;
  ret = add_arg.init(tenant_id, ls_id, arb_member, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = srv_proxy.to(dst_server).add_arb(add_arg, add_result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, add_result.get_result());

  ObRemoveArbArg remove_arg;
  ObRemoveArbResult remove_result;
  ret = remove_arg.init(tenant_id, ls_id, arb_member, timeout_us);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = srv_proxy.to(dst_server).remove_arb(remove_arg, remove_result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, remove_result.get_result());

  ObCreateArbArg create_arg;
  ObCreateArbResult create_result;
  share::ObTenantRole tenant_role(ObTenantRole::PRIMARY_TENANT);
  ret = create_arg.init(tenant_id, ls_id, tenant_role);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = srv_proxy.to(dst_server).create_arb(create_arg, create_result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, create_result.get_result());

  ObDeleteArbArg delete_arg;
  ObDeleteArbResult delete_result;
  ret = delete_arg.init(tenant_id, ls_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = srv_proxy.to(dst_server).delete_arb(delete_arg, delete_result);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, delete_result.get_result());
}
} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
