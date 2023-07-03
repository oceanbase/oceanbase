/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define USING_LOG_PREFIX SHARE
#define protected public
#define private public

#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "share/location_cache/ob_location_service.h" // ObLocationService

using namespace unittest;

namespace oceanbase
{
namespace share
{
using namespace common;

class TestLocationService : public unittest::ObSimpleClusterTestBase
{
public:
  TestLocationService() : unittest::ObSimpleClusterTestBase("test_location_service") {}
};

TEST_F(TestLocationService, test_auto_clear_caches)
{
  int ret = OB_SUCCESS;
  uint64_t user_tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, create_tenant("tt1"));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(user_tenant_id));
  ASSERT_TRUE(is_user_tenant(user_tenant_id));
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  ObLocationService *location_service = GCTX.location_service_;
  ASSERT_TRUE(OB_NOT_NULL(location_service));
  ObLSLocationService *ls_location_service = &(location_service->ls_location_service_);
  ASSERT_TRUE(OB_NOT_NULL(ls_location_service));
  const ObLSID &user_ls_id = ObLSID(1001);
  ObLSLocation location;

  // assert caches exist
  usleep(ls_location_service->RENEW_LS_LOCATION_INTERVAL_US);
  ASSERT_EQ(OB_SUCCESS, ls_location_service->get_from_cache_(GCONF.cluster_id, user_tenant_id, user_ls_id, location));
  ASSERT_TRUE(location.get_cache_key() == ObLSLocationCacheKey(GCONF.cluster_id, user_tenant_id, user_ls_id));
  location.reset();
  ASSERT_EQ(OB_SUCCESS, ls_location_service->get_from_cache_(GCONF.cluster_id, meta_tenant_id, SYS_LS, location));
  ASSERT_TRUE(location.get_cache_key() == ObLSLocationCacheKey(GCONF.cluster_id, meta_tenant_id, SYS_LS));

  // drop tenant force
  ASSERT_EQ(OB_SUCCESS, delete_tenant("tt1"));

  // meta tenant is dropped in schema and user tenant unit has been gc
  bool is_dropped = false;
  ASSERT_EQ(OB_SUCCESS, GSCHEMASERVICE.check_if_tenant_has_been_dropped(meta_tenant_id, is_dropped));
  ASSERT_TRUE(is_dropped);
  bool tenant_unit_exist = true;
  while (tenant_unit_exist && OB_SUCC(ret)) {
    if (OB_FAIL(check_tenant_exist(tenant_unit_exist, "tt1"))) {
      SERVER_LOG(WARN, "check_tenant_exist failed", KR(ret));
    }
  }
  LOG_INFO("tenant unit gc finished", KR(ret), K(user_tenant_id));

  // auto clear caches successfully
  usleep(ls_location_service->CLEAR_CACHE_INTERVAL);
  usleep(ls_location_service->RENEW_LS_LOCATION_BY_RPC_INTERVAL_US + GCONF.rpc_timeout);
  ASSERT_EQ(OB_CACHE_NOT_HIT, ls_location_service->get_from_cache_(GCONF.cluster_id, user_tenant_id, user_ls_id, location));
  ASSERT_EQ(OB_CACHE_NOT_HIT, ls_location_service->get_from_cache_(GCONF.cluster_id, meta_tenant_id, SYS_LS, location));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
