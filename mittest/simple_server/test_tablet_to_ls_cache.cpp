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
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using namespace common;
using namespace transaction;

class TestTabletToLSCache : public unittest::ObSimpleClusterTestBase
{
public:
  TestTabletToLSCache() : unittest::ObSimpleClusterTestBase("test_tablet_to_ls_cache") {}
};

TEST_F(TestTabletToLSCache, tablet_to_ls_cache)
{
  // 0. init
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  const int64_t TOTAL_NUM = 10;
  ObSEArray<ObTabletLSPair, TOTAL_NUM> tablet_ls_pairs;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
  ASSERT_EQ(true, is_valid_tenant_id(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  MTL_SWITCH(tenant_id) {
    ObLSService *ls_service = MTL(ObLSService *);
    ASSERT_EQ(true, OB_NOT_NULL(ls_service));
    ObTransService *tx_service = MTL(ObTransService *);
    ASSERT_EQ(true, OB_NOT_NULL(tx_service));
    int64_t base_size = tx_service->tablet_to_ls_cache_.size();

    // 1. create tablet
    ASSERT_EQ(OB_SUCCESS, batch_create_table(tenant_id, sql_proxy, TOTAL_NUM, tablet_ls_pairs));
    ASSERT_EQ(TOTAL_NUM, tablet_ls_pairs.count());
    ASSERT_EQ(base_size + TOTAL_NUM, tx_service->tablet_to_ls_cache_.size());

    // 2. check and get ls
    ObLSID ls_id;
    ARRAY_FOREACH(tablet_ls_pairs, i) {
      const ObTabletLSPair &pair = tablet_ls_pairs.at(i);
      bool is_local = false;
      ls_id.reset();
      ASSERT_EQ(OB_SUCCESS, tx_service->check_and_get_ls_info(pair.get_tablet_id(), ls_id, is_local));
      ASSERT_EQ(pair.get_ls_id().id(), ls_id.id());
      ASSERT_EQ(true, is_local);
    }

    // 3. drop table
    ObArray<ObTabletID> remove_tablet_ids;
    ObLSHandle ls_handle;
    ls_id = tablet_ls_pairs.at(0).get_ls_id();
    ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
    ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();
    ARRAY_FOREACH(tablet_ls_pairs, i) {
      const ObTabletLSPair &pair = tablet_ls_pairs.at(i);
      remove_tablet_ids.push_back(pair.get_tablet_id());
    }
    ASSERT_EQ(OB_SUCCESS, batch_drop_table(tenant_id, sql_proxy, TOTAL_NUM));
    // after drop table, the tablets won't be removed immediately, so need to call remove_tablets
    ASSERT_EQ(OB_SUCCESS, ls_tablet_svr->remove_tablets(remove_tablet_ids));
    ASSERT_EQ(base_size, tx_service->tablet_to_ls_cache_.size());
  }
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
