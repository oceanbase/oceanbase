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

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>

#define private public
#define protected public

#include "lib/oblog/ob_log.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_mgr.h"
#undef private

namespace  oceanbase
{
namespace unittest
{

class TestTenantSnapshotMgr : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }
  virtual void SetUp()
  {
    ASSERT_EQ(OB_SUCCESS, ls_snapshot_mgr_.init(&meta_handler_));
    ASSERT_EQ(OB_SUCCESS, tsnap_mgr_.init(&ls_snapshot_mgr_, &meta_handler_));
  }
  virtual void TearDown()
  {
    ls_snapshot_mgr_.destroy();
    tsnap_mgr_.destroy();
  }

public:
  ObTenantSnapshotMgr tsnap_mgr_;
  ObLSSnapshotMgr ls_snapshot_mgr_;
  ObTenantMetaSnapshotHandler meta_handler_;
};

TEST_F(TestTenantSnapshotMgr, test_acquire_and_get)
{
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  ObTenantSnapshotID tsnap_id;
  ObLSSnapshot *ls_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_id(tenant_id, tsnap_id));
  ObTenantSnapshot *tenant_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS, tsnap_mgr_.acquire_tenant_snapshot(tsnap_id, tenant_snapshot));
  ASSERT_NE(nullptr, tenant_snapshot);
  tsnap_mgr_.revert_tenant_snapshot(tenant_snapshot);

  ObTenantSnapshot *tsnap_ptr = nullptr;
  ASSERT_EQ(OB_SUCCESS, tsnap_mgr_.get_tenant_snapshot(tsnap_id, tsnap_ptr));
  ASSERT_EQ(tenant_snapshot, tsnap_ptr);
  tsnap_mgr_.revert_tenant_snapshot(tsnap_ptr);
}

TEST_F(TestTenantSnapshotMgr, test_del_tenant_snapshot)
{
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  ObTenantSnapshotID tsnap_id;
  ObLSSnapshot *ls_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_id(tenant_id, tsnap_id));
  ObTenantSnapshot *tenant_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS, tsnap_mgr_.acquire_tenant_snapshot(tsnap_id, tenant_snapshot));
  ASSERT_NE(nullptr, tenant_snapshot);
  ASSERT_EQ(OB_SUCCESS, tsnap_mgr_.del_tenant_snapshot(tsnap_id));
  ASSERT_EQ(0, tsnap_mgr_.tenant_snapshot_map_.size());

  tsnap_mgr_.revert_tenant_snapshot(tenant_snapshot);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tenant_snapshot_mgr.log*");
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tenant_snapshot_mgr.log", true);
  return RUN_ALL_TESTS();
}
