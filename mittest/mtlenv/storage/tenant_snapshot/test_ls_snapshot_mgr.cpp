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
#include "storage/tenant_snapshot/ob_ls_snapshot_mgr.h"
#include "storage/slog_ckpt/ob_tenant_meta_snapshot_handler.h"
#include "rootserver/tenant_snapshot/ob_tenant_snapshot_util.h"
#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"
#undef private

namespace  oceanbase
{
namespace unittest
{

class TestLSSnapshotMgr : public ::testing::Test
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
    ASSERT_EQ(OB_SUCCESS, mgr_.init(&meta_handler_));
  }
  virtual void TearDown()
  {
    mgr_.destroy();
  }

public:
  ObLSSnapshotMgr mgr_;
  ObTenantMetaSnapshotHandler meta_handler_;
};

TEST_F(TestLSSnapshotMgr, test_ls_snapshot_acquire_and_get)
{
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  ObTenantSnapshotID tsnap_id;
  ObLSSnapshot *ls_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_id(tenant_id, tsnap_id));
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_ls_snapshot(tsnap_id, ls_id, ls_snapshot));
  ASSERT_NE(nullptr, ls_snapshot);
  mgr_.revert_ls_snapshot(ls_snapshot);

  ObLSSnapshot *ls_snapshot_ptr2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr_.get_ls_snapshot(tsnap_id, ls_id, ls_snapshot_ptr2));
  ASSERT_EQ(ls_snapshot, ls_snapshot_ptr2);
  mgr_.revert_ls_snapshot(ls_snapshot_ptr2);
}

TEST_F(TestLSSnapshotMgr, test_del_ls_snapshot)
{
  uint64_t tenant_id = 1002;
  ObLSID ls_id(1001);
  ObTenantSnapshotID tsnap_id;
  ObLSSnapshot *ls_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_id(tenant_id, tsnap_id));
  ASSERT_EQ(OB_SUCCESS, mgr_.acquire_ls_snapshot(tsnap_id, ls_id, ls_snapshot));
  ASSERT_NE(nullptr, ls_snapshot);
  ASSERT_EQ(OB_SUCCESS, mgr_.del_ls_snapshot(tsnap_id, ls_id));
  mgr_.revert_ls_snapshot(ls_snapshot);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ls_snapshot_mgr.log*");
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ls_snapshot_mgr.log", true);
  return RUN_ALL_TESTS();
}
