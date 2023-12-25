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

class TestTenantSnapshotDef : public ::testing::Test
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
    tsnap_mgr_.destroy();
  }

public:
  ObLSSnapshotMgr ls_snapshot_mgr_;
  ObTenantSnapshotMgr tsnap_mgr_;
  ObTenantMetaSnapshotHandler meta_handler_;
};

TEST_F(TestTenantSnapshotDef, test_build_all_ls_snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1002;
  ObTenantSnapshotID tsnap_id;
  ObLSSnapshot *ls_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_id(tenant_id, tsnap_id));
  ObTenantSnapshot *tenant_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS, tsnap_mgr_.acquire_tenant_snapshot(tsnap_id, tenant_snapshot));
  ASSERT_NE(nullptr, tenant_snapshot);

  ObArray<ObLSID> creating_ls_id_arr;
  ASSERT_EQ(OB_SUCCESS, creating_ls_id_arr.push_back(ObLSID(1)));
  ASSERT_EQ(OB_SUCCESS, creating_ls_id_arr.push_back(ObLSID(1001)));
  ASSERT_EQ(2, creating_ls_id_arr.size());

  tenant_snapshot->build_all_snapshots_(creating_ls_id_arr);
  ObArray<ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, meta_handler_.get_all_ls_snapshot(tsnap_id, ls_ids));
  tsnap_mgr_.revert_tenant_snapshot(tenant_snapshot);


  bool is_tenant_snap_exist = false;
  ObArray<ObTenantSnapshotID> snapshot_ids;
  meta_handler_.get_all_tenant_snapshot(snapshot_ids);
  ARRAY_FOREACH_N(snapshot_ids, i, count) {
    if (snapshot_ids.at(i).id() == tsnap_id.id()) {
      is_tenant_snap_exist = true;
    }
  }
  ASSERT_TRUE(is_tenant_snap_exist);
}

TEST_F(TestTenantSnapshotDef, test_gc_tenant_snapshot)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1002;
  ObTenantSnapshotID tsnap_id;
  ObLSSnapshot *ls_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            rootserver::ObTenantSnapshotUtil::generate_tenant_snapshot_id(tenant_id, tsnap_id));
  ObTenantSnapshot *tenant_snapshot = nullptr;
  ASSERT_EQ(OB_SUCCESS, tsnap_mgr_.acquire_tenant_snapshot(tsnap_id, tenant_snapshot));
  ASSERT_NE(nullptr, tenant_snapshot);

  ObArray<ObLSID> creating_ls_id_arr;
  ASSERT_EQ(OB_SUCCESS, creating_ls_id_arr.push_back(ObLSID(1)));
  ASSERT_EQ(OB_SUCCESS, creating_ls_id_arr.push_back(ObLSID(1001)));
  ASSERT_EQ(2, creating_ls_id_arr.size());

  tenant_snapshot->build_all_snapshots_(creating_ls_id_arr);

  bool is_tenant_snap_exist = false;
  ObArray<ObTenantSnapshotID> snapshot_ids;
  meta_handler_.get_all_tenant_snapshot(snapshot_ids);
  ARRAY_FOREACH_N(snapshot_ids, i, count) {
    if (snapshot_ids.at(i).id() == tsnap_id.id()) {
      is_tenant_snap_exist = true;
    }
  }
  ASSERT_TRUE(is_tenant_snap_exist);
  ASSERT_EQ(OB_SUCCESS, tenant_snapshot->gc_tenant_snapshot_());
  ASSERT_FALSE(tenant_snapshot->meta_existed_);
  tsnap_mgr_.revert_tenant_snapshot(tenant_snapshot);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tenant_snapshot_def.log*");
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tenant_snapshot_def.log", true);
  return RUN_ALL_TESTS();
}
