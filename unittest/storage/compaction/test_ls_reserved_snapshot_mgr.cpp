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
#define USING_LOG_PREFIX STORAGE
#include <gmock/gmock.h>
#define private public
#define protected public
#include "unittest/storage/test_dml_common.h"
namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace storage;

namespace storage
{
int ObLSReservedSnapshotMgr::sync_clog(const int64_t new_reserved_snapshot)
{
  LOG_INFO("mock sync clog", K(new_reserved_snapshot));
  return OB_SUCCESS;
}
}

namespace unittest
{

class TestLSReservedSnapshotMgr : public ::testing::Test
{
public:
  TestLSReservedSnapshotMgr()
  : ls_id_(TEST_LS_ID)
  {}
  ~TestLSReservedSnapshotMgr() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  static constexpr int64_t TEST_TENANT_ID = 1;
  static constexpr int64_t TEST_LS_ID = 9001;
  share::ObLSID ls_id_;
};

void TestLSReservedSnapshotMgr::SetUpTestCase()
{
  int ret = OB_SUCCESS;
  ret = MockTenantModuleEnv::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);

  // ls service cannot service before ObServerCheckpointSlogHandler starts
  // running
  ObServerStorageMetaService::get_instance().is_started_ = true;
  // create ls
  ObLSHandle ls_handle;
  ret = TestDmlCommon::create_ls(TEST_TENANT_ID, ObLSID(TEST_LS_ID), ls_handle);
  if (OB_SUCCESS != ret) {
    LOG_ERROR("[FATAL ERROR] failed to create ls", K(ret));
  }
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestLSReservedSnapshotMgr::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  ret = MTL(ObLSService*)->remove_ls(ObLSID(TEST_LS_ID));
  ASSERT_EQ(OB_SUCCESS, ret);

  MockTenantModuleEnv::get_instance().destroy();
}

TEST_F(TestLSReservedSnapshotMgr, test_add_del_dependent_tablet)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLSReservedSnapshotMgr &reserved_snapshot_mgr = ls_handle.get_ls()->reserved_snapshot_mgr_;
  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.try_sync_reserved_snapshot(100, true/*update_flag*/));
  ASSERT_EQ(100, reserved_snapshot_mgr.min_reserved_snapshot_);

  const ObTabletID tablet_id(100);

  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.add_dependent_medium_tablet(tablet_id));
  ASSERT_EQ(OB_ENTRY_EXIST, reserved_snapshot_mgr.add_dependent_medium_tablet(tablet_id));

  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.del_dependent_medium_tablet(tablet_id));
  ASSERT_EQ(OB_HASH_NOT_EXIST, reserved_snapshot_mgr.del_dependent_medium_tablet(tablet_id));

  ASSERT_EQ(100, reserved_snapshot_mgr.min_reserved_snapshot_);

  const ObTabletID tablet_id2(200);
  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.add_dependent_medium_tablet(tablet_id));
  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.add_dependent_medium_tablet(tablet_id2));

  // have dependent tablet, can not push min_reserved_snapshot_
  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.try_sync_reserved_snapshot(200, true/*update_flag*/));
  ASSERT_EQ(100, reserved_snapshot_mgr.min_reserved_snapshot_);
  ASSERT_EQ(200, reserved_snapshot_mgr.next_reserved_snapshot_);

  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.try_sync_reserved_snapshot(300, true/*update_flag*/));
  ASSERT_EQ(100, reserved_snapshot_mgr.min_reserved_snapshot_);
  ASSERT_EQ(300, reserved_snapshot_mgr.next_reserved_snapshot_);

  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.del_dependent_medium_tablet(tablet_id));
  ASSERT_EQ(100, reserved_snapshot_mgr.min_reserved_snapshot_);
  ASSERT_EQ(300, reserved_snapshot_mgr.next_reserved_snapshot_);
  // no dependent tablet, push min_reserved_snapshot_
  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.del_dependent_medium_tablet(tablet_id2));
  ASSERT_EQ(300, reserved_snapshot_mgr.min_reserved_snapshot_);
  ASSERT_EQ(0, reserved_snapshot_mgr.next_reserved_snapshot_);

  reserved_snapshot_mgr.min_reserved_snapshot_ = 0;
  reserved_snapshot_mgr.next_reserved_snapshot_ = 0;
}

TEST_F(TestLSReservedSnapshotMgr, test_update_with_smaller_snapshot)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ret = ls_svr->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObLSReservedSnapshotMgr &reserved_snapshot_mgr = ls_handle.get_ls()->reserved_snapshot_mgr_;
  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.try_sync_reserved_snapshot(100, true/*update_flag*/));
  ASSERT_EQ(100, reserved_snapshot_mgr.min_reserved_snapshot_);

  // can't update with smaller snapshot
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, reserved_snapshot_mgr.try_sync_reserved_snapshot(50, true/*update_flag*/));

  const ObTabletID tablet_id(100);

  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.add_dependent_medium_tablet(tablet_id));
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, reserved_snapshot_mgr.try_sync_reserved_snapshot(50, true/*update_flag*/));

  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.del_dependent_medium_tablet(tablet_id));
  ASSERT_EQ(OB_SNAPSHOT_DISCARDED, reserved_snapshot_mgr.try_sync_reserved_snapshot(50, true/*update_flag*/));

  ASSERT_EQ(100, reserved_snapshot_mgr.min_reserved_snapshot_);

  ASSERT_EQ(OB_SUCCESS, reserved_snapshot_mgr.try_sync_reserved_snapshot(150, true/*update_flag*/));
  ASSERT_EQ(150, reserved_snapshot_mgr.min_reserved_snapshot_);
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ls_reserved_snapshot.log*");
  OB_LOGGER.set_file_name("test_ls_reserved_snapshot.log");
  OB_LOGGER.set_log_level("DEBUG");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
