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

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#define USING_LOG_PREFIX STORAGE

#define protected public
#define private public
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "mittest/mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"
#include "storage/slog_ckpt/ob_tenant_meta_snapshot_handler.h"
#include "storage/slog_ckpt/ob_tenant_checkpoint_slog_handler.h"
#include "storage/tablet/ob_tablet_persister.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;

class TestMetaSnapshot : public TestIndexBlockDataPrepare
{
public:
  TestMetaSnapshot();
  ~TestMetaSnapshot();
  virtual void TearDown();
  int persist_tablet(ObTabletHandle &new_tablet_handle);
  int check_integrity(
      const ObMetaDiskAddr &tablet_addr,
      const ObTenantSnapshotID &snapshot_id,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id);
};

TestMetaSnapshot::TestMetaSnapshot()
  : TestIndexBlockDataPrepare(
      "Test Meta Snapshot",
      MINI_MERGE,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      65536)
{
}

TestMetaSnapshot::~TestMetaSnapshot()
{
}

void TestMetaSnapshot::TearDown()
{
  int ret = OB_SUCCESS;
  ObSArray<ObTenantSnapshotID> ids;
  if (OB_FAIL(ObTenantMetaSnapshotHandler::get_all_tenant_snapshot(ids))) {
    LOG_ERROR("fail to get tenant snapshot", K(ret));
  } else {
    for (int i = 0; i < ids.count(); i++) {
      if (OB_FAIL(ObTenantMetaSnapshotHandler::delete_tenant_snapshot(ids[i]))) {
        LOG_ERROR("fail to delete tenant snapshot", K(ret), K(ids[i]));
      }
    }
  }
  TestIndexBlockDataPrepare::TearDown();
}

int TestMetaSnapshot::persist_tablet(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ObMacroBlockHandle macro_handle;
  ObUpdateTabletPointerParam param;

  if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
  } else if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->get_shared_block_reader_writer().switch_block(macro_handle))) {
    LOG_WARN("fail to switch shared meta block", K(ret));
  } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*(tablet_handle.get_obj()), new_tablet_handle))) {
    LOG_WARN("fail to persist and transform tablet", K(ret), K(tablet_handle));
  } else if (OB_FAIL(new_tablet_handle.get_obj()->get_updating_tablet_pointer_param(param))) {
    LOG_WARN("fail to get updating tablet pointer parameters", K(ret));
  } else if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->compare_and_swap_tablet(ObTabletMapKey(ls_id, tablet_id), tablet_handle, new_tablet_handle, param))) {
    LOG_WARN("fail to cas tablet", K(ret), K(ls_id), K(tablet_id), K(tablet_handle), K(new_tablet_handle), K(param));
  } else if (OB_FAIL(MTL(ObTenantCheckpointSlogHandler*)->get_shared_block_reader_writer().switch_block(macro_handle))) {
    LOG_WARN("fail to switch shared meta block", K(ret));
  }
  return ret;
}

int TestMetaSnapshot::check_integrity(
    const ObMetaDiskAddr &tablet_addr,
    const ObTenantSnapshotID &snapshot_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  blocksstable::MacroBlockId tablet_meta_entry;
  ObLinkedMacroBlockItemReader item_reader;
  bool found = false;
  ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "test");

  if (OB_FAIL(ObTenantMetaSnapshotHandler::get_ls_snapshot(snapshot_id, ls_id, tablet_meta_entry))) {
    LOG_WARN("fail to get ls snapshot", K(ret), K(snapshot_id), K(ls_id));
  } else if (OB_FAIL(item_reader.init(tablet_meta_entry, mem_attr))) {
    LOG_WARN("fail to init item reader", K(ret), K(tablet_meta_entry));
  } else {
    ObUpdateTabletLog slog;
    ObMetaDiskAddr addr;
    while (OB_SUCC(ret)) {
      char *item_buf = nullptr;
      int64_t item_buf_len = 0;
      int64_t pos = 0;
      if (OB_FAIL(item_reader.get_next_item(item_buf, item_buf_len, addr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next item", K(ret));
        } else {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("tablet snapshot doesn't exist", K(ret));
        }
      } else if (OB_FAIL(slog.deserialize(item_buf, item_buf_len, pos))) {
        LOG_WARN("fail to deserialize slog", K(ret));
      } else if (slog.tablet_id_ == tablet_id) {
        if (slog.disk_addr_ != tablet_addr) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet disk doesn't match", K(ret), K(slog.disk_addr_), K(tablet_addr));
        } else {
          found = true;
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!found)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("ls snapshot doesn't exist", K(ret));
  }
  return ret;
}

TEST_F(TestMetaSnapshot, test_create_delete)
{
  omt::ObTenant *tenant = static_cast<omt::ObTenant*>(share::ObTenantEnv::get_tenant());
  ObTenantSuperBlock super_block;
  // test create snapshots
  int64_t max_num = ObTenantSuperBlock::MAX_SNAPSHOT_NUM;
  for (int i = 0; i < max_num - 1; i++) {
    ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_tenant_snapshot(ObTenantSnapshotID(i)));
  }
  ASSERT_NE(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_tenant_snapshot(ObTenantSnapshotID(1)));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_tenant_snapshot(ObTenantSnapshotID(max_num - 1)));
  super_block = tenant->get_super_block();
  ASSERT_EQ(max_num, super_block.snapshot_cnt_);
  ASSERT_NE(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_tenant_snapshot(ObTenantSnapshotID(34)));

  // test get all tenant snapshot
  ObSArray<ObTenantSnapshotID> ids;
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::get_all_tenant_snapshot(ids));
  ASSERT_EQ(max_num, ids.count());

  // check snapshot
  ObLSID single_ls_id(ls_id_);
  ObTenantSnapshotMeta snapshot;
  ObTenantSnapshotID snapshot_id(1);
  share::SCN clog_max_scn;
  ASSERT_EQ(OB_SUCCESS, super_block.get_snapshot(snapshot_id, snapshot));
  ASSERT_EQ(true, IS_EMPTY_BLOCK_LIST(snapshot.ls_meta_entry_));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_single_ls_snapshot(snapshot_id, single_ls_id, clog_max_scn));
  super_block = tenant->get_super_block();
  ASSERT_EQ(OB_SUCCESS, super_block.get_snapshot(snapshot_id, snapshot));
  ASSERT_EQ(false, IS_EMPTY_BLOCK_LIST(snapshot.ls_meta_entry_));

  // test find snapshot error
  ObLSID ls_id(1);
  MacroBlockId tablet_meta_entry;
  ASSERT_NE(OB_SUCCESS, ObTenantMetaSnapshotHandler::get_ls_snapshot(ObTenantSnapshotID(34), ls_id, tablet_meta_entry));

  // test delete snapshots
  ASSERT_NE(OB_SUCCESS, ObTenantMetaSnapshotHandler::delete_tenant_snapshot(ObTenantSnapshotID(34)));
  for (int i = 0; i < max_num; i++) {
    ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::delete_tenant_snapshot(ObTenantSnapshotID(i)));
    super_block = tenant->get_super_block();
    ASSERT_EQ(max_num - i - 1, super_block.snapshot_cnt_);
  }
}

TEST_F(TestMetaSnapshot, test_ref_cnt_delete_tenant_snapshot)
{
  ObLSID ls_id(ls_id_);

  // persist tablet
  int64_t ref_cnt = 0;
  int64_t offset = 0;
  int64_t size = 0;
  ObBlockManager::BlockInfo block_info;
  MacroBlockId table_store_id;
  ObTabletHandle new_tablet_handle;
  share::SCN clog_max_scn;

  ASSERT_EQ(OB_SUCCESS, persist_tablet(new_tablet_handle));
  ObTablet *new_tablet = new_tablet_handle.get_obj();
  ASSERT_EQ(OB_SUCCESS, new_tablet->table_store_addr_.addr_.get_block_addr(table_store_id, offset, size));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ref_cnt = block_info.ref_cnt_;

  // create snapshot and check ref cnt
  ObTenantSnapshotID snapshot_id(1);
  ASSERT_NE(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_single_ls_snapshot(snapshot_id, ls_id, clog_max_scn));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_tenant_snapshot(snapshot_id));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_single_ls_snapshot(snapshot_id, ls_id, clog_max_scn));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(ref_cnt + 1, block_info.ref_cnt_);

  // delete snapshot and check ref cnt
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::delete_tenant_snapshot(snapshot_id));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(ref_cnt, block_info.ref_cnt_);
}

TEST_F(TestMetaSnapshot, test_ref_cnt_delete_ls_snapshot)
{
  ObLSID ls_id(ls_id_);

  // persist tablet
  int64_t ref_cnt = 0;
  int64_t offset = 0;
  int64_t size = 0;
  ObBlockManager::BlockInfo block_info;
  MacroBlockId table_store_id;
  ObTabletHandle new_tablet_handle;
  share::SCN clog_max_scn;

  ASSERT_EQ(OB_SUCCESS, persist_tablet(new_tablet_handle));
  ObTablet *new_tablet = new_tablet_handle.get_obj();
  ASSERT_EQ(OB_SUCCESS, new_tablet->table_store_addr_.addr_.get_block_addr(table_store_id, offset, size));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ref_cnt = block_info.ref_cnt_;

  // create snapshot and check ref cnt
  ObTenantSnapshotID snapshot_id(1);
  ASSERT_NE(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_single_ls_snapshot(snapshot_id, ls_id, clog_max_scn));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_tenant_snapshot(snapshot_id));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_single_ls_snapshot(snapshot_id, ls_id, clog_max_scn));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(ref_cnt + 1, block_info.ref_cnt_);

  // delete snapshot and check ref cnt
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::delete_single_ls_snapshot(snapshot_id, ls_id));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(ref_cnt, block_info.ref_cnt_);
}

TEST_F(TestMetaSnapshot, test_create_integrity)
{
  int ret = OB_SUCCESS;

  // persist tablet
  ObTabletHandle new_tablet_handle;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ASSERT_EQ(OB_SUCCESS, persist_tablet(new_tablet_handle));

  // create snapshot and check integrity
  blocksstable::MacroBlockId tablet_meta_entry;
  ObTenantSnapshotID snapshot_id(1);
  share::SCN clog_max_scn;
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_tenant_snapshot(snapshot_id));
  ASSERT_NE(OB_SUCCESS, check_integrity(new_tablet_handle.get_obj()->tablet_addr_, snapshot_id, ls_id, tablet_id));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_single_ls_snapshot(snapshot_id, ls_id, clog_max_scn));
  ASSERT_EQ(OB_SUCCESS, check_integrity(new_tablet_handle.get_obj()->tablet_addr_, snapshot_id, ls_id, tablet_id));
}

TEST_F(TestMetaSnapshot, test_single_ls_snapshot)
{
  ObLSID ls_id(ls_id_);
  ObTenantSnapshotID snapshot_id(1);

  // create tenant snapshot and delete ls snapshot
  blocksstable::MacroBlockId tablet_meta_entry;
  share::SCN clog_max_scn;
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_tenant_snapshot(snapshot_id));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTenantMetaSnapshotHandler::get_ls_snapshot(snapshot_id, ls_id, tablet_meta_entry));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_single_ls_snapshot(snapshot_id, ls_id, clog_max_scn));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::get_ls_snapshot(snapshot_id, ls_id, tablet_meta_entry));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::delete_single_ls_snapshot(snapshot_id, ls_id));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTenantMetaSnapshotHandler::get_ls_snapshot(snapshot_id, ls_id, tablet_meta_entry));

  // create tablet and create single ls snapshot
  ObTabletHandle new_tablet_handle;
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ASSERT_EQ(OB_SUCCESS, persist_tablet(new_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, ObTenantMetaSnapshotHandler::create_single_ls_snapshot(snapshot_id, ls_id, clog_max_scn));

  // check integrity
  ASSERT_EQ(OB_SUCCESS, check_integrity(new_tablet_handle.get_obj()->tablet_addr_, snapshot_id, ls_id, tablet_id));
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_meta_snapshot.log*");
  OB_LOGGER.set_file_name("test_meta_snapshot.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_2_0_0);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
