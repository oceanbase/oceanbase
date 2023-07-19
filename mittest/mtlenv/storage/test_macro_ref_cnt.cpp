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
#include <unordered_map>

#define protected public
#define private public

#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "mittest/mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/schema_utils.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace blocksstable;
using namespace storage;

class TestTabletRefCnt : public TestIndexBlockDataPrepare
{
public:
  TestTabletRefCnt();
  ~TestTabletRefCnt();
};

TestTabletRefCnt::TestTabletRefCnt()
  : TestIndexBlockDataPrepare(
      "Test Tablet Ref Cnt",
      MINI_MERGE,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      65536)
{
}

TestTabletRefCnt::~TestTabletRefCnt()
{
}

void convert_ctx_to_map(
    const common::ObSEArray<ObSharedBlocksWriteCtx, 16> &tablet_meta_write_ctxs,
    const common::ObSEArray<ObSharedBlocksWriteCtx, 16> &sstable_meta_write_ctxs,
    std::unordered_map<int64_t, int64_t> &ref_cnts)
{
  MacroBlockId macro_id;
  int64_t offset;
  int64_t size;
  ObBlockManager::BlockInfo block_info;

  for (int64_t i = 0; i < tablet_meta_write_ctxs.count(); i++) {
    ASSERT_EQ(OB_SUCCESS, tablet_meta_write_ctxs[i].addr_.get_block_addr(macro_id, offset, size));
    if (ref_cnts.count(macro_id.block_index_) == 0) {
      {
        ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
        ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
      }
      ref_cnts[macro_id.block_index_] = block_info.ref_cnt_;
    }
  }

  for (int64_t i = 0; i < tablet_meta_write_ctxs.count(); i++) {
    ASSERT_EQ(OB_SUCCESS, tablet_meta_write_ctxs[i].addr_.get_block_addr(macro_id, offset, size));
    ref_cnts[macro_id.block_index_]++;
  }

  for (int64_t i = 0; i < sstable_meta_write_ctxs.count(); i++) {
    ASSERT_EQ(OB_SUCCESS, sstable_meta_write_ctxs[i].addr_.get_block_addr(macro_id, offset, size));
    ref_cnts[macro_id.block_index_]++;
  }
}

TEST_F(TestTabletRefCnt, test_persist_tablet)
{
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  common::ObArenaAllocator allocator;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> tablet_meta_write_ctxs;
  common::ObSEArray<ObSharedBlocksWriteCtx, 16> sstable_meta_write_ctxs;
  ObTabletHandle new_tablet_handle;
  std::unordered_map<int64_t, int64_t> ref_cnts;
  ObBlockManager::BlockInfo block_info;

  MacroBlockId macro_id;
  int64_t offset;
  int64_t size;
  ObMacroBlockHandle macro_handle;

  // persist 4k tablet
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::recursively_persist(
      *(tablet_handle.get_obj()), allocator, tablet_meta_write_ctxs, sstable_meta_write_ctxs, new_tablet_handle));
  convert_ctx_to_map(tablet_meta_write_ctxs, sstable_meta_write_ctxs, ref_cnts);
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_4k_tablet(allocator, new_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, new_tablet_handle.get_obj()->tablet_addr_.get_block_addr(macro_id, offset, size));
  ref_cnts[macro_id.block_index_]++; // tablet_meta_write_ctxs doesn't contain tablet_addr

  // check ref cnt
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  ASSERT_EQ(block_info.ref_cnt_, ref_cnts[macro_id.block_index_]);
  for (int64_t i = 0; i < tablet_meta_write_ctxs.count(); i++) {
    ASSERT_EQ(OB_SUCCESS, tablet_meta_write_ctxs[i].addr_.get_block_addr(macro_id, offset, size));
    {
      ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
      ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
    }
    ASSERT_EQ(block_info.ref_cnt_, ref_cnts[macro_id.block_index_]);
  }

  // transform memory
  ObTabletHandle tmp_tablet_handle;
  ObTabletMapKey key(ls_id, tablet_id);
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::acquire_tablet_from_pool(
      ObTabletPoolType::TP_LARGE, key, tmp_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::transform_tablet_memory_footprint(
      *(new_tablet_handle.get_obj()), (char *)(tmp_tablet_handle.get_obj()), tmp_tablet_handle.get_buf_len()));
  ASSERT_EQ(true, tmp_tablet_handle.get_obj()->hold_ref_cnt_);
  tmp_tablet_handle.get_obj()->hold_ref_cnt_ = false;
}

TEST_F(TestTabletRefCnt, test_meta_ref_cnt)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));

  int64_t offset = 0;
  int64_t size = 0;
  ObBlockManager::BlockInfo block_info;
  MacroBlockId table_store_id;
  ObMacroBlockHandle macro_handle;
  int64_t ref_cnt = 0;

  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletHandle new_tablet_handle;

  // persist 4k tablet
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantCheckpointSlogHandler*)->get_shared_block_reader_writer().switch_block(macro_handle));
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, MTL(ObTenantCheckpointSlogHandler*)->get_shared_block_reader_writer().switch_block(macro_handle));
  ObTablet *new_tablet = new_tablet_handle.get_obj();
  ASSERT_EQ(OB_SUCCESS, new_tablet->table_store_addr_.addr_.get_block_addr(table_store_id, offset, size));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ref_cnt = block_info.ref_cnt_;

  // increase macro ref cnt
  ASSERT_EQ(OB_SUCCESS, new_tablet->inc_macro_ref_cnt());
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(ref_cnt * 2, block_info.ref_cnt_);

  // decrease macro ref cnt
  new_tablet->dec_macro_ref_cnt();
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(ref_cnt, block_info.ref_cnt_);

  // deserialize tablet
  ObTenantCheckpointSlogHandler *ckpt_handler = MTL(ObTenantCheckpointSlogHandler*);
  ObTabletHandle tmp_tablet_handle;
  ObTabletMapKey key(ls_id, tablet_id);
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  ObArenaAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, ckpt_handler->read_from_disk(new_tablet->tablet_addr_, allocator, buf, buf_len));
  ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_handle));
  tmp_tablet_handle.get_obj()->tablet_addr_ = new_tablet->tablet_addr_;
  ASSERT_EQ(OB_SUCCESS, tmp_tablet_handle.get_obj()->deserialize(allocator, buf, buf_len, pos));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(ref_cnt * 2, block_info.ref_cnt_);
}

TEST_F(TestTabletRefCnt, test_data_ref_cnt)
{
  ObBlockManager::BlockInfo block_info;
  MacroBlockId macro_id;
  int64_t ref_cnt = 0;
  common::ObArenaAllocator tmp_allocator("CacheSST");
  ObSafeArenaAllocator safe_allocator(tmp_allocator);
  ObSSTableMetaHandle meta_handle;
  ObMacroIdIterator iterator;
  ASSERT_EQ(OB_SUCCESS, sstable_.get_meta(meta_handle, &safe_allocator));
  ASSERT_EQ(OB_SUCCESS, meta_handle.get_sstable_meta().get_macro_info().get_data_block_iter(iterator));
  ASSERT_EQ(OB_SUCCESS, iterator.get_next_macro_id(macro_id));

  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  ref_cnt = block_info.ref_cnt_;

  // increase macro ref cnt
  bool inc_success;
  ASSERT_EQ(OB_SUCCESS, sstable_.inc_macro_ref(inc_success));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  ASSERT_EQ(ref_cnt + 1, block_info.ref_cnt_);

  // decrease macro ref cnt
  sstable_.dec_macro_ref();
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  ASSERT_EQ(ref_cnt, block_info.ref_cnt_);
}

TEST_F(TestTabletRefCnt, test_empty_shell_macro_ref_cnt)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id(ls_id_);
  ObTabletID tablet_id(10000009);
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTablet *tablet = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ObLSTabletService *ls_tablet_svr = ls_handle.get_ls()->get_tablet_svr();

  // create and get empty shell
  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_tablet_svr->update_tablet_to_empty_shell(tablet_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle tablet_handle;
  ret = ls_tablet_svr->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  tablet = tablet_handle.get_obj();

  // check increasing macro ref cnt for empty shell tablet with file addr
  ASSERT_EQ(false, tablet->hold_ref_cnt_);
  ASSERT_EQ(OB_SUCCESS, tablet->inc_macro_ref_cnt());
  ASSERT_EQ(true, tablet->hold_ref_cnt_);

  // check increasing macro ref cnt for empty shell tablet with file addr
  MacroBlockId macro_id;
  int64_t offset;
  int64_t size;
  ObBlockManager::BlockInfo block_info;
  int64_t ref_cnt = 0;
  ObTabletHandle new_tablet_handle;
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle));
  ObTablet *new_tablet = new_tablet_handle.get_obj();
  ASSERT_EQ(OB_SUCCESS, new_tablet->tablet_addr_.get_block_addr(macro_id, offset, size));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  ref_cnt = block_info.ref_cnt_;

  ASSERT_EQ(OB_SUCCESS, new_tablet->inc_macro_ref_cnt());
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  ASSERT_EQ(ref_cnt + 1, block_info.ref_cnt_);

  new_tablet->dec_macro_ref_cnt();
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  ASSERT_EQ(ref_cnt, block_info.ref_cnt_);
}

TEST_F(TestTabletRefCnt, test_linked_block_ref_cnt)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle tmp_handle;
  ObSharedBlockReaderWriter &shared_rw = MTL(ObTenantCheckpointSlogHandler*)->get_shared_block_reader_writer();
  ASSERT_EQ(OB_SUCCESS, shared_rw.switch_block(tmp_handle));
  common::ObArenaAllocator arena_allocator("unittest");
  ObSharedBlocksWriteCtx write_ctx;
  static const int64_t BLOCK_CNT = 10;

  // write linked blocks and wait
  char *buffer = static_cast<char*>(arena_allocator.alloc(4096));
  ObSharedBlockWriteInfo write_info;
  ObSharedBlockLinkHandle write_handle;
  write_info.buffer_ = buffer;
  write_info.offset_ = 0;
  write_info.size_ = 4096;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  for (int64_t i = 0; i < BLOCK_CNT; i++) {
    ASSERT_EQ(OB_SUCCESS, shared_rw.async_link_write(write_info, write_handle));
  }
  ASSERT_EQ(OB_SUCCESS, write_handle.get_write_ctx(write_ctx));

  // increase macro blocks' ref cnt and check
  bool inc_success = false;
  ObBlockManager::BlockInfo block_info;
  MacroBlockId macro_id = write_ctx.addr_.block_id();
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  int64_t ref_cnt = block_info.ref_cnt_;

  ASSERT_EQ(OB_SUCCESS, ObTablet::inc_linked_block_ref_cnt(write_ctx.addr_, inc_success));
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, macro_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(macro_id, block_info));
  }
  ASSERT_EQ(ref_cnt + BLOCK_CNT, block_info.ref_cnt_);
}

} // storage
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet_ref_cnt.log*");
  OB_LOGGER.set_file_name("test_tablet_ref_cnt.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_1_0_0);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}