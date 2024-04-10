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

#include <errno.h>
#include <gtest/gtest.h>
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define protected public
#define private public
#include "ob_index_block_data_prepare.h"
#include "storage/blocksstable/ob_shared_macro_block_manager.h"

namespace oceanbase
{
namespace blocksstable
{

class TestSharedMacroBlk : public TestIndexBlockDataPrepare
{
public:
  TestSharedMacroBlk();
  ~TestSharedMacroBlk();
};

TestSharedMacroBlk::TestSharedMacroBlk()
  : TestIndexBlockDataPrepare(
      "Test Shared Macro Block",
      MINI_MERGE,
      false,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      10)
{
}

TestSharedMacroBlk::~TestSharedMacroBlk()
{
}

TEST_F(TestSharedMacroBlk, test_used_size_mgr)
{
  ObArenaAllocator allocator;
  ObSharedMacroBlockMgr shared_mgr;
  OK(shared_mgr.init());
  int32_t size = 0;
  MacroBlockId id1(0, MacroBlockId::AUTONOMIC_BLOCK_INDEX, 0);
  const int64_t illegal_size = ObSharedMacroBlockMgr::SMALL_SSTABLE_STHRESHOLD_SIZE;
  OK(shared_mgr.add_block(id1, illegal_size));
  OK(shared_mgr.free_block(id1, illegal_size - 1));
  OK(shared_mgr.free_block(id1, 1));
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, shared_mgr.block_used_size_.get(id1, size));

  const int64_t legal_size = ObSharedMacroBlockMgr::SMALL_SSTABLE_STHRESHOLD_SIZE - 1;
  OK(shared_mgr.add_block(id1, legal_size));
  OK(shared_mgr.free_block(id1, legal_size)); // delete id from mgr
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, shared_mgr.block_used_size_.get(id1, size));

  const int64_t recyclable_size = ObSharedMacroBlockMgr::RECYCLABLE_BLOCK_SIZE - 1;
  OK(shared_mgr.add_block(id1, recyclable_size));

  ObArray<MacroBlockId> block_id_set;
  OK(shared_mgr.get_recyclable_blocks(allocator, block_id_set));
  ASSERT_EQ(block_id_set.count(), 0);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, shared_mgr.block_used_size_.get(id1, size)); // auto-clear

  OB_SERVER_BLOCK_MGR.inc_ref(id1); // add id to block_map in manager;
  OK(shared_mgr.add_block(id1, recyclable_size));
  block_id_set.reset();
  OK(shared_mgr.get_recyclable_blocks(allocator, block_id_set));
  ASSERT_EQ(block_id_set.count(), 1);
  ASSERT_EQ(block_id_set.at(0), id1);
}

TEST_F(TestSharedMacroBlk, test_rebuild_sstable)
{
  ObTabletID tablet_id(TestIndexBlockDataPrepare::tablet_id_);
  ObLSID ls_id(ls_id_);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObLSService *ls_svr = MTL(ObLSService*);
  ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle));
  ObSharedMacroBlockMgr *shared_blk_mgr = MTL(ObSharedMacroBlockMgr*);
  ObArenaAllocator allocator;
  ObSSTableIndexBuilder *sstable_index_builder = OB_NEW(ObSSTableIndexBuilder, "SSTableIdx");
  ObIndexBlockRebuilder *index_block_rebuilder = OB_NEW(ObIndexBlockRebuilder, "IdxRebuilder");
  ObSSTable sstable;

  // rebuild sstable
  tablet_handle.get_obj()->tablet_meta_.snapshot_version_ = 12;
  int ret = shared_blk_mgr->rebuild_sstable(allocator,
      *(tablet_handle.get_obj()), sstable_, 0, *sstable_index_builder, *index_block_rebuilder, sstable);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(true, sstable_.meta_->basic_meta_.check_basic_meta_equality(sstable.meta_->basic_meta_));

  // get old and new sstable
  ObMacroBlockHandle old_handle;
  ObMacroBlockReadInfo read_info;
  ObMacroIdIterator id_iterator;
  ASSERT_EQ(OB_SUCCESS, sstable_.meta_->macro_info_.get_data_block_iter(id_iterator));
  ASSERT_EQ(OB_SUCCESS, id_iterator.get_next_macro_id(read_info.macro_block_id_));
  read_info.offset_ = sstable_.meta_->macro_info_.nested_offset_;
  read_info.size_ = sstable_.meta_->macro_info_.nested_size_;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  ASSERT_NE(nullptr, read_info.buf_ = reinterpret_cast<char*>(allocator.alloc(read_info.size_)));
  ASSERT_EQ(OB_SUCCESS, ObBlockManager::read_block(read_info, old_handle));

  ObMacroBlockHandle new_handle;
  id_iterator.reset();
  ASSERT_EQ(OB_SUCCESS, sstable.meta_->macro_info_.get_data_block_iter(id_iterator));
  ASSERT_EQ(OB_SUCCESS, id_iterator.get_next_macro_id(read_info.macro_block_id_));
  read_info.offset_ = sstable.meta_->macro_info_.nested_offset_;
  read_info.size_ = sstable.meta_->macro_info_.nested_size_;
  ASSERT_EQ(OB_SUCCESS, ObBlockManager::read_block(read_info, new_handle));

  // compare two sstables
  ASSERT_EQ(sstable_.meta_->macro_info_.nested_size_, sstable.meta_->macro_info_.nested_size_);
  ASSERT_EQ(0, MEMCMP(old_handle.get_buffer(), new_handle.get_buffer(), sstable.meta_->macro_info_.nested_size_));

  OB_DELETE(ObSSTableIndexBuilder, "SSTableIdx", sstable_index_builder);
  OB_DELETE(ObIndexBlockRebuilder, "IdxRebuilder", index_block_rebuilder);
}

TEST_F(TestSharedMacroBlk, test_invalid_write)
{
  const int64_t BUF_SIZE = 8192; // 8K
  ObArenaAllocator allocator;
  ObSharedMacroBlockMgr *shared_blk_mgr = MTL(ObSharedMacroBlockMgr*);
  char *buf = static_cast<char *>(allocator.alloc(BUF_SIZE));
  MEMSET(buf, 9, BUF_SIZE);
  ObBlockInfo block_info;
  // first valid write
  ObMacroBlocksWriteCtx write_ctx_a;
  ASSERT_EQ(OB_SUCCESS, shared_blk_mgr->write_block(buf, BUF_SIZE, block_info, write_ctx_a));
  // invalid write
  ObMacroBlocksWriteCtx write_ctx_b;
  ASSERT_NE(OB_SUCCESS, shared_blk_mgr->write_block(buf, -1, block_info, write_ctx_b));
  // invalid write
  ObMacroBlocksWriteCtx write_ctx_c;
  int64_t tmp_offset = shared_blk_mgr->offset_;
  shared_blk_mgr->offset_ = -1;
  ASSERT_NE(OB_SUCCESS, shared_blk_mgr->write_block(buf, BUF_SIZE, block_info, write_ctx_c));
  shared_blk_mgr->offset_ = tmp_offset;
  // second valid write
  ObMacroBlocksWriteCtx write_ctx_d;
  ASSERT_EQ(OB_SUCCESS, shared_blk_mgr->write_block(buf, BUF_SIZE, block_info, write_ctx_d));
  // read and check buf
  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_ = block_info.macro_id_;
  read_info.size_ = block_info.nested_size_;
  read_info.offset_ = block_info.nested_offset_;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  ASSERT_NE(nullptr, read_info.buf_ = reinterpret_cast<char*>(allocator.alloc(read_info.size_)));
  ObMacroBlockHandle read_handle;
  ASSERT_EQ(OB_SUCCESS, ObBlockManager::read_block(read_info, read_handle));
  MEMCMP(read_handle.get_buffer(), buf, BUF_SIZE);
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_shared_macro_block.log*");
  OB_LOGGER.set_file_name("test_shared_macro_block.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_1_0_0);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
