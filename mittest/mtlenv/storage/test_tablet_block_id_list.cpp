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
#include "storage/tablet/ob_tablet_block_aggregated_info.h"
#include "storage/slog_ckpt/ob_linked_macro_block_writer.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "mittest/mtlenv/storage/blocksstable/ob_index_block_data_prepare.h"
#include "storage/schema_utils.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"

using namespace oceanbase::blocksstable;
namespace oceanbase
{
namespace storage
{
const int64_t TEST_LINKED_NUM = ObTabletMacroInfo::ID_COUNT_THRESHOLD / 3;
class TestBlockIdList : public TestIndexBlockDataPrepare
{
public:
  TestBlockIdList();
  virtual ~TestBlockIdList() = default;
  int init_info_set(ObArenaAllocator &allocator, const int64_t id_count, ObBlockInfoSet &info_set);
};

TestBlockIdList::TestBlockIdList()
  : TestIndexBlockDataPrepare(
      "Test Tablet Ref Cnt",
      MINI_MERGE,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      65536)
{
}

int TestBlockIdList::init_info_set(ObArenaAllocator &allocator, const int64_t id_count, ObBlockInfoSet &info_set)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < id_count; i++) {
    MacroBlockId tmp_macro_id(i + 1, i + 1, 0);
    if (OB_FAIL(info_set.data_block_info_set_.set_refactored(tmp_macro_id))) {
      LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
    } else if (OB_FAIL(info_set.meta_block_info_set_.set_refactored(tmp_macro_id))) {
      LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
    } else if (OB_FAIL(info_set.shared_meta_block_info_set_.set_refactored(tmp_macro_id))) {
      LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
    } else if (OB_FAIL(info_set.shared_data_block_info_map_.set_refactored(tmp_macro_id, i + 5))) {
      LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
    }
  }
  return ret;
}

TEST_F(TestBlockIdList, test_id_list)
{
  ObBlockInfoSet info_set;
  ObArenaAllocator allocator;
  ObTabletMacroInfo macro_info;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObBlockManager::BlockInfo block_info;
  bool inc_success = false;

  // empty set
  ASSERT_EQ(OB_SUCCESS, info_set.init());
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, linked_writer));
  ASSERT_EQ(0, macro_info.meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.data_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.shared_data_block_info_arr_.cnt_);

  // normal set
  macro_info.reset();
  MacroBlockId macro_id(0, 0, 0);
  info_set.data_block_info_set_.set_refactored(macro_id);
  info_set.meta_block_info_set_.set_refactored(macro_id);
  info_set.shared_meta_block_info_set_.set_refactored(macro_id);
  info_set.shared_data_block_info_map_.set_refactored(macro_id, 10);
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, linked_writer));
  ASSERT_EQ(true, IS_EMPTY_BLOCK_LIST(macro_info.entry_block_));
  ASSERT_EQ(1, macro_info.shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(1, macro_info.shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(1, macro_info.data_block_info_arr_.cnt_);
  ASSERT_EQ(1, macro_info.meta_block_info_arr_.cnt_);

  info_set.data_block_info_set_.reuse();
  info_set.meta_block_info_set_.reuse();
  info_set.shared_meta_block_info_set_.reuse();
  info_set.shared_data_block_info_map_.reuse();

  // large set
  int64_t linked_ref_cnt = 0;
  macro_info.reset();
  macro_id = MacroBlockId(1, 1, 0);
  ASSERT_EQ(OB_SUCCESS, init_info_set(allocator, TEST_LINKED_NUM, info_set));
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, linked_writer));
  ASSERT_EQ(false, IS_EMPTY_BLOCK_LIST(macro_info.entry_block_));
  ASSERT_EQ(0, macro_info.shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.data_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.meta_block_info_arr_.cnt_);
}

TEST_F(TestBlockIdList, test_serialize_deep_copy)
{
  ObArenaAllocator allocator;
  ObLinkedMacroBlockItemWriter linked_writer;

  // linked macro info without meta_block_id and shared_meta_block_id
  linked_writer.reset();
  ObBlockInfoSet info_set;
  ObTabletMacroInfo macro_info;
  ASSERT_EQ(OB_SUCCESS, info_set.init());
  for (int64_t i = 0; i < ObTabletMacroInfo::ID_COUNT_THRESHOLD; i++) {
    MacroBlockId tmp_macro_id(i + 1, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, info_set.data_block_info_set_.set_refactored(tmp_macro_id));
    ASSERT_EQ(OB_SUCCESS, info_set.shared_data_block_info_map_.set_refactored(tmp_macro_id, i + 5));
  }
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, linked_writer));
  ASSERT_EQ(0, macro_info.data_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info.meta_block_info_arr_.cnt_);

  int64_t serialize_size = macro_info.get_serialize_size();
  char *buf = (char *)allocator.alloc(serialize_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro_info.serialize(buf, serialize_size, pos));
  ObTabletMacroInfo deserialize_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialize_info.deserialize(allocator, buf, serialize_size, pos));
  ASSERT_EQ(deserialize_info.entry_block_, macro_info.entry_block_);
  ASSERT_EQ(0, deserialize_info.data_block_info_arr_.cnt_);
  ASSERT_EQ(0, deserialize_info.shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(0, deserialize_info.shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, deserialize_info.meta_block_info_arr_.cnt_);

  int64_t deep_copy_size = macro_info.get_deep_copy_size();
  buf = (char *)allocator.alloc(deep_copy_size);
  ObTabletMacroInfo *deep_copy_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, macro_info.deep_copy(buf, deep_copy_size, deep_copy_info));
  ASSERT_EQ(deep_copy_info->entry_block_, macro_info.entry_block_);
  ASSERT_EQ(0, deep_copy_info->data_block_info_arr_.cnt_);
  ASSERT_EQ(0, deep_copy_info->shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(0, deep_copy_info->shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, deep_copy_info->meta_block_info_arr_.cnt_);

  // memory macro info without meta_block_id and shared_meta_block_id
  linked_writer.reset();
  ObBlockInfoSet info_set_2;
  ObTabletMacroInfo macro_info_2;
  static const int64_t MEMORY_ID_CNT = 100;
  ASSERT_EQ(OB_SUCCESS, info_set_2.init());
  for (int64_t i = 0; i < MEMORY_ID_CNT; i++) {
    MacroBlockId tmp_macro_id(i + 1, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, info_set_2.data_block_info_set_.set_refactored(tmp_macro_id));
    ASSERT_EQ(OB_SUCCESS, info_set_2.shared_data_block_info_map_.set_refactored(tmp_macro_id, i + 5));
  }
  ASSERT_EQ(OB_SUCCESS, macro_info_2.init(allocator, info_set_2, linked_writer));
  ASSERT_EQ(true, IS_EMPTY_BLOCK_LIST(macro_info_2.entry_block_));
  ASSERT_EQ(MEMORY_ID_CNT, macro_info_2.data_block_info_arr_.cnt_);
  ASSERT_EQ(MEMORY_ID_CNT, macro_info_2.shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info_2.shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, macro_info_2.meta_block_info_arr_.cnt_);

  serialize_size = macro_info_2.get_serialize_size();
  buf = (char *)allocator.alloc(serialize_size);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, macro_info_2.serialize(buf, serialize_size, pos));
  deserialize_info.reset();
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialize_info.deserialize(allocator, buf, serialize_size, pos));
  ASSERT_EQ(true, IS_EMPTY_BLOCK_LIST(deserialize_info.entry_block_));
  ASSERT_EQ(MEMORY_ID_CNT, deserialize_info.data_block_info_arr_.cnt_);
  ASSERT_EQ(MEMORY_ID_CNT, deserialize_info.shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(0, deserialize_info.shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, deserialize_info.meta_block_info_arr_.cnt_);

  deep_copy_size = macro_info_2.get_deep_copy_size();
  buf = (char *)allocator.alloc(deep_copy_size);
  deep_copy_info = nullptr;
  ASSERT_EQ(OB_SUCCESS, macro_info_2.deep_copy(buf, deep_copy_size, deep_copy_info));
  ASSERT_EQ(deep_copy_info->entry_block_, macro_info_2.entry_block_);
  ASSERT_EQ(MEMORY_ID_CNT, deep_copy_info->data_block_info_arr_.cnt_);
  ASSERT_EQ(MEMORY_ID_CNT, deep_copy_info->shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(0, deep_copy_info->shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(0, deep_copy_info->meta_block_info_arr_.cnt_);
}

TEST_F(TestBlockIdList, test_meta_macro_ref_cnt)
{
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
  ASSERT_EQ(1, block_info.ref_cnt_);

  ASSERT_EQ(OB_SUCCESS, new_tablet->inc_macro_ref_cnt());
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(2, block_info.ref_cnt_);

  new_tablet->dec_macro_ref_cnt();
  {
    ObBucketHashWLockGuard lock_guard(OB_SERVER_BLOCK_MGR.bucket_lock_, table_store_id.hash());
    ASSERT_EQ(OB_SUCCESS, OB_SERVER_BLOCK_MGR.block_map_.get(table_store_id, block_info));
  }
  ASSERT_EQ(1, block_info.ref_cnt_);
}

TEST_F(TestBlockIdList, test_info_iterator)
{
  int64_t linked_block_num = 0;
  ObMacroInfoIterator macro_iter;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObArenaAllocator allocator;
  ObTabletBlockInfo block_info;

  // linked macro info
  ObBlockInfoSet info_set;
  ObTabletMacroInfo macro_info;
  ASSERT_EQ(OB_SUCCESS, info_set.init());
  ASSERT_EQ(OB_SUCCESS, init_info_set(allocator, TEST_LINKED_NUM, info_set));
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, linked_writer));
  ASSERT_EQ(OB_SUCCESS, macro_iter.init(ObTabletMacroType::MAX, macro_info));
  for (int64_t i = 0; i < TEST_LINKED_NUM * 4; i++) {
    block_info.reset();
    ASSERT_EQ(OB_SUCCESS, macro_iter.get_next(block_info));
    ASSERT_EQ(OB_HASH_EXIST, info_set.data_block_info_set_.exist_refactored(block_info.macro_id_));
  }
  linked_block_num = macro_iter.block_reader_.get_meta_block_list().count();
  for (int64_t i = 0; i < linked_block_num; i++) {
    block_info.reset();
    ASSERT_EQ(OB_SUCCESS, macro_iter.get_next(block_info));
    ASSERT_EQ(OB_HASH_NOT_EXIST, info_set.data_block_info_set_.exist_refactored(block_info.macro_id_));
  }
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next(block_info));

  macro_iter.destroy(); // iterate targeted ids
  ASSERT_NE(OB_SUCCESS, macro_iter.init(ObTabletMacroType::INVALID_TYPE, macro_info));
  ASSERT_EQ(OB_SUCCESS, macro_iter.init(ObTabletMacroType::SHARED_DATA_BLOCK, macro_info));
  for (int64_t i = 0; i < TEST_LINKED_NUM; i++) {
    block_info.reset();
    ASSERT_EQ(OB_SUCCESS, macro_iter.get_next(block_info));
    ASSERT_EQ(OB_HASH_EXIST, info_set.data_block_info_set_.exist_refactored(block_info.macro_id_));
    ASSERT_EQ(ObTabletMacroType::SHARED_DATA_BLOCK, block_info.block_type_);
    ASSERT_NE(OB_DEFAULT_MACRO_BLOCK_SIZE, block_info.occupy_size_);
  }
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next(block_info));

  // memory macro info
  linked_writer.reset();
  ObBlockInfoSet info_set_2;
  ObTabletMacroInfo macro_info_2;
  ASSERT_EQ(OB_SUCCESS, info_set_2.init());
  ASSERT_EQ(OB_SUCCESS, init_info_set(allocator, 15, info_set_2));
  ASSERT_EQ(OB_SUCCESS, macro_info_2.init(allocator, info_set_2, linked_writer));
  macro_iter.destroy();
  ASSERT_EQ(OB_SUCCESS, macro_iter.init(ObTabletMacroType::MAX, macro_info_2));
  for (int64_t i = 0; i < 60; i++) {
    block_info.reset();
    ASSERT_EQ(OB_SUCCESS, macro_iter.get_next(block_info));
    ASSERT_EQ(OB_HASH_EXIST, info_set_2.data_block_info_set_.exist_refactored(block_info.macro_id_));
  }
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next(block_info));

  macro_iter.destroy(); // iterate targeted ids
  ASSERT_EQ(OB_SUCCESS, macro_iter.init(ObTabletMacroType::META_BLOCK, macro_info_2));
  for (int64_t i = 0; i < 15; i++) {
    ASSERT_EQ(OB_SUCCESS, macro_iter.get_next(block_info));
    ASSERT_EQ(OB_HASH_EXIST, info_set.data_block_info_set_.exist_refactored(block_info.macro_id_));
    ASSERT_EQ(ObTabletMacroType::META_BLOCK, block_info.block_type_);
    ASSERT_EQ(OB_DEFAULT_MACRO_BLOCK_SIZE, block_info.occupy_size_);
  }
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next(block_info));

  // empty macro info
  linked_writer.reset();
  ObBlockInfoSet info_set_3;
  ObTabletMacroInfo macro_info_3;
  ASSERT_EQ(OB_SUCCESS, info_set_3.init());
  ASSERT_EQ(OB_SUCCESS, macro_info_3.init(allocator, info_set_3, linked_writer));
  macro_iter.destroy();
  ASSERT_EQ(OB_SUCCESS, macro_iter.init(ObTabletMacroType::MAX, macro_info_3));
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next(block_info));

  // linked macro info without meta_block_id and shared_meta_block_id
  linked_writer.reset();
  ObBlockInfoSet info_set_4;
  ObTabletMacroInfo macro_info_4;
  ASSERT_EQ(OB_SUCCESS, info_set_4.init());
  for (int64_t i = 0; i < ObTabletMacroInfo::ID_COUNT_THRESHOLD; i++) {
    MacroBlockId tmp_macro_id(i + 1, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, info_set_4.data_block_info_set_.set_refactored(tmp_macro_id));
    ASSERT_EQ(OB_SUCCESS, info_set_4.shared_data_block_info_map_.set_refactored(tmp_macro_id, i + 5));
  }
  ASSERT_EQ(OB_SUCCESS, macro_info_4.init(allocator, info_set_4, linked_writer));
  macro_iter.destroy();
  ASSERT_EQ(OB_SUCCESS, macro_iter.init(ObTabletMacroType::MAX, macro_info_4));
  for (int64_t i = 0; i < ObTabletMacroInfo::ID_COUNT_THRESHOLD * 2; i++) {
    block_info.reset();
    ASSERT_EQ(OB_SUCCESS, macro_iter.get_next(block_info));
    ASSERT_EQ(OB_HASH_EXIST, info_set_4.data_block_info_set_.exist_refactored(block_info.macro_id_));
  }
  linked_block_num = macro_iter.block_reader_.get_meta_block_list().count();
  for (int64_t i = 0; i < linked_block_num; i++) {
    block_info.reset();
    ASSERT_EQ(OB_SUCCESS, macro_iter.get_next(block_info));
    ASSERT_EQ(OB_HASH_NOT_EXIST, info_set_4.data_block_info_set_.exist_refactored(block_info.macro_id_));
  }
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next(block_info));

  // memory macro info without meta_block_id and shared_meta_block_id
  linked_writer.reset();
  ObBlockInfoSet info_set_5;
  ObTabletMacroInfo macro_info_5;
  static const int64_t memory_id_cnt = 100;
  ASSERT_EQ(OB_SUCCESS, info_set_5.init());
  for (int64_t i = 0; i < memory_id_cnt; i++) {
    MacroBlockId tmp_macro_id(i + 1, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, info_set_5.data_block_info_set_.set_refactored(tmp_macro_id));
    ASSERT_EQ(OB_SUCCESS, info_set_5.shared_data_block_info_map_.set_refactored(tmp_macro_id, i + 5));
  }
  ASSERT_EQ(OB_SUCCESS, macro_info_5.init(allocator, info_set_5, linked_writer));
  macro_iter.destroy();
  ASSERT_EQ(OB_SUCCESS, macro_iter.init(ObTabletMacroType::MAX, macro_info_5));
  for (int64_t i = 0; i < memory_id_cnt * 2; i++) {
    block_info.reset();
    ASSERT_EQ(OB_SUCCESS, macro_iter.get_next(block_info));
    ASSERT_EQ(OB_HASH_EXIST, info_set_5.data_block_info_set_.exist_refactored(block_info.macro_id_));
  }
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next(block_info));
}

TEST_F(TestBlockIdList, test_empty_shell_macro_ref_cnt)
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

TEST_F(TestBlockIdList, test_linked_block_ref_cnt)
{
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
  system("rm -f test_tablet_block_id_list.log*");
  OB_LOGGER.set_file_name("test_tablet_block_id_list.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_4_1_0_0);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}