// owner: gaishun.gs
// owner group: storage

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

#define private public
#define protected public
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
struct BlockInfoComp
{
  bool operator()(const ObTabletBlockInfo &a, const ObTabletBlockInfo &b) const
  {
    if (a.macro_id_ != b.macro_id_) {
      return MEMCMP(&a.macro_id_, &b.macro_id_, sizeof(MacroBlockId)) < 0;
    }
    // don't compare occupy_size
    return a.block_type_ < b.block_type_;
  }
};

typedef std::set<ObTabletBlockInfo, BlockInfoComp> block_info_set_t;
class MyBlockInfoSet;
class TestBlockIdList : public TestIndexBlockDataPrepare
{
public:
  TestBlockIdList();
  virtual ~TestBlockIdList() = default;
  int init_info_set(ObArenaAllocator &allocator, const int64_t id_count, ObBlockInfoSet &info_set);
  int init_info_set_rand(const int64_t cnt, ObBlockInfoSet &info_set, MyBlockInfoSet &std_set);
  ObTabletBlockInfo gen_rand_block_info(const ObTabletMacroType target_type);
  static int64_t get_block_cnt(const ObBlockInfoSet &info_set)
  {
    const ObBlockInfoSet::TabletMacroSet &meta_block_info_set = info_set.meta_block_info_set_;
    const ObBlockInfoSet::TabletMacroSet &data_block_info_set = info_set.data_block_info_set_;
    const ObBlockInfoSet::TabletMacroSet &shared_meta_block_info_set = info_set.shared_meta_block_info_set_;
    const ObBlockInfoSet::TabletMacroMap &shared_data_block_info_map = info_set.clustered_data_block_info_map_;
    return meta_block_info_set.size()
           + data_block_info_set.size()
           + shared_meta_block_info_set.size()
           + shared_data_block_info_map.size();
  }
  static int write_macro_info(ObIAllocator &allocator, const ObTabletMacroInfo &macro_info, ObSharedObjectsWriteCtx &write_ctx);
  static int read_macro_info(ObArenaAllocator &allocator, const ObMetaDiskAddr &addr, ObTabletMacroInfo *&macro_info);
  static bool check_macro_info(ObMacroInfoIterator &macro_info_iter, const MyBlockInfoSet &all_blocks_set);
  void inner_test_macro_info_io(const bool test_inline);
public:
  ObRandom rand_;
};

class MyBlockInfoSet final
{
public:
  MyBlockInfoSet()
    : data_blocks_cnt_(0),
      meta_blocks_cnt_(0),
      shared_meta_blocks_cnt_(0),
      shared_data_blocks_cnt_(0),
      sslog_row_cnt_(0),
      linked_blocks_cnt_(0)
  {}

  int insert(const ObTabletBlockInfo &block_info)
  {
    int ret = OB_SUCCESS;
    all_blocks_set_.insert(block_info);
    switch(block_info.block_type()) {
    case ObTabletMacroType::DATA_BLOCK:
      ++data_blocks_cnt_;
      break;
    case ObTabletMacroType::META_BLOCK:
      ++meta_blocks_cnt_;
      break;
    case ObTabletMacroType::SHARED_META_BLOCK:
      ++shared_meta_blocks_cnt_;
      break;
    case ObTabletMacroType::SHARED_DATA_BLOCK:
      ++shared_data_blocks_cnt_;
      break;
    case ObTabletMacroType::LINKED_BLOCK:
      ++linked_blocks_cnt_;
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected block type", K(ret), K(block_info));
    }
    return ret;
  }

  bool has_block(const ObTabletBlockInfo &block_info) const
  {
    return 0 != all_blocks_set_.count(block_info);
  }

  int64_t get_block_cnt(const ObTabletMacroType target_type) const
  {
    int res = 0;
    if (ObTabletMacroType::MAX == target_type) {
      res = data_blocks_cnt_
            + meta_blocks_cnt_
            + shared_meta_blocks_cnt_
            + shared_data_blocks_cnt_
            + sslog_row_cnt_
            + linked_blocks_cnt_;
    } else if (ObTabletMacroType::DATA_BLOCK == target_type) {
      res = data_blocks_cnt_;
    } else if (ObTabletMacroType::META_BLOCK == target_type) {
      res = meta_blocks_cnt_;
    } else if (ObTabletMacroType::SHARED_META_BLOCK == target_type) {
      res = shared_meta_blocks_cnt_;
    } else if (ObTabletMacroType::SHARED_DATA_BLOCK == target_type) {
      res = shared_data_blocks_cnt_;
    } else if (ObTabletMacroType::LINKED_BLOCK == target_type) {
      res = linked_blocks_cnt_;
    }
    return res;
  }

private:
  block_info_set_t all_blocks_set_;
  int64_t data_blocks_cnt_;
  int64_t meta_blocks_cnt_;
  int64_t shared_meta_blocks_cnt_;
  int64_t shared_data_blocks_cnt_;
  int64_t sslog_row_cnt_;
  int64_t linked_blocks_cnt_;
};

TestBlockIdList::TestBlockIdList()
  : TestIndexBlockDataPrepare(
      "Test Tablet Ref Cnt",
      MINI_MERGE,
      OB_DEFAULT_MACRO_BLOCK_SIZE,
      10000,
      65536)
{
  rand_.seed(ObTimeUtility::current_time());
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
    } else if (OB_FAIL(info_set.clustered_data_block_info_map_.set_refactored(tmp_macro_id, i + 5))) {
      LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
    }
  }
  return ret;
}

int TestBlockIdList::init_info_set_rand(const int64_t cnt, ObBlockInfoSet &info_set, MyBlockInfoSet &my_set)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
    if (OB_SUCC(ret)) {
      ObTabletBlockInfo block_info = gen_rand_block_info(ObTabletMacroType::DATA_BLOCK);
      const MacroBlockId &tmp_macro_id = block_info.macro_id();
      if (OB_FAIL(info_set.data_block_info_set_.set_refactored(tmp_macro_id))) {
        LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
      } else if (OB_FAIL(my_set.insert(block_info))) {
        LOG_WARN("failed to add block info", K(ret), K(block_info));
      }
    }
    if (OB_SUCC(ret)) {
      ObTabletBlockInfo block_info = gen_rand_block_info(ObTabletMacroType::META_BLOCK);
      const MacroBlockId &tmp_macro_id = block_info.macro_id();
      if (OB_FAIL(info_set.meta_block_info_set_.set_refactored(tmp_macro_id))) {
        LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
      } else if (OB_FAIL(my_set.insert(block_info))) {
        LOG_WARN("failed to add block info", K(ret), K(block_info));
      }
    }
    if (OB_SUCC(ret)) {
      ObTabletBlockInfo block_info = gen_rand_block_info(ObTabletMacroType::SHARED_META_BLOCK);
      const MacroBlockId &tmp_macro_id = block_info.macro_id();
      if (OB_FAIL(info_set.shared_meta_block_info_set_.set_refactored(tmp_macro_id))) {
        LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
      } else if (OB_FAIL(my_set.insert(block_info))) {
        LOG_WARN("failed to add block info", K(ret), K(block_info));
      }
    }
    if (OB_SUCC(ret)) {
      ObTabletBlockInfo block_info = gen_rand_block_info(ObTabletMacroType::SHARED_DATA_BLOCK);
      const MacroBlockId &tmp_macro_id = block_info.macro_id();
      if (OB_FAIL(info_set.clustered_data_block_info_map_.set_refactored(tmp_macro_id, 123456))) {
        LOG_WARN("fail to set refactored for info set", K(ret), K(tmp_macro_id));
      } else if (OB_FAIL(my_set.insert(block_info))) {
        LOG_WARN("failed to add block info", K(ret), K(block_info));
      }
    }
  }
  return ret;
}

ObTabletBlockInfo TestBlockIdList::gen_rand_block_info(const ObTabletMacroType target_type)
{
  static int64_t start_seq = 0;
  static const int64_t range = 1000;
  blocksstable::MacroBlockId macro_id(rand_.get(start_seq, start_seq + range - 1),
                                      rand_.get(start_seq, start_seq + range - 1),
                                      rand_.get(start_seq, start_seq + range - 1),
                                      rand_.get(start_seq, start_seq + range - 1));
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_LOCAL);
  macro_id.set_version_v2();
  EXPECT_TRUE(macro_id.is_valid());
  start_seq += range;
  return ObTabletBlockInfo(macro_id, target_type, 123456);
}

int TestBlockIdList::write_macro_info(ObIAllocator &allocator, const ObTabletMacroInfo &macro_info, ObSharedObjectsWriteCtx &write_ctx)
{
  int ret = OB_SUCCESS;
  const int64_t size = macro_info.get_serialize_size();
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null meta serivce", K(ret), KP(meta_service));
  } else if (OB_ISNULL(buf = (char *)allocator.alloc(size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate buffer", K(ret), K(size), KP(buf));
  } else if (OB_FAIL(macro_info.serialize(buf, size, pos))) {
    LOG_WARN("failed to serialize macro info", K(ret), KP(buf), K(size));
  } else {
    ObSharedObjectWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = size;
    write_info.ls_epoch_ = 0;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    ObStorageObjectOpt opt;
    opt.set_private_meta_macro_object_opt(123456, 0);
    ObSharedObjectWriteHandle handle;
    if (OB_FAIL(meta_service->get_shared_object_reader_writer().async_write(write_info, opt, handle))) {
      LOG_WARN("failed to async write", K(ret), K(write_info));
    } else if (OB_FAIL(handle.get_write_ctx(write_ctx))) {
      LOG_WARN("failed to get write ctx", K(ret), K(handle));
    }
  }
  return ret;
}

int TestBlockIdList::read_macro_info(ObArenaAllocator &allocator, const ObMetaDiskAddr &addr, ObTabletMacroInfo *&macro_info)
{
  int ret = OB_SUCCESS;
  macro_info = nullptr;
  ObSharedObjectReadHandle read_handle(allocator);
  ObSharedObjectReadInfo read_info;
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
  read_info.addr_ = addr;
  read_info.ls_epoch_ = 0;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  char *buf = nullptr;
  int64_t buf_len = 0;
  int64_t pos = 0;
  void *macro_info_buf = nullptr;
  if (OB_FAIL(ObSharedObjectReaderWriter::async_read(read_info, read_handle))) {
    LOG_WARN("failed to do read", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait())) {
    LOG_WARN("failed to wait for read handle", K(ret), K(read_handle));
  } else if (OB_FAIL(read_handle.get_data(allocator, buf, buf_len))) {
    LOG_WARN("failed to get data from read handle", K(ret), K(read_handle), KP(buf), K(buf_len));
  } else if (OB_ISNULL(macro_info_buf = allocator.alloc(sizeof(ObTabletMacroInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate macro_info_buf", K(ret));
  } else if (FALSE_IT(macro_info = new (macro_info_buf) ObTabletMacroInfo())) {
  } else if (OB_FAIL(macro_info->deserialize(allocator, buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize macro info", K(ret), KP(buf), K(buf_len), K(pos));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(macro_info)) {
      macro_info->~ObTabletMacroInfo();
      allocator.free(macro_info);
      macro_info = nullptr;
      macro_info_buf = nullptr;
    }
    if (OB_NOT_NULL(macro_info_buf)) {
      allocator.free(macro_info_buf);
      macro_info_buf = nullptr;
    }
  }
  return ret;
}

bool TestBlockIdList::check_macro_info(ObMacroInfoIterator &macro_info_iter, const MyBlockInfoSet &all_blocks_set)
{
  int ret = OB_SUCCESS;
  int64_t iter_cnt = 0;
  while (OB_SUCC(ret)) {
    ObTabletBlockInfo block_info;
    if (OB_FAIL(macro_info_iter.get_next(block_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next item from macro info iter", K(ret));
        return false;
      }
      // iter end
      ret = OB_SUCCESS;
      break;
    }
    ++iter_cnt;
    //printf("%d: %s\n", iter_cnt, ObCStringHelper().convert(block_info));
    if (!all_blocks_set.has_block(block_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block not exists", K(ret), K(block_info));
      return false;
    }
  }
  // {
  //   int i = 0;
  //   for (const ObTabletBlockInfo &block_info : all_blocks_set.all_blocks_set_) {
  //     printf("expected %d: %s\n", ++i, ObCStringHelper().convert(block_info));
  //   }
  // }
  return all_blocks_set.get_block_cnt(macro_info_iter.target_type_) == iter_cnt;
}

TEST_F(TestBlockIdList, test_id_list)
{
  ObBlockInfoSet info_set;
  ObArenaAllocator allocator;
  ObTabletMacroInfo macro_info;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObBlockManager::BlockInfo block_info;
  bool inc_success = false;

  ObLinkedMacroInfoWriteParam write_param;
  write_param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
  write_param.tablet_id_ = ObTabletID(1001);
  write_param.tablet_private_transfer_epoch_ = 1;
  write_param.start_macro_seq_ = 1;
  // empty set
  ASSERT_EQ(OB_SUCCESS, info_set.init());
  ASSERT_EQ(OB_SUCCESS, linked_writer.init_for_macro_info(write_param));
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, &linked_writer));
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
  info_set.clustered_data_block_info_map_.set_refactored(macro_id, 10);
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, &linked_writer));
  ASSERT_EQ(true, IS_EMPTY_BLOCK_LIST(macro_info.entry_block_));
  ASSERT_EQ(1, macro_info.shared_data_block_info_arr_.cnt_);
  ASSERT_EQ(1, macro_info.shared_meta_block_info_arr_.cnt_);
  ASSERT_EQ(1, macro_info.data_block_info_arr_.cnt_);
  ASSERT_EQ(1, macro_info.meta_block_info_arr_.cnt_);

  info_set.data_block_info_set_.reuse();
  info_set.meta_block_info_set_.reuse();
  info_set.shared_meta_block_info_set_.reuse();
  info_set.clustered_data_block_info_map_.reuse();

  // large set
  int64_t linked_ref_cnt = 0;
  macro_info.reset();
  macro_id = MacroBlockId(1, 1, 0);
  ASSERT_EQ(OB_SUCCESS, init_info_set(allocator, TEST_LINKED_NUM, info_set));
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, &linked_writer));
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
  ObLinkedMacroInfoWriteParam write_param;
  write_param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
  write_param.tablet_id_ = ObTabletID(1001);
  write_param.tablet_private_transfer_epoch_ = 1;
  write_param.start_macro_seq_ = 1;
  ASSERT_EQ(OB_SUCCESS, info_set.init());
  ASSERT_EQ(OB_SUCCESS, linked_writer.init_for_macro_info(write_param));
  for (int64_t i = 0; i < ObTabletMacroInfo::ID_COUNT_THRESHOLD; i++) {
    MacroBlockId tmp_macro_id(i + 1, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, info_set.data_block_info_set_.set_refactored(tmp_macro_id));
    ASSERT_EQ(OB_SUCCESS, info_set.clustered_data_block_info_map_.set_refactored(tmp_macro_id, i + 5));
  }
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, &linked_writer));
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
    ASSERT_EQ(OB_SUCCESS, info_set_2.clustered_data_block_info_map_.set_refactored(tmp_macro_id, i + 5));
  }
  ASSERT_EQ(OB_SUCCESS, macro_info_2.init(allocator, info_set_2, &linked_writer));
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
  ObStorageObjectHandle object_handle;
  int64_t ref_cnt = 0;

  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletHandle new_tablet_handle;
  blocksstable::ObStorageObjectOpt default_opt;

  // persist 4k tablet
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int32_t private_transfer_epoch = -1;
  ASSERT_EQ(OB_SUCCESS, tablet->get_private_transfer_epoch(private_transfer_epoch));
  const int64_t tablet_meta_version = 0;
  const ObTabletPersisterParam param(data_version, ls_id,  ls_handle.get_ls()->get_ls_epoch(), tablet_id, private_transfer_epoch, tablet_meta_version);
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  ASSERT_EQ(OB_SUCCESS, meta_service->get_shared_object_reader_writer().switch_object(object_handle, default_opt));
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, meta_service->get_shared_object_reader_writer().switch_object(object_handle, default_opt));
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
  ObLinkedMacroInfoWriteParam write_param;
  write_param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
  write_param.tablet_id_ = ObTabletID(1001);
  write_param.tablet_private_transfer_epoch_ = 1;
  write_param.start_macro_seq_ = 1;

  // linked macro info
  ObBlockInfoSet info_set;
  ObTabletMacroInfo macro_info;
  ASSERT_EQ(OB_SUCCESS, info_set.init());
  ASSERT_EQ(OB_SUCCESS, linked_writer.init_for_macro_info(write_param));
  ASSERT_EQ(OB_SUCCESS, init_info_set(allocator, TEST_LINKED_NUM, info_set));
  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, &linked_writer));
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
  ASSERT_EQ(OB_SUCCESS, linked_writer.init_for_macro_info(write_param));
  ASSERT_EQ(OB_SUCCESS, init_info_set(allocator, 15, info_set_2));
  ASSERT_EQ(OB_SUCCESS, macro_info_2.init(allocator, info_set_2, &linked_writer));
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
  ASSERT_EQ(OB_SUCCESS, linked_writer.init_for_macro_info(write_param));
  ASSERT_EQ(OB_SUCCESS, macro_info_3.init(allocator, info_set_3, &linked_writer));
  macro_iter.destroy();
  ASSERT_EQ(OB_SUCCESS, macro_iter.init(ObTabletMacroType::MAX, macro_info_3));
  ASSERT_EQ(OB_ITER_END, macro_iter.get_next(block_info));

  // linked macro info without meta_block_id and shared_meta_block_id
  linked_writer.reset();
  ObBlockInfoSet info_set_4;
  ObTabletMacroInfo macro_info_4;
  ASSERT_EQ(OB_SUCCESS, info_set_4.init());
  ASSERT_EQ(OB_SUCCESS, linked_writer.init_for_macro_info(write_param));
  for (int64_t i = 0; i < ObTabletMacroInfo::ID_COUNT_THRESHOLD; i++) {
    MacroBlockId tmp_macro_id(i + 1, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, info_set_4.data_block_info_set_.set_refactored(tmp_macro_id));
    ASSERT_EQ(OB_SUCCESS, info_set_4.clustered_data_block_info_map_.set_refactored(tmp_macro_id, i + 5));
  }
  ASSERT_EQ(OB_SUCCESS, macro_info_4.init(allocator, info_set_4, &linked_writer));
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
  ASSERT_EQ(OB_SUCCESS, linked_writer.init_for_macro_info(write_param));
  for (int64_t i = 0; i < memory_id_cnt; i++) {
    MacroBlockId tmp_macro_id(i + 1, i + 1, 0);
    ASSERT_EQ(OB_SUCCESS, info_set_5.data_block_info_set_.set_refactored(tmp_macro_id));
    ASSERT_EQ(OB_SUCCESS, info_set_5.clustered_data_block_info_map_.set_refactored(tmp_macro_id, i + 5));
  }
  ASSERT_EQ(OB_SUCCESS, macro_info_5.init(allocator, info_set_5, &linked_writer));
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
  const uint64_t data_version = DATA_CURRENT_VERSION;

  // create and get empty shell
  ret = TestTabletHelper::create_tablet(ls_handle, tablet_id, schema, allocator_, ObTabletStatus::Status::DELETED);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_tablet_svr->update_tablet_to_empty_shell(data_version, tablet_id);
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
  int32_t private_transfer_epoch = -1;
  ASSERT_EQ(OB_SUCCESS, tablet->get_private_transfer_epoch(private_transfer_epoch));
  const int64_t tablet_meta_version = 0;
  const ObTabletPersisterParam param(data_version, ls_id,  ls_handle.get_ls()->get_ls_epoch(), tablet_id, private_transfer_epoch, tablet_meta_version);
  ASSERT_EQ(OB_SUCCESS, ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_tablet_handle));
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
  ObStorageObjectHandle object_handle;
  ObTenantStorageMetaService *meta_service = MTL(ObTenantStorageMetaService*);
  ObSharedObjectReaderWriter &shared_rw = meta_service->get_shared_object_reader_writer();
  blocksstable::ObStorageObjectOpt default_opt;
  ASSERT_EQ(OB_SUCCESS, shared_rw.switch_object(object_handle, default_opt));
  common::ObArenaAllocator arena_allocator("unittest");
  ObSharedObjectsWriteCtx write_ctx;
  static const int64_t BLOCK_CNT = 10;

  // write linked blocks and wait
  char *buffer = static_cast<char*>(arena_allocator.alloc(4096));
  ObSharedObjectWriteInfo write_info;
  ObSharedObjectLinkHandle write_handle;
  write_info.buffer_ = buffer;
  write_info.offset_ = 0;
  write_info.size_ = 4096;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  blocksstable::ObStorageObjectOpt curr_opt;
  curr_opt.set_private_object_opt();
  for (int64_t i = 0; i < BLOCK_CNT; i++) {
    ASSERT_EQ(OB_SUCCESS, shared_rw.async_link_write(write_info, curr_opt, write_handle));
  }
  ASSERT_EQ(OB_SUCCESS, write_handle.get_write_ctx(write_ctx));

  // increase macro blocks' ref cnt and check
  bool inc_success = false;
  ObBlockManager::BlockInfo block_info;
  MacroBlockId macro_id;
  ASSERT_EQ(OB_SUCCESS, write_ctx.addr_.get_macro_block_id(macro_id));
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

void TestBlockIdList::inner_test_macro_info_io(const bool test_inline)
{
  ObMacroInfoIterator macro_iter;
  ObLinkedMacroBlockItemWriter linked_writer;
  ObArenaAllocator allocator;
  ObTabletBlockInfo block_info;
  ObLinkedMacroInfoWriteParam write_param;
  write_param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
  write_param.tablet_id_ = ObTabletID(1001);
  write_param.tablet_private_transfer_epoch_ = 1;
  write_param.start_macro_seq_ = 1;

  // linked macro info
  ObBlockInfoSet info_set;
  ObTabletMacroInfo macro_info;
  MyBlockInfoSet all_blocks_set;

  const int64_t block_cnt = test_inline ? 3 : ObTabletMacroInfo::ID_COUNT_THRESHOLD;

  ASSERT_EQ(OB_SUCCESS, info_set.init());
  ASSERT_EQ(OB_SUCCESS, linked_writer.init_for_macro_info(write_param));
  ASSERT_EQ(OB_SUCCESS, init_info_set_rand(block_cnt, info_set, all_blocks_set));
  if (test_inline) {
    ASSERT_EQ(get_block_cnt(info_set), all_blocks_set.get_block_cnt(ObTabletMacroType::MAX));
  }

  ASSERT_EQ(OB_SUCCESS, macro_info.init(allocator, info_set, &linked_writer));
  // entry block should be empty if inline
  ASSERT_EQ(test_inline, IS_EMPTY_BLOCK_LIST(macro_info.entry_block_));
  ObSharedObjectsWriteCtx write_ctx;
  ASSERT_EQ(OB_SUCCESS, write_macro_info(allocator, macro_info, write_ctx));

  if (!test_inline) {
    // also count on linked blocks if not inline
    const ObIArray<MacroBlockId> &meta_block_list = linked_writer.get_meta_block_list();
    for (int64_t i = 0; i < meta_block_list.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, all_blocks_set.insert(ObTabletBlockInfo(meta_block_list.at(i), ObTabletMacroType::LINKED_BLOCK, 0)));
    }
  }

  // read macro info from storage
  {
    const ObMetaDiskAddr macro_info_addr = write_ctx.addr_;
    ObTabletMacroInfo *macro_info_from_disk = nullptr;
    ASSERT_EQ(OB_SUCCESS, read_macro_info(allocator, macro_info_addr, macro_info_from_disk));
    ASSERT_NE(nullptr, macro_info_from_disk);
    // check
    ObMacroInfoIterator all_iter;
    ASSERT_EQ(OB_SUCCESS, all_iter.init(ObTabletMacroType::MAX, *macro_info_from_disk));
    ASSERT_TRUE(check_macro_info(all_iter, all_blocks_set));
    all_iter.destroy();

    ObMacroInfoIterator iter0;
    ASSERT_EQ(OB_SUCCESS, iter0.init(ObTabletMacroType::DATA_BLOCK, *macro_info_from_disk));
    ASSERT_TRUE(check_macro_info(iter0, all_blocks_set));
    iter0.destroy();

    ObMacroInfoIterator iter1;
    ASSERT_EQ(OB_SUCCESS, iter1.init(ObTabletMacroType::META_BLOCK, *macro_info_from_disk));
    ASSERT_TRUE(check_macro_info(iter1, all_blocks_set));
    iter1.destroy();

    ObMacroInfoIterator iter2;
    ASSERT_EQ(OB_SUCCESS, iter2.init(ObTabletMacroType::SHARED_META_BLOCK, *macro_info_from_disk));
    ASSERT_TRUE(check_macro_info(iter2, all_blocks_set));
    iter2.destroy();

    ObMacroInfoIterator iter3;
    ASSERT_EQ(OB_SUCCESS, iter3.init(ObTabletMacroType::SHARED_DATA_BLOCK, *macro_info_from_disk));
    ASSERT_TRUE(check_macro_info(iter3, all_blocks_set));
    iter3.destroy();
  }
}

TEST_F(TestBlockIdList, test_macro_info_io_inline)
{
  inner_test_macro_info_io(true);
}

TEST_F(TestBlockIdList, test_macro_info_io_not_inline)
{
  inner_test_macro_info_io(false);
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
