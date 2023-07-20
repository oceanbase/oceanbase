/**
 * Copyright (c) 2022 OceanBase
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
#define protected public
#define private public
#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#include "storage/blockstore/ob_shared_block_reader_writer.h"
#include "share/io/ob_io_define.h"
#include "share/io/ob_io_manager.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/meta_mem/ob_storage_meta_cache.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
static ObSimpleMemLimitGetter getter;

namespace  unittest
{
class TestSharedBlockRWriter : public::testing::Test
{
public:
  TestSharedBlockRWriter() {}
  virtual ~TestSharedBlockRWriter() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void create_empty_sstable(ObSSTable &empty_sstable);
  ObArenaAllocator allocator_;
};

void TestSharedBlockRWriter::SetUpTestCase()
{
  STORAGE_LOG(INFO, "SetUpTestCase");

  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSharedBlockRWriter::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSharedBlockRWriter::SetUp()
{
  int ret = OB_SUCCESS;
  const ObTenantIOConfig &io_config = ObTenantIOConfig::default_instance();
  if (OB_FAIL(ObIOManager::get_instance().add_tenant_io_manager(OB_SERVER_TENANT_ID, io_config))) {
    STORAGE_LOG(WARN, "add tenant io config failed");
  }
}

void TestSharedBlockRWriter::TearDown()
{
}

void TestSharedBlockRWriter::create_empty_sstable(ObSSTable &empty_sstable)
{
  ObTabletCreateSSTableParam param;
  param.encrypt_id_ = 0;
  param.master_key_id_ = 0;
  MEMSET(param.encrypt_key_, 0, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
  param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  param.table_key_.tablet_id_ = 10;
  param.table_key_.version_range_.snapshot_version_ = 1;
  param.max_merged_trans_version_ = 1;

  param.schema_version_ = 1;
  param.create_snapshot_version_ = 0;
  param.progressive_merge_round_ = 0;
  param.progressive_merge_step_ = 0;

  ObTableMode table_mode;
  param.table_mode_ = table_mode;
  param.index_type_ = ObIndexType::INDEX_TYPE_IS_NOT;
  param.rowkey_column_cnt_ = 1;
  param.root_block_addr_.set_none_addr();
  param.data_block_macro_meta_addr_.set_none_addr();
  param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param.data_index_tree_height_ = 0;
  param.index_blocks_cnt_ = 0;
  param.data_blocks_cnt_ = 0;
  param.micro_block_cnt_ = 0;
  param.use_old_macro_block_count_ = 0;
  param.data_checksum_ = 0;
  param.occupy_size_ = 0;
  param.ddl_scn_.set_min();
  param.filled_tx_scn_.set_min();
  param.original_size_ = 0;
  param.ddl_scn_.set_min();
  param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param.column_cnt_ = 1;
  OK(ObSSTableMergeRes::fill_column_checksum_for_empty_major(param.column_cnt_, param.column_checksums_));
  OK(empty_sstable.init(param, &allocator_));
}

TEST_F(TestSharedBlockRWriter, test_rwrite_easy_block)
{
  ObSharedBlockReaderWriter rwriter;
  OK(rwriter.init(true/*need align*/, false/*need cross*/));
  ObMetaDiskAddr addr;
  MacroBlockId macro_id;
  ObMacroBlockCommonHeader common_header;
  common_header.reset();
  common_header.set_attr(ObMacroBlockCommonHeader::MacroBlockType::SharedMetaData);
  const int64_t header_size = common_header.get_serialize_size();
  ASSERT_EQ(rwriter.offset_, header_size);
  ASSERT_TRUE(rwriter.hanging_);
  ASSERT_EQ(rwriter.data_.pos_, header_size);


  char s[10] = "";
  int test_round = 10;
  while (test_round--) {
    ObSharedBlockWriteInfo write_info;
    ObSharedBlockWriteHandle write_handle;
    ObSharedBlockReadHandle read_handle;
    for (int i = 0; i < 10; ++i) {
      s[i] = '0' + test_round;
    }
    write_info.buffer_ = s;
    write_info.offset_ = 0;
    write_info.size_ = 10;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    OK(rwriter.async_write(write_info, write_handle));
    ASSERT_TRUE(write_handle.is_valid());

    ObSharedBlocksWriteCtx write_ctx;
    ObSharedBlockReadInfo read_info;
    OK(write_handle.get_write_ctx(write_ctx));
    ASSERT_EQ(write_ctx.addr_.size_, 10 + sizeof(ObSharedBlockHeader));
    if (test_round == 9) {
      macro_id = write_ctx.addr_.block_id();
    }

    read_info.addr_ = write_ctx.addr_;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    OK(rwriter.async_read(read_info, read_handle));
    char *buf = nullptr;
    int64_t buf_len = 0;
    OK(read_handle.get_data(allocator_, buf, buf_len));
    ASSERT_EQ(10, buf_len);
    for (int64_t i = 0; i < buf_len; ++i) {
      ASSERT_EQ(buf[i], '0' + test_round);
    }
  }

  // test check shared blk
  ObMacroBlockReadInfo read_info;
  ObMacroBlockHandle macro_handle;
  read_info.macro_block_id_ = macro_id;
  read_info.offset_ = 0;
  read_info.size_ = (2L << 20);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  const int64_t io_timeout_ms =
      std::max(GCONF._data_storage_io_timeout / 1000, DEFAULT_IO_WAIT_TIME_MS);

  OK(ObBlockManager::async_read_block(read_info, macro_handle));
  OK(macro_handle.wait(io_timeout_ms));
  OK((ObSSTableMacroBlockChecker::check(macro_handle.get_buffer(),
    macro_handle.get_data_size(), ObMacroBlockCheckLevel::CHECK_LEVEL_PHYSICAL)));
}

TEST_F(TestSharedBlockRWriter, test_batch_write_easy_block)
{
  ObSharedBlockReaderWriter rwriter;
  OK(rwriter.init(true/*need align*/, false/*need cross*/));
  int test_round = 10;

  ObSharedBlockWriteInfo write_info;
  ObArray<ObSharedBlockWriteInfo> write_infos;
  char s[10][100];
  for (int i = 0; i < test_round; ++i) {
    for (int j = 0; j <10; ++j) {
      s[i][j] = '0' + i;
    }
    write_info.buffer_ = s[i];
    write_info.offset_ = 0;
    write_info.size_ = 10;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_infos.push_back(write_info);
  }

  ObSharedBlockBatchHandle write_handle;
  OK(rwriter.async_batch_write(write_infos, write_handle));
  ASSERT_TRUE(write_handle.is_valid());

  ObArray<ObSharedBlocksWriteCtx> write_ctxs;
  OK(write_handle.batch_get_write_ctx(write_ctxs));
  ASSERT_EQ(test_round, write_ctxs.count());

  for (int i = 0; i < test_round; ++i) {
    ObSharedBlockReadInfo read_info;
    ObSharedBlockReadHandle read_handle;
    read_info.addr_ = write_ctxs.at(i).addr_;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    OK(rwriter.async_read(read_info, read_handle));
    char *buf = nullptr;
    int64_t buf_len = 0;
    OK(read_handle.get_data(allocator_, buf, buf_len));
    ASSERT_EQ(10, buf_len);
    for (int64_t j = 0; j < buf_len; ++j) {
      ASSERT_EQ(buf[j], '0' + i);
    }
  }
}


TEST_F(TestSharedBlockRWriter, test_link_write)
{
  ObSharedBlockReaderWriter rwriter;
  OK(rwriter.init(true/*need align*/, false/*need cross*/));
  int test_round = 10;

  ObSharedBlockWriteInfo write_info;
  ObSharedBlockLinkHandle write_handle;
  char s[10];
  for (int i = 0; i < test_round; ++i) {
    for (int j = 0; j < 10; ++j) {
      s[j] = '0' + i;
    }
    write_info.buffer_ = s;
    write_info.offset_ = 0;
    write_info.size_ = 10;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    OK(rwriter.async_link_write(write_info, write_handle));
    ASSERT_TRUE(write_handle.is_valid());
  }

  ObSharedBlocksWriteCtx write_ctx;
  ObSharedBlockLinkIter iter;
  OK(write_handle.get_write_ctx(write_ctx));
  OK(iter.init(write_ctx.addr_));
  char *buf = nullptr;
  int64_t buf_len = 0;
  for (int i = 0; i < test_round; ++i) {
    OK(iter.get_next_block(allocator_, buf, buf_len));
    ASSERT_EQ(10, buf_len);
    for (int64_t j = 0; j < buf_len; ++j) {
      ASSERT_EQ(buf[j], '0'+9-i);
    }
  }
  ASSERT_EQ(OB_ITER_END, iter.get_next_block(allocator_, buf, buf_len));
}

TEST_F(TestSharedBlockRWriter, test_cb_single_write)
{
  ObSharedBlockReaderWriter rwriter;
  OK(rwriter.init(true/*need align*/, false/*need cross*/));

  ObSSTable empty_sstable;
  create_empty_sstable(empty_sstable);
  const int64_t sstable_size = empty_sstable.get_serialize_size();
  char *sstable_buf = static_cast<char *>(allocator_.alloc(sstable_size));
  int64_t pos = 0;
  OK(empty_sstable.serialize(sstable_buf, sstable_size, pos));

  ObSharedBlockWriteInfo write_info;
  ObSharedBlockWriteHandle write_handle;
  write_info.buffer_ = sstable_buf;
  write_info.offset_ = 0;
  write_info.size_ = sstable_size;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  OK(rwriter.async_write(write_info, write_handle));
  ASSERT_TRUE(write_handle.is_valid());

  ObSharedBlocksWriteCtx write_ctx;
  OK(write_handle.get_write_ctx(write_ctx));

  ObStorageMetaHandle meta_handle;
  OK(meta_handle.cache_handle_.new_value(allocator_));
  const ObStorageMetaValue::MetaType meta_type = ObStorageMetaValue::SSTABLE;
  ObTablet tablet;
  ObTablet *fake_tablet = &tablet;
  uint64_t tenant_id = 1;
  ObStorageMetaKey meta_key;
  meta_key.tenant_id_ = tenant_id;
  meta_key.phy_addr_ = write_ctx.addr_;
  ObStorageMetaCache::ObStorageMetaIOCallback cb(
      meta_type, meta_key, meta_handle.cache_handle_, &allocator_, fake_tablet);
  ObSharedBlockReadInfo read_info;
  ObSharedBlockReadHandle read_handle;
  read_info.addr_ = write_ctx.addr_;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_callback_ = &cb;
  OK(rwriter.async_read(read_info, read_handle));
  OK(read_handle.wait());
  // test the buf of read handle
  char *buf = nullptr;
  int64_t buf_len = 0;
  OK(read_handle.get_data(allocator_, buf, buf_len));
  ASSERT_EQ(sstable_size, buf_len);
}

TEST_F(TestSharedBlockRWriter, test_cb_batch_write)
{
  ObSharedBlockReaderWriter rwriter;
  OK(rwriter.init(true/*need align*/, false/*need cross*/));
  int test_round = 10;

  ObSharedBlockWriteInfo write_info;
  ObArray<ObSharedBlockWriteInfo> write_infos;
  char s[10][100];
  for (int i = 0; i < test_round; ++i) {
    for (int j = 0; j <10; ++j) {
      s[i][j] = '0' + i;
    }
    write_info.buffer_ = s[i];
    write_info.offset_ = 0;
    write_info.size_ = 10;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_infos.push_back(write_info);
  }

  ObSSTable empty_sstable;
  create_empty_sstable(empty_sstable);
  const int64_t sstable_size = empty_sstable.get_serialize_size();
  char *sstable_buf = static_cast<char *>(allocator_.alloc(sstable_size));
  int64_t pos = 0;
  OK(empty_sstable.serialize(sstable_buf, sstable_size, pos));
  write_info.buffer_ = sstable_buf;
  write_info.offset_ = 0;
  write_info.size_ = sstable_size;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  write_infos.push_back(write_info);

  ObSharedBlockBatchHandle write_handle;
  OK(rwriter.async_batch_write(write_infos, write_handle));
  ASSERT_TRUE(write_handle.is_valid());

  ObArray<ObSharedBlocksWriteCtx> write_ctxs;
  OK(write_handle.batch_get_write_ctx(write_ctxs));
  ASSERT_EQ(test_round + 1, write_ctxs.count());

  ObStorageMetaHandle meta_handle;
  OK(meta_handle.cache_handle_.new_value(allocator_));
  const ObStorageMetaValue::MetaType meta_type = ObStorageMetaValue::SSTABLE;
  ObTablet tablet;
  ObTablet *fake_tablet = &tablet;
  uint64_t tenant_id = 1;
  ObStorageMetaKey meta_key;
  meta_key.tenant_id_ = tenant_id;
  meta_key.phy_addr_ = write_ctxs[test_round].addr_;
  ObStorageMetaCache::ObStorageMetaIOCallback cb(
      meta_type, meta_key, meta_handle.cache_handle_, &allocator_, fake_tablet);
  ObSharedBlockReadInfo read_info;
  ObSharedBlockReadHandle read_handle;
  read_info.addr_ = write_ctxs[test_round].addr_;
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.io_callback_ = &cb;
  OK(rwriter.async_read(read_info, read_handle));
  OK(read_handle.wait());
  // test the buf of read handle
  char *buf = nullptr;
  int64_t buf_len = 0;
  OK(read_handle.get_data(allocator_, buf, buf_len));
  ASSERT_EQ(sstable_size, buf_len);
}

TEST_F(TestSharedBlockRWriter, test_batch_write_switch_block)
{
  // test switch block when batch write, which means hanging_=true
  ObSharedBlockReaderWriter rwriter;
  OK(rwriter.init(true/*need align*/, false/*need cross*/));
  ObSharedBlockWriteInfo write_info;
  ObArray<ObSharedBlockWriteInfo> write_infos;
  int64_t data_size = 3L << 10; // 3K
  char s[3L << 10] = "";
  for (int i = 0; i < data_size; ++i) {
    s[i] = '0' + (i % 10);
  }
  write_info.buffer_ = s;
  write_info.offset_ = 0;
  write_info.size_ = data_size;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  OK(write_infos.push_back(write_info));
  OK(write_infos.push_back(write_info));

  ObMacroBlockCommonHeader common_header;
  common_header.reset();
  common_header.set_attr(ObMacroBlockCommonHeader::MacroBlockType::SharedMetaData);
  const int64_t header_size = common_header.get_serialize_size();
  ASSERT_EQ(rwriter.data_.pos_, header_size);

  rwriter.offset_ = (2L << 20) - (4L << 10); // 2M - 4K
  rwriter.align_offset_ = rwriter.offset_;
  rwriter.data_.advance(rwriter.offset_ - header_size);

  ObSharedBlockBatchHandle write_handle;
  OK(rwriter.async_batch_write(write_infos, write_handle));
  ASSERT_TRUE(write_handle.is_valid());

  ObArray<ObSharedBlocksWriteCtx> write_ctxs;
  OK(write_handle.batch_get_write_ctx(write_ctxs));
  ASSERT_EQ(write_infos.count(), write_ctxs.count());

  for (int i = 0; i < write_ctxs.count(); ++i) {
    ObSharedBlockReadInfo read_info;
    ObSharedBlockReadHandle read_handle;
    read_info.addr_ = write_ctxs.at(i).addr_;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
    OK(rwriter.async_read(read_info, read_handle));
    char *buf = nullptr;
    int64_t buf_len = 0;
    OK(read_handle.get_data(allocator_, buf, buf_len));
    ASSERT_EQ(data_size, buf_len);
    for (int64_t j = 0; j < data_size; ++j) {
      ASSERT_EQ(buf[j], '0' + (j % 10));
    }
  }
}

TEST_F(TestSharedBlockRWriter, test_batch_write_bug1)
{
  /* batch_write_bug1:
    The store_size of the last block in batch_write is not aligned to 4K
    when hanging_ = true && we need switch_block.
    This may result in 4016 read error of other blocks flushed by next async_write.
  */
  ObSharedBlockReaderWriter rwriter;
  OK(rwriter.init(true/*need align*/, false/*need cross*/));

  rwriter.offset_ = (2L << 20) - 10; // nearly to 2M, ready to switch block
  rwriter.align_offset_ = (2L << 20) - 4096;
  rwriter.hanging_ = true;

  ObSharedBlockWriteInfo write_info; // the last block
  int64_t data_size = 1L << 10; // 1K
  char s[1L << 10] = "";
  for (int i = 0; i < data_size; ++i) {
    s[i] = '0' + (i % 10);
  }
  write_info.buffer_ = s;
  write_info.offset_ = 0;
  write_info.size_ = data_size;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);

  ObSharedBlockWriteHandle block_handle;
  OK(rwriter.async_write(write_info, block_handle));
  ObSharedBlocksWriteCtx write_ctx;
  OK(block_handle.get_write_ctx(write_ctx));
  const ObMetaDiskAddr &addr = write_ctx.addr_;

  ObMacroBlockCommonHeader common_header;
  common_header.reset();
  common_header.set_attr(ObMacroBlockCommonHeader::MacroBlockType::SharedMetaData);
  const int64_t header_size = common_header.get_serialize_size();
  ASSERT_EQ(addr.offset_, 0 + header_size);
  ASSERT_EQ(addr.size_, data_size + sizeof(ObSharedBlockHeader));
  ASSERT_EQ(rwriter.offset_, 4096);
  ASSERT_EQ(rwriter.align_offset_, 4096);
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_shared_block_reader_writer.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_shared_block_reader_writer.log", true);
  srand(time(NULL));
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
