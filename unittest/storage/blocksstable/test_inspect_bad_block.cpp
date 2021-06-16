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
#define private public
#include "ob_data_file_prepare.h"
#include "storage/ob_sstable_test.h"

#include "storage/blocksstable/ob_store_file.h"
#include "storage/compaction/ob_micro_block_iterator.h"
#include "storage/blocksstable/ob_macro_block_checker.h"
#undef private
namespace oceanbase {
using namespace blocksstable;
using namespace compaction;
using namespace common;
using namespace storage;

namespace unittest {
static const uint32_t TEST_TOTAL_BLOCK_COUNT = 100;
static const uint32_t TEST_ROWKEY_COLUMN_COUNT = 5;
static const uint32_t TEST_COLUMN_COUNT = ObExtendType - 1;
static const uint32_t TEST_MACRO_BLOCK_SIZE = 2 * 1024 * 1024;

class TestInspectBadBlock : public TestDataFilePrepare {
public:
  TestInspectBadBlock();
  virtual ~TestInspectBadBlock();
  virtual void SetUp();
  virtual void TearDown();

protected:
  void prepare_schema();
  void prepare_data(const int64_t row_cnt, ObSSTable& sstable);
  void get_macro_block(const int32_t macro_idx, ObMacroBlockHandle& macro_block_handle, ObFullMacroBlockMeta& meta);
  void check_macro_block();

protected:
  ObSSTable sstable_;
  ObTableSchema table_schema_;
  ObITable::TableKey table_key_;
  ObRowGenerate row_generate_;
  ObArenaAllocator allocator_;
  const char* compressor_name_;
  bool need_calc_physical_checksum_;
};

TestInspectBadBlock::TestInspectBadBlock()
    : TestDataFilePrepare("TestInspectBadBlock", TEST_MACRO_BLOCK_SIZE, TEST_TOTAL_BLOCK_COUNT),
      allocator_(ObModIds::TEST),
      need_calc_physical_checksum_(false)
{
  compressor_name_ = "none";
}

TestInspectBadBlock::~TestInspectBadBlock()
{}

void TestInspectBadBlock::SetUp()
{
  TestDataFilePrepare::SetUp();
  prepare_schema();
  ASSERT_EQ(OB_SUCCESS, sstable_.init(table_key_));
  prepare_data(50000, sstable_);
  srand(static_cast<uint32_t>(time(NULL)));
}

void TestInspectBadBlock::TearDown()
{
  table_schema_.reset();
  sstable_.destroy();
  row_generate_.reset();
  allocator_.reuse();
  TestDataFilePrepare::TearDown();
}

void TestInspectBadBlock::prepare_schema()
{
  int64_t table_id = combine_id(1, 3002);
  // init table key
  table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key_.pkey_ = ObPartitionKey(table_id, 0, 0);
  table_key_.table_id_ = table_id;
  table_key_.version_ = ObVersion(1, 0);
  table_key_.trans_version_range_.multi_version_start_ = 0;  // ?
  table_key_.trans_version_range_.base_version_ = 0;
  table_key_.trans_version_range_.snapshot_version_ = 20;

  // init schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_inspect_bad_block"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_COUNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_COUNT);
  table_schema_.set_compress_func_name(compressor_name_);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  // init column
  ObColumnSchemaV2 column;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  ObObjMeta meta_type;
  for (int64_t i = 0; i < TEST_COLUMN_COUNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_data_length(1);  //?
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    meta_type.set_type(obj_type);
    column.set_meta_type(meta_type);
    if (ob_is_string_type(obj_type) && obj_type != ObHexStringType) {
      meta_type.set_collation_level(CS_LEVEL_IMPLICIT);
      meta_type.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      column.set_meta_type(meta_type);
    }
    if (obj_type == common::ObVarcharType) {
      column.set_rowkey_position(1);  //?
    } else if (obj_type == common::ObCharType) {
      column.set_rowkey_position(2);
    } else if (obj_type == common::ObDoubleType) {
      column.set_rowkey_position(3);
    } else if (obj_type == common::ObNumberType) {
      column.set_rowkey_position(4);
    } else if (obj_type == common::ObUNumberType) {
      column.set_rowkey_position(5);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void TestInspectBadBlock::prepare_data(const int64_t row_cnt, ObSSTable& sstable)
{
  int ret = OB_SUCCESS;

  ObCreateSSTableParamWithTable param;
  param.table_key_ = table_key_;
  param.logical_data_version_ = table_key_.version_;
  param.schema_ = &table_schema_;
  param.schema_version_ = 10;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;

  ObDataStoreDesc data_desc;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  int64_t data_version = 1;

  ObPGKey pg_key(combine_id(1, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(pg_key, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = data_desc.init(table_schema_,
      data_version,
      NULL,
      1,
      MAJOR_MERGE,
      true,
      true,
      pg_key,
      pg_guard.get_partition_group()->get_storage_file_handle());
  ASSERT_EQ(OB_SUCCESS, ret);
  data_desc.need_calc_physical_checksum_ = need_calc_physical_checksum_;
  param.pg_key_ = data_desc.pg_key_;
  ret = sstable.open(param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = writer.open(data_desc, start_seq);  // differ with table_key_.pkey
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_COUNT];
  row.row_val_.assign(cells, TEST_COLUMN_COUNT);
  uint32_t gen_cnt = 0;
  while (gen_cnt < row_cnt) {
    ret = row_generate_.get_next_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ++gen_cnt;
  }

  // close sstable
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, sstable_.append_macro_blocks(writer.get_macro_block_write_ctx()));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sstable.close();
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestInspectBadBlock::get_macro_block(
    const int32_t macro_idx, ObMacroBlockHandle& macro_block_handle, ObFullMacroBlockMeta& meta)
{
  MacroBlockId macro_id(0, 0, 1, macro_idx);
  ASSERT_EQ(sstable_.get_meta(macro_id, meta), OB_SUCCESS);
  ASSERT_TRUE(meta.is_valid());

  ObMacroBlockReadInfo read_info;
  ObMacroBlockCtx block_ctx;
  block_ctx.sstable_block_id_.macro_block_id_ = macro_id;
  read_info.macro_block_ctx_ = &block_ctx;
  read_info.offset_ = 0;
  read_info.size_ = get_file_system().get_macro_block_size();
  read_info.io_desc_.category_ = SYS_IO;
  read_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_COMPACT_READ;
  ASSERT_EQ(OB_STORE_FILE.read_block(read_info, macro_block_handle), OB_SUCCESS);
}

TEST_F(TestInspectBadBlock, check_macro_block)
{
  ObFileSystemInspectBadBlockTask task;
  ObFullMacroBlockMeta meta;
  MacroBlockId macro_id(0, 0, 1, 2);
  ObMacroBlockInfoPair pair;
  ObTenantFileKey file_key;
  pair.block_id_ = macro_id;
  ASSERT_EQ(sstable_.get_meta(macro_id, meta), OB_SUCCESS);
  ASSERT_TRUE(meta.is_valid());
  pair.meta_ = meta;
  EXPECT_EQ(task.check_macro_block(pair, file_key), OB_SUCCESS);
}

TEST_F(TestInspectBadBlock, ObMacroBlockLoader)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle macro_handle;
  ObFullMacroBlockMeta meta;
  get_macro_block(2, macro_handle, meta);
  ASSERT_TRUE(meta.is_valid());
  ASSERT_NE(macro_handle.get_buffer(), nullptr);
  ASSERT_GT(macro_handle.get_data_size(), 0);

  ObFileSystemInspectBadBlockTask task;
  ObMacroBlockLoader macro_loader;
  ASSERT_EQ(macro_loader.init(macro_handle.get_buffer(), macro_handle.get_data_size(), &meta), OB_SUCCESS);
  int64_t loop_count = 0;
  blocksstable::ObMicroBlock micro_block;
  while (OB_SUCC(macro_loader.next_micro_block(micro_block))) {
    EXPECT_NE(micro_block.data_.get_buf(), nullptr);
    EXPECT_GT(micro_block.data_.get_buf_size(), 0);
    ++loop_count;
  }
  EXPECT_EQ(loop_count, macro_loader.get_micro_block_infos().count());
}

void TestInspectBadBlock::check_macro_block()
{
  ObMacroBlockHandle macro_handle;
  ObFullMacroBlockMeta meta;
  get_macro_block(2, macro_handle, meta);
  ASSERT_TRUE(meta.is_valid());
  ASSERT_NE(macro_handle.get_buffer(), nullptr);
  ASSERT_GT(macro_handle.get_data_size(), 0);

  ObSSTableMacroBlockChecker macro_checker;
  EXPECT_NE(macro_checker.check(NULL, 100, meta), OB_SUCCESS);
  const char* buf = macro_handle.get_buffer();
  const int64_t buf_size = macro_handle.get_data_size();
  EXPECT_NE(macro_checker.check(buf, 0, meta), OB_SUCCESS);
  //  const ObSSTableMacroBlockHeader *sstable_header = reinterpret_cast<const ObSSTableMacroBlockHeader *>(buf +
  //  ObMacroBlockCommonHeader::get_serialize_size()); STORAGE_LOG(WARN, "debug_check_header, ",
  //  K(sizeof(ObSSTableMacroBlockHeader)),
  //      K(sstable_header->data_seq_), K(meta.meta_->data_seq_), K(sstable_header->compressor_name_),
  //      K(meta.schema_->compressor_));
  EXPECT_EQ(macro_checker.check(buf, buf_size, meta), OB_SUCCESS);
  EXPECT_EQ(macro_checker.check(buf, buf_size, meta, CHECK_LEVEL_NOTHING), OB_SUCCESS);
  EXPECT_EQ(macro_checker.check(buf, buf_size, meta, CHECK_LEVEL_MACRO), OB_SUCCESS);
  EXPECT_EQ(macro_checker.check(buf, buf_size, meta, CHECK_LEVEL_ROW), OB_SUCCESS);
  EXPECT_EQ(macro_checker.check(buf, buf_size, meta, CHECK_LEVEL_AUTO), OB_SUCCESS);
  EXPECT_NE(macro_checker.check(buf, buf_size, meta, CHECK_LEVEL_MAX), OB_SUCCESS);
}

class TestMacroBlock_LZ4 : public TestInspectBadBlock {
public:
  TestMacroBlock_LZ4()
  {
    compressor_name_ = "lz4_1.0";
  }
};

class TestMacroBlock_Snappy : public TestInspectBadBlock {
public:
  TestMacroBlock_Snappy()
  {
    compressor_name_ = "snappy_1.0";
  }
};

class TestMacroBlock_Zlib : public TestInspectBadBlock {
public:
  TestMacroBlock_Zlib()
  {
    compressor_name_ = "zlib_1.0";
  }
};

class TestMacroBlock_Zstd : public TestInspectBadBlock {
public:
  TestMacroBlock_Zstd()
  {
    compressor_name_ = "zstd_1.0";
  }
};

class TestMacroBlock_Zstd_1_3_8 : public TestInspectBadBlock {
public:
  TestMacroBlock_Zstd_1_3_8()
  {
    compressor_name_ = "zstd_1.3.8";
  }
};

TEST_F(TestInspectBadBlock, MacroBlockChecker)
{
  check_macro_block();
}

TEST_F(TestMacroBlock_LZ4, MacroBlockChecker)
{
  check_macro_block();
}

TEST_F(TestMacroBlock_Snappy, MacroBlockChecker)
{
  check_macro_block();
}

TEST_F(TestMacroBlock_Zlib, MacroBlockChecker)
{
  check_macro_block();
}

TEST_F(TestMacroBlock_Zstd, MacroBlockChecker)
{
  check_macro_block();
}

TEST_F(TestMacroBlock_Zstd_1_3_8, MacroBlockChecker)
{
  check_macro_block();
}

TEST_F(TestInspectBadBlock, report_bad_block)
{
  int ret = OB_SUCCESS;
  int64_t checksum = 0;
  char error_msg[common::OB_MAX_ERROR_MSG_LEN];
  MEMSET(error_msg, 0, sizeof(error_msg));
  EXPECT_EQ(databuff_printf(error_msg, sizeof(error_msg), "Checksum error of sstable header, checksum=%ld", checksum),
      OB_SUCCESS);
  ret = OB_CHECKSUM_ERROR;
  int32_t bad_block_idx = ObStoreFileSystem::RESERVED_MACRO_BLOCK_INDEX;
  MacroBlockId macro_id(0, 0, 1, bad_block_idx);
  EXPECT_EQ(OB_STORE_FILE.report_bad_block(macro_id, ret, error_msg), OB_SUCCESS);
  common::ObArray<ObBadBlockInfo> bad_block_infos;
  EXPECT_EQ(OB_STORE_FILE.get_bad_block_infos(bad_block_infos), OB_SUCCESS);
  EXPECT_EQ(bad_block_infos.count(), 1);
  ASSERT_TRUE(bad_block_infos.at(0).macro_block_id_ == macro_id);
  EXPECT_EQ(OB_STORE_FILE.is_bad_block(macro_id), true);
}

#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_micro_block_index_cache.h"

TEST(TestIOManager, callback_size)
{
  int64_t micro_block_cb = sizeof(blocksstable::ObMicroBlockCache::ObMicroBlockIOCallback);
  int64_t multi_block_cb = sizeof(blocksstable::ObMicroBlockCache::ObMultiBlockIOCallback);
  int64_t micro_index_cb = sizeof(blocksstable::ObMicroBlockIndexCache::ObMicroBlockIndexIOCallback);
  COMMON_LOG(WARN, "callback size", K(micro_block_cb), K(multi_block_cb), K(micro_index_cb));
}

class TestPhysicalChecksum : public TestInspectBadBlock {
public:
  TestPhysicalChecksum()
  {
    need_calc_physical_checksum_ = true;
  }
};

TEST_F(TestPhysicalChecksum, physical_check)
{
  check_macro_block();
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  UNUSED(argc);
  UNUSED(argv);
  // oceanbase::common::ObLogger::get_logger().set_log_level(OB_LOG_LEVEL_INFO);
  //::testing::InitGoogleTest(&argc, argv);
  return 0;  // RUN_ALL_TESTS();
}
