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

#define USING_LOG_PREFIX STORAGE

#define private public
#define protected public

#include "common/ob_tablet_id.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/schema_utils.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/high_availability/ob_physical_copy_task.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;

void ObCompactionBufferWriter::reset()
{
}

namespace unittest
{
static ObSimpleMemLimitGetter getter;

class TestRootBlockInfo : public TestDataFilePrepare
{
public:
  TestRootBlockInfo();
  virtual ~TestRootBlockInfo() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
private:
  void prepare_tablet_read_info();
  void prepare_block_root();
  static const int64_t BUF_LEN = 10;
  static const int64_t ROWKEY_COL_CNT = 2;
  static constexpr int64_t COLUMN_CNT = 4;
protected:
  ObRootBlockInfo root_info_;
  ObTableReadInfo table_read_info_;
  ObMicroBlockDesMeta des_meta_;
  ObMicroBlockData block_data_;
  ObMetaDiskAddr block_addr_;
  ObArenaAllocator allocator_;
};

TestRootBlockInfo::TestRootBlockInfo()
  : TestDataFilePrepare(&getter, "TestRootBlockInfo", 2 * 1024 * 1024, 2048),
    root_info_(),
    table_read_info_(),
    des_meta_(),
    block_data_(),
    block_addr_(),
    allocator_()
{
}

void TestRootBlockInfo::SetUp()
{
  TestDataFilePrepare::SetUp();
  prepare_tablet_read_info();
  des_meta_.encrypt_id_ = ObCipherOpMode::ob_invalid_mode;
  des_meta_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  ASSERT_TRUE(!root_info_.is_valid());
  root_info_.reset();
  ASSERT_TRUE(!root_info_.is_valid());
  prepare_block_root();
}

void TestRootBlockInfo::TearDown()
{
  root_info_.reset();
  ASSERT_TRUE(!root_info_.is_valid());
  table_read_info_.reset();
  TestDataFilePrepare::TearDown();
}

void TestRootBlockInfo::prepare_tablet_read_info()
{
  const int64_t schema_version = 1;
  common::ObSEArray<share::schema::ObColDesc, COLUMN_CNT> columns;
  for (int i = 0; i < COLUMN_CNT; ++i) {
    share::schema::ObColDesc desc;
    desc.col_id_ = OB_APP_MIN_COLUMN_ID + i;
    desc.col_type_.set_int();
    desc.col_order_ = ObOrderType::ASC;
    ASSERT_EQ(OB_SUCCESS, columns.push_back(desc));
  }
  ASSERT_EQ(OB_SUCCESS, table_read_info_.init(allocator_, schema_version, ROWKEY_COL_CNT, lib::is_oracle_mode(), columns, nullptr/*storage_cols_index*/));
}

void TestRootBlockInfo::prepare_block_root()
{
  const int64_t block_size = 2L * 1024 * 1024L;
  ObMicroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(block_size, ROWKEY_COL_CNT, COLUMN_CNT));
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(allocator_, COLUMN_CNT));
  ObObj obj;
  for (int64_t i = 0; i < COLUMN_CNT; ++i) {
    obj.set_int(OB_APP_MIN_COLUMN_ID + i);
    ASSERT_EQ(OB_SUCCESS, row.storage_datums_[i].from_obj_enhance(obj));
  }
  row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
  row.count_ = COLUMN_CNT;
  ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  ObMicroBlockDesc micro_desc;
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block_desc(micro_desc));

  ObMicroBlockHeader *header = const_cast<ObMicroBlockHeader *>(micro_desc.header_);
  ASSERT_NE(nullptr, header);
  ASSERT_EQ(true, header->is_valid());
  header->data_length_ = micro_desc.buf_size_;
  header->data_zlength_ = micro_desc.buf_size_;
  header->data_checksum_ = ob_crc64_sse42(0, micro_desc.buf_, micro_desc.buf_size_);
  header->original_length_ = micro_desc.buf_size_;
  header->set_header_checksum();

  const int64_t size = header->header_size_ + micro_desc.buf_size_;
  char *buf = static_cast<char *>(allocator_.alloc(size));
  ASSERT_TRUE(nullptr != buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, micro_desc.header_->serialize(buf, size, pos));
  MEMCPY(buf + pos, micro_desc.buf_, micro_desc.buf_size_);

  block_addr_.offset_ = 1000;
  block_addr_.size_ = size;
  block_addr_.type_ = ObMetaDiskAddr::BLOCK;
  block_data_.buf_ = buf;
  block_data_.size_ = size;
  block_data_.type_ = ObMicroBlockData::INDEX_BLOCK;
  ObMacroBlockWriteInfo write_info;
  ObMacroBlockHandle handle;
  const int64_t buf_size = block_size;
  char *io_buf = static_cast<char *>(allocator_.alloc(buf_size));
  ASSERT_TRUE(nullptr != io_buf);
  MEMCPY(io_buf + block_addr_.offset_, buf, block_addr_.size_);
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  write_info.buffer_ = io_buf;
  write_info.size_ = buf_size;
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  ASSERT_EQ(OB_SUCCESS, ObBlockManager::write_block(write_info, handle));
  block_addr_.second_id_ = handle.get_macro_id().second_id();
  ASSERT_EQ(OB_SUCCESS, root_info_.init_root_block_info(allocator_, block_addr_,
      block_data_, static_cast<ObRowStoreType>(header->row_store_type_)));
  ASSERT_TRUE(root_info_.is_valid());
}

class TestSSTableMacroInfo : public TestRootBlockInfo
{
public:
  TestSSTableMacroInfo();
  virtual ~TestSSTableMacroInfo() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
private:
  ObTabletCreateSSTableParam param_;
};

TestSSTableMacroInfo::TestSSTableMacroInfo()
  : TestRootBlockInfo(),
    param_()
{
}

void TestSSTableMacroInfo::SetUp()
{
  TestRootBlockInfo::SetUp();
  param_.data_block_macro_meta_addr_ = block_addr_;
  param_.data_block_macro_meta_ = block_data_;
}

void TestSSTableMacroInfo::TearDown()
{
  TestRootBlockInfo::TearDown();
}

class TestSSTableMeta : public TestDataFilePrepare
{
public:
  TestSSTableMeta();
  virtual ~TestSSTableMeta() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
private:
  void prepare_create_sstable_param();
  ObTabletCreateSSTableParam param_;
  ObTabletID tablet_id_;
  int64_t data_version_;
  int64_t snapshot_version_;
  share::schema::ObTableSchema table_schema_;
  common::ObArenaAllocator allocator_;
};

TestSSTableMeta::TestSSTableMeta()
  : TestDataFilePrepare(&getter, "TestBaseSSTableMeta", 2 * 1024 * 1024, 2048),
    param_(),
    tablet_id_(1),
    data_version_(0),
    snapshot_version_(0),
    table_schema_(),
    allocator_()
{
}

void TestSSTableMeta::SetUp()
{
  TestDataFilePrepare::SetUp();
  TestSchemaUtils::prepare_data_schema(table_schema_);
  prepare_create_sstable_param();
}

void TestSSTableMeta::TearDown()
{
  table_schema_.reset();
  TestDataFilePrepare::TearDown();
}

void TestSSTableMeta::prepare_create_sstable_param()
{
  const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param_.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  param_.table_key_.tablet_id_ = tablet_id_;
  param_.table_key_.version_range_.base_version_ = ObVersionRange::MIN_VERSION;
  param_.table_key_.version_range_.snapshot_version_ = snapshot_version_;
  param_.schema_version_ = table_schema_.get_schema_version();
  param_.create_snapshot_version_ = 0;
  param_.progressive_merge_round_ = table_schema_.get_progressive_merge_round();
  param_.progressive_merge_step_ = 0;
  param_.table_mode_ = table_schema_.get_table_mode_struct();
  param_.index_type_ = table_schema_.get_index_type();
  param_.rowkey_column_cnt_ = table_schema_.get_rowkey_column_num()
          + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  param_.root_block_addr_.set_none_addr();
  param_.data_block_macro_meta_addr_.set_none_addr();
  param_.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param_.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  param_.data_index_tree_height_ = 0;
  param_.index_blocks_cnt_ = 0;
  param_.data_blocks_cnt_ = 0;
  param_.micro_block_cnt_ = 0;
  param_.use_old_macro_block_count_ = 0;
  param_.column_cnt_ = table_schema_.get_column_count() + multi_version_col_cnt;
  param_.data_checksum_ = 0;
  param_.occupy_size_ = 0;
  param_.ddl_scn_.set_min();
  param_.filled_tx_scn_.set_min();
  param_.original_size_ = 0;
  param_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param_.encrypt_id_ = 0;
  param_.master_key_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSTableMergeRes::fill_column_checksum_for_empty_major(param_.column_cnt_, param_.column_checksums_));
}

class TestMigrationSSTableParam : public TestSSTableMeta
{
public:
  TestMigrationSSTableParam();
  virtual ~TestMigrationSSTableParam() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
private:
  storage::ObStorageSchema storage_schema_;
  ObSSTableMeta sstable_meta_;
  storage::ObITable::TableKey table_key_;
};

TestMigrationSSTableParam::TestMigrationSSTableParam()
  : storage_schema_(),
    sstable_meta_(),
    table_key_()
{
}

void TestMigrationSSTableParam::SetUp()
{
  TestSSTableMeta::SetUp();
  ASSERT_EQ(OB_SUCCESS, storage_schema_.init(allocator_, table_schema_, lib::get_compat_mode()));
  ASSERT_TRUE(!sstable_meta_.is_valid());
  sstable_meta_.reset();
  ASSERT_TRUE(!sstable_meta_.is_valid());
  ASSERT_EQ(OB_SUCCESS, sstable_meta_.init(param_, allocator_));
  ASSERT_TRUE(sstable_meta_.is_valid());
  ASSERT_TRUE(sstable_meta_.data_root_info_.is_valid());
  ASSERT_TRUE(sstable_meta_.macro_info_.is_valid());
  ASSERT_TRUE(sstable_meta_.get_col_checksum_cnt() > 0);
  table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  table_key_.tablet_id_ = 1101;
  table_key_.version_range_.base_version_ = 0;
  table_key_.version_range_.snapshot_version_ = 11;
  ASSERT_TRUE(table_key_.is_valid());
}

void TestMigrationSSTableParam::TearDown()
{
  table_key_.reset();
  sstable_meta_.reset();
  storage_schema_.reset();
  TestSSTableMeta::TearDown();
}

TEST_F(TestMigrationSSTableParam, test_check_sstable_meta)
{
  int ret = OB_SUCCESS;
  ObSSTableCopyFinishTask finish_task;
  finish_task.is_inited_ = true;
  ObMigrationSSTableParam mig_param;
  ASSERT_TRUE(!mig_param.is_valid());
  mig_param.reset();
  ASSERT_TRUE(!mig_param.is_valid());
  mig_param.basic_meta_ = sstable_meta_.get_basic_meta();
  for (int64_t i = 0; i < sstable_meta_.get_col_checksum_cnt(); ++i) {
    ASSERT_EQ(OB_SUCCESS, mig_param.column_checksums_.push_back(sstable_meta_.get_col_checksum()[i]));
  }
  mig_param.table_key_ = table_key_;
  ASSERT_TRUE(mig_param.is_valid());
  ASSERT_TRUE(mig_param.basic_meta_.is_valid());
  ASSERT_TRUE(mig_param.column_checksums_.count() > 0);
  ASSERT_TRUE(mig_param.table_key_.is_valid());
  ASSERT_EQ(sstable_meta_.get_basic_meta(), mig_param.basic_meta_);
  ASSERT_EQ(table_key_, mig_param.table_key_);
  ASSERT_EQ(sstable_meta_.get_col_checksum_cnt(), mig_param.column_checksums_.count());
  ret = finish_task.check_sstable_meta_(mig_param, sstable_meta_);
  ASSERT_EQ(OB_SUCCESS, ret);

  mig_param.basic_meta_.row_count_ = sstable_meta_.get_basic_meta().row_count_ + 1;
  ret = finish_task.check_sstable_meta_(mig_param, sstable_meta_);
  ASSERT_EQ(OB_INVALID_DATA, ret);
}


} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_physical_copy_task.log*");
  OB_LOGGER.set_file_name("test_physical_copy_task.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
