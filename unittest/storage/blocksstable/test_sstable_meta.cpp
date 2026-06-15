/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#define USING_LOG_PREFIX STORAGE

#define private public
#define protected public

#include "src/share/io/io_schedule/ob_io_mclock.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/schema_utils.h"
#include "storage/test_tablet_helper.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;
using namespace compaction;
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
  static void SetUpTestCase()
  {
    int ret = ObTimerService::get_instance().start();
    ASSERT_TRUE(OB_SUCCESS == ret || OB_INIT_TWICE == ret);
  }
  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }
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
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  TestDataFilePrepare::SetUp();
  prepare_tablet_read_info();
  des_meta_.encrypt_id_ = ObCipherOpMode::ob_invalid_mode;
  des_meta_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  des_meta_.row_store_type_ = ObRowStoreType::ENCODING_ROW_STORE;
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
  ObMicroBlockWriter<> writer;
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
  ASSERT_EQ(OB_SUCCESS, writer.build_micro_block_desc_in_unittest(micro_desc));

  ObMicroBlockHeader *header = const_cast<ObMicroBlockHeader *>(micro_desc.header_);
  ASSERT_NE(nullptr, header);
  ASSERT_EQ(true, header->is_valid());

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
  ObRowStoreType row_store_type = ObRowStoreType::ENCODING_ROW_STORE;
  ASSERT_EQ(OB_SUCCESS, root_info_.init_root_block_info(allocator_, block_addr_, block_data_, row_store_type));
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
  void prepare_create_sstable_param();
private:
  ObTabletCreateSSTableParam param_;
  ObTabletID tablet_id_;
  int64_t data_version_;
  int64_t snapshot_version_;
  share::schema::ObTableSchema table_schema_;
  common::ObArenaAllocator allocator_;
};

TestSSTableMacroInfo::TestSSTableMacroInfo()
  : TestRootBlockInfo(),
    param_(),
    tablet_id_(1),
    data_version_(0),
    snapshot_version_(0),
    table_schema_(),
    allocator_()
{
}

void TestSSTableMacroInfo::SetUp()
{
  TestRootBlockInfo::SetUp();
  TestSchemaUtils::prepare_data_schema(table_schema_);
  prepare_create_sstable_param();
  param_.data_block_macro_meta_addr_ = block_addr_;
  param_.data_block_macro_meta_ = block_data_;
}

void TestSSTableMacroInfo::TearDown()
{
  TestRootBlockInfo::TearDown();
}

void TestSSTableMacroInfo::prepare_create_sstable_param()
{
  param_.set_init_value_for_column_store_();
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
  param_.latest_row_store_type_ = ObRowStoreType::DUMMY_ROW_STORE;
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
  param_.tx_data_recycle_scn_.set_min();
  param_.original_size_ = 0;
  param_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param_.encrypt_id_ = 0;
  param_.master_key_id_ = 0;
  param_.recycle_version_ = 0;
  param_.root_macro_seq_ = 0;
  param_.row_count_ = 0;
  param_.sstable_logic_seq_ = 0;
  param_.nested_offset_ = 0;
  param_.nested_size_ = 0;
  param_.rec_scn_.set_min();
  ASSERT_EQ(OB_SUCCESS, ObSSTableMergeRes::fill_column_checksum_for_empty_major(param_.column_cnt_, param_.column_checksums_));
}

class TestSSTableMeta : public TestDataFilePrepare
{
public:
  TestSSTableMeta();
  virtual ~TestSSTableMeta() = default;
  virtual void SetUp() override;
  virtual void TearDown() override;
  void construct_sstable(
    const ObTabletID &tablet_id,
    blocksstable::ObSSTable &sstable,
    common::ObArenaAllocator &allocator,
    int64_t data_block_count,
    int64_t other_block_count);
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

void TestSSTableMeta::construct_sstable(
    const ObTabletID &tablet_id,
    blocksstable::ObSSTable &sstable,
    common::ObArenaAllocator &allocator,
    int64_t data_block_count,
    int64_t other_block_count)
{
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);

  ObTabletCreateSSTableParam param;
  TestTabletHelper::prepare_sstable_param(tablet_id, schema, param);
  MacroBlockId block_id(0, 10000, 0);

  for (int64_t i = 0; i < data_block_count; i++) {
    block_id.block_index_ = ObRandom::rand(1, 10<<10);
    ASSERT_EQ(OB_SUCCESS, param.data_block_ids_.push_back(block_id));
  }

  for (int64_t i = 0; i < other_block_count; i++) {
    block_id.block_index_ = ObRandom::rand(1, 10<<10);
    ASSERT_EQ(OB_SUCCESS, param.other_block_ids_.push_back(block_id));
  }

  int ret = sstable.init(param, &allocator);
  ASSERT_EQ(common::OB_SUCCESS, ret);
}

void TestSSTableMeta::prepare_create_sstable_param()
{
  param_.set_init_value_for_column_store_();
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
  param_.tx_data_recycle_scn_.set_min();
  param_.original_size_ = 0;
  param_.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  param_.encrypt_id_ = 0;
  param_.master_key_id_ = 0;
  param_.recycle_version_ = 0;
  param_.root_macro_seq_ = 0;
  param_.row_count_ = 0;
  param_.sstable_logic_seq_ = 0;
  param_.nested_offset_ = 0;
  param_.nested_size_ = 0;
  param_.rec_scn_.set_min();
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
  ASSERT_TRUE(sstable_meta_.uncommit_tx_info_.is_valid());
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

// index row is invalid
TEST_F(TestRootBlockInfo, DISABLED_test_load_and_transform_root_block)
{
  ASSERT_TRUE(root_info_.get_addr().is_block());
  ASSERT_EQ(OB_SUCCESS, root_info_.load_root_block_data(allocator_, des_meta_));
  ASSERT_EQ(OB_SUCCESS, root_info_.transform_root_block_extra_buf(allocator_));
}

TEST_F(TestRootBlockInfo, test_serialize_and_deserialize)
{
  int64_t pos = 0;

  // test block address.
  const int64_t buf_len_1 = root_info_.get_serialize_size();
  char *buf = new char [buf_len_1];
  ASSERT_EQ(OB_SUCCESS, root_info_.serialize(buf, buf_len_1, pos));
  ASSERT_EQ(buf_len_1, pos);
  ASSERT_TRUE(root_info_.is_valid());
  ASSERT_TRUE(root_info_.get_addr().is_block());

  ObRootBlockInfo tmp_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_info.deserialize(allocator_, des_meta_, buf, buf_len_1, pos));
  ASSERT_EQ(buf_len_1, pos);
  ASSERT_TRUE(tmp_info.is_valid());
  ASSERT_EQ(root_info_.get_addr(), tmp_info.get_addr());
  ASSERT_EQ(0, memcmp(tmp_info.block_data_.get_buf(), block_data_.get_buf(), block_data_.get_buf_size()));
  delete [] buf;

  // test memory address.
  root_info_.reset();
  ASSERT_TRUE(!root_info_.is_valid());
  const char data[BUF_LEN] = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'};
  ObMetaDiskAddr addr;
  addr.type_ = ObMetaDiskAddr::MEM;
  addr.size_ = block_data_.size_;
  ObMicroBlockData block_data;
  ASSERT_EQ(OB_SUCCESS, block_data.init_with_prepare_micro_header(block_data_.buf_, block_data_.size_));
  block_data.type_ = ObMicroBlockData::INDEX_BLOCK;
  const ObRowStoreType row_store_type = ObRowStoreType::ENCODING_ROW_STORE;
  ASSERT_EQ(OB_SUCCESS, root_info_.init_root_block_info(allocator_, addr, block_data, row_store_type));
  ASSERT_TRUE(root_info_.is_valid());
  ASSERT_TRUE(root_info_.get_addr().is_memory());

  pos = 0;
  const int64_t buf_len_2 = root_info_.get_serialize_size();
  buf = (char *)allocator_.alloc(buf_len_2);
  ASSERT_EQ(OB_SUCCESS, root_info_.serialize(buf, buf_len_2, pos));
  ASSERT_EQ(buf_len_2, pos);
  ASSERT_TRUE(root_info_.is_valid());

  pos = 0;
  tmp_info.reset();
  ASSERT_EQ(OB_SUCCESS, tmp_info.deserialize(allocator_, des_meta_, buf, buf_len_2, pos));
  ASSERT_EQ(buf_len_2, pos);
  ASSERT_TRUE(tmp_info.is_valid());
  ASSERT_EQ(root_info_.get_addr(), tmp_info.get_addr());
  ASSERT_TRUE(tmp_info.get_addr().is_memory());
  ASSERT_EQ(0, memcmp(tmp_info.block_data_.get_buf(), block_data.buf_, BUF_LEN));
  allocator_.free(buf);
  buf = nullptr;

  // test none address.
  root_info_.reset();
  ASSERT_TRUE(!root_info_.is_valid());
  addr.set_none_addr();
  block_data.type_ = ObMicroBlockData::INDEX_BLOCK;
  ASSERT_EQ(OB_SUCCESS, root_info_.init_root_block_info(allocator_, addr, block_data, row_store_type));
  ASSERT_TRUE(root_info_.is_valid());
  ASSERT_TRUE(root_info_.get_addr().is_none());

  const int64_t buf_len_3= root_info_.get_serialize_size();
  buf = (char *)allocator_.alloc(buf_len_3);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, root_info_.serialize(buf, buf_len_3, pos));
  ASSERT_EQ(buf_len_3, pos);
  ASSERT_TRUE(root_info_.is_valid());
  ASSERT_TRUE(root_info_.get_addr().is_none());

  pos = 0;
  tmp_info.reset();
  ASSERT_EQ(OB_SUCCESS, tmp_info.deserialize(allocator_, des_meta_, buf, buf_len_3, pos));
  ASSERT_EQ(buf_len_3, pos);
  ASSERT_TRUE(tmp_info.is_valid());
  ASSERT_EQ(root_info_.get_addr(), tmp_info.get_addr());
  allocator_.free(buf);
  buf = nullptr;
}

TEST_F(TestSSTableMacroInfo, test_serialize_and_deserialize)
{
  ObSSTableMacroInfo sstable_macro_info;
  ASSERT_TRUE(!sstable_macro_info.is_valid());
  sstable_macro_info.reset();
  ASSERT_TRUE(!sstable_macro_info.is_valid());
  ASSERT_EQ(OB_SUCCESS, sstable_macro_info.init_macro_info(allocator_, param_));
  ASSERT_TRUE(sstable_macro_info.is_valid());
  ASSERT_EQ(block_addr_, sstable_macro_info.get_macro_meta_addr());
  ASSERT_EQ(0, sstable_macro_info.data_block_count_);
  ASSERT_EQ(0, sstable_macro_info.other_block_count_);

  int64_t pos = 0;
  const int64_t buf_len = sstable_macro_info.get_serialize_size();
  char *buf = new char [buf_len];
  ASSERT_EQ(OB_SUCCESS, sstable_macro_info.serialize(buf, buf_len, pos));
  ASSERT_TRUE(sstable_macro_info.is_valid());

  ObSSTableMacroInfo tmp_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_info.deserialize(allocator_, des_meta_, buf, buf_len, pos));
  ASSERT_TRUE(tmp_info.is_valid());
  ASSERT_EQ(block_addr_, tmp_info.get_macro_meta_addr());
  ASSERT_EQ(0, tmp_info.data_block_count_);
  ASSERT_EQ(0, tmp_info.other_block_count_);
  ASSERT_EQ(sstable_macro_info.macro_meta_info_.addr_, tmp_info.macro_meta_info_.addr_);
  ASSERT_EQ(sstable_macro_info.data_block_count_, tmp_info.data_block_count_);
  ASSERT_EQ(sstable_macro_info.other_block_count_, tmp_info.other_block_count_);
}

TEST_F(TestSSTableMacroInfo, test_huge_block_ids)
{
  ObSSTableMacroInfo sstable_macro_info;
  MacroBlockId block_id(0, 10000, 0);
  ASSERT_EQ(OB_SUCCESS, param_.other_block_ids_.push_back(block_id));
  const int64_t block_cnt_threshold = blocksstable::ObSSTableMacroInfo::BLOCK_CNT_THRESHOLD;
  for (int64_t i = 0; i < block_cnt_threshold; i++) {
    block_id.block_index_ = ObRandom::rand(1, 10<<10);
    ASSERT_EQ(OB_SUCCESS, param_.data_block_ids_.push_back(block_id));
  }
  ASSERT_EQ(OB_SUCCESS, sstable_macro_info.init_macro_info(allocator_, param_));
  int64_t pos = 0;
  const int64_t buf_len = sstable_macro_info.get_serialize_size();
  char *buf = new char[buf_len];
  ASSERT_NE(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(0, sstable_macro_info.linked_block_count_);

  ObTabletID tablet_id(200001); // fake
  int64_t macro_start_seq = 1000;
  ObLinkedMacroInfoWriteParam write_param;
  write_param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
  write_param.tablet_id_ = tablet_id;
  write_param.tablet_private_transfer_epoch_ = 0;
  write_param.start_macro_seq_ = macro_start_seq;
  ObSharedObjectsWriteCtx linked_block_write_ctx;
  ObSArray<ObSharedObjectsWriteCtx> total_ctxs;
  ASSERT_EQ(OB_SUCCESS, sstable_macro_info.persist_block_ids(allocator_,
                                                             write_param,
                                                             macro_start_seq,
                                                             linked_block_write_ctx));
  ASSERT_EQ(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(1, sstable_macro_info.linked_block_count_);
  ASSERT_EQ(OB_SUCCESS, sstable_macro_info.serialize(buf, buf_len, pos));
  ASSERT_EQ(1, linked_block_write_ctx.block_ids_.count());
  ASSERT_EQ(OB_SUCCESS, total_ctxs.push_back(linked_block_write_ctx));

  ObSSTableMacroInfo tmp_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_info.deserialize(allocator_, des_meta_, buf, buf_len, pos));
  ObMacroIdIterator iter;
  ASSERT_EQ(OB_SUCCESS, tmp_info.get_data_block_iter(iter));
  for (int i = 0; i < blocksstable::ObSSTableMacroInfo::BLOCK_CNT_THRESHOLD; i++) {
    MacroBlockId macro_id;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_macro_id(macro_id));
    ASSERT_EQ(param_.data_block_ids_.at(i), macro_id);
  }
  int64_t data_blk_cnt0 = -1, data_blk_cnt1 = -1;
  int64_t other_blk_cnt0 = -1, other_blk_cnt1 = -1;
  int64_t linked_blk_cnt0 = -1, linked_blk_cnt1 = -1;
  ASSERT_EQ(OB_SUCCESS, sstable_macro_info.get_block_count(data_blk_cnt0, other_blk_cnt0, linked_blk_cnt0));
  ASSERT_EQ(OB_SUCCESS, tmp_info.get_block_count(data_blk_cnt1, other_blk_cnt1, linked_blk_cnt1));
  ASSERT_EQ(data_blk_cnt0, data_blk_cnt1);
  ASSERT_EQ(other_blk_cnt0, other_blk_cnt1);
  ASSERT_EQ(linked_blk_cnt0, linked_blk_cnt1);
  ASSERT_EQ(sstable_macro_info.entry_id_, tmp_info.entry_id_);

  ObSSTableMacroInfo deep_copy_info;
  const int64_t variable_size = sstable_macro_info.get_variable_size();
  char *d_buf = new char [variable_size];
  pos = 0;
  sstable_macro_info.deep_copy(d_buf, variable_size, pos, deep_copy_info);
  ASSERT_EQ(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold, sstable_macro_info.data_block_count_);
  int64_t other_blk_cnt = -1, linked_blk_cnt = -1;
  ASSERT_EQ(OB_SUCCESS, sstable_macro_info.get_other_block_count(other_blk_cnt));
  ASSERT_EQ(OB_SUCCESS, sstable_macro_info.get_linked_block_count(linked_blk_cnt));
  ASSERT_EQ(1, other_blk_cnt);
  ASSERT_EQ(1, linked_blk_cnt);
  iter.reset();
  ASSERT_EQ(OB_SUCCESS, deep_copy_info.get_data_block_iter(iter));
  for (int i = 0; i < blocksstable::ObSSTableMacroInfo::BLOCK_CNT_THRESHOLD; i++) {
    MacroBlockId macro_id;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_macro_id(macro_id));
    ASSERT_EQ(param_.data_block_ids_.at(i), macro_id);
  }
  {
    int64_t data_blk_cnt0 = -1, data_blk_cnt1 = -1;
    int64_t other_blk_cnt0 = -1, other_blk_cnt1 = -1;
    int64_t linked_blk_cnt0 = -1, linked_blk_cnt1 = -1;
    ASSERT_EQ(OB_SUCCESS, sstable_macro_info.get_block_count(data_blk_cnt0, other_blk_cnt0, linked_blk_cnt0));
    ASSERT_EQ(OB_SUCCESS, deep_copy_info.get_block_count(data_blk_cnt1, other_blk_cnt1, linked_blk_cnt1));
    ASSERT_EQ(data_blk_cnt0, data_blk_cnt1);
    ASSERT_EQ(other_blk_cnt0, other_blk_cnt1);
    ASSERT_EQ(linked_blk_cnt0, linked_blk_cnt1);
  }
}

TEST_F(TestSSTableMeta, test_common_sstable_persister_linked_block)
{
  ObTabletID tablet_id(99999);
  blocksstable::ObSSTable sstable;
  ObSSTableMetaHandle meta_handle;
  ObSArray<ObSharedObjectsWriteCtx> total_write_ctxs;
  ObSharedObjectsWriteCtx linked_block_write_ctx;
  int64_t macro_start_seq = 100;
  int64_t snapshot_version = 0;
  int64_t block_cnt_threshold = blocksstable::ObSSTableMacroInfo::BLOCK_CNT_THRESHOLD;
  construct_sstable(tablet_id, sstable, allocator_,
      block_cnt_threshold - 5000 /*data_block_count*/,
      1 /*other_block_count*/);
  ASSERT_EQ(OB_SUCCESS, sstable.get_meta(meta_handle));
  const ObSSTableMacroInfo &sstable_macro_info = meta_handle.get_sstable_meta().get_macro_info();
  ASSERT_NE(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold - 5000, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(0, sstable_macro_info.linked_block_count_);

  ObLinkedMacroInfoWriteParam write_param;
  write_param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
  write_param.tablet_id_ = tablet_id;
  write_param.tablet_private_transfer_epoch_ = 0;
  ASSERT_EQ(OB_SUCCESS, sstable.persist_linked_block_if_need(
                                  allocator_,
                                  write_param,
                                  macro_start_seq,
                                  linked_block_write_ctx));
  ASSERT_NE(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold - 5000, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(0, sstable_macro_info.linked_block_count_);
  total_write_ctxs.push_back(linked_block_write_ctx);

  // 幂等
  ASSERT_EQ(OB_SUCCESS, sstable.persist_linked_block_if_need(
                                  allocator_,
                                  write_param,
                                  macro_start_seq,
                                  linked_block_write_ctx));
  ASSERT_NE(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold - 5000, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(0, sstable_macro_info.linked_block_count_);
  total_write_ctxs.push_back(linked_block_write_ctx);

  const uint64_t data_version = DATA_CURRENT_VERSION;
  const int64_t size = sstable.get_serialize_size(data_version);
  char *full_buf = static_cast<char *>(allocator_.alloc(size));
  int64_t pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, sstable.serialize(data_version, full_buf, size, pos));
  blocksstable::ObSSTable tmp_sstable;
  pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, tmp_sstable.deserialize(allocator_, full_buf, size, pos));
  ASSERT_EQ(OB_SUCCESS, tmp_sstable.get_meta(meta_handle));
  const ObSSTableMacroInfo &tmp_sstable_macro_info = meta_handle.get_sstable_meta().get_macro_info();
  ASSERT_NE(nullptr, tmp_sstable_macro_info.data_block_ids_);
  ASSERT_NE(nullptr, tmp_sstable_macro_info.other_block_ids_);
  ASSERT_EQ(nullptr, tmp_sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold - 5000, tmp_sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, tmp_sstable_macro_info.other_block_count_);
  ASSERT_EQ(0, tmp_sstable_macro_info.linked_block_count_);

  ASSERT_EQ(OB_SUCCESS, tmp_sstable.persist_linked_block_if_need(
                                  allocator_,
                                  write_param,
                                  macro_start_seq,
                                  linked_block_write_ctx));
  ASSERT_NE(nullptr, tmp_sstable_macro_info.data_block_ids_);
  ASSERT_NE(nullptr, tmp_sstable_macro_info.other_block_ids_);
  ASSERT_EQ(nullptr, tmp_sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold - 5000, tmp_sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, tmp_sstable_macro_info.other_block_count_);
  ASSERT_EQ(0, tmp_sstable_macro_info.linked_block_count_);

}

TEST_F(TestSSTableMeta, test_huge_sstable_persister_linked_block)
{
  ObTabletID tablet_id(99999);
  blocksstable::ObSSTable sstable;
  ObSSTableMetaHandle meta_handle;
  ObSArray<ObSharedObjectsWriteCtx> total_write_ctxs;
  ObSharedObjectsWriteCtx linked_block_write_ctx;
  int64_t macro_start_seq = 100;
  int64_t snapshot_version = 0;
  int64_t block_cnt_threshold = blocksstable::ObSSTableMacroInfo::BLOCK_CNT_THRESHOLD;
  construct_sstable(tablet_id, sstable, allocator_,
      block_cnt_threshold /*data_block_count*/,
      1 /*other_block_count*/);
  ASSERT_EQ(OB_SUCCESS, sstable.get_meta(meta_handle));
  const ObSSTableMacroInfo &sstable_macro_info = meta_handle.get_sstable_meta().get_macro_info();
  ASSERT_NE(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(0, sstable_macro_info.linked_block_count_);

  ObLinkedMacroInfoWriteParam write_param;
  write_param.type_ = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
  write_param.tablet_id_ = tablet_id;
  write_param.tablet_private_transfer_epoch_ = 0;
  write_param.start_macro_seq_ = macro_start_seq;
  ASSERT_EQ(OB_SUCCESS, sstable.persist_linked_block_if_need(
                                  allocator_,
                                  write_param,
                                  macro_start_seq,
                                  linked_block_write_ctx));
  ASSERT_EQ(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(1, sstable_macro_info.linked_block_count_);
  total_write_ctxs.push_back(linked_block_write_ctx);

  // 幂等
  ASSERT_EQ(OB_SUCCESS, sstable.persist_linked_block_if_need(
                                  allocator_,
                                  write_param,
                                  macro_start_seq,
                                  linked_block_write_ctx));
  ASSERT_EQ(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(1, sstable_macro_info.linked_block_count_);
  total_write_ctxs.push_back(linked_block_write_ctx);

  const uint64_t data_version = DATA_CURRENT_VERSION;
  const int64_t size = sstable.get_serialize_size(data_version);
  char *full_buf = static_cast<char *>(allocator_.alloc(size));
  int64_t pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, sstable.serialize(data_version, full_buf, size, pos));
  blocksstable::ObSSTable tmp_sstable;
  pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, tmp_sstable.deserialize(allocator_, full_buf, size, pos));
  ASSERT_EQ(OB_SUCCESS, tmp_sstable.get_meta(meta_handle));
  const ObSSTableMacroInfo &tmp_sstable_macro_info = meta_handle.get_sstable_meta().get_macro_info();
  ASSERT_EQ(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(1, sstable_macro_info.linked_block_count_);

  ASSERT_EQ(OB_SUCCESS, tmp_sstable.persist_linked_block_if_need(
                                  allocator_,
                                  write_param,
                                  macro_start_seq,
                                  linked_block_write_ctx));
  ASSERT_EQ(nullptr, sstable_macro_info.data_block_ids_);
  ASSERT_EQ(nullptr, sstable_macro_info.other_block_ids_);
  ASSERT_NE(nullptr, sstable_macro_info.linked_block_ids_);
  ASSERT_EQ(block_cnt_threshold, sstable_macro_info.data_block_count_);
  ASSERT_EQ(1, sstable_macro_info.other_block_count_);
  ASSERT_EQ(1, sstable_macro_info.linked_block_count_);
}

TEST_F(TestSSTableMeta, test_empty_sstable_serialize_and_deserialize)
{
  ObSSTableMeta sstable_meta;
  ASSERT_TRUE(!sstable_meta.is_valid());
  sstable_meta.reset();
  ASSERT_TRUE(!sstable_meta.is_valid());
  ASSERT_EQ(OB_SUCCESS, sstable_meta.init(param_, allocator_));
  ASSERT_TRUE(sstable_meta.is_valid());
  ASSERT_TRUE(sstable_meta.data_root_info_.is_valid());
  ASSERT_TRUE(sstable_meta.macro_info_.is_valid());
  ASSERT_TRUE(sstable_meta.get_col_checksum_cnt() > 0);

  int64_t pos = 0;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  const int64_t buf_len = sstable_meta.get_serialize_size(data_version);
  char *buf = new char [buf_len];
  ASSERT_EQ(OB_SUCCESS, sstable_meta.serialize(data_version, buf, buf_len, pos));
  ASSERT_TRUE(sstable_meta.is_valid());
  ASSERT_TRUE(sstable_meta.data_root_info_.is_valid());
  ASSERT_TRUE(sstable_meta.macro_info_.is_valid());
  ASSERT_TRUE(sstable_meta.get_col_checksum_cnt() > 0);

  ObSSTableMeta tmp_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_meta.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_TRUE(tmp_meta.is_valid());
  ASSERT_TRUE(tmp_meta.data_root_info_.is_valid());
  ASSERT_TRUE(tmp_meta.macro_info_.is_valid());
  ASSERT_TRUE(tmp_meta.get_col_checksum_cnt() > 0);
  ASSERT_EQ(sstable_meta.basic_meta_, tmp_meta.basic_meta_);
  ASSERT_EQ(sstable_meta.get_col_checksum_cnt(), tmp_meta.get_col_checksum_cnt());
  ASSERT_EQ(sstable_meta.data_root_info_.addr_, tmp_meta.data_root_info_.addr_);
  ASSERT_EQ(sstable_meta.macro_info_.data_block_count_, tmp_meta.macro_info_.data_block_count_);
  ASSERT_EQ(sstable_meta.macro_info_.other_block_count_, tmp_meta.macro_info_.other_block_count_);
  free(buf);
}

TEST_F(TestSSTableMeta, test_cosstable_illegal_serialize)
{
  ObTabletID tablet_id(99999);
  blocksstable::ObSSTable sstable;
  construct_sstable(tablet_id, sstable, allocator_,
      1 /*data_block_count*/,
      1 /*other_block_count*/);
  sstable.key_.table_type_ = ObITable::COLUMN_ORIENTED_SSTABLE;
  const int64_t buf_len = sstable.get_serialize_size(DATA_CURRENT_VERSION);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_TRUE(nullptr != buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, sstable.serialize(DATA_CURRENT_VERSION, buf, buf_len, pos));
  ASSERT_EQ(OB_ERR_UNEXPECTED, sstable.serialize_full_table(DATA_CURRENT_VERSION, buf, buf_len, pos));
  allocator_.reset();
}

TEST_F(TestSSTableMeta, test_sstable_deep_copy)
{
  ObSSTable full_sstable;
  ASSERT_EQ(OB_SUCCESS, full_sstable.init(param_, &allocator_));
  const int64_t buf_len = full_sstable.get_deep_copy_size();
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_TRUE(nullptr != buf);
  ObIStorageMetaObj *value = nullptr;
  ASSERT_EQ(OB_SUCCESS, full_sstable.deep_copy(buf, buf_len, value));
  ObSSTable *tiny_sstable = static_cast<ObSSTable*>(value);
  ASSERT_EQ(full_sstable.key_, tiny_sstable->key_);
  ASSERT_EQ(full_sstable.valid_for_reading_, tiny_sstable->valid_for_reading_);
  ASSERT_EQ(true, full_sstable.is_tmp_sstable_);
  ASSERT_EQ(false, tiny_sstable->is_tmp_sstable_);
  ASSERT_EQ(full_sstable.meta_->basic_meta_, tiny_sstable->meta_->basic_meta_);
  ASSERT_EQ(full_sstable.meta_->data_root_info_.addr_, tiny_sstable->meta_->data_root_info_.addr_);
  ASSERT_EQ(full_sstable.meta_->data_root_info_.block_data_.type_, tiny_sstable->meta_->data_root_info_.block_data_.type_);
  ASSERT_EQ(full_sstable.meta_->macro_info_.macro_meta_info_.block_data_.type_, tiny_sstable->meta_->macro_info_.macro_meta_info_.block_data_.type_);
  ASSERT_EQ(full_sstable.meta_->macro_info_.macro_meta_info_.addr_, tiny_sstable->meta_->macro_info_.macro_meta_info_.addr_);
  ASSERT_EQ(full_sstable.meta_->macro_info_.data_block_count_, tiny_sstable->meta_->macro_info_.data_block_count_);
  ASSERT_EQ(full_sstable.meta_->macro_info_.other_block_count_, tiny_sstable->meta_->macro_info_.other_block_count_);
  ASSERT_EQ(full_sstable.meta_->macro_info_.linked_block_count_, tiny_sstable->meta_->macro_info_.linked_block_count_);
  ASSERT_EQ(full_sstable.meta_->macro_info_.entry_id_, tiny_sstable->meta_->macro_info_.entry_id_);
  ASSERT_EQ(full_sstable.meta_->get_col_checksum_cnt(), tiny_sstable->meta_->get_col_checksum_cnt());
  ASSERT_EQ(0, memcmp(full_sstable.meta_->get_col_checksum(), tiny_sstable->meta_->get_col_checksum(), full_sstable.meta_->get_col_checksum_cnt()));
  ASSERT_EQ(full_sstable.meta_->is_inited_, tiny_sstable->meta_->is_inited_);
}

TEST_F(TestSSTableMeta, test_sstable_meta_deep_copy)
{
  int ret = OB_SUCCESS;
  const int64_t buf_size = 8 << 10; //8K
  char *base_buf = (char*)ob_malloc(buf_size, ObMemAttr());
  MEMSET(base_buf, 0, buf_size);
  ObSSTableMeta *meta_ptr = new (base_buf)ObSSTableMeta();
  ObSSTableMeta &src_meta = *meta_ptr;
  // add salt
  src_meta.basic_meta_.data_checksum_ = 20240514;

  ObArenaAllocator arena_allocator;
  src_meta.column_ckm_struct_.reserve(arena_allocator, 3);
  src_meta.column_ckm_struct_.column_checksums_[0] = 1111;
  src_meta.column_ckm_struct_.column_checksums_[1] = 2222;
  src_meta.column_ckm_struct_.column_checksums_[2] = 3333;

  src_meta.tx_ctx_.tx_descs_ = (ObTxContext::ObTxDesc*)ob_malloc_align(4<<10, 2 * sizeof(ObTxContext::ObTxDesc), ObMemAttr());
  ret = src_meta.tx_ctx_.push_back({987, 654});
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = src_meta.tx_ctx_.push_back({123, 456});
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, src_meta.tx_ctx_.count_);
  ASSERT_EQ(2 * sizeof(ObTxContext::ObTxDesc), src_meta.tx_ctx_.get_variable_size());
  src_meta.tx_ctx_.len_ = src_meta.tx_ctx_.get_serialize_size();

  src_meta.uncommit_tx_info_.tx_infos_ = (ObUncommitTxDesc *)ob_malloc_align(4<<10, 2 * sizeof(ObUncommitTxDesc), ObMemAttr());
  ret = src_meta.uncommit_tx_info_.push_back(ObUncommitTxDesc(987, 654));
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = src_meta.uncommit_tx_info_.push_back(ObUncommitTxDesc(123, 456));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, src_meta.uncommit_tx_info_.uncommit_tx_desc_count_);
  ASSERT_EQ(2 * sizeof(ObUncommitTxDesc), src_meta.uncommit_tx_info_.get_deep_copy_size());
  // test deep copy from dynamic memory meta to flat memory meta
  int64_t pos = 0;
  char *flat_buf_1 = (char*)ob_malloc(buf_size, ObMemAttr());
  MEMSET(flat_buf_1, 0, buf_size);
  int64_t deep_copy_size = src_meta.get_deep_copy_size();
  ObSSTableMeta *flat_meta_1;
  ret = src_meta.deep_copy(flat_buf_1, deep_copy_size, pos, flat_meta_1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(deep_copy_size, pos);
  OB_LOG(INFO, "cooper", K(src_meta), K(sizeof(ObSSTableMeta)), K(deep_copy_size));
  OB_LOG(INFO, "cooper", K(*flat_meta_1));
  ASSERT_EQ(src_meta.basic_meta_, flat_meta_1->basic_meta_);
  OB_LOG(INFO, "kyle", K(src_meta.macro_info_), K(flat_meta_1->macro_info_), K(sizeof(src_meta.macro_info_)), K(sizeof(flat_meta_1->macro_info_)));
  ASSERT_EQ(0, MEMCMP((char*)&src_meta.data_root_info_, (char*)&flat_meta_1->data_root_info_, sizeof(src_meta.data_root_info_)));
  ASSERT_EQ(0, MEMCMP((char*)&src_meta.macro_info_, (char*)&flat_meta_1->macro_info_, sizeof(src_meta.macro_info_)));
  // ASSERT_EQ(0, MEMCMP((char*)&src_meta.cg_sstables_, (char*)&flat_meta_1->cg_sstables_, sizeof(src_meta.cg_sstables_)));
  ASSERT_EQ(0, MEMCMP(src_meta.column_ckm_struct_.column_checksums_,
                      flat_meta_1->column_ckm_struct_.column_checksums_,
                      src_meta.column_ckm_struct_.count_ * sizeof(int64_t)));
  ASSERT_EQ(src_meta.tx_ctx_.len_, flat_meta_1->tx_ctx_.len_);
  ASSERT_EQ(src_meta.tx_ctx_.count_, flat_meta_1->tx_ctx_.count_);
  ASSERT_EQ(0, MEMCMP(src_meta.tx_ctx_.tx_descs_, flat_meta_1->tx_ctx_.tx_descs_, flat_meta_1->tx_ctx_.get_variable_size()));
  ASSERT_EQ(0, MEMCMP(src_meta.uncommit_tx_info_.tx_infos_, flat_meta_1->uncommit_tx_info_.tx_infos_, flat_meta_1->uncommit_tx_info_.get_deep_copy_size()));
  // test deep copy from flat memory meta to flat memory meta
  pos = 0;
  char *flat_buf_2 = (char*)ob_malloc_align(4<<10, buf_size, ObMemAttr());
  MEMSET(flat_buf_2, 0, buf_size);
  deep_copy_size = flat_meta_1->get_deep_copy_size();
  ObSSTableMeta *flat_meta_2;
  ret = flat_meta_1->deep_copy(flat_buf_2, deep_copy_size, pos, flat_meta_2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(deep_copy_size, pos);
  OB_LOG(INFO, "cooper", K(*flat_meta_1), K(deep_copy_size));
  OB_LOG(INFO, "cooper", K(*flat_meta_2));
  OB_LOG(INFO, "feidu, data_root_info_1", K(sizeof(flat_meta_1->data_root_info_)));
  hex_dump(&flat_meta_1->data_root_info_, sizeof(flat_meta_1->data_root_info_), true, OB_LOG_LEVEL_WARN);
  OB_LOG(INFO, "feidu, data_root_info_2", K(sizeof(flat_meta_2->data_root_info_)));
  hex_dump(&flat_meta_2->data_root_info_, sizeof(flat_meta_2->data_root_info_), true, OB_LOG_LEVEL_WARN);
  OB_LOG(INFO, "feidu, macro_info_1", K(sizeof(flat_meta_1->macro_info_)));
  hex_dump(&flat_meta_1->macro_info_, sizeof(flat_meta_1->macro_info_), true, OB_LOG_LEVEL_WARN);
  OB_LOG(INFO, "feidu, macro_info_2", K(sizeof(flat_meta_2->macro_info_)));
  hex_dump(&flat_meta_2->macro_info_, sizeof(flat_meta_2->macro_info_), true, OB_LOG_LEVEL_WARN);
  OB_LOG(INFO, "feidu, basic_meta_1", K(sizeof(flat_meta_1->basic_meta_)));
  hex_dump(&flat_meta_1->basic_meta_, sizeof(flat_meta_1->basic_meta_), true, OB_LOG_LEVEL_WARN);
  OB_LOG(INFO, "feidu, basic_meta_2", K(sizeof(flat_meta_2->basic_meta_)));
  hex_dump(&flat_meta_2->basic_meta_, sizeof(flat_meta_2->basic_meta_), true, OB_LOG_LEVEL_WARN);
  ASSERT_EQ(0, MEMCMP((char*)&flat_meta_1->basic_meta_, (char*)&flat_meta_2->basic_meta_, sizeof(flat_meta_1->basic_meta_)));
  ASSERT_EQ(0, MEMCMP(&flat_meta_1->data_root_info_, &flat_meta_2->data_root_info_, sizeof(flat_meta_1->data_root_info_)));
  ASSERT_EQ(0, MEMCMP(&flat_meta_1->macro_info_, &flat_meta_2->macro_info_, sizeof(flat_meta_1->macro_info_)));
  ASSERT_EQ(0, MEMCMP((char*)&flat_meta_1->cg_sstables_, (char*)&flat_meta_2->cg_sstables_, sizeof(flat_meta_1->cg_sstables_)));
  ASSERT_EQ(0, MEMCMP(flat_meta_1->column_ckm_struct_.column_checksums_,
                      flat_meta_2->column_ckm_struct_.column_checksums_,
                      flat_meta_1->column_ckm_struct_.count_ * sizeof(int64_t)));
  ASSERT_EQ(flat_meta_2->tx_ctx_.len_, flat_meta_1->tx_ctx_.len_);
  ASSERT_EQ(flat_meta_2->tx_ctx_.count_, flat_meta_1->tx_ctx_.count_);
  ASSERT_NE(flat_meta_1->tx_ctx_.tx_descs_, flat_meta_2->tx_ctx_.tx_descs_);
  ASSERT_EQ(0, MEMCMP(flat_meta_2->tx_ctx_.tx_descs_, flat_meta_1->tx_ctx_.tx_descs_, flat_meta_1->tx_ctx_.get_variable_size()));
}

TEST_F(TestMigrationSSTableParam, test_empty_sstable_serialize_and_deserialize)
{
  ObMigrationSSTableParam mig_param;
  ASSERT_TRUE(!mig_param.is_valid());
  mig_param.reset();
  ASSERT_TRUE(!mig_param.is_valid());
  mig_param.basic_meta_ = sstable_meta_.get_basic_meta();
  for (int64_t i = 0; i < sstable_meta_.get_col_checksum_cnt(); ++i) {
    ASSERT_EQ(OB_SUCCESS, mig_param.column_checksums_.push_back(sstable_meta_.get_col_checksum()[i]));
  }
  mig_param.table_key_ = table_key_;
  char str[15] ="test serialize";
  mig_param.root_block_addr_.set_mem_addr(0, 15);
  mig_param.root_block_buf_ = str;
  MacroBlockId block_id(4096,0,0);
  mig_param.data_block_macro_meta_addr_.set_block_addr(block_id, 1024, 2048, ObMetaDiskAddr::DiskType::BLOCK);
  ASSERT_EQ(nullptr, mig_param.data_block_macro_meta_buf_);
  mig_param.is_meta_root_ = true;

  ASSERT_TRUE(mig_param.is_valid());
  ASSERT_TRUE(mig_param.basic_meta_.is_valid());
  ASSERT_TRUE(mig_param.column_checksums_.count() > 0);
  ASSERT_TRUE(mig_param.table_key_.is_valid());
  ASSERT_EQ(sstable_meta_.get_basic_meta(), mig_param.basic_meta_);
  ASSERT_EQ(table_key_, mig_param.table_key_);
  ASSERT_EQ(sstable_meta_.get_col_checksum_cnt(), mig_param.column_checksums_.count());

  int64_t pos = 0;
  const int64_t buf_len = mig_param.get_serialize_size();
  char *buf = new char [buf_len];
  ASSERT_EQ(OB_SUCCESS, mig_param.serialize(buf, buf_len, pos));
  ASSERT_TRUE(mig_param.is_valid());
  ASSERT_TRUE(mig_param.basic_meta_.is_valid());
  ASSERT_TRUE(mig_param.column_checksums_.count() > 0);
  ASSERT_TRUE(mig_param.table_key_.is_valid());

  ObMigrationSSTableParam tmp_param;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_param.deserialize(buf, buf_len, pos));
  ASSERT_TRUE(tmp_param.is_valid());
  ASSERT_TRUE(tmp_param.basic_meta_.is_valid());
  ASSERT_TRUE(tmp_param.column_checksums_.count() > 0);
  ASSERT_TRUE(tmp_param.table_key_.is_valid());
  ASSERT_EQ(tmp_param.basic_meta_, mig_param.basic_meta_);
  ASSERT_EQ(tmp_param.table_key_, mig_param.table_key_);
  ASSERT_EQ(tmp_param.column_checksums_.count(), mig_param.column_checksums_.count());
  ASSERT_EQ(tmp_param.root_block_addr_, mig_param.root_block_addr_);
  ASSERT_EQ(0, strcmp(tmp_param.root_block_buf_, mig_param.root_block_buf_));
  ASSERT_EQ(tmp_param.data_block_macro_meta_addr_, mig_param.data_block_macro_meta_addr_);
  ASSERT_EQ(nullptr, tmp_param.data_block_macro_meta_buf_);
  ASSERT_EQ(tmp_param.is_meta_root_, mig_param.is_meta_root_);
}

TEST_F(TestMigrationSSTableParam, test_migrate_sstable)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObTabletCreateSSTableParam src_sstable_param;
  src_sstable_param.set_init_value_for_column_store_();
  src_sstable_param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  src_sstable_param.table_key_.tablet_id_ = tablet_id_;
  src_sstable_param.table_key_.version_range_.base_version_ = ObVersionRange::MIN_VERSION;
  src_sstable_param.table_key_.version_range_.snapshot_version_ = snapshot_version_;
  src_sstable_param.schema_version_ = 3;
  src_sstable_param.create_snapshot_version_ = 0;
  src_sstable_param.progressive_merge_round_ = 0;
  src_sstable_param.progressive_merge_step_ = 0;
  src_sstable_param.table_mode_ = table_schema_.get_table_mode_struct();
  src_sstable_param.index_type_ = table_schema_.get_index_type();
  src_sstable_param.rowkey_column_cnt_ = 1;
  src_sstable_param.root_block_addr_.set_none_addr();
  src_sstable_param.data_block_macro_meta_addr_.set_none_addr();
  src_sstable_param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  src_sstable_param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  src_sstable_param.data_index_tree_height_ = 0;
  src_sstable_param.index_blocks_cnt_ = 0;
  src_sstable_param.data_blocks_cnt_ = 0;
  src_sstable_param.micro_block_cnt_ = 0;
  src_sstable_param.use_old_macro_block_count_ = 0;
  src_sstable_param.column_cnt_ = 1;
  src_sstable_param.data_checksum_ = 0;
  src_sstable_param.occupy_size_ = 0;
  src_sstable_param.ddl_scn_.set_min();
  src_sstable_param.filled_tx_scn_.set_min();
  src_sstable_param.tx_data_recycle_scn_.set_min();
  src_sstable_param.original_size_ = 0;
  src_sstable_param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
  src_sstable_param.encrypt_id_ = 1234;
  src_sstable_param.master_key_id_ = 5678;
  src_sstable_param.table_shared_flag_.set_shared_sstable();
  src_sstable_param.recycle_version_ = 0;
  src_sstable_param.root_macro_seq_ = 0;
  src_sstable_param.row_count_ = 0;
  src_sstable_param.sstable_logic_seq_ = 0;
  src_sstable_param.nested_offset_ = 0;
  src_sstable_param.nested_size_ = 0;
  src_sstable_param.rec_scn_.set_min();
  ret = src_sstable_param.column_checksums_.push_back(2022);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSTableMeta src_meta;
  ret = src_meta.init(src_sstable_param, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObMigrationSSTableParam mig_param;
  mig_param.basic_meta_ = src_meta.get_basic_meta();
  mig_param.table_key_ = src_sstable_param.table_key_;
  for (int64_t i = 0; i < sstable_meta_.get_col_checksum_cnt(); ++i) {
    ASSERT_EQ(OB_SUCCESS, mig_param.column_checksums_.push_back(src_meta.get_col_checksum()[i]));
  }

  ObTabletCreateSSTableParam dest_sstable_param;
  common::ObArray<blocksstable::MacroBlockId> data_block_ids;
  common::ObArray<blocksstable::MacroBlockId> other_block_ids;
  ret = dest_sstable_param.init_for_ha(mig_param, data_block_ids, other_block_ids);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_TRUE(dest_sstable_param.encrypt_id_ == src_sstable_param.encrypt_id_);
  ASSERT_TRUE(dest_sstable_param.table_shared_flag_ == src_sstable_param.table_shared_flag_);
}

TEST_F(TestSSTableMeta, test_sstable_skip_index_row_serialize_and_deserialize)
{
  // Test Goal: Verify serialize/deserialize of ObSSTableMeta WITHOUT skip index data
  // Verifications:
  //   1. After init, has_sstable_skip_index() returns false (no skip index by default)
  //   2. Serialization succeeds and pos equals get_serialize_size()
  //   3. After deserialization, skip index is still absent
  //   4. get_variable_size() and get_deep_copy_size() are consistent between src and dest
  ObSSTableMeta sstable_meta;
  ASSERT_TRUE(!sstable_meta.is_valid());
  sstable_meta.reset();
  ASSERT_TRUE(!sstable_meta.is_valid());
  ASSERT_EQ(OB_SUCCESS, sstable_meta.init(param_, allocator_));
  ASSERT_TRUE(sstable_meta.is_valid());

  // Initially no skip index row
  ASSERT_FALSE(sstable_meta.has_sstable_skip_index());
  ASSERT_EQ(nullptr, sstable_meta.get_sstable_skip_index_buf());
  ASSERT_EQ(0, sstable_meta.get_sstable_skip_index_size());

  // Test serialize and deserialize without skip index row
  int64_t pos = 0;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t buf_len = sstable_meta.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable_meta.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObSSTableMeta tmp_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_meta.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(tmp_meta.is_valid());
  ASSERT_FALSE(tmp_meta.has_sstable_skip_index());
  ASSERT_EQ(nullptr, tmp_meta.get_sstable_skip_index_buf());
  ASSERT_EQ(0, tmp_meta.get_sstable_skip_index_size());

  // Verify get_variable_size and get_deep_copy_size
  ASSERT_EQ(sstable_meta.get_variable_size(), tmp_meta.get_variable_size());
  ASSERT_EQ(sstable_meta.get_deep_copy_size(), tmp_meta.get_deep_copy_size());

  allocator_.free(buf);
  buf = nullptr;
}

TEST_F(TestSSTableMeta, test_sstable_skip_index_row_with_data)
{
  // Test Goal: Verify serialize/deserialize of ObSSTableMeta WITH skip index data
  // Verifications:
  //   1. After setting skip index in param, init succeeds and has_sstable_skip_index() == true
  //   2. get_variable_size() includes the skip index data size
  //   3. Serialization succeeds and pos equals get_serialize_size()
  //   4. After deserialization, skip index data content is correct
  //   5. get_variable_size() and get_deep_copy_size() are consistent between src and dest
  //   6. Deserialized buffer pointer differs from original (independent memory copy)
  const char *mock_skip_index_data = "mock_skip_index_row_data_for_test";
  const int64_t mock_skip_index_size = strlen(mock_skip_index_data) + 1;
  char *skip_index_buf = static_cast<char *>(allocator_.alloc(mock_skip_index_size));
  ASSERT_NE(nullptr, skip_index_buf);
  MEMCPY(skip_index_buf, mock_skip_index_data, mock_skip_index_size);

  // Set skip index on param_
  param_.sstable_skip_index_.set(skip_index_buf, mock_skip_index_size);

  ObSSTableMeta sstable_meta;
  ASSERT_EQ(OB_SUCCESS, sstable_meta.init(param_, allocator_));
  ASSERT_TRUE(sstable_meta.is_valid());

  // Should have skip index row now
  ASSERT_TRUE(sstable_meta.has_sstable_skip_index());
  ASSERT_NE(nullptr, sstable_meta.get_sstable_skip_index_buf());
  ASSERT_EQ(mock_skip_index_size, sstable_meta.get_sstable_skip_index_size());
  ASSERT_EQ(0, MEMCMP(sstable_meta.get_sstable_skip_index_buf(), mock_skip_index_data, mock_skip_index_size));

  // Verify skip index data is included in variable size
  const int64_t var_size_with_skip_index = sstable_meta.get_variable_size();
  ASSERT_GE(var_size_with_skip_index, mock_skip_index_size);

  // Test serialize and deserialize with skip index row
  int64_t pos = 0;
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t buf_len = sstable_meta.get_serialize_size(data_version);
  char *buf = static_cast<char *>(allocator_.alloc(buf_len));
  ASSERT_NE(nullptr, buf);
  ASSERT_EQ(OB_SUCCESS, sstable_meta.serialize(data_version, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);

  ObSSTableMeta tmp_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_meta.deserialize(allocator_, buf, buf_len, pos));
  ASSERT_EQ(buf_len, pos);
  ASSERT_TRUE(tmp_meta.is_valid());
  ASSERT_TRUE(tmp_meta.has_sstable_skip_index());
  ASSERT_NE(nullptr, tmp_meta.get_sstable_skip_index_buf());
  ASSERT_EQ(mock_skip_index_size, tmp_meta.get_sstable_skip_index_size());
  ASSERT_EQ(0, MEMCMP(tmp_meta.get_sstable_skip_index_buf(), mock_skip_index_data, mock_skip_index_size));

  // Verify deserialized meta has same variable size and deep copy size
  ASSERT_EQ(sstable_meta.get_variable_size(), tmp_meta.get_variable_size());
  ASSERT_EQ(sstable_meta.get_deep_copy_size(), tmp_meta.get_deep_copy_size());

  // Verify buffer is independent (different pointer after deserialize)
  ASSERT_NE(sstable_meta.get_sstable_skip_index_buf(), tmp_meta.get_sstable_skip_index_buf());

  allocator_.free(buf);
  buf = nullptr;
}

TEST_F(TestSSTableMeta, test_sstable_skip_index_row_deep_copy)
{
  // Test Goal: Verify deep_copy of ObSSTableMeta WITH skip index data
  // Verifications:
  //   1. First deep copy: dynamic memory meta -> flat memory meta succeeds
  //   2. Skip index data is correctly deep copied (content matches, pointer differs)
  //   3. get_variable_size() and get_deep_copy_size() are consistent after deep copy
  //   4. Second deep copy: flat memory meta -> flat memory meta succeeds
  //   5. All three buffers (src, dest1, dest2) have different pointers
  //   6. basic_meta_ is correctly copied in each deep copy
  const char *mock_skip_index_data = "deep_copy_test_skip_index_data";
  const int64_t mock_skip_index_size = strlen(mock_skip_index_data) + 1;
  char *skip_index_buf = static_cast<char *>(allocator_.alloc(mock_skip_index_size));
  ASSERT_NE(nullptr, skip_index_buf);
  MEMCPY(skip_index_buf, mock_skip_index_data, mock_skip_index_size);

  // Set skip index on param_
  param_.sstable_skip_index_.set(skip_index_buf, mock_skip_index_size);

  ObSSTableMeta src_meta;
  ASSERT_EQ(OB_SUCCESS, src_meta.init(param_, allocator_));
  ASSERT_TRUE(src_meta.is_valid());
  ASSERT_TRUE(src_meta.has_sstable_skip_index());

  // First deep copy: from dynamic memory meta to flat memory meta
  int64_t deep_copy_size = src_meta.get_deep_copy_size();
  char *dest_buf_1 = static_cast<char *>(allocator_.alloc(deep_copy_size));
  ASSERT_NE(nullptr, dest_buf_1);
  MEMSET(dest_buf_1, 0, deep_copy_size);

  int64_t pos = 0;
  ObSSTableMeta *dest_meta_1 = nullptr;
  ASSERT_EQ(OB_SUCCESS, src_meta.deep_copy(dest_buf_1, deep_copy_size, pos, dest_meta_1));
  ASSERT_NE(nullptr, dest_meta_1);
  ASSERT_TRUE(dest_meta_1->is_valid());

  // Verify first deep copy result
  ASSERT_TRUE(dest_meta_1->has_sstable_skip_index());
  ASSERT_NE(nullptr, dest_meta_1->get_sstable_skip_index_buf());
  ASSERT_EQ(mock_skip_index_size, dest_meta_1->get_sstable_skip_index_size());
  ASSERT_EQ(0, MEMCMP(dest_meta_1->get_sstable_skip_index_buf(), mock_skip_index_data, mock_skip_index_size));

  // Verify that buffers are different (deep copied, not shallow copy)
  ASSERT_NE(src_meta.get_sstable_skip_index_buf(), dest_meta_1->get_sstable_skip_index_buf());

  // Verify get_variable_size and get_deep_copy_size are consistent
  ASSERT_EQ(src_meta.get_variable_size(), dest_meta_1->get_variable_size());
  ASSERT_EQ(src_meta.get_deep_copy_size(), dest_meta_1->get_deep_copy_size());

  // Second deep copy: from flat memory meta to flat memory meta
  int64_t deep_copy_size_2 = dest_meta_1->get_deep_copy_size();
  char *dest_buf_2 = static_cast<char *>(allocator_.alloc(deep_copy_size_2));
  ASSERT_NE(nullptr, dest_buf_2);
  MEMSET(dest_buf_2, 0, deep_copy_size_2);

  pos = 0;
  ObSSTableMeta *dest_meta_2 = nullptr;
  ASSERT_EQ(OB_SUCCESS, dest_meta_1->deep_copy(dest_buf_2, deep_copy_size_2, pos, dest_meta_2));
  ASSERT_NE(nullptr, dest_meta_2);
  ASSERT_TRUE(dest_meta_2->is_valid());

  // Verify second deep copy result
  ASSERT_TRUE(dest_meta_2->has_sstable_skip_index());
  ASSERT_NE(nullptr, dest_meta_2->get_sstable_skip_index_buf());
  ASSERT_EQ(mock_skip_index_size, dest_meta_2->get_sstable_skip_index_size());
  ASSERT_EQ(0, MEMCMP(dest_meta_2->get_sstable_skip_index_buf(), mock_skip_index_data, mock_skip_index_size));

  // Verify all three buffers are different
  ASSERT_NE(src_meta.get_sstable_skip_index_buf(), dest_meta_2->get_sstable_skip_index_buf());
  ASSERT_NE(dest_meta_1->get_sstable_skip_index_buf(), dest_meta_2->get_sstable_skip_index_buf());

  // Verify basic_meta is correctly copied
  ASSERT_EQ(src_meta.basic_meta_, dest_meta_1->basic_meta_);
  ASSERT_EQ(dest_meta_1->basic_meta_, dest_meta_2->basic_meta_);

  allocator_.free(dest_buf_1);
  allocator_.free(dest_buf_2);
}

TEST_F(TestSSTableMeta, test_sstable_skip_index_row_deep_copy_without_data)
{
  // Test Goal: Verify deep_copy of ObSSTableMeta WITHOUT skip index data
  // Verifications:
  //   1. Deep copy succeeds when no skip index data is present
  //   2. After deep copy, destination also has no skip index (buf == nullptr, size == 0)
  //   3. get_variable_size() and get_deep_copy_size() match between src and dest
  ObSSTableMeta src_meta;
  ASSERT_EQ(OB_SUCCESS, src_meta.init(param_, allocator_));
  ASSERT_TRUE(src_meta.is_valid());
  ASSERT_FALSE(src_meta.has_sstable_skip_index());

  // Deep copy
  int64_t deep_copy_size = src_meta.get_deep_copy_size();
  char *dest_buf = static_cast<char *>(allocator_.alloc(deep_copy_size));
  ASSERT_NE(nullptr, dest_buf);
  MEMSET(dest_buf, 0, deep_copy_size);

  int64_t pos = 0;
  ObSSTableMeta *dest_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, src_meta.deep_copy(dest_buf, deep_copy_size, pos, dest_meta));
  ASSERT_NE(nullptr, dest_meta);
  ASSERT_TRUE(dest_meta->is_valid());

  // Verify deep copy result - should also have no skip index
  ASSERT_FALSE(dest_meta->has_sstable_skip_index());
  ASSERT_EQ(nullptr, dest_meta->get_sstable_skip_index_buf());
  ASSERT_EQ(0, dest_meta->get_sstable_skip_index_size());

  // Verify sizes match
  ASSERT_EQ(src_meta.get_variable_size(), dest_meta->get_variable_size());
  ASSERT_EQ(src_meta.get_deep_copy_size(), dest_meta->get_deep_copy_size());

  allocator_.free(dest_buf);
}

TEST_F(TestSSTableMeta, test_sstable_skip_index_row_backward_compatibility)
{
  // Test Goal: Verify backward compatibility of skip index serialization
  // Verifications:
  //   1. Serialize with new version (4.6.1) includes skip index, size is larger
  //   2. Serialize with old version (4.5.0) excludes skip index, size is smaller
  //   3. Old format data can be deserialized, has_sstable_skip_index() == false
  //   4. New format data can be deserialized with correct skip index content
  //   5. basic_meta_ is identical between old and new format deserialized data
  //   6. Deep copy after deserialization works correctly
  const char *mock_skip_index_data = "backward_compat_skip_index_data";
  const int64_t mock_skip_index_size = strlen(mock_skip_index_data) + 1;
  char *skip_index_buf = static_cast<char *>(allocator_.alloc(mock_skip_index_size));
  ASSERT_NE(nullptr, skip_index_buf);
  MEMCPY(skip_index_buf, mock_skip_index_data, mock_skip_index_size);

  param_.sstable_skip_index_.set(skip_index_buf, mock_skip_index_size);

  ObSSTableMeta sstable_meta;
  ASSERT_EQ(OB_SUCCESS, sstable_meta.init(param_, allocator_));
  ASSERT_TRUE(sstable_meta.is_valid());
  ASSERT_TRUE(sstable_meta.has_sstable_skip_index());

  // Serialize with 4.5.1 version (includes skip index)
  int64_t pos = 0;
  const uint64_t new_version = DATA_VERSION_4_6_1_0;
  int64_t new_buf_len = sstable_meta.get_serialize_size(new_version);
  char *new_buf = static_cast<char *>(allocator_.alloc(new_buf_len));
  ASSERT_NE(nullptr, new_buf);
  ASSERT_EQ(OB_SUCCESS, sstable_meta.serialize(new_version, new_buf, new_buf_len, pos));
  ASSERT_EQ(new_buf_len, pos);

  // Serialize with 4.5.0 version (without skip index) to get old format size
  int64_t old_pos = 0;
  const uint64_t old_version = DATA_VERSION_4_5_0_0;
  int64_t old_buf_len = sstable_meta.get_serialize_size(old_version);
  char *old_buf = static_cast<char *>(allocator_.alloc(old_buf_len));
  ASSERT_NE(nullptr, old_buf);
  ASSERT_EQ(OB_SUCCESS, sstable_meta.serialize(old_version, old_buf, old_buf_len, old_pos));
  ASSERT_EQ(old_buf_len, old_pos);

  // Old format should be smaller (no skip index)
  ASSERT_LT(old_buf_len, new_buf_len);

  // Test: Old version code can deserialize old format data
  ObSSTableMeta old_meta;
  old_pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_meta.deserialize(allocator_, old_buf, old_buf_len, old_pos));
  ASSERT_EQ(old_buf_len, old_pos);
  ASSERT_TRUE(old_meta.is_valid());
  ASSERT_FALSE(old_meta.has_sstable_skip_index());

  // Test: New version code can deserialize new format data
  ObSSTableMeta new_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_meta.deserialize(allocator_, new_buf, new_buf_len, pos));
  ASSERT_EQ(new_buf_len, pos);
  ASSERT_TRUE(new_meta.is_valid());
  ASSERT_TRUE(new_meta.has_sstable_skip_index());
  ASSERT_EQ(mock_skip_index_size, new_meta.get_sstable_skip_index_size());
  ASSERT_EQ(0, MEMCMP(new_meta.get_sstable_skip_index_buf(), mock_skip_index_data, mock_skip_index_size));

  // Test: New version code can deserialize old format data
  ObSSTableMeta new_meta_2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_meta_2.deserialize(allocator_, old_buf, old_buf_len, pos));
  ASSERT_EQ(old_buf_len, pos);
  ASSERT_TRUE(new_meta_2.is_valid());
  ASSERT_FALSE(new_meta_2.has_sstable_skip_index());
  ASSERT_EQ(0, new_meta_2.get_sstable_skip_index_size());
  ASSERT_EQ(nullptr, new_meta_2.get_sstable_skip_index_buf());

  // Verify basic_meta is same between old and new format
  ASSERT_EQ(old_meta.basic_meta_, new_meta.basic_meta_);

  // Test deep copy after deserialization
  int64_t deep_copy_size = new_meta.get_deep_copy_size();
  char *deep_buf = static_cast<char *>(allocator_.alloc(deep_copy_size));
  ASSERT_NE(nullptr, deep_buf);
  MEMSET(deep_buf, 0, deep_copy_size);

  int64_t deep_pos = 0;
  ObSSTableMeta *deep_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, new_meta.deep_copy(deep_buf, deep_copy_size, deep_pos, deep_meta));
  ASSERT_NE(nullptr, deep_meta);
  ASSERT_TRUE(deep_meta->is_valid());
  ASSERT_TRUE(deep_meta->has_sstable_skip_index());
  ASSERT_EQ(mock_skip_index_size, deep_meta->get_sstable_skip_index_size());
  ASSERT_EQ(0, MEMCMP(deep_meta->get_sstable_skip_index_buf(), mock_skip_index_data, mock_skip_index_size));
  ASSERT_NE(new_meta.get_sstable_skip_index_buf(), deep_meta->get_sstable_skip_index_buf());

  allocator_.free(new_buf);
  allocator_.free(old_buf);
  allocator_.free(deep_buf);
}

TEST_F(TestSSTableMeta, test_sstable_skip_index_serialize_deserialize_roundtrip)
{
  // Test Goal: Verify serialize -> deserialize -> serialize roundtrip produces identical results
  // Verifications:
  //   1. First serialize succeeds with correct size
  //   2. Deserialize from first buffer succeeds
  //   3. Second serialize from deserialized meta has same size as first
  //   4. Two serialized buffers are byte-identical (MEMCMP == 0)
  //   5. Final deserialized meta has correct skip index content
  const char *mock_skip_index_data = "roundtrip_test_data_12345";
  const int64_t mock_skip_index_size = strlen(mock_skip_index_data) + 1;
  char *skip_index_buf = static_cast<char *>(allocator_.alloc(mock_skip_index_size));
  ASSERT_NE(nullptr, skip_index_buf);
  MEMCPY(skip_index_buf, mock_skip_index_data, mock_skip_index_size);

  param_.sstable_skip_index_.set(skip_index_buf, mock_skip_index_size);

  ObSSTableMeta original_meta;
  ASSERT_EQ(OB_SUCCESS, original_meta.init(param_, allocator_));
  ASSERT_TRUE(original_meta.is_valid());

  // First serialize
  const uint64_t data_version = DATA_CURRENT_VERSION;
  int64_t buf_len_1 = original_meta.get_serialize_size(data_version);
  char *buf_1 = static_cast<char *>(allocator_.alloc(buf_len_1));
  ASSERT_NE(nullptr, buf_1);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, original_meta.serialize(data_version, buf_1, buf_len_1, pos));
  ASSERT_EQ(buf_len_1, pos);

  // First deserialize
  ObSSTableMeta deserialized_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialized_meta.deserialize(allocator_, buf_1, buf_len_1, pos));
  ASSERT_TRUE(deserialized_meta.is_valid());

  // Second serialize (from deserialized meta)
  int64_t buf_len_2 = deserialized_meta.get_serialize_size(data_version);
  ASSERT_EQ(buf_len_1, buf_len_2);  // Sizes should match

  char *buf_2 = static_cast<char *>(allocator_.alloc(buf_len_2));
  ASSERT_NE(nullptr, buf_2);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, deserialized_meta.serialize(data_version, buf_2, buf_len_2, pos));
  ASSERT_EQ(buf_len_2, pos);

  // Verify serialized buffers are identical
  ASSERT_EQ(0, MEMCMP(buf_1, buf_2, buf_len_1));

  // Second deserialize and verify
  ObSSTableMeta final_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, final_meta.deserialize(allocator_, buf_2, buf_len_2, pos));
  ASSERT_TRUE(final_meta.is_valid());
  ASSERT_TRUE(final_meta.has_sstable_skip_index());
  ASSERT_EQ(mock_skip_index_size, final_meta.get_sstable_skip_index_size());
  ASSERT_EQ(0, MEMCMP(final_meta.get_sstable_skip_index_buf(), mock_skip_index_data, mock_skip_index_size));

  allocator_.free(buf_1);
  allocator_.free(buf_2);
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_sstable_meta.log*");
  OB_LOGGER.set_file_name("test_sstable_meta.log");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
