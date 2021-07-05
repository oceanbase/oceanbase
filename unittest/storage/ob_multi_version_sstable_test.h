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
#define protected public
#include "share/ob_tenant_mgr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "mockcontainer/mock_ob_iterator.h"
#include "blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_macro_block_meta_mgr.h"
#include "ob_sstable_test.h"

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest {
static const int64_t MACRO_BLOCK_SIZE = 64 * 1024;
class ObMultiVersionMockIterator {
private:
  int64_t rowkey_cnt_;
};

class ObMultiVersionSSTableTest : public TestDataFilePrepare {
public:
  ObMultiVersionSSTableTest(const char* file)
      : TestDataFilePrepare(file, MACRO_BLOCK_SIZE), is_open_(false), data_iter_cursor_(0)
  {
    ObAddr self;
    rpc::frame::ObReqTransport req_transport(NULL, NULL);
    obrpc::ObSrvRpcProxy rpc_proxy;
    obrpc::ObCommonRpcProxy rs_rpc_proxy_;
    share::ObRsMgr rs_mgr_;
    int64_t tenant_id = TENANT_ID;
    self.set_ip_addr("127.0.0.1", 8086);
    ObTenantManager::get_instance().init(
        self, rpc_proxy, rs_rpc_proxy_, rs_mgr_, &req_transport, &ObServerConfig::get_instance());
    ObTenantManager::get_instance().add_tenant(tenant_id);
    ObTenantManager::get_instance().set_tenant_mem_limit(
        tenant_id, 2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
  }
  virtual ~ObMultiVersionSSTableTest()
  {}
  virtual void SetUp()
  {
    TestDataFilePrepare::SetUp();
  }
  virtual void TearDown()
  {
    sstable_meta_.reset();
    sstable_.destroy();
    gap_sstable_.destroy();
    major_sstable_.destroy();
    block_cache_ws_.reset();
    for (int i = 0; i < MAX_MICRO_BLOCK_CNT; i++) {
      data_iter_[i].reset();
    }
    rowkey_cnt_ = 0;
    desc_.reset();
    macro_writer_.reset();
    is_open_ = false;
    data_iter_cursor_ = 0;
    TestDataFilePrepare::TearDown();
    writer_.reuse();
  }
  void prepare_empty_sstable(const char** data, const int64_t rowkey_cnt, const char* compressor_name = "none",
      const ObRowStoreType row_store_type = FLAT_ROW_STORE);
  void prepare_data_start(const char** data, const int64_t rowkey_cnt, const int64_t snapshot_version,
      const char* compressor_name = "none", const ObRowStoreType row_store_type = FLAT_ROW_STORE,
      const int64_t data_version = DATA_VERSION);
  void prepare_one_macro(const char** data, const int64_t micro_cnt,
      const int64_t max_merged_trans_version = INT64_MAX - 2, const char* col_id_array = nullptr,
      const int64_t* col_cnt = nullptr, const bool contain_uncommitted = false);
  void prepare_data_end();
  void prepare_data_start(ObSSTable& sstable, const char** micro_data, const int64_t rowkey_cnt,
      const int64_t snapshot_version, const char* compressor_name = "none",
      const ObRowStoreType row_store_type = FLAT_ROW_STORE, const int64_t data_version = DATA_VERSION,
      const int64_t start_log_ts = -1, const int64_t end_log_ts = -1, const int64_t max_log_ts = -1);
  void prepare_data_end(ObSSTable& sstable);
  void prepare_data(const char** data, const int64_t micro_cnt, const int64_t rowkey_cnt,
      const int64_t snapshot_version, const char* compressor_name = "none",
      const ObRowStoreType row_store_type = FLAT_ROW_STORE, const int64_t data_version = DATA_VERSION,
      const char* col_id_array = nullptr, const int64_t* col_cnt = nullptr);
  void prepare_sstable_meta(const int64_t snapshot_version = INT64_MAX, const int64_t data_version = DATA_VERSION);
  void prepare_data_store_desc(const char* compressor_name = "none",
      const ObRowStoreType row_store_type = FLAT_ROW_STORE, const int64_t data_version = DATA_VERSION);
  void build_micro_block(ObMockIterator& data_iter, ObStoreRowkey& start_key, blocksstable::ObMicroBlock& micro_block,
      const ObRowStoreType row_store_type = FLAT_ROW_STORE);
  void build_sparse_micro_block(ObMockIterator& data_iter, ObStoreRowkey& start_key,
      blocksstable::ObMicroBlock& micro_block, ObMockStoreRowIterator& col_ids, int& pos, const int64_t* col_cnt);
  void build_micro_block_data(ObMockIterator& data_iter, ObMicroBlockData& block_data, ObMicroBlockData& payload_data,
      ObStoreRowkey& end_key, const ObRowStoreType row_store_type = FLAT_ROW_STORE);
  void build_sparse_micro_block_data(ObMockIterator& data_iter, ObMicroBlockData& block_data,
      ObMicroBlockData& payload_data, ObStoreRowkey& end_key, ObMockStoreRowIterator& col_ids, int& pos,
      const int64_t* col_cnt);
  void make_range(const ObStoreRow* start, const ObStoreRow* end, ObExtStoreRange& ext_range);
  int compare_skip_iter(ObMockIterator& res_iter, ObStoreRowIterator* scanner,
      const common::ObIArray<SkipInfo>& skip_infos, const common::ObIArray<ObStoreRowkey*>& gap_keys,
      const common::ObArray<int64_t>& skip_range_idx);
  int convert_iter_to_gapkeys(
      const int64_t rowkey_column_cnt, ObMockIterator& gap_iter, common::ObIArray<ObStoreRowkey*>& gap_keys);
  bool is_open() const
  {
    return is_open_;
  }

private:
  void prepare_gap_schema();
  void prepare_gap_sstable_data(const int load_type);
  void convert_to_multi_row(const ObStoreRow& org_row, const int64_t snapshot_version, const ObRowDml row_dml,
      const bool is_compacted_row, const bool is_last_row, const bool is_first_row, ObStoreRow& multi_row);

protected:
  static const int64_t DATA_VERSION = 2;
  static const int64_t MICRO_BLOCK_SIZE = 4 * 1024;
  static const int64_t MAX_MICRO_BLOCK_CNT = 20;
  static const int64_t SCHEMA_VERSION = 2;
  static const int64_t TEST_COLUMN_CNT = 4;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  enum LoadDataType {
    ALL_DELETE = 0,
    ALL_INSERT = 1,
    MIX_DELETE_WITH_UPDATE = 2,
  };

  ObMockIterator data_iter_[MAX_MICRO_BLOCK_CNT];
  ObSSTable sstable_;
  ObSSTable major_sstable_;
  ObSSTable gap_sstable_;
  ObRowGenerate row_generate_;
  ObMacroBlockWriter macro_writer_;
  ObBlockCacheWorkingSet block_cache_ws_;
  ObSSTableBaseMeta sstable_meta_;
  ObDataStoreDesc desc_;
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  ObArenaAllocator allocator_;
  ObColumnMap column_map_;
  bool is_open_;
  int64_t data_iter_cursor_;
  ObMicroBlockWriter writer_;
  ObITable::TableKey table_key_;
  ObTableSchema table_schema_;
  memtable::ObMemtableCtxFactory memctx_factory_;
  ObBlockMarkDeletionMaker mark_deletion_maker_;
  uint16_t column_ids_[OB_MAX_COLUMN_NUMBER];
  ObMacroBlockMetaV2 meta_;
  ObMacroBlockSchemaInfo schema_;
  ObFullMacroBlockMeta full_meta_;
  ObTableSchema minor_table_schema_;
};

void ObMultiVersionSSTableTest::prepare_gap_schema()
{
  ObColumnSchemaV2 column;
  uint64_t table_id = combine_id(TENANT_ID, TABLE_ID);
  int64_t micro_block_size = 16 * 1024;
  ObObjType obj_types[TEST_COLUMN_CNT] = {ObIntType, ObVarcharType, ObIntType, ObDateTimeType};

  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_multi_version_gap"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_row_store_type(FLAT_ROW_STORE);
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = obj_types[i];
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(10);
    if (i < TEST_ROWKEY_COLUMN_CNT) {
      column.set_rowkey_position(i + 1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void ObMultiVersionSSTableTest::prepare_gap_sstable_data(const int load_data_type)
{
  int ret = OB_SUCCESS;
  int column_num = 256;
  ObObj cells[column_num];
  uint16_t col_ids[column_num];
  ObObj multi_cells[column_num];
  uint16_t multi_col_ids[column_num];
  ObStoreRow row;
  ObStoreRow multi_row;
  row.row_val_.cells_ = cells;
  row.column_ids_ = col_ids;
  row.row_val_.count_ = table_schema_.get_column_count();
  multi_row.row_val_.cells_ = multi_cells;
  multi_row.row_val_.count_ = table_schema_.get_column_count() + 2;
  multi_row.column_ids_ = multi_col_ids;
  ObITable::TableKey table_key;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);

  table_key.table_type_ = ObITable::MAJOR_SSTABLE;
  table_key.pkey_ = ObPartitionKey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
  table_key.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  table_key.version_ = ObVersion(1, 0);
  table_key.trans_version_range_.multi_version_start_ = 0;
  table_key.trans_version_range_.base_version_ = 0;
  table_key.trans_version_range_.snapshot_version_ = 0;

  ObCreateSSTableParamWithTable param;
  param.table_key_ = table_key;
  param.logical_data_version_ = table_key.version_;
  param.create_snapshot_version_ = 1;
  param.progressive_merge_end_version_ = 0;
  param.progressive_merge_start_version_ = 0;
  param.schema_ = &table_schema_;
  param.schema_version_ = 0;
  param.checksum_method_ = blocksstable::CCM_VALUE_ONLY;
  param.pg_key_ =
      ObPGKey(combine_id(TENANT_ID, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  param.has_compact_row_ = true;

  major_sstable_.destroy();
  ASSERT_EQ(OB_SUCCESS, major_sstable_.init(param.table_key_));
  ASSERT_EQ(OB_SUCCESS, major_sstable_.set_storage_file_handle(get_storage_file_handle()));
  ASSERT_EQ(OB_SUCCESS, major_sstable_.open(param));
  ASSERT_EQ(OB_SUCCESS, major_sstable_.close());

  param.table_key_.table_type_ = ObITable::MULTI_VERSION_MINOR_SSTABLE;
  gap_sstable_.destroy();
  ASSERT_EQ(OB_SUCCESS, gap_sstable_.init(param.table_key_));
  ASSERT_EQ(OB_SUCCESS, gap_sstable_.set_storage_file_handle(get_storage_file_handle()));
  ASSERT_EQ(OB_SUCCESS, gap_sstable_.open(param));

  ObDataStoreDesc desc;
  ObMultiVersionColDescGenerate multi_version_col_desc_gen;
  ret = multi_version_col_desc_gen.init(&table_schema_);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObMultiVersionRowInfo* multi_version_row_info = NULL;
  ret = multi_version_col_desc_gen.generate_multi_version_row_info(multi_version_row_info);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(multi_version_row_info->is_valid());

  ObPGKey pg_key(combine_id(1, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(pg_key, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = desc.init(table_schema_,
      table_key.version_,
      multi_version_row_info,
      1,
      MINOR_MERGE /*is major*/,
      false,
      false,
      pg_key,
      pg_guard.get_partition_group()->get_storage_file_handle());
  ASSERT_EQ(OB_SUCCESS, ret);

  mark_deletion_maker_.reset();
  ret = mark_deletion_maker_.tables_handle_.add_table(&major_sstable_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret =
      mark_deletion_maker_.init_(table_schema_, table_key.pkey_, table_key.table_id_, INT64_MAX - 2, &memctx_factory_);
  ASSERT_EQ(OB_SUCCESS, ret);
  desc.mark_deletion_maker_ = &mark_deletion_maker_;
  ret = writer.open(desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  row_generate_.reset();
  ret = row_generate_.init(table_schema_, &allocator_, true, true);
  int64_t i = 0;
  if (ALL_DELETE == load_data_type) {
    while (OB_SUCC(ret) && i < 1000) {
      ret = row_generate_.get_next_row(i, row);
      ASSERT_EQ(OB_SUCCESS, ret);
      convert_to_multi_row(row, 5L, T_DML_DELETE, true, false, true, multi_row);
      ret = writer.append_row(multi_row);
      convert_to_multi_row(row, 4L, T_DML_UPDATE, false, false, false, multi_row);
      ret = writer.append_row(multi_row);
      convert_to_multi_row(row, 3L, T_DML_UPDATE, false, true, false, multi_row);
      ret = writer.append_row(multi_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ++i;
    }
  } else if (ALL_INSERT == load_data_type) {
    while (OB_SUCC(ret) && i < 1000) {
      ret = row_generate_.get_next_row(i, row);
      ASSERT_EQ(OB_SUCCESS, ret);
      convert_to_multi_row(row, 5L, T_DML_UPDATE, true, false, true, multi_row);
      ret = writer.append_row(multi_row);
      convert_to_multi_row(row, 3L, T_DML_INSERT, false, true, false, multi_row);
      ret = writer.append_row(multi_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ++i;
    }
  } else {
    while (OB_SUCC(ret) && i < 1000) {
      ret = row_generate_.get_next_row(i, row);
      ASSERT_EQ(OB_SUCCESS, ret);
      convert_to_multi_row(row, 5L, T_DML_DELETE, true, false, true, multi_row);
      ret = writer.append_row(multi_row);
      convert_to_multi_row(row, 4L, T_DML_UPDATE, false, false, false, multi_row);
      ret = writer.append_row(multi_row);
      convert_to_multi_row(row, 3L, T_DML_UPDATE, false, true, false, multi_row);
      ret = writer.append_row(multi_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ++i;
    }
    while (OB_SUCC(ret) && i < 2000) {
      ret = row_generate_.get_next_row(i, row);
      ASSERT_EQ(OB_SUCCESS, ret);
      convert_to_multi_row(row, 5L, T_DML_UPDATE, true, false, false, multi_row);
      ret = writer.append_row(multi_row);
      convert_to_multi_row(row, 3L, T_DML_UPDATE, false, true, true, multi_row);
      ret = writer.append_row(multi_row);
      ASSERT_EQ(OB_SUCCESS, ret);
      ++i;
    }
  }
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  OK(gap_sstable_.append_macro_blocks(writer.get_macro_block_write_ctx()));
  ASSERT_EQ(OB_SUCCESS, gap_sstable_.close());
}

void ObMultiVersionSSTableTest::prepare_sstable_meta(
    const int64_t snapshot_version /* = INT64_MAX*/, const int64_t data_version)
{
  ObSSTableColumnMeta col_meta;
  sstable_meta_.index_id_ = combine_id(TENANT_ID, TABLE_ID);
  sstable_meta_.data_version_ = data_version;
  sstable_meta_.logical_data_version_ = data_version;
  sstable_meta_.rowkey_column_count_ = rowkey_cnt_;
  sstable_meta_.table_type_ = USER_TABLE;
  sstable_meta_.column_cnt_ = column_cnt_;
  sstable_meta_.total_sstable_count_ = 1;
  sstable_meta_.column_metas_.set_capacity(static_cast<int32_t>(column_cnt_));
  sstable_meta_.set_allocator(allocator_);
  sstable_meta_.schema_version_ = SCHEMA_VERSION;
  sstable_meta_.pg_key_ =
      ObPGKey(combine_id(TENANT_ID, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  sstable_meta_.multi_version_rowkey_type_ = ObMultiVersionRowkeyHelpper::MVRC_VERSION_AFTER_3_0;
  sstable_meta_.sstable_format_version_ = ObSSTableBaseMeta::SSTABLE_FORMAT_VERSION_6;
  sstable_meta_.max_merged_trans_version_ = snapshot_version;
  STORAGE_LOG(INFO, "update sstable meta", K(sstable_meta_));
  for (int i = 0; i < column_cnt_; i++) {
    col_meta.column_id_ = i + OB_APP_MIN_COLUMN_ID;
    if (i == rowkey_cnt_ - 2) {
      col_meta.column_id_ = OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
    }
    if (i == rowkey_cnt_ - 1) {
      col_meta.column_id_ = OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
    }
    OK(sstable_meta_.column_metas_.push_back(col_meta));
    column_ids_[i] = static_cast<uint16_t>(col_meta.column_id_);
  }
}

void ObMultiVersionSSTableTest::prepare_data_store_desc(
    const char* compressor_name, const ObRowStoreType row_store_type, const int64_t data_version)
{
  int ret = common::OB_SUCCESS;
  desc_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  desc_.data_version_ = data_version;
  desc_.micro_block_size_ = MICRO_BLOCK_SIZE;
  desc_.micro_block_size_limit_ = get_storage_env().default_block_size_ - 8192;
  desc_.macro_block_size_ = get_storage_env().default_block_size_;
  desc_.macro_store_size_ = get_storage_env().default_block_size_ * 9 / 10;
  desc_.row_column_count_ = column_cnt_;
  desc_.rowkey_column_count_ = rowkey_cnt_;
  desc_.schema_rowkey_col_cnt_ = rowkey_cnt_ - 2;
  desc_.column_index_scale_ = 1;
  desc_.row_store_type_ = row_store_type;
  desc_.schema_version_ = SCHEMA_VERSION;
  desc_.snapshot_version_ = DATA_VERSION;
  strcpy(desc_.compressor_name_, compressor_name);
  for (int i = 0; i < column_cnt_; i++) {
    desc_.column_ids_[i] = i + OB_APP_MIN_COLUMN_ID;
    desc_.column_types_[i] = data_iter_[0].get_column_type()[i];
    if (i == rowkey_cnt_ - 2) {
      desc_.column_ids_[i] = OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
    } else if (i == rowkey_cnt_ - 1) {
      desc_.column_ids_[i] = OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
    }
  }
  schema_.compressor_ = desc_.compressor_name_;
  full_meta_.meta_ = &meta_;
  full_meta_.schema_ = &schema_;
  ret = desc_.pg_key_.init(desc_.table_id_, 1, 0);
  ASSERT_EQ(common::OB_SUCCESS, ret);

  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(desc_.pg_key_, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = desc_.file_handle_.assign(pg_guard.get_partition_group()->get_storage_file_handle());
}

void ObMultiVersionSSTableTest::build_micro_block(ObMockIterator& data_iter, ObStoreRowkey& start_key,
    blocksstable::ObMicroBlock& micro_block, const ObRowStoreType row_store_type)
{
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  build_micro_block_data(data_iter, block_data, payload_data, end_key, row_store_type);
  micro_block.data_ = block_data;
  micro_block.payload_data_ = payload_data;
  micro_block.range_.start_key_ = start_key;
  micro_block.range_.end_key_ = end_key;
  micro_block.range_.border_flag_.unset_inclusive_start();
  micro_block.range_.border_flag_.set_inclusive_end();
  micro_block.column_map_ = &column_map_;
  micro_block.row_store_type_ = row_store_type;
  micro_block.meta_ = full_meta_;
  micro_block.header_version_ = RECORD_HEADER_VERSION_V2;
  micro_block.origin_data_size_ = payload_data.get_buf_size();
  STORAGE_LOG(INFO, "endkey", K(end_key));
}

void ObMultiVersionSSTableTest::build_sparse_micro_block(ObMockIterator& data_iter, ObStoreRowkey& start_key,
    blocksstable::ObMicroBlock& micro_block, ObMockStoreRowIterator& col_ids, int& pos, const int64_t* col_cnt)
{
  STORAGE_LOG(INFO, "build_sparse_micro_block");
  ObStoreRowkey end_key;
  ObMicroBlockData block_data;
  ObMicroBlockData payload_data;
  build_sparse_micro_block_data(data_iter, block_data, payload_data, end_key, col_ids, pos, col_cnt);
  micro_block.data_ = block_data;
  micro_block.payload_data_ = payload_data;
  micro_block.range_.start_key_ = start_key;
  micro_block.range_.end_key_ = end_key;
  micro_block.range_.border_flag_.unset_inclusive_start();
  micro_block.range_.border_flag_.set_inclusive_end();
  micro_block.column_map_ = &column_map_;
  micro_block.row_store_type_ = SPARSE_ROW_STORE;
  micro_block.meta_ = full_meta_;
  micro_block.header_version_ = RECORD_HEADER_VERSION_V2;
  micro_block.origin_data_size_ = payload_data.get_buf_size();
  STORAGE_LOG(INFO, "endkey", K(end_key));
}

void ObMultiVersionSSTableTest::build_micro_block_data(ObMockIterator& data_iter, ObMicroBlockData& block_data,
    ObMicroBlockData& payload_data, ObStoreRowkey& end_key, const ObRowStoreType row_store_type)
{
  const ObStoreRow* row = NULL;
  char* buf = NULL;
  int64_t size = 0;
  writer_.reuse();
  OK(writer_.init(MACRO_BLOCK_SIZE, rowkey_cnt_, column_cnt_, row_store_type));
  for (int64_t i = 0; i < data_iter.count(); i++) {
    OK(data_iter.get_row(i, row));
    ASSERT_TRUE(NULL != row);
    if (desc_.enable_sparse_format()) {
      const_cast<ObStoreRow*>(row)->column_ids_ = column_ids_;
      const_cast<ObStoreRow*>(row)->is_sparse_row_ = true;
    }
    STORAGE_LOG(INFO, "remain buf size", K(writer_.data_buffer_.remain()), K(writer_.index_buffer_.remain()));
    OK(writer_.append_row(*row));
  }
  OK(writer_.build_block(buf, size));
  ObRecordHeaderV3 record_header;
  record_header.magic_ = MICRO_BLOCK_HEADER_MAGIC;
  record_header.header_length_ = static_cast<int8_t>(ObRecordHeaderV3::get_serialize_size(RECORD_HEADER_VERSION_V2, 0));
  record_header.version_ = RECORD_HEADER_VERSION_V2;
  record_header.header_checksum_ = 0;
  record_header.reserved16_ = 0;
  record_header.data_length_ = size;
  record_header.data_zlength_ = size;
  record_header.data_checksum_ = ob_crc64_sse42(0, buf, size);
  record_header.set_header_checksum();
  const int64_t total_size = record_header.get_serialize_size() + size;
  char* buf_tmp = static_cast<char*>(allocator_.alloc(total_size));
  ASSERT_NE(nullptr, buf_tmp);
  MEMCPY(buf_tmp, &record_header, record_header.get_serialize_size());
  MEMCPY(buf_tmp + record_header.get_serialize_size(), buf, size);
  block_data.buf_ = buf_tmp;
  block_data.size_ = size + record_header.get_serialize_size();
  payload_data.buf_ = buf;
  payload_data.size_ = size;
  end_key.assign(row->row_val_.cells_, rowkey_cnt_);
}

void ObMultiVersionSSTableTest::build_sparse_micro_block_data(ObMockIterator& data_iter, ObMicroBlockData& block_data,
    ObMicroBlockData& payload_data, ObStoreRowkey& end_key, ObMockStoreRowIterator& col_ids, int& pos,
    const int64_t* col_cnt)
{
  const ObStoreRow* row = NULL;
  const ObStoreRow* col_id_row = NULL;
  char* buf = NULL;
  int64_t size = 0;
  writer_.reuse();
  OK(writer_.init(MACRO_BLOCK_SIZE, rowkey_cnt_, 0, SPARSE_ROW_STORE));
  for (int64_t i = 0; i < data_iter.count(); i++) {
    OK(data_iter.get_row(i, row));
    ASSERT_TRUE(NULL != row);
    const_cast<ObStoreRow*>(row)->column_ids_ = column_ids_;
    OK(col_ids.get_row(pos, col_id_row));
    for (int i = 0; i < col_cnt[pos]; ++i) {
      const_cast<ObStoreRow*>(row)->column_ids_[i] = col_id_row->row_val_.cells_[i].get_int();
    }
    const_cast<ObStoreRow*>(row)->is_sparse_row_ = true;
    const_cast<ObStoreRow*>(row)->row_val_.count_ = col_cnt[pos];
    STORAGE_LOG(INFO, "remain buf size", K(writer_.data_buffer_.remain()), K(writer_.index_buffer_.remain()));
    OK(writer_.append_row(*row));
    STORAGE_LOG(INFO, "row", K(*row));
    ++pos;
  }
  OK(writer_.build_block(buf, size));
  ObRecordHeaderV3 record_header;
  record_header.magic_ = MICRO_BLOCK_HEADER_MAGIC;
  record_header.header_length_ = static_cast<int8_t>(ObRecordHeaderV3::get_serialize_size(RECORD_HEADER_VERSION_V2, 0));
  record_header.version_ = RECORD_HEADER_VERSION_V2;
  record_header.header_checksum_ = 0;
  record_header.reserved16_ = 0;
  record_header.data_length_ = size;
  record_header.data_zlength_ = size;
  record_header.data_checksum_ = ob_crc64_sse42(0, buf, size);
  record_header.set_header_checksum();
  const int64_t total_size = record_header.get_serialize_size() + size;
  char* buf_tmp = static_cast<char*>(allocator_.alloc(total_size));
  ASSERT_NE(nullptr, buf_tmp);
  MEMCPY(buf_tmp, &record_header, record_header.get_serialize_size());
  MEMCPY(buf_tmp + record_header.get_serialize_size(), buf, size);
  block_data.buf_ = buf_tmp;
  block_data.size_ = size + record_header.get_serialize_size();
  payload_data.buf_ = buf;
  payload_data.size_ = size;
  end_key.assign(row->row_val_.cells_, rowkey_cnt_);
}

void ObMultiVersionSSTableTest::prepare_empty_sstable(
    const char** micro_data, const int64_t rowkey_cnt, const char* compressor_name, const ObRowStoreType row_store_type)
{
  table_key_.table_type_ = SPARSE_ROW_STORE == row_store_type ? ObITable::MULTI_VERSION_SPARSE_MINOR_SSTABLE
                                                              : ObITable::MULTI_VERSION_MINOR_SSTABLE;
  table_key_.pkey_ = ObPartitionKey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
  table_key_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  table_key_.version_ = ObVersion(1, 0);
  table_key_.trans_version_range_.multi_version_start_ = 0;
  table_key_.trans_version_range_.base_version_ = 0;
  table_key_.trans_version_range_.snapshot_version_ = 0;

  OK(data_iter_[0].from(micro_data[0]));
  rowkey_cnt_ = rowkey_cnt;
  column_cnt_ = data_iter_[0].get_column_cnt();

  prepare_sstable_meta();
  prepare_data_store_desc(compressor_name, row_store_type);
  common::ObArray<share::schema::ObColDesc> out_cols;
  for (int i = 0; i < column_cnt_; i++) {
    share::schema::ObColDesc col_desc;
    col_desc.col_type_ = *data_iter_[0].get_column_type();
    col_desc.col_id_ = column_ids_[i];
    OK(out_cols.push_back(col_desc));
  }
  OK(column_map_.init(allocator_, SCHEMA_VERSION, rowkey_cnt_, column_cnt_, out_cols));

  OK(sstable_.init(table_key_));
  OK(sstable_.set_storage_file_handle(get_storage_file_handle()));
  OK(sstable_.open(sstable_meta_));

  OK(sstable_.close());
}

void ObMultiVersionSSTableTest::prepare_data_start(const char** micro_data, const int64_t rowkey_cnt,
    const int64_t snapshot_version, const char* compressor_name, const ObRowStoreType row_store_type,
    const int64_t data_version)
{
  prepare_data_start(sstable_, micro_data, rowkey_cnt, snapshot_version, compressor_name, row_store_type, data_version);
}

void ObMultiVersionSSTableTest::prepare_data_start(ObSSTable& sstable, const char** micro_data,
    const int64_t rowkey_cnt, const int64_t snapshot_version,
    const char* compressor_name,          // = "none",
    const ObRowStoreType row_store_type,  // = FLAT_ROW_STORE
    const int64_t data_version,           // = DATA_VERSION
    const int64_t start_log_ts,           // = -1
    const int64_t end_log_ts,             // = -1
    const int64_t max_log_ts)             // = -1

{
  int ret = OB_SUCCESS;
  ObMacroDataSeq start_seq(0);
  ASSERT_FALSE(is_open_);

  macro_writer_.reset();
  column_map_.reset();
  sstable_meta_.reset();
  desc_.reset();
  writer_.reuse();
  data_iter_cursor_ = 0;

  for (int64_t i = 0; i < MAX_MICRO_BLOCK_CNT; ++i) {
    data_iter_[i].reset();
  }

  table_key_.table_type_ = SPARSE_ROW_STORE == row_store_type ? ObITable::MULTI_VERSION_SPARSE_MINOR_SSTABLE
                                                              : ObITable::MULTI_VERSION_MINOR_SSTABLE;
  table_key_.pkey_ = ObPartitionKey(combine_id(TENANT_ID, TABLE_ID), 0, 0);
  table_key_.table_id_ = combine_id(TENANT_ID, TABLE_ID);
  table_key_.version_ = ObVersion(1, 0);
  table_key_.trans_version_range_.multi_version_start_ = 0;
  table_key_.trans_version_range_.base_version_ = 0;
  table_key_.trans_version_range_.snapshot_version_ = snapshot_version;
  table_key_.log_ts_range_.start_log_ts_ = start_log_ts;
  table_key_.log_ts_range_.end_log_ts_ = end_log_ts;
  table_key_.log_ts_range_.max_log_ts_ = max_log_ts;

  OK(data_iter_[0].from(micro_data[0]));
  rowkey_cnt_ = rowkey_cnt;
  column_cnt_ = data_iter_[0].get_column_cnt();

  prepare_sstable_meta(snapshot_version);
  prepare_data_store_desc(compressor_name, row_store_type, data_version);
  common::ObArray<share::schema::ObColDesc> out_cols;
  for (int i = 0; i < column_cnt_; i++) {
    share::schema::ObColDesc col_desc;
    col_desc.col_type_ = *data_iter_[0].get_column_type();
    col_desc.col_id_ = column_ids_[i];
    OK(out_cols.push_back(col_desc));
  }
  OK(column_map_.init(allocator_, SCHEMA_VERSION, rowkey_cnt_, column_cnt_, out_cols));

  OK(sstable.init(table_key_));
  OK(sstable.set_storage_file_handle(get_storage_file_handle()));
  OK(sstable.open(sstable_meta_));

  ret = macro_writer_.open(desc_, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  is_open_ = true;
}

void ObMultiVersionSSTableTest::prepare_one_macro(const char** micro_data, const int64_t micro_cnt,
    const int64_t max_merged_trans_version, const char* col_id_array, const int64_t* col_cnt,
    const bool contain_uncommitted)
{
  int64_t parsed_micro_cnt = 0;
  ASSERT_TRUE(data_iter_cursor_ < MAX_MICRO_BLOCK_CNT);
  ASSERT_TRUE(is_open_);
  blocksstable::ObMicroBlock micro_block;
  ObStoreRowkey last_end_key;
  ObMicroBlockDesc desc;
  STORAGE_LOG(INFO, "prepare_one_macro", K(micro_cnt), K(col_cnt));
  int index = 0;
  ObMockStoreRowIterator input_column_id_iter;
  macro_writer_.macro_blocks_[macro_writer_.current_index_].update_max_merged_trans_version(max_merged_trans_version);
  if (contain_uncommitted) {
    macro_writer_.macro_blocks_[macro_writer_.current_index_].set_contain_uncommitted_row();
  }
  if (0 == data_iter_cursor_) {
    if (SPARSE_ROW_STORE == desc_.row_store_type_) {
      ASSERT_EQ(OB_SUCCESS, input_column_id_iter.from(col_id_array));
      build_sparse_micro_block(
          data_iter_[0], ObStoreRowkey::MIN_STORE_ROWKEY, micro_block, input_column_id_iter, index, col_cnt);
    } else {
      build_micro_block(data_iter_[0], ObStoreRowkey::MIN_STORE_ROWKEY, micro_block, desc_.row_store_type_);
    }
    macro_writer_.last_key_ = ObStoreRowkey::MIN_STORE_ROWKEY;
    last_end_key = micro_block.range_.end_key_;
    OK(macro_writer_.build_micro_block_desc(micro_block, desc));
    STORAGE_LOG(DEBUG, "force split", K(parsed_micro_cnt), K(micro_cnt));
    OK(macro_writer_.write_micro_block(desc, parsed_micro_cnt == micro_cnt - 1));
    parsed_micro_cnt++;
    data_iter_cursor_++;
  }

  for (; parsed_micro_cnt < micro_cnt && data_iter_cursor_ < MAX_MICRO_BLOCK_CNT;
       parsed_micro_cnt++, data_iter_cursor_++) {
    OK(data_iter_[data_iter_cursor_].from(micro_data[parsed_micro_cnt]));
    ObStoreRow* last_row = NULL;
    OK(data_iter_[data_iter_cursor_ - 1].get_row(data_iter_[data_iter_cursor_ - 1].count() - 1, last_row));
    ASSERT_TRUE(NULL != last_row);
    ObStoreRowkey start_key(last_row->row_val_.cells_, rowkey_cnt_);
    if (SPARSE_ROW_STORE == desc_.row_store_type_) {
      ASSERT_EQ(OB_SUCCESS, input_column_id_iter.from(col_id_array));
      build_sparse_micro_block(
          data_iter_[data_iter_cursor_], start_key, micro_block, input_column_id_iter, index, col_cnt);
    } else {
      build_micro_block(data_iter_[data_iter_cursor_], start_key, micro_block, desc_.row_store_type_);
    }
    STORAGE_LOG(INFO, "prepare_one_macro", K(micro_cnt), K(col_cnt));
    macro_writer_.last_key_ = last_end_key;
    OK(macro_writer_.build_micro_block_desc(micro_block, desc));
    if (parsed_micro_cnt == micro_cnt - 1) {
      const bool force_split = true;
      STORAGE_LOG(DEBUG, "force split", K(parsed_micro_cnt), K(micro_cnt));
      OK(macro_writer_.write_micro_block(desc, force_split));
    } else {
      OK(macro_writer_.write_micro_block(desc));
    }
    last_end_key = micro_block.range_.end_key_;
  }
}

void ObMultiVersionSSTableTest::prepare_data_end()
{
  prepare_data_end(sstable_);
}

void ObMultiVersionSSTableTest::prepare_data_end(ObSSTable& sstable)
{
  ASSERT_TRUE(is_open_);
  OK(macro_writer_.close());
  OK(sstable.append_macro_blocks(macro_writer_.get_macro_block_write_ctx()));
  OK(sstable.close());
  is_open_ = false;
}

void ObMultiVersionSSTableTest::prepare_data(const char** micro_data, const int64_t micro_cnt, const int64_t rowkey_cnt,
    const int64_t snapshot_version, const char* compressor_name, const ObRowStoreType row_store_type,
    const int64_t data_version, const char* col_id_array, const int64_t* col_cnt)
{
  prepare_data_start(micro_data, rowkey_cnt, snapshot_version, compressor_name, row_store_type, data_version);
  prepare_one_macro(micro_data, micro_cnt, snapshot_version, col_id_array, col_cnt);
  prepare_data_end();
}

void ObMultiVersionSSTableTest::make_range(const ObStoreRow* start, const ObStoreRow* end, ObExtStoreRange& ext_range)
{
  ext_range.get_range().start_key_.assign(start->row_val_.cells_, rowkey_cnt_ - 2);
  ext_range.get_range().end_key_.assign(end->row_val_.cells_, rowkey_cnt_ - 2);
  ext_range.get_range().border_flag_.set_inclusive_start();
  ext_range.get_range().border_flag_.set_inclusive_end();
  ASSERT_TRUE(OB_SUCCESS == ext_range.to_collation_free_range_on_demand_and_cutoff_range(allocator_));
}

int ObMultiVersionSSTableTest::compare_skip_iter(ObMockIterator& res_iter, ObStoreRowIterator* scanner,
    const common::ObIArray<SkipInfo>& skip_infos, const common::ObIArray<ObStoreRowkey*>& gap_keys,
    const common::ObArray<int64_t>& skip_range_idx)
{
  int ret = OB_SUCCESS;
  int ret1 = OB_SUCCESS;
  int ret2 = OB_SUCCESS;
  const ObStoreRow* left_row = NULL;
  const ObStoreRow* right_row = NULL;
  bool bool_ret = true;
  int64_t idx = 0;
  while (OB_SUCC(ret)) {
    ret1 = res_iter.get_next_row(left_row);
    ret2 = scanner->get_next_row(right_row);
    STORAGE_LOG(INFO, "get_next_row", K(idx), K(*left_row), K(*right_row));
    if (ret1 == ret2) {
      if (common::OB_SUCCESS == ret1 && left_row && right_row) {
        bool_ret = ObMockIterator::equals(*left_row, *right_row, false, false);
        STORAGE_LOG(INFO, "compare result", K(bool_ret));
        ret = bool_ret ? OB_SUCCESS : OB_ERROR;
      } else {
        // must be OB_ITER_END
        ret = common::OB_ITER_END == ret1 ? OB_SUCCESS : ret1;
        break;
      }
    } else {
      ret = OB_ERROR;
    }

    if (OB_SUCC(ret)) {
      idx++;
      int64_t i = 0;
      for (i = 0; OB_SUCC(ret) && i < skip_infos.count(); ++i) {
        if (skip_infos.at(i).start_key_ == idx) {
          break;
        }
      }

      if (i != skip_infos.count()) {
        idx = skip_infos.at(i).gap_key_;
        OB_LOGGER.set_log_level("DEBUG");
        STORAGE_LOG(DEBUG, "start to skip range");
        ret = scanner->skip_range(skip_range_idx.at(i), gap_keys.at(i), true);
        STORAGE_LOG(DEBUG, "end to skip range", K(ret));
        OB_LOGGER.set_log_level("INFO");
        STORAGE_LOG(INFO, "skip range to", K(*gap_keys.at(i)), K(*left_row));
      }
    }
  }
  return ret;
}

int ObMultiVersionSSTableTest::convert_iter_to_gapkeys(
    const int64_t rowkey_column_cnt, ObMockIterator& gap_iter, common::ObIArray<ObStoreRowkey*>& gap_keys)
{
  int ret = OB_SUCCESS;
  const ObStoreRow* row = NULL;
  gap_keys.reset();
  while (OB_SUCC(ret)) {
    if (OB_FAIL(gap_iter.get_next_row(row))) {
      if (OB_ITER_END) {
        ret = OB_SUCCESS;
        break;
      }
    } else {
      void* buf = NULL;
      if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObStoreRowkey)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "fail to alloc memory", K(ret));
      } else {
        ObStoreRowkey* rowkey = new (buf) ObStoreRowkey();
        ObStoreRowkey org_rowkey;
        org_rowkey.assign(row->row_val_.cells_, rowkey_column_cnt);
        if (OB_FAIL(org_rowkey.deep_copy(*rowkey, allocator_))) {
          STORAGE_LOG(WARN, "fail to deep copy rowkey", K(ret));
        } else if (OB_FAIL(gap_keys.push_back(rowkey))) {
          STORAGE_LOG(WARN, "fail to push back gap key", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObMultiVersionSSTableTest::convert_to_multi_row(const ObStoreRow& org_row, const int64_t snapshot_version,
    const ObRowDml row_dml, const bool is_compacted_row, const bool is_last_row, const bool is_first_row,
    ObStoreRow& multi_row)
{
  if (OB_ISNULL(org_row.column_ids_) || OB_ISNULL(multi_row.column_ids_)) {
    STORAGE_LOG(ERROR, "column id array is null", K(org_row.column_ids_), K(multi_row.column_ids_));
  }
  for (int64_t i = 0; i < table_schema_.get_rowkey_column_num(); ++i) {
    multi_row.row_val_.cells_[i] = org_row.row_val_.cells_[i];
    multi_row.column_ids_[i] = org_row.column_ids_[i];
  }
  multi_row.row_val_.cells_[table_schema_.get_rowkey_column_num()].set_int(-snapshot_version);
  multi_row.column_ids_[table_schema_.get_rowkey_column_num()] = OB_HIDDEN_TRANS_VERSION_COLUMN_ID;
  multi_row.row_val_.cells_[table_schema_.get_rowkey_column_num() + 1].set_int(0);
  multi_row.column_ids_[table_schema_.get_rowkey_column_num() + 1] = OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID;
  for (int64_t i = table_schema_.get_rowkey_column_num(); i < table_schema_.get_column_count(); ++i) {
    multi_row.row_val_.cells_[i + 2] = org_row.row_val_.cells_[i];
    multi_row.column_ids_[i + 2] = org_row.column_ids_[i];
  }

  multi_row.row_type_flag_.reset();
  multi_row.dml_ = row_dml;
  multi_row.is_sparse_row_ = org_row.is_sparse_row_;
  if (T_DML_DELETE == row_dml) {
    multi_row.flag_ = common::ObActionFlag::OP_DEL_ROW;
  } else {
    multi_row.flag_ = common::ObActionFlag::OP_ROW_EXIST;
  }
  multi_row.first_dml_ = T_DML_UPDATE;

  if (is_compacted_row) {
    multi_row.row_type_flag_.set_compacted_multi_version_row(true);
  }
  if (is_last_row) {
    multi_row.row_type_flag_.set_last_multi_version_row(true);
  }
  if (is_first_row) {
    multi_row.row_type_flag_.set_first_multi_version_row(true);
  }
}

}  // namespace unittest
}  // namespace oceanbase
