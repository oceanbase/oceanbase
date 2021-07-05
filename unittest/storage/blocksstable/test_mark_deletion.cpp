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
#define protected public
#define private public
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/slog/ob_base_storage_logger.h"
#include "storage/blocksstable/ob_micro_block_index_mgr.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "ob_row_generate.h"
#include "ob_data_file_prepare.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestMarkDeletion : public TestDataFilePrepare {
public:
  TestMarkDeletion();
  virtual ~TestMarkDeletion();
  virtual void SetUp();
  virtual void TearDown();

protected:
  void prepare_schema();
  void prepare_data();
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObColumnMap column_map_;
  ObSSTable inc_store_;
  ObITable::TableKey table_key_;
  ObArenaAllocator allocator_;
};

TestMarkDeletion::TestMarkDeletion() : TestDataFilePrepare("TestMacroBlockWriter", 2 * 1024 * 1024)
{
  ObAddr self;
  rpc::frame::ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  obrpc::ObCommonRpcProxy rs_rpc_proxy;
  share::ObRsMgr rs_mgr;
  int64_t tenant_id = 1;
  self.set_ip_addr("127.0.0.1", 8086);
  ObTenantManager::get_instance().init(
      self, rpc_proxy, rs_rpc_proxy, rs_mgr, &req_transport, &ObServerConfig::get_instance());
  ObTenantManager::get_instance().add_tenant(tenant_id);
  ObTenantManager::get_instance().set_tenant_mem_limit(
      tenant_id, 2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
}

TestMarkDeletion::~TestMarkDeletion()
{}

void TestMarkDeletion::SetUp()
{
  int ret = OB_SUCCESS;
  TestDataFilePrepare::SetUp();
  prepare_schema();
  row_generate_.reset();
  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  table_key_.pkey_.table_id_ = table_schema_.get_table_id();
  table_key_.pkey_.part_id_ = 1;
  table_key_.pkey_.part_cnt_ = 1;
  table_key_.pkey_.assit_id_ = 0;
  table_key_.table_id_ = table_schema_.get_table_id();
  table_key_.table_type_ = ObITable::TableType::MINOR_SSTABLE;
  table_key_.version_ = 1;
  table_key_.trans_version_range_.base_version_ = 1;
  table_key_.trans_version_range_.multi_version_start_ = 1;
  table_key_.trans_version_range_.snapshot_version_ = INT64_MAX - 2;
  ASSERT_TRUE(table_key_.is_valid());
}

void TestMarkDeletion::TearDown()
{
  row_generate_.reset();
  table_schema_.reset();
  TestDataFilePrepare::TearDown();
}

void TestMarkDeletion::prepare_schema()
{
  ObColumnSchemaV2 column;
  uint64_t table_id = combine_id(1, 3001);
  int64_t micro_block_size = 16 * 1024;
  column_map_.reuse();
  // init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_row_writer"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(TEST_ROWKEY_COLUMN_CNT);
  table_schema_.set_max_used_column_id(TEST_COLUMN_CNT);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  table_schema_.set_storage_format_version(OB_STORAGE_FORMAT_VERSION_V4);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if (obj_type == common::ObIntType) {
      column.set_rowkey_position(1);
      column.set_order_in_rowkey(ObOrderType::ASC);
    } else if (obj_type == common::ObVarcharType) {
      column.set_rowkey_position(2);
      column.set_order_in_rowkey(ObOrderType::DESC);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  // build column map
  ObArray<ObColDesc> col_desc_array;
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_desc_array));
  ObObjMeta meta;
  meta.set_type(common::ObIntType);
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  col_desc_array[0].col_type_ = meta;
  meta.set_type(common::ObVarcharType);
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  col_desc_array[0].col_type_ = meta;
  int64_t store_index = 2;
  for (int64_t i = 0; i < TEST_COLUMN_CNT; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    if (common::ObIntType != obj_type && common::ObVarcharType != obj_type) {
      ObObjMeta meta;
      meta.set_type(obj_type);
      meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      col_desc_array[store_index].col_type_ = meta;
      ++store_index;
    }
  }
  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          table_schema_.get_schema_version(),
          table_schema_.get_rowkey_column_num(),
          TEST_COLUMN_CNT,
          col_desc_array));
}

void TestMarkDeletion::prepare_data()
{
  int ret = OB_SUCCESS;
  int64_t data_version = 1;
  // ObSSTable *data_sstable = NULL;
  ObDataStoreDesc data_desc;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);

  ObRowGenerate data_row_generate;
  ObPartitionMeta meta;

  meta.table_id_ = table_schema_.get_table_id();
  meta.data_version_ = data_version;
  meta.index_table_count_ = 1;
  meta.store_type_ = MINOR_SSSTORE;

  // prepare base sstable
  // ret = base_store_.create_new_sstable(data_sstable, 1);
  // ASSERT_EQ(OB_SUCCESS, ret);
  // ASSERT_TRUE(NULL != data_sstable);
  ObCreateSSTableParamWithTable param;
  param.table_key_ = table_key_;
  param.logical_data_version_ = table_key_.version_;
  param.create_snapshot_version_ = INT64_MAX - 2;
  param.progressive_merge_end_version_ = 0;
  param.progressive_merge_start_version_ = 0;
  param.schema_ = &table_schema_;
  param.schema_version_ = 0;

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
  ret = writer.open(data_desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.cells_ = cells;
  row.row_val_.count_ = TEST_COLUMN_CNT;
  ObStoreRow idx_row;
  int64_t free_macro_block_cnt = get_file_system().get_free_macro_block_count();
  int64_t row_id = 0;
  while (free_macro_block_cnt - get_file_system().get_free_macro_block_count() > 0) {
    ret = row_generate_.get_next_row(row_id, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_id;
  }

  int64_t border_row_id = row_id;
  row_id += 10000;
  free_macro_block_cnt = get_file_system().get_free_macro_block_count();
  while (free_macro_block_cnt - get_file_system().get_free_macro_block_count() > 0) {
    ret = row_generate_.get_next_row(row_id, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ++row_id;
  }
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);

  // prepare inc ssstore
  ret = inc_store_.init(table_key_);
  ASSERT_EQ(OB_SUCCESS, ret);
  // ObSSTable *inc_sstable = NULL;
  writer.reset();

  ret = writer.open(data_desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);
  // inc_store_.create_new_sstable(inc_sstable, 1);
  // ASSERT_TRUE(NULL != inc_sstable);
  ret = inc_store_.open(param);
  ASSERT_EQ(OB_SUCCESS, ret);

  for (int64_t i = border_row_id + 1; i < border_row_id + 10000; ++i) {
    ret = row_generate_.get_next_row(i, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = writer.append_row(row);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = inc_store_.append_macro_blocks(writer.get_macro_block_write_ctx());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = inc_store_.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  // ret = inc_store_.add_sstable(table_schema_.get_table_id(), inc_sstable);
  // ASSERT_EQ(OB_SUCCESS, ret);

  // int64_t lsn = 0;
  // ret = SLOGGER.commit(lsn);
  // ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMarkDeletion, write)
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.cells_ = cells;
  row.row_val_.count_ = TEST_COLUMN_CNT;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObDataStoreDesc desc;
  int64_t data_version = 1;

  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObPGKey pg_key(combine_id(1, table_schema_.get_tablegroup_id()), 1, table_schema_.get_partition_cnt());
  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(pg_key, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = desc.init(table_schema_,
      data_version,
      NULL,
      1,
      MAJOR_MERGE,
      true,
      true,
      pg_key,
      pg_guard.get_partition_group()->get_storage_file_handle());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.open(desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = row_generate_.get_next_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.append_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.build_micro_block(false);
  ASSERT_EQ(OB_SUCCESS, ret);
  writer.macro_blocks_[0].macro_block_deletion_flag_ = true;

  ret = row_generate_.get_next_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.append_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.build_micro_block(false);
  ret = writer.close();

  ObMacroBlockMetaHandle meta_handle;
  const ObMacroBlockMeta* meta = NULL;
  ObFullMacroBlockMeta full_meta;

  full_meta = writer.get_macro_block_write_ctx().macro_block_meta_list_.at(0);
  ASSERT_TRUE(full_meta.is_valid());
  ASSERT_EQ(false, full_meta.meta_->macro_block_deletion_flag_);

  SLOGGER.abort();
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
