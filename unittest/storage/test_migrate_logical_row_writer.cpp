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
#include "storage/blocksstable/ob_row_generate.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/config/ob_server_config.h"
#include "storage/ob_partition_base_data_ob_reader.h"
#include "storage/ob_migrate_logic_row_writer.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestMigrateLogicalRowWriter : public TestDataFilePrepare {
public:
  TestMigrateLogicalRowWriter();
  virtual ~TestMigrateLogicalRowWriter();
  virtual void SetUp();
  virtual void TearDown();

protected:
  void prepare_schema();
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObColumnMap column_map_;
  ObArenaAllocator allocator_;
};

TestMigrateLogicalRowWriter::TestMigrateLogicalRowWriter()
    : TestDataFilePrepare("TestMigrateLogicalRowWriter", 2 * 1024 * 1024), column_map_()
{}

TestMigrateLogicalRowWriter::~TestMigrateLogicalRowWriter()
{}

void TestMigrateLogicalRowWriter::SetUp()
{
  int ret = OB_SUCCESS;
  TestDataFilePrepare::SetUp();
  prepare_schema();
  row_generate_.reset();
  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestMigrateLogicalRowWriter::TearDown()
{
  row_generate_.reset();
  table_schema_.reset();
  TestDataFilePrepare::TearDown();
}

void TestMigrateLogicalRowWriter::prepare_schema()
{
  ObColumnSchemaV2 column;
  int64_t table_id = 3001;
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
    } else if (obj_type == common::ObVarcharType) {
      column.set_rowkey_position(2);
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
  col_desc_array[1].col_type_ = meta;
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

TEST_F(TestMigrateLogicalRowWriter, border)
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  ObDataStoreDesc desc;
  ObLogicRowFetcher logic_row_fetcher;
  ObMigrateLogicRowWriter logic_row_writer;
  ObPGKey pg_key(combine_id(1, 3001), 1, 0);

  ObIPartitionGroupGuard pg_guard;
  ObStorageFile* file = NULL;
  ret = ObFileSystemUtil::get_pg_file_with_guard(pg_key, pg_guard, file);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObStorageFileHandle& file_handle = pg_guard.get_partition_group()->get_storage_file_handle();
  ret = logic_row_writer.init(&logic_row_fetcher, pg_key, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  logic_row_writer.reset();
  // invalid use
  ret = logic_row_writer.init(NULL, pg_key, file_handle);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -f test_migrate_logical_row_writer.log*");
  OB_LOGGER.set_file_name("test_migrate_logical_row_writer.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest: test_migrate_logical_row_writer");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
