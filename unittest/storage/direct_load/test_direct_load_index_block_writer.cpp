/**
 * Copyright (c) 2023 OceanBase
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
#include <cstdlib>
#include <ctime>
#include "../unittest/storage/blocksstable/ob_data_file_prepare.h"
#include "../unittest/storage/blocksstable/ob_row_generate.h"
#include "observer/table_load/ob_table_load_partition_location.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "storage/direct_load/ob_direct_load_tmp_file.h"
#include "storage/direct_load/ob_direct_load_sstable_scanner.h"
#include "storage/ob_i_store.h"
namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

static ObSimpleMemLimitGetter getter;

namespace unittest
{
class TestIndexBlockWriter : public TestDataFilePrepare
{
public:
  static const int64_t rowkey_column_count = 2;
  // Every ObObjType from ObTinyIntType to ObHexStringType inclusive.
  // Skip ObNullType and ObExtendType because for external usage, a column type
  // can't be NULL or NOP.
  static const int64_t column_num = ObHexStringType;
  static const int64_t macro_block_size = 2L * 8 * 1024L;
  static const int64_t SNAPSHOT_VERSION = 2;

public:
  TestIndexBlockWriter() : TestDataFilePrepare(&getter, "TestIndexBlockWriter", 2 * 1024 * 1024, 2048){};
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase() {}
  static void TearDownTestCase() {}
  void test_alloc(char *&ptr, const int64_t size);

private:
  void prepare_schema();

protected:
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObDirectLoadTmpFileManager *file_mgr_;
  ObArenaAllocator allocator_;
};

void TestIndexBlockWriter::test_alloc(char *&ptr, const int64_t size)
{
  ptr = reinterpret_cast<char *>(allocator_.alloc(size));
  ASSERT_TRUE(nullptr != ptr);
}

void TestIndexBlockWriter::prepare_schema()
{
  ObColumnSchemaV2 column;
  int64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  // init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_macro_file"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(rowkey_column_count);
  table_schema_.set_max_used_column_id(column_num);
  table_schema_.set_block_size(micro_block_size);
  table_schema_.set_compress_func_name("none");
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    if (obj_type == common::ObIntType) {
      column.set_rowkey_position(1);
    } else if (obj_type == common::ObNumberType) {
      column.set_rowkey_position(2);
    } else {
      column.set_rowkey_position(0);
    }
    column.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  ObTmpFileManager::get_instance().destroy();
}

void TestIndexBlockWriter::SetUp()
{
  int ret = OB_SUCCESS;
  // init file
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TestDataFilePrepare::SetUp();
  prepare_schema();
  file_mgr_ = OB_NEWx(ObDirectLoadTmpFileManager, (&allocator_));
  ret = file_mgr_->init(table_schema_.get_tenant_id());
  ASSERT_EQ(OB_SUCCESS, ret);
  // init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_));

  ret = getter.add_tenant(1, 8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  ret = ObTmpFileManager::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);
  ObTenantEnv::set_tenant(&tenant_ctx);
}

void TestIndexBlockWriter::TearDown()
{
  file_mgr_->~ObDirectLoadTmpFileManager();
  table_schema_.reset();
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  TestDataFilePrepare::TearDown();
}

TEST_F(TestIndexBlockWriter, test_write_and_read)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> array;
  int64_t sum = 0;
  ObDirectLoadIndexBlockWriter index_block_writer;
  ObDirectLoadIndexBlockReader index_block_reader;
  uint64_t tenant_id = table_schema_.get_tenant_id();
  ObDirectLoadTmpFileHandle file_handle;

  const int64_t index_block_size = DIO_ALIGN_SIZE;
  int64_t dir_id = -1;
  ret = file_mgr_->alloc_dir(dir_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret= file_mgr_->alloc_file(dir_id, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  // init index block writer
  ret = index_block_writer.init(tenant_id, DIO_ALIGN_SIZE, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 1000; ++i) {
    ObDirectLoadIndexBlockItem item;
    sum += rand() % 1024;
    item.end_offset_ = sum;
    array.push_back(item.end_offset_);
    ret = index_block_writer.append_row(100, item);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  ret = index_block_writer.close();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = index_block_reader.init(1, DIO_ALIGN_SIZE, file_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < 1000; ++i) {
    ObDirectLoadIndexInfo info;
    ret = index_block_reader.get_index_info(i, info);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_TRUE(array.at(i) == (info.offset_ + info.size_));
  }
}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_direct_load_index_block_writer.log");
  OB_LOGGER.set_file_name("test_direct_load_index_block_writer.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
