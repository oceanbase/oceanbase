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
#include "storage/blocksstable/ob_micro_block_writer.h"
#include "storage/blocksstable/ob_micro_block_reader.h"
#include "storage/ob_i_store.h"
#include "ob_row_generate.h"
#include "storage/blocksstable/ob_column_map.h"

namespace oceanbase {
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest {
class TestMicroBlockWriter : public ::testing::Test {
public:
  static const int64_t rowkey_column_count = 2;
  // Every ObObjType from ObTinyIntType to ObHexStringType inclusive.
  // Skip ObNullType and ObExtendType because for external usage, a column type
  // can't be NULL or NOP.
  static const int64_t column_num = ObHexStringType;
  static const int64_t macro_block_size = 2L * 1024 * 1024L;

public:
  TestMicroBlockWriter() : allocator_(ObModIds::TEST){};
  void SetUp();
  virtual void TearDown()
  {}
  static void SetUpTestCase()
  {}
  static void TearDownTestCase()
  {}
  void test_alloc(char*& ptr, const int64_t size);

protected:
  ObRowGenerate row_generate_;
  ObColumnMap column_map_;
  ObArenaAllocator allocator_;
};

void TestMicroBlockWriter::test_alloc(char*& ptr, const int64_t size)
{
  ptr = reinterpret_cast<char*>(allocator_.alloc(size));
  ASSERT_TRUE(nullptr != ptr);
}

void TestMicroBlockWriter::SetUp()
{
  const int64_t table_id = 3001;
  ObTableSchema table_schema;
  ObColumnSchemaV2 column;
  // init table schema
  table_schema.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("test_row_writer"));
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_column_count);
  table_schema.set_max_used_column_id(column_num);
  // init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_collation_type(CS_TYPE_BINARY);
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
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  // init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema));
}

TEST_F(TestMicroBlockWriter, test_init)
{
  int ret = OB_SUCCESS;
  ObMicroBlockWriter writer;

  // invalid macro_block_size
  ret = writer.init(0, 2, 5);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // invalid column_count
  ret = writer.init(1024, 2, 0);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // invalid rowkey_column_count
  ret = writer.init(1024, 6, 5);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // success
  ret = writer.init(1024, 2, 5);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(sizeof(ObMicroBlockHeader) + 4, writer.get_block_size());
  ASSERT_EQ(0, writer.get_row_count());
  ASSERT_EQ(sizeof(ObMicroBlockHeader), writer.get_data_size());
  ObString str;
  str.assign_ptr(NULL, 0);
  ASSERT_TRUE(str == writer.get_last_rowkey());
}

TEST_F(TestMicroBlockWriter, append_success)
{
  int ret = OB_SUCCESS;
  const int64_t test_row_num = 100;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow row;

  ObMicroBlockWriter writer;
  ret = writer.init(macro_block_size, rowkey_column_count, column_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int64_t i = 0; i < test_row_num; ++i) {
    row.row_val_.cells_ = objs;
    row.row_val_.count_ = column_num;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, writer.append_row(row));
  }
  char* buf = NULL;
  int64_t size = 0;
  ret = writer.build_block(buf, size);
  ASSERT_EQ(OB_SUCCESS, ret);

  // check ObMicroBlockWriter right by ObMicroBlockReader and ObRowGenerate
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
  for (int64_t i = 0; i < column_num; ++i) {
    ObObjType obj_type = row.row_val_.cells_[i].get_type();
    ObObjMeta obj_meta;
    obj_meta.set_type(obj_type);
    if (ObVarcharType == obj_type || ObCharType == obj_type || ObExtendType == obj_type) {
      obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    }
    columns[i].col_type_ = obj_meta;
  }
  ASSERT_EQ(OB_SUCCESS,
      column_map_.init(allocator_,
          row_generate_.get_schema().get_schema_version(),
          row_generate_.get_schema().get_rowkey_column_num(),
          column_num,
          columns));
  ObMicroBlockReader reader;
  ObMicroBlockData block(buf, size);
  ret = reader.init(block, &column_map_);
  ASSERT_EQ(OB_SUCCESS, ret);
  oceanbase::common::ObObj block_objs[column_num];
  ObStoreRow read_row;
  read_row.row_val_.cells_ = block_objs;
  read_row.row_val_.count_ = column_num;
  for (int64_t i = 0; i < test_row_num; ++i) {
    ASSERT_EQ(OB_SUCCESS, reader.get_row(i, read_row));
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, row_generate_.check_one_row(read_row, exist));
    ASSERT_TRUE(exist) << "i: " << i;
    // every obj should equal
    for (int64_t j = 0; j < column_num; ++j) {
      ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(i, row));
      ASSERT_TRUE(row.row_val_.cells_[j] == read_row.row_val_.cells_[j])
          << "\n i: " << i << " j: " << j << "\n writer:  " << to_cstring(row.row_val_.cells_[j])
          << "\n reader:  " << to_cstring(read_row.row_val_.cells_[j]);
    }
  }
}

TEST_F(TestMicroBlockWriter, append_row_error)
{
  int ret = OB_SUCCESS;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow row;
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = column_num;
  // not init
  ObMicroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  ASSERT_EQ(OB_NOT_INIT, writer.append_row(row));
  // column count not equal
  ret = writer.init(macro_block_size, rowkey_column_count, column_num + 1);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  ASSERT_EQ(OB_INVALID_ARGUMENT, writer.append_row(row));
  // fail to write not support type
  ret = writer.init(macro_block_size, rowkey_column_count, column_num);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  row.row_val_.cells_[0].set_type(static_cast<ObObjType>(51));
  ASSERT_EQ(OB_NOT_SUPPORTED, writer.append_row(row));
}

TEST_F(TestMicroBlockWriter, build_block_error)
{
  char* buf = NULL;
  int64_t size = 0;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow row;
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = column_num;
  // not init
  ObMicroBlockWriter writer;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(row));
  ASSERT_EQ(OB_NOT_INIT, writer.build_block(buf, size));
}

TEST_F(TestMicroBlockWriter, init_max_column_count)
{
  // data_buffer_ and index_buffer_ in ObMicroBlockWriter should init succeed
  ObMicroBlockWriter writer;
  int64_t ret = writer.init(common::OB_DEFAULT_MACRO_BLOCK_SIZE, 1, common::OB_ROW_MAX_COLUMNS_COUNT);

  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestMicroBlockWriter, append_large_row)
{
  // create a large row whose size is 1.5MB
  int64_t large_row_col_cnt = 2;
  oceanbase::common::ObObj objs[large_row_col_cnt];
  ObStoreRow row;
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = large_row_col_cnt;
  row.flag_ = common::ObActionFlag::OP_ROW_EXIST;

  int32_t value1_size = 1024 * 1024;
  char* ptr1 = nullptr;
  test_alloc(ptr1, value1_size);
  ASSERT_TRUE(nullptr != ptr1);
  oceanbase::common::ObObj obj1;
  obj1.set_varchar(ptr1, value1_size);
  objs[0] = obj1;

  int32_t value2_size = 512 * 1024;
  char* ptr2 = nullptr;
  test_alloc(ptr2, value2_size);
  oceanbase::common::ObObj obj2;
  obj1.set_varchar(ptr2, value2_size);
  objs[0] = obj2;

  ObMicroBlockWriter writer;
  int64_t ret = writer.init(common::OB_DEFAULT_MACRO_BLOCK_SIZE, 1, large_row_col_cnt);
  ASSERT_EQ(OB_SUCCESS, ret);

  // append large row;
  ret = writer.append_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  system("rm -rf test_micro_block_writer.log");
  OB_LOGGER.set_file_name("test_micro_block_writer.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
