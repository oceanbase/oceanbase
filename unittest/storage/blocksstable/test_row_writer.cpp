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
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "lib/number/ob_number_v2.h"
#include "ob_row_generate.h"

#define protected public
#define private public

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace number;
using namespace share::schema;

namespace unittest
{
class TestRowWriter : public ::testing::Test
{
public:
  static const int64_t rowkey_column_count = 1;
  // Every ObObjType from ObTinyIntType to ObHexStringType inclusive.
  // Skip ObNullType and ObExtendType because for external usage, a column type
  // can't be NULL or NOP.
  static const int64_t column_num = ObHexStringType;

public:
  TestRowWriter();
  virtual void SetUp();
  virtual void TearDown();
  void alloc(char *&ptr, const int64_t size);

protected:
  ObRowGenerate row_generate_;
  ObColumnMap column_map_;

private:
  ObArenaAllocator allocator_;
};

TestRowWriter::TestRowWriter()
  :allocator_(ObModIds::TEST)
{

}

void TestRowWriter::SetUp()
{
  const int64_t table_id = 3001;
  ObTableSchema table_schema;
  ObColumnSchemaV2 column;
  oceanbase::common::ObClusterVersion::get_instance().refresh_cluster_version("1.4.70");
  //init table schema
  table_schema.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema.set_table_name("test_row_writer"));
  table_schema.set_tenant_id(1);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_column_count);
  table_schema.set_max_used_column_id(column_num);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < column_num; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_table_id(table_id);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    if(obj_type == common::ObIntType){
      column.set_rowkey_position(1);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
  //init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema));
}

void TestRowWriter::TearDown()
{

}

void TestRowWriter::alloc(char *&ptr, const int64_t size)
{
  ptr = reinterpret_cast<char*>(allocator_.alloc(size));
  ASSERT_TRUE(NULL != ptr);
}

TEST_F(TestRowWriter, test_init)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow valid_store_row;
  ObStoreRow invalid_store_row;
  valid_store_row.row_val_.cells_ = objs;
  valid_store_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(valid_store_row));

  ObNewRow invalid_new_row;
  ObNewRow valid_new_row;
  valid_new_row.cells_ = objs;
  valid_new_row.count_ = column_num;
  char *buf = NULL;
  alloc(buf, 2 * 1024 * 1024);
  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  //init success
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ret = row_writer.write(rowkey_column_count, valid_store_row, buf, 2 * 1024 * 1024,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SUCCESS, ret);

  pos = 0;
  ret = row_writer.write(valid_new_row, buf, 2 * 1024 * 1024, FLAT_ROW_STORE, pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  //invalid buf
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ret = row_writer.write(rowkey_column_count, valid_store_row, NULL, 2 * 1024 * 1024,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  pos = 0;
  ret = row_writer.write(valid_new_row, NULL, 2 * 1024 * 1024, FLAT_ROW_STORE, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, pos);

  //invalid buf_size
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ret = row_writer.write(rowkey_column_count, valid_store_row, buf, 0,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  //invalid pos
  pos = 2 * 1024 * 1024;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ret = row_writer.write(rowkey_column_count, valid_store_row, buf, 2 * 1024 * 1024,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
  ASSERT_EQ(2 * 1024 * 1024, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  //invalid store_row
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ret = row_writer.write(rowkey_column_count, invalid_store_row, buf, 2 * 1024 * 1024,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  //invalid rowkey_column_count
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ret = row_writer.write(-1, valid_store_row, buf, 2 * 1024 * 1024,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  //invalid new_row
  pos = 0;
  ret = row_writer.write(invalid_new_row, buf, 2 * 1024 * 1024, FLAT_ROW_STORE, pos);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(0, pos);
}

TEST_F(TestRowWriter, buf_not_enough)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow valid_store_row;
  ObStoreRow invalid_store_row;
  valid_store_row.row_val_.cells_ = objs;
  valid_store_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(valid_store_row));

  ObNewRow invalid_new_row;
  ObNewRow valid_new_row;
  valid_new_row.cells_ = objs;
  valid_new_row.count_ = column_num;
  char *buf = NULL;
  alloc(buf, 2 * 1024 * 1024);
  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;

  //row header buf not enough
  ret = row_writer.write(rowkey_column_count, valid_store_row,
      buf, 4, pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  //rowkey data not enough
  ret = row_writer.write(rowkey_column_count, valid_store_row, buf, 8,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  //normal data not enough
  ret = row_writer.write(rowkey_column_count, valid_store_row, buf, 200,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  //column index not enough
  ret = row_writer.write(1, valid_store_row, buf, 430 - 16,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  //new row not enough
  ret = row_writer.write(valid_new_row, buf, 1, FLAT_ROW_STORE, pos);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
  ASSERT_EQ(0, pos);

  //buf not enough
  pos = 10;
  ret = row_writer.write(valid_new_row, buf, pos, FLAT_ROW_STORE, pos);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH, ret);
}

TEST_F(TestRowWriter, data_type)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;

  ObFlatRowReader row_reader;
  ObStoreRow reader_row;
  oceanbase::common::ObObj reader_objs[column_num];
  reader_row.row_val_.cells_ = reader_objs;
  reader_row.row_val_.count_ = column_num;

  char *buf = NULL;
  alloc(buf, 2 * 1024 * 1024);
  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;
  int64_t read_pos = 0;
  const int64_t test_row_num = 10;

  for(int64_t j = 0; j < test_row_num; ++j){
    pos = 0;
    rowkey_start_pos = 0;
    rowkey_length = 0;
    read_pos = 0;

    //test every ObObjType from ObNullType to ObExtendType
    bool exist = false;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
    //column index size is four, one, two will tested normally
    if(0 == j){
      char *ptr = NULL;
      ObString str;
      //TODO: OB_MAX_VARCHAR_LENGTH now is 4 * 1024 * 1024, too large to support
      alloc(ptr, 256L * 1024);
      memset(ptr, 0, 256L * 1024);
      str.assign_ptr(ptr, 256L * 1024);

      for (int64_t i = 0; i < column_num; ++i){
        if(ObVarcharType == objs[i].get_type()){
          objs[i].set_varchar(str);
          objs[i].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          break;
        }
      }
    }

    ret = row_writer.write(rowkey_column_count, writer_row, buf, 2 * 1024 * 1024,
        pos, rowkey_start_pos, rowkey_length);
    ASSERT_EQ(OB_SUCCESS, ret);
    //init ObColumnMap
    column_map_.reuse();
    ObArray<ObColDesc> columns;
    ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));
    for (int64_t i = 0; i < column_num; ++i){
        ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
        ObObjMeta obj_meta;
        obj_meta.set_type(obj_type);
        if (ObVarcharType == obj_type || ObCharType == obj_type) {
          obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
        }
        columns[i].col_type_ = obj_meta;
    }
    ASSERT_EQ(OB_SUCCESS, column_map_.init(allocator_,
                                           row_generate_.get_schema().get_schema_version(),
                                           row_generate_.get_schema().get_rowkey_column_num(),
                                           column_num,
                                           columns));

    ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
    ASSERT_TRUE(OB_SUCCESS == ret)
        << "\n j: " << j;

    if(0 != j){//we has change the rule
      ASSERT_EQ(OB_SUCCESS, row_generate_.check_one_row(reader_row, exist));
      ASSERT_TRUE(exist);
      ASSERT_TRUE(column_num  == reader_row.row_val_.count_);
    }
    //every obj should equal
    for (int64_t i = 0; i < column_num ; ++i){
      ASSERT_TRUE(writer_row.row_val_.cells_[i] == reader_row.row_val_.cells_[i])
        << "\n i: " << i
        << "\n writer:  "<< to_cstring(writer_row.row_val_.cells_[i])
        << "\n reader:  " << to_cstring(reader_row.row_val_.cells_[i]);
    }
  }
  //unsupported data type
  objs[0].set_type(static_cast<ObObjType>(ObMaxType));
  pos = 0;
  rowkey_start_pos = 0;
  rowkey_length = 0;
  ret = row_writer.write(rowkey_column_count, writer_row, buf, 2 * 1024 * 1024,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);
}

TEST_F(TestRowWriter, char_binary_overflow)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow valid_store_row;
  ObStoreRow invalid_store_row;
  valid_store_row.row_val_.cells_ = objs;
  valid_store_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(valid_store_row));

  ObObj obj;
  ObString str;
  char *buf = NULL;
  char *ptr = NULL;
  alloc(buf, 2 * 1024 * 1024);
  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;

  alloc(ptr, OB_MAX_VARCHAR_LENGTH + 1);
  memset(ptr, 0, OB_MAX_VARCHAR_LENGTH + 1);
  str.assign_ptr(ptr, OB_MAX_VARCHAR_LENGTH + 1);
  obj.set_varchar(str);
  obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  valid_store_row.row_val_.cells_[0] = obj;
  ret = row_writer.write(1, valid_store_row, buf, 2 * 1024 * 1024,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);

  obj.set_binary(str);
  valid_store_row.row_val_.cells_[0] = obj;
  ret = row_writer.write(1, valid_store_row, buf, 2 * 1024 * 1024,
      pos, rowkey_start_pos, rowkey_length);
  ASSERT_EQ(OB_SIZE_OVERFLOW, ret);
  ASSERT_EQ(0, pos);
  ASSERT_EQ(0, rowkey_start_pos);
  ASSERT_EQ(0, rowkey_length);
}

TEST_F(TestRowWriter, write_only_key_column)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  oceanbase::common::ObObj objs[column_num];
  ObStoreRow valid_store_row;
  ObStoreRow invalid_store_row;
  valid_store_row.row_val_.cells_ = objs;
  valid_store_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(valid_store_row));

  int32_t buf_size = 1024;
  char *buf = NULL;
  alloc(buf, buf_size);

  char *key_ptr = nullptr;
  alloc(key_ptr, 512);
  ObObj key_obj;
  key_obj.set_varchar(key_ptr, 512);
  key_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  valid_store_row.row_val_.cells_[0] = key_obj;

  char *col_ptr = nullptr;
  alloc(col_ptr, 1024);
  ObObj col_obj;
  col_obj.set_varchar(col_ptr, 1024);
  col_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  valid_store_row.row_val_.cells_[1] = col_obj;

  int64_t pos = 0;
  int64_t rowkey_start_pos = 0;
  int64_t rowkey_length = 0;

  ret = row_writer.write(1, valid_store_row, buf, buf_size,
      pos, rowkey_start_pos, rowkey_length, true/*only_row_key*/);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_LE(0, pos);
  ASSERT_LE(0, rowkey_start_pos);
  // 1 byte collation type + 4 byte length + 512 byte data length
  ASSERT_EQ(517, rowkey_length);
}



}//end namespace tests
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
