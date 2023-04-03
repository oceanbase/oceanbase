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
#include "storage/blocksstable/ob_sort_temp_macro_block_writer.h"
#include "storage/blocksstable/ob_sort_temp_macro_block_reader.h"
#include "storage/blocksstable/ob_data_file.h"
#include "ob_row_generate.h"
#include "ob_data_file_prepare.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace share::schema;

namespace unittest
{
class TestSortTempMacroBlockReaderWriter : public TestDataFilePrepare
{
public:
  TestSortTempMacroBlockReaderWriter();
  virtual ~TestSortTempMacroBlockReaderWriter();
  virtual void prepare_data(const int64_t buf_capacity, char *buf, ObIArray<int64_t> &start_indexes);
  virtual void prepare_one_buffer(const int64_t start_index, char *buf, int64_t &end_index);
  virtual void check_data(const char *buf, const int64_t buf_len, const int64_t start_index);
  virtual void SetUp();
  virtual void TearDown();
protected:
  void prepare_schema();
private:
  static const int64_t TEST_COLUMN_CNT = ObExtendType - 1;
  static const int64_t TEST_ROWKEY_COLUMN_CNT = 2;
  ObTableSchema table_schema_;
  ObRowGenerate row_generate_;
  ObColumnMap column_map_;
};

TestSortTempMacroBlockReaderWriter::TestSortTempMacroBlockReaderWriter()
  : TestDataFilePrepare("TestSortTempMacroBlockReaderWriter")
{
}

TestSortTempMacroBlockReaderWriter::~TestSortTempMacroBlockReaderWriter()
{
}

void TestSortTempMacroBlockReaderWriter::prepare_schema()
{
  ObColumnSchemaV2 column;
  int64_t table_id = 3001;
  int64_t micro_block_size = 16 * 1024;
  //init table schema
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
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for(int64_t i = 0; i < TEST_COLUMN_CNT; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i + 1);
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(i);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(obj_type);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(1);
    if(obj_type == common::ObIntType){
      column.set_rowkey_position(1);
    } else if(obj_type == common::ObVarcharType) {
      column.set_rowkey_position(2);
    } else {
      column.set_rowkey_position(0);
    }
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}


void TestSortTempMacroBlockReaderWriter::SetUp()
{
  int ret = OB_SUCCESS;
  TestDataFilePrepare::SetUp();
  prepare_schema();
  row_generate_.reset();
  ret = row_generate_.init(table_schema_, &allocator_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestSortTempMacroBlockReaderWriter::TearDown()
{
  row_generate_.reset();
  table_schema_.reset();
  TestDataFilePrepare::TearDown();
}

void TestSortTempMacroBlockReaderWriter::prepare_one_buffer(const int64_t start_index, char *buf, int64_t &end_index)
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  int64_t buf_pos = sizeof(int64_t);
  int64_t header_pos = 0;
  ObObj cells[TEST_COLUMN_CNT];
  row.row_val_.cells_ = cells;
  row.row_val_.count_ = TEST_COLUMN_CNT;
  const int64_t buf_capacity = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  for (int64_t i = start_index; OB_SUCC(ret) && buf_pos < buf_capacity; ++i) {
    ret = row_generate_.get_next_row(i, row);
    ASSERT_EQ(OB_SUCCESS, ret);
    if (buf_pos + row.get_serialize_size() <= buf_capacity) {
      ASSERT_EQ(OB_SUCCESS, row.serialize(buf, buf_capacity, buf_pos));
    } else {
      end_index = i;
      break;
    }
  }
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, sizeof(int64_t), header_pos, buf_pos));
}

void TestSortTempMacroBlockReaderWriter::prepare_data(const int64_t buf_capacity, char *buf, ObIArray<int64_t> &start_indexes)
{
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  const int64_t macro_block_buffer_count = buf_capacity / macro_block_size;
  int64_t buf_pos = 0;
  ASSERT_EQ(OB_SUCCESS, start_indexes.push_back(0));
  for (int64_t i = 0; i < macro_block_buffer_count; ++i) {
    int64_t end_index = 0;
    prepare_one_buffer(start_indexes.at(i), buf + buf_pos, end_index);
    buf_pos += macro_block_size;
    ASSERT_EQ(OB_SUCCESS, start_indexes.push_back(end_index));
  }
}

void TestSortTempMacroBlockReaderWriter::check_data(const char *buf, const int64_t buf_len, const int64_t start_index)
{
  int ret = OB_SUCCESS;
  int64_t header_pos = 0;
  int64_t data_len = 0;
  int64_t data_pos = 0;
  const char *data = NULL;
  int64_t i = start_index;
  ObStoreRow lhs_row;
  ObStoreRow rhs_row;
  ObObj lhs_cells[TEST_COLUMN_CNT];
  ObObj rhs_cells[TEST_COLUMN_CNT];
  lhs_row.row_val_.cells_ = lhs_cells;
  lhs_row.row_val_.count_ = TEST_COLUMN_CNT;
  rhs_row.row_val_.cells_ = rhs_cells;
  rhs_row.row_val_.count_ = TEST_COLUMN_CNT;
  ret = serialization::decode_i64(buf, buf_len, header_pos, &data_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  data = buf + header_pos;
  while (data_pos < data_len - sizeof(int64_t)) {
    ret = lhs_row.deserialize(data, data_len, data_pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "", K(data_pos), K(data_len));
    ret = row_generate_.get_next_row(i, rhs_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "", K(lhs_row.row_val_), K(rhs_row.row_val_));
    ASSERT_TRUE(lhs_row.row_val_ == rhs_row.row_val_);
    ++i;
  }
}

TEST_F(TestSortTempMacroBlockReaderWriter, test_write)
{
  int ret = OB_SUCCESS;
  ObSortTempMacroBlockWriter writer;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  const int64_t buf_capacity = macro_block_size * 10;
  char buf[buf_capacity];
  const int64_t io_timeout_ms = common::DEFAULT_IO_WAIT_TIME_MS;
  ObArray<MacroBlockId> macro_blocks;
  ObArray<int64_t> start_indexes;

  // invalid use
  ret = writer.write_buffer(buf, buf_capacity, io_timeout_ms);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = writer.open(10);
  ASSERT_EQ(OB_SUCCESS, ret);

  // write zero data
  ret = writer.write_buffer(buf, 0, io_timeout_ms);
  ASSERT_NE(OB_SUCCESS, ret);

  // write size % macro_block_size != 0
  ret = writer.write_buffer(buf, macro_block_size + 1, io_timeout_ms);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = writer.write_buffer(buf, macro_block_size * 11, io_timeout_ms);
  ASSERT_NE(OB_SUCCESS, ret);

  // write one macro_block_size buffer
  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ASSERT_EQ(OB_SUCCESS, ret);
  prepare_data(buf_capacity, buf, start_indexes);
  ASSERT_EQ(11, start_indexes.count());
  ret = writer.write_buffer(buf, macro_block_size, io_timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.get_macro_block_list(macro_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, macro_blocks.count());
  ret = writer.write_buffer(buf, macro_block_size * 3, io_timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.get_macro_block_list(macro_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, macro_blocks.count());
  SLOGGER.abort();
}

TEST_F(TestSortTempMacroBlockReaderWriter, test_read)
{
  int ret = OB_SUCCESS;
  ObSortTempMacroBlockWriter writer;
  ObSortTempMacroBlockReader reader;
  const int64_t macro_block_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  const int64_t buf_capacity = macro_block_size * 10;
  char buf[buf_capacity];
  char read_buf[buf_capacity];
  MEMSET(buf, 0, buf_capacity);
  MEMSET(read_buf, 0, buf_capacity);
  int64_t buf_len = 0;
  const int64_t io_timeout_ms = common::DEFAULT_IO_WAIT_TIME_MS;
  ObArray<MacroBlockId> macro_blocks;
  ObArray<int64_t> start_indexes;
  ret = SLOGGER.begin(OB_LOG_CS_DAILY_MERGE);
  ret = writer.open(3);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, ret);
  prepare_data(buf_capacity, buf, start_indexes);
  ASSERT_EQ(11, start_indexes.count());
  ret = writer.write_buffer(buf, macro_block_size, io_timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.get_macro_block_list(macro_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, macro_blocks.count());
  ret = writer.write_buffer(buf + macro_block_size, macro_block_size * 3, io_timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.get_macro_block_list(macro_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(4, macro_blocks.count());
  ret = writer.wait_io_finish(io_timeout_ms);
  ASSERT_EQ(OB_SUCCESS, ret);

  //reader not init
  ObArray<MacroBlockId> prev_macro_blocks;
  ObArray<MacroBlockId> next_macro_blocks;
  for (int64_t i = 0; i < macro_blocks.count() / 2; ++i) {
    ASSERT_EQ(OB_SUCCESS, prev_macro_blocks.push_back(macro_blocks.at(i)));
  }
  for (int64_t i = macro_blocks.count() / 2; i < macro_blocks.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, next_macro_blocks.push_back(macro_blocks.at(i)));
  }
  ret = reader.prefetch(prev_macro_blocks);
  ASSERT_NE(OB_SUCCESS, ret);
  ret = reader.open(2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.prefetch(prev_macro_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = reader.fetch_buffer(io_timeout_ms, buf_capacity, read_buf, buf_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buf_len, macro_block_size * 2);
  ret = reader.prefetch(next_macro_blocks);
  ASSERT_EQ(OB_SUCCESS, ret);
  check_data(read_buf, macro_block_size, start_indexes.at(0));
  check_data(read_buf + macro_block_size, macro_block_size, start_indexes.at(1));
  ret = reader.fetch_buffer(io_timeout_ms, buf_capacity, read_buf + macro_block_size * 2, buf_len);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(buf_len, macro_block_size * 2);
  check_data(read_buf + macro_block_size * 2, macro_block_size, start_indexes.at(2));
  check_data(read_buf + macro_block_size * 3, macro_block_size, start_indexes.at(3));
  SLOGGER.abort();
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
