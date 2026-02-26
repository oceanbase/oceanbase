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

#define protected public
#define private public

#include "lib/number/ob_number_v2.h"
#include "ob_row_generate.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/ob_i_store.h"
#include "unittest/storage/mock_ob_table_read_info.h"
#include "ob_data_file_prepare.h"

#include <chrono>
#include <gtest/gtest.h>

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
  static constexpr int64_t ROWKEY_CNT = 1;
  static constexpr int64_t COLUMN_CNT = ObHexStringType;
  static constexpr int64_t TABLE_ID = 3001;

public:
  TestRowWriter() {}

  static void SetUpTestCase()
  {
    uint64_t version = cal_version(4, 3, 5, 2);
    ASSERT_EQ(OB_SUCCESS, ObClusterVersion::get_instance().init(version));
    oceanbase::ObClusterVersion::get_instance().update_data_version(version);
  }

  virtual void SetUp()
  {
    prepare_schema();
  }

  virtual void TearDown() {}

  char *alloc(const int64_t size);

  void prepare_schema();

  void prepare_large_schema(int64_t seed, int64_t column_cnt, ObTableSchema &schema);

  void diff_test(const ObDatumRow &row,
                 const ObIArray<ObColDesc> *col_descs,
                 ObITableReadInfo *read_info,
                 ObIArray<int64_t> *update_array,
                 ObIArray<int64_t> &memorys,
                 ObIArray<int64_t> &memorys_old,
                 ObIArray<double> &write,
                 ObIArray<double> &write_old,
                 ObIArray<double> &read,
                 ObIArray<double> &read_old);

protected:
  ObTableSchema table_schema_;

private:
  ObArenaAllocator allocator_;
};

char *TestRowWriter::alloc(const int64_t size)
{
  char *buffer = reinterpret_cast<char *>(allocator_.alloc(size));
  return buffer;
}

void TestRowWriter::prepare_schema()
{
  char name[OB_MAX_FILE_NAME_LENGTH];

  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_row_writer"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(TABLE_ID);
  table_schema_.set_rowkey_column_num(ROWKEY_CNT);
  table_schema_.set_max_used_column_id(COLUMN_CNT);

  for (int64_t i = 0; i < COLUMN_CNT; i++) {
    ObColumnSchemaV2 column;

    ASSERT_GE(sprintf(name, "test%020ld", i), 0);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_table_id(TABLE_ID);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_data_type(static_cast<ObObjType>(i + 1));
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

    if (column.get_data_type() == ObIntType) {
      column.set_rowkey_position(1);
    } else {
      column.set_rowkey_position(0);
    }

    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
}

void TestRowWriter::prepare_large_schema(int64_t seed, int64_t column_cnt, ObTableSchema &schema)
{
  char name[OB_MAX_FILE_NAME_LENGTH];
  ObColumnSchemaV2 column;
  ObRandom random;
  random.seed(seed);

  ASSERT_EQ(OB_SUCCESS, schema.set_table_name("test_row_writer"));
  schema.set_tenant_id(1);
  schema.set_tablegroup_id(1);
  schema.set_database_id(1);
  schema.set_table_id(TABLE_ID);
  schema.set_rowkey_column_num(ROWKEY_CNT);
  schema.set_max_used_column_id(COLUMN_CNT);

  // prepare rowkey
  column.reset();
  ASSERT_GE(sprintf(name, "test%020ld", 0l), 0);
  ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
  column.set_collation_type(CS_TYPE_BINARY);
  column.set_table_id(TABLE_ID);
  column.set_column_id(0 + OB_APP_MIN_COLUMN_ID);
  column.set_data_type(ObIntType);
  column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  column.set_rowkey_position(1);
  ASSERT_EQ(OB_SUCCESS, schema.add_column(column));

  for (int64_t i = 1; i < column_cnt; i++) {
    column.reset();

    ASSERT_GE(sprintf(name, "test%020ld", i), 0);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_collation_type(CS_TYPE_BINARY);
    column.set_table_id(TABLE_ID);
    column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
    column.set_data_type(static_cast<ObObjType>(random.get(1, ObHexStringType - 1)));
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

    column.set_rowkey_position(0);

    ASSERT_EQ(OB_SUCCESS, schema.add_column(column));
  }
}

void TestRowWriter::diff_test(const ObDatumRow &row,
                              const ObIArray<ObColDesc> *col_descs,
                              ObITableReadInfo *read_info,
                              ObIArray<int64_t> *update_array,
                              ObIArray<int64_t> &memorys,
                              ObIArray<int64_t> &memorys_old,
                              ObIArray<double> &write,
                              ObIArray<double> &write_old,
                              ObIArray<double> &read,
                              ObIArray<double> &read_old)
{
  constexpr size_t WARM_ROW_NUMBER = 1e6;
  constexpr size_t TEST_ROW_NUMBER = 1e5;

  ObRowWriter writer;
  ObRowWriterV0 writer_v0;
  ObDatumRow read_row;

  char *buf = nullptr;
  int64_t len = 0;

  {
    // warmup
    int waste_time = 0;
    for (int64_t i = 0; i < WARM_ROW_NUMBER; i++) {
      waste_time += row.storage_datums_[i % row.count_].len_;
    }
  }

  {
    // test old writer speed
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < TEST_ROW_NUMBER; i++) {
      writer_v0.reset();
      ASSERT_EQ(OB_SUCCESS, writer_v0.write(ROWKEY_CNT, row, update_array, buf, len));
    }
    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();
    write_old.push_back(TEST_ROW_NUMBER / seconds);
    memorys_old.push_back(len);
  }

  {
    // test old reader speed
    ObRowReaderV0 reader;
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < TEST_ROW_NUMBER; i++) {
      reader.reset();
      ASSERT_EQ(OB_SUCCESS, reader.read_row(buf, len, read_info, read_row));
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto seconds = std::chrono::duration<double>(end - start).count();
    read_old.push_back(TEST_ROW_NUMBER / seconds);
  }

  {
    // test write speed
    auto start = std::chrono::high_resolution_clock::now();
    for (int64_t i = 0; i < TEST_ROW_NUMBER; i++) {
      ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, update_array, col_descs, buf, len));
    }
    auto end = std::chrono::high_resolution_clock::now();
    double seconds = std::chrono::duration<double>(end - start).count();
    write.push_back(TEST_ROW_NUMBER / seconds);
    memorys.push_back(len);
  }

  {
    // test read speed
    ObRowReader reader;
    auto start = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < TEST_ROW_NUMBER; i++) {
      ASSERT_EQ(OB_SUCCESS, reader.read_row(buf, len, read_info, read_row));
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto seconds = std::chrono::duration<double>(end - start).count();
    read.push_back(TEST_ROW_NUMBER / seconds);
  }

  {
    // check result
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, ROWKEY_CNT, update_array, col_descs, buf, len));
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(len, sizeof(ObRowHeader));
  }
}

TEST_F(TestRowWriter, write_use_external_buffer)
{
  ObRowGenerate row_generate;
  ObDatumRow row, empty_row;
  ASSERT_EQ(OB_SUCCESS, row.init(COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema_, &allocator_));
  ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));

  ObRowWriter writer;
  int64_t writed_len = 0;
  int64_t buf_len = 4096;
  char *buf = alloc(buf_len);
  ASSERT_NE(buf, nullptr);

  // invalid buffer
  ASSERT_NE(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, nullptr, nullptr, 100, writed_len));

  // invalid buffer length
  ASSERT_NE(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, nullptr, buf, -1, writed_len));

  // invalid rowkey cnt
  ASSERT_NE(OB_SUCCESS, writer.write(99999, row, nullptr, nullptr, buf, buf_len, writed_len));

  // invalid empty row
  ASSERT_NE(OB_SUCCESS,
            writer.write(ROWKEY_CNT, empty_row, nullptr, nullptr, buf, buf_len, writed_len));

  // buffer is not enough for row header
  ASSERT_EQ(
      OB_BUF_NOT_ENOUGH,
      writer.write(ROWKEY_CNT, row, nullptr, nullptr, buf, sizeof(ObRowHeader) - 1, writed_len));

  // buffer is not enough for data
  ASSERT_EQ(
      OB_BUF_NOT_ENOUGH,
      writer.write(ROWKEY_CNT, row, nullptr, nullptr, buf, sizeof(ObRowHeader) + 3, writed_len));

  // update array contains rowkey
  {
    ObSEArray<int64_t, 4> update_array;
    update_array.push_back(0);
    ASSERT_NE(OB_SUCCESS,
              writer.write(ROWKEY_CNT, row, &update_array, nullptr, buf, buf_len, writed_len));
  }

  // update array is not ascend
  {
    ObSEArray<int64_t, 4> update_array;
    update_array.push_back(4);
    update_array.push_back(3);
    ASSERT_NE(OB_SUCCESS,
              writer.write(ROWKEY_CNT, row, &update_array, nullptr, buf, buf_len, writed_len));
  }

  // normal case
  ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, nullptr, buf, buf_len, writed_len));
}

TEST_F(TestRowWriter, write_use_local_buffer)
{
  ObRowGenerate row_generate;
  ObDatumRow row, empty_row;
  ASSERT_EQ(OB_SUCCESS, row.init(COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema_, &allocator_));
  ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));

  ObRowWriter writer;
  char *buf = nullptr;
  int64_t len = 0;

  // invalid rowkey cnt
  ASSERT_NE(OB_SUCCESS, writer.write(99999, row, nullptr, nullptr, buf, len));

  // invalid empty row
  ASSERT_NE(OB_SUCCESS, writer.write(ROWKEY_CNT, empty_row, nullptr, nullptr, buf, len));

  // normal case
  ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, nullptr, buf, len));
  ASSERT_NE(buf, nullptr);
  ASSERT_GE(len, sizeof(ObRowHeader));
}

TEST_F(TestRowWriter, write_rowkey)
{
  ObTableSchema table_schema;
  ObRowGenerate row_generate;

  prepare_large_schema(1, 200, table_schema);
  ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema, &allocator_));

  ObObj objs[200];
  ObStoreRow row;
  row.row_val_.cells_ = objs;
  row.row_val_.count_ = 200;
  ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));

  for (int64_t i = 0; i <= 200; i++) {
    ObStoreRowkey rowkey;
    rowkey.assign(row.row_val_.cells_, i);

    ObRowWriter writer;
    char *buf = nullptr;
    int64_t len = 0;

    // normal case
    if (i <= 128) {
      ASSERT_EQ(OB_SUCCESS, writer.write_lock_rowkey(rowkey, nullptr, buf, len));
      ASSERT_NE(buf, nullptr);
      ASSERT_GE(len, sizeof(ObRowHeader));
    } else {
      ASSERT_EQ(OB_BUF_NOT_ENOUGH, writer.write_lock_rowkey(rowkey, nullptr, buf, len));
    }
  }
}

TEST_F(TestRowWriter, write_different_rowkey_size_correct)
{
  ObRowGenerate row_generate;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema_, &allocator_));

  ObRowWriter writer;
  char *buf = nullptr;
  int64_t len = 0;
  for (int64_t i = 0; i <= COLUMN_CNT; i++) {
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));

    ASSERT_EQ(OB_SUCCESS, writer.write(i, row, nullptr, nullptr, buf, len));
    ASSERT_EQ(OB_SUCCESS, ObRowWriter::check_write_result(row, i, nullptr, nullptr, buf, len));
  }
}

TEST_F(TestRowWriter, stable_write)
{
  constexpr int64_t TEST_TIMES = 10000;

  char prev_buffer[100][10000];
  int64_t prev_len[100];

  memset(prev_len, 0, sizeof(prev_len));

  ObRandom random;
  ObRowWriter writer;
  for (size_t i = 0; i < TEST_TIMES; i++) {
    size_t row_id = random.get(2, 90);

    ObDatumRow row;
    ObRowGenerate row_generate;
    ObTableSchema table_schema;
    prepare_large_schema(row_id, row_id, table_schema);
    ASSERT_EQ(OB_SUCCESS, row.init(row_id));
    ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema, &allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row_id, row));

    char *buf = nullptr;
    int64_t len = 0;
    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, nullptr, buf, len));
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, ROWKEY_CNT, nullptr, nullptr, buf, len));
    if (prev_len[row_id] == 0) {
      prev_len[row_id] = len;
      memcpy(prev_buffer[row_id], buf, len);
    }
    ASSERT_EQ(prev_len[row_id], len);
    ASSERT_EQ(0, memcmp(prev_buffer[row_id], buf, len));
  }
}

TEST_F(TestRowWriter, write_full_row_correct)
{
  constexpr int64_t TEST_TIMES = 10000;

  ObRowGenerate row_generate;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema_, &allocator_));

  ObRowWriter writer;
  char *buf = nullptr;
  int64_t len = 0;
  for (int64_t i = 0; i < TEST_TIMES; i++) {
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));

    // TODO: fix flag
    // for (int64_t j = 0; j < COLUMN_CNT; j++) {
    //   row.storage_datums_[j].flag_ = j % 4;
    // }

    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, nullptr, buf, len));
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, ROWKEY_CNT, nullptr, nullptr, buf, len));
  }
}

TEST_F(TestRowWriter, write_correct_with_update_array)
{
  constexpr int64_t TEST_TIMES = 10000;

  ObRowGenerate row_generate;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema_, &allocator_));

  ObRowWriter writer;
  char *buf = nullptr;
  int64_t len = 0;

  // write with normal update_array
  for (int64_t i = 0; i < TEST_TIMES; i++) {
    ObSEArray<int64_t, 4> update_array;
    for (int j = 0; j < 4; j++) {
      if (j == 0) {
        update_array.push_back(ROWKEY_CNT + rand() % (COLUMN_CNT - ROWKEY_CNT));
      } else {
        int64_t last = update_array[update_array.count() - 1];
        int64_t next = ROWKEY_CNT + last + rand() % (COLUMN_CNT - ROWKEY_CNT - last + 1);
        if (next > last && next < COLUMN_CNT) {
          update_array.push_back(next);
        }
      }
    }

    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, &update_array, nullptr, buf, len));
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, ROWKEY_CNT, &update_array, nullptr, buf, len));
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(len, sizeof(ObRowHeader));
  }

  // write with empty update array
  for (int64_t i = 0; i < TEST_TIMES; i++) {
    ObSEArray<int64_t, 4> update_array;

    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, &update_array, nullptr, buf, len));
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, ROWKEY_CNT, &update_array, nullptr, buf, len));
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(len, sizeof(ObRowHeader));
  }
}

TEST_F(TestRowWriter, write_correct_with_col_descs)
{
  constexpr int64_t TEST_TIMES = 10000;

  ObRowGenerate row_generate;
  ObSEArray<ObColDesc, COLUMN_CNT> col_descs;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema_, &allocator_));
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));

  ObRowWriter writer;
  char *buf = nullptr;
  int64_t len = 0;
  for (int64_t i = 0; i < TEST_TIMES; i++) {
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, &col_descs, buf, len));
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, ROWKEY_CNT, nullptr, &col_descs, buf, len));
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(len, sizeof(ObRowHeader));
  }
}

TEST_F(TestRowWriter, col_descs_will_zip_int)
{
  constexpr int64_t TEST_TIMES = 10000;

  ObRowGenerate row_generate;
  ObSEArray<ObColDesc, COLUMN_CNT> col_descs;
  ObDatumRow row;
  ASSERT_EQ(OB_SUCCESS, row.init(COLUMN_CNT));
  ASSERT_EQ(OB_SUCCESS, row_generate.init(table_schema_, &allocator_));
  ASSERT_EQ(OB_SUCCESS, table_schema_.get_column_ids(col_descs));

  ObRowWriter writer;
  char *buf = nullptr, *zip_buf = nullptr;
  int64_t len = 0, zip_len = 0;
  for (int64_t i = 0; i < TEST_TIMES; i++) {
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));

    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, &col_descs, zip_buf, zip_len));
    ASSERT_NE(zip_buf, nullptr);
    ASSERT_GE(zip_len, sizeof(ObRowHeader));

    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, nullptr, buf, len));
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(len, sizeof(ObRowHeader));

    ASSERT_LT(zip_len, len);
  }
}

TEST_F(TestRowWriter, large_row_correct)
{
  constexpr int64_t array[]
      = {40, 50, 60, 70, 80, 90, 100, 110, 200, 300, 400, 500, 600, 800, 1000, 1200};
  for (int64_t i = 0; i < sizeof(array) / sizeof(int64_t); i++) {
    ObTableSchema schema;
    ObRowGenerate row_generate;
    ObDatumRow row;
    ObRowWriter writer;
    ObSEArray<ObColDesc, COLUMN_CNT> col_descs;

    prepare_large_schema(i, array[i], schema);
    ASSERT_EQ(OB_SUCCESS, row.init(schema.get_column_count()));
    ASSERT_EQ(OB_SUCCESS, row_generate.init(schema, &allocator_));
    ASSERT_EQ(OB_SUCCESS, schema.get_column_ids(col_descs));

    // without update array
    char *buf = nullptr;
    int64_t len = 0;
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, nullptr, &col_descs, buf, len));
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, ROWKEY_CNT, nullptr, &col_descs, buf, len));
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(len, sizeof(ObRowHeader));

    // with update_array
    ObSEArray<int64_t, 4> update_array;
    for (int j = 0; j < 4; j++) {
      if (j == 0) {
        update_array.push_back(ROWKEY_CNT + rand() % (row.get_column_count() - ROWKEY_CNT));
      } else {
        int64_t last = update_array[update_array.count() - 1];
        int64_t next
            = ROWKEY_CNT + last + rand() % (row.get_column_count() - ROWKEY_CNT - last + 1);
        if (next > last && next < COLUMN_CNT) {
          update_array.push_back(next);
        }
      }
    }

    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(row));
    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row, &update_array, &col_descs, buf, len));
    ASSERT_EQ(
        OB_SUCCESS,
        ObRowWriter::check_write_result(row, ROWKEY_CNT, &update_array, &col_descs, buf, len));
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(len, sizeof(ObRowHeader));
  }
}

TEST_F(TestRowWriter, large_row_full_test)
{
  constexpr int64_t array[] = {10, 20, 30, 40, 60, 80, 100, 120, 200, 400, 600, 1000, 1200};

  ObSEArray<int64_t, sizeof(array)> memorys, memorys_old;
  ObSEArray<double, sizeof(array)> write, read, write_old, read_old;

  for (int64_t i = 0; i < sizeof(array) / sizeof(int64_t); i++) {
    ObTableSchema schema;
    ObRowGenerate row_generate;
    ObDatumRow row;
    ObSEArray<ObColDesc, COLUMN_CNT> col_descs;

    prepare_large_schema(i, array[i], schema);
    ASSERT_EQ(OB_SUCCESS, row.init(schema.get_column_count()));
    ASSERT_EQ(OB_SUCCESS, row_generate.init(schema, &allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(0x1122334455667788ll, row));
    ASSERT_EQ(OB_SUCCESS, schema.get_column_ids(col_descs));

    diff_test(row,
              &col_descs,
              nullptr,
              nullptr,
              memorys,
              memorys_old,
              write,
              write_old,
              read,
              read_old);
  }

  STORAGE_LOG(INFO, "write: ", K(write));
  STORAGE_LOG(INFO, "write_old: ", K(write_old));
  STORAGE_LOG(INFO, "read: ", K(read));
  STORAGE_LOG(INFO, "read_old: ", K(read_old));
  STORAGE_LOG(INFO, "memory: ", K(memorys));
  STORAGE_LOG(INFO, "memory_old: ", K(memorys_old));
}

TEST_F(TestRowWriter, large_row_full_sparse_test)
{
  constexpr int64_t array[] = {10, 20, 30, 40, 60, 80, 100, 120, 200, 400, 600, 1000, 1200};

  ObSEArray<int64_t, sizeof(array)> memorys, memorys_old;
  ObSEArray<double, sizeof(array)> write, read, write_old, read_old;

  for (int64_t i = 0; i < sizeof(array) / sizeof(int64_t); i++) {
    ObTableSchema schema;
    ObRowGenerate row_generate;
    ObDatumRow row;
    ObSEArray<ObColDesc, COLUMN_CNT> col_descs;

    prepare_large_schema(i, array[i], schema);
    ASSERT_EQ(OB_SUCCESS, row.init(schema.get_column_count()));
    ASSERT_EQ(OB_SUCCESS, row_generate.init(schema, &allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(0x1122334455667788ll, row));
    ASSERT_EQ(OB_SUCCESS, schema.get_column_ids(col_descs));

    ObRandom random;
    random.seed(i);
    ObSEArray<int64_t, 4> update_array;
    for (int j = 0; j < max(2ll, array[i] * 0.05); j++) {
      if (j == 0) {
        update_array.push_back(ROWKEY_CNT
                               + random.get(0, 65536) % (row.get_column_count() - ROWKEY_CNT));
      } else {
        int64_t last = update_array[update_array.count() - 1];
        int64_t next = last + random.get(0, 65536) % (row.get_column_count() - last + 1);
        if (next > last && next < array[i]) {
          update_array.push_back(next);
        }
      }
    }

    diff_test(row,
              &col_descs,
              nullptr,
              &update_array,
              memorys,
              memorys_old,
              write,
              write_old,
              read,
              read_old);
  }

  STORAGE_LOG(INFO, "write: ", K(write));
  STORAGE_LOG(INFO, "write_old: ", K(write_old));
  STORAGE_LOG(INFO, "read: ", K(read));
  STORAGE_LOG(INFO, "read_old: ", K(read_old));
  STORAGE_LOG(INFO, "memory: ", K(memorys));
  STORAGE_LOG(INFO, "memory_old: ", K(memorys_old));
}

TEST_F(TestRowWriter, large_row_full_dense_read_some_test)
{
  constexpr int64_t array[] = {10, 20, 30, 40, 60, 80, 100, 120, 200, 400, 600, 1000, 1200};

  ObSEArray<int64_t, sizeof(array)> memorys, memorys_old;
  ObSEArray<double, sizeof(array)> write, read, write_old, read_old;

  for (int64_t i = 0; i < sizeof(array) / sizeof(int64_t); i++) {
    ObTableSchema schema;
    ObRowGenerate row_generate;
    ObDatumRow row;
    ObSEArray<ObColDesc, COLUMN_CNT> col_descs;

    prepare_large_schema(i, array[i], schema);
    ASSERT_EQ(OB_SUCCESS, row.init(schema.get_column_count()));
    ASSERT_EQ(OB_SUCCESS, row_generate.init(schema, &allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(0x1122334455667788ll, row));
    ASSERT_EQ(OB_SUCCESS, schema.get_column_ids(col_descs));

    ObRandom random;
    MockObTableReadInfo read_info;
    ObSEArray<ObColDesc, COLUMN_CNT> read_cols;
    ObColDesc col_desc;
    random.seed(i);
    for (int64_t i = 0; i < row.count_; i++) {
      if (random.get(0, 7) == 0) {
        col_desc.col_id_ = i + common::OB_APP_MIN_COLUMN_ID;
        read_cols.push_back(col_desc);
      }
    }
    read_info.init(allocator_, row.count_, ROWKEY_CNT, lib::is_oracle_mode(), read_cols);

    diff_test(row,
              &col_descs,
              &read_info,
              nullptr,
              memorys,
              memorys_old,
              write,
              write_old,
              read,
              read_old);
  }

  STORAGE_LOG(INFO, "write: ", K(write));
  STORAGE_LOG(INFO, "write_old: ", K(write_old));
  STORAGE_LOG(INFO, "read: ", K(read));
  STORAGE_LOG(INFO, "read_old: ", K(read_old));
  STORAGE_LOG(INFO, "memory: ", K(memorys));
  STORAGE_LOG(INFO, "memory_old: ", K(memorys_old));
}

TEST_F(TestRowWriter, large_row_full_sparse_read_some_test)
{
  constexpr int64_t array[] = {10, 20, 30, 40, 60, 80, 100, 120, 200, 400, 600, 1000, 1200};

  ObSEArray<int64_t, sizeof(array)> memorys, memorys_old;
  ObSEArray<double, sizeof(array)> write, read, write_old, read_old;

  for (int64_t i = 0; i < sizeof(array) / sizeof(int64_t); i++) {
    ObTableSchema schema;
    ObRowGenerate row_generate;
    ObDatumRow row;
    ObSEArray<ObColDesc, COLUMN_CNT> col_descs;

    prepare_large_schema(i, array[i], schema);
    ASSERT_EQ(OB_SUCCESS, row.init(schema.get_column_count()));
    ASSERT_EQ(OB_SUCCESS, row_generate.init(schema, &allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(0x1122334455667788ll, row));
    ASSERT_EQ(OB_SUCCESS, schema.get_column_ids(col_descs));

    ObRandom random;
    random.seed(i);
    ObSEArray<int64_t, 4> update_array;
    for (int j = 0; j < max(2ll, array[i] * 0.05); j++) {
      if (j == 0) {
        update_array.push_back(ROWKEY_CNT
                               + random.get(0, 65536) % (row.get_column_count() - ROWKEY_CNT));
      } else {
        int64_t last = update_array[update_array.count() - 1];
        int64_t next = last + random.get(0, 65536) % (row.get_column_count() - last + 1);
        if (next > last && next < array[i]) {
          update_array.push_back(next);
        }
      }
    }

    MockObTableReadInfo read_info;
    ObSEArray<ObColDesc, COLUMN_CNT> read_cols;
    ObColDesc col_desc;
    for (int64_t i = 0; i < row.count_; i++) {
      if (random.get(0, 7) == 0) {
        col_desc.col_id_ = i + common::OB_APP_MIN_COLUMN_ID;
        read_cols.push_back(col_desc);
      }
    }
    read_info.init(allocator_, row.count_, ROWKEY_CNT, lib::is_oracle_mode(), read_cols);

    diff_test(row,
              &col_descs,
              &read_info,
              &update_array,
              memorys,
              memorys_old,
              write,
              write_old,
              read,
              read_old);
  }

  STORAGE_LOG(INFO, "write: ", K(write));
  STORAGE_LOG(INFO, "write_old: ", K(write_old));
  STORAGE_LOG(INFO, "read: ", K(read));
  STORAGE_LOG(INFO, "read_old: ", K(read_old));
  STORAGE_LOG(INFO, "memory: ", K(memorys));
  STORAGE_LOG(INFO, "memory_old: ", K(memorys_old));
}

TEST_F(TestRowWriter, null_row_is_like_nop_row)
{
  constexpr int64_t TEST_TIMES = 1e5;

  ObSEArray<ObColDesc, COLUMN_CNT> col_descs;
  ObDatumRow row_null, row_nop;
  ObRowGenerate row_generate;
  ObTableSchema schema;
  ObRandom random;
  prepare_large_schema(0, 40, schema);
  ASSERT_EQ(OB_SUCCESS, row_null.init(schema.get_column_count()));
  ASSERT_EQ(OB_SUCCESS, row_nop.init(schema.get_column_count()));
  ASSERT_EQ(OB_SUCCESS, row_generate.init(schema, &allocator_));
  ASSERT_EQ(OB_SUCCESS, schema.get_column_ids(col_descs));
  random.seed(100);

  ObRowWriter writer;
  char *buf = nullptr;
  int64_t len_null = 0, len_nop = 0;

  for (int64_t i = 0; i < TEST_TIMES; i++) {
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(i, row_null));
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(i, row_nop));

    for (int64_t j = ROWKEY_CNT; j < COLUMN_CNT; j++) {
      if (random.get_int32() % 4 > 0) {
        row_null.storage_datums_[j].set_null();
        row_nop.storage_datums_[j].set_nop();
      }
    }

    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row_null, nullptr, &col_descs, buf, len_null));
    ASSERT_EQ(
        OB_SUCCESS,
        ObRowWriter::check_write_result(row_null, ROWKEY_CNT, nullptr, &col_descs, buf, len_null));
    ASSERT_EQ(OB_SUCCESS, writer.write(ROWKEY_CNT, row_nop, nullptr, &col_descs, buf, len_nop));
    ASSERT_EQ(
        OB_SUCCESS,
        ObRowWriter::check_write_result(row_nop, ROWKEY_CNT, nullptr, &col_descs, buf, len_nop));

    // row writer should handle null row as it is nop row, so their length should be equal
    ASSERT_EQ(len_nop, len_null);
  }
}

TEST_F(TestRowWriter, write_no_rowkey_row)
{
  constexpr int64_t array[] = {0, 10, 20, 30, 40, 60, 80, 100, 120, 200, 400, 600, 1000, 1200};

  ObRowWriter writer;
  ObRowReader reader;
  char *buf = nullptr;
  int64_t len = 0;

  for (int64_t i = 0; i < sizeof(array) / sizeof(int64_t); i++) {
    ObTableSchema schema;
    ObRowGenerate row_generate;
    ObDatumRow row, read_row;
    ObSEArray<ObColDesc, COLUMN_CNT> col_descs;

    prepare_large_schema(i, array[i], schema);
    ASSERT_EQ(OB_SUCCESS, row.init(schema.get_column_count()));
    ASSERT_EQ(OB_SUCCESS, row_generate.init(schema, &allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(0x1122334455667788ll, row));
    ASSERT_EQ(OB_SUCCESS, schema.get_column_ids(col_descs));

    // no rowkey row, but write full col
    ASSERT_EQ(OB_SUCCESS, writer.write(0, row, nullptr, &col_descs, buf, len));
    ASSERT_EQ(OB_SUCCESS, ObRowWriter::check_write_result(row, 0, nullptr, &col_descs, buf, len));

    ObRandom random;
    random.seed(i);
    ObSEArray<int64_t, 4> update_array;
    for (int j = 0; array[i] >= 2 & j < max(2ll, array[i] * 0.05); j++) {
      if (j == 0) {
        update_array.push_back(random.get(0, 65536) % row.get_column_count());
      } else {
        int64_t last = update_array[update_array.count() - 1];
        int64_t next = last + random.get(0, 65536) % (row.get_column_count() - last + 1);
        if (next > last && next < array[i]) {
          update_array.push_back(next);
        }
      }
    }

    // no rowkey row, but write sparse
    ASSERT_EQ(OB_SUCCESS, writer.write(0, row, &update_array, &col_descs, buf, len));
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, 0, &update_array, &col_descs, buf, len));

    MockObTableReadInfo read_info;
    ObSEArray<ObColDesc, COLUMN_CNT> read_cols;
    ObColDesc col_desc;
    for (int64_t i = 0; i < row.count_ + 10; i++) {
      if (random.get(0, 2) == 0) {
        col_desc.col_id_ = i + common::OB_APP_MIN_COLUMN_ID;
        read_cols.push_back(col_desc);
      }
    }
    read_info.init(allocator_, row.count_, 0, lib::is_oracle_mode(), read_cols);

    // no rowkey row, but write sparse && read sparse and read out of limit
    ASSERT_EQ(OB_SUCCESS, writer.write(0, row, &update_array, &col_descs, buf, len));
    ASSERT_EQ(OB_SUCCESS, reader.read_row(buf, len, &read_info, read_row));
  }
}

TEST_F(TestRowWriter, random_all_operation_test)
{
  constexpr int TEST_NUMBER = 1000;

  ObRandom random;
  ObRowWriter writer;
  ObRowReader reader;
  for (int64_t i = 0; i < TEST_NUMBER; i++) {
    // random a column number
    int64_t column_number = random.get(1, 1000);

    // random a rowkey number
    int64_t rowkey_number = random.get(0, min(column_number, 128));

    // random a schema
    ObTableSchema schema;
    ObSEArray<ObColDesc, COLUMN_CNT> col_descs;
    prepare_large_schema(random.get(0, 1000000), column_number, schema);
    ASSERT_EQ(OB_SUCCESS, schema.get_column_ids(col_descs));

    // random a row
    ObRowGenerate row_generate;
    ObDatumRow row;
    ASSERT_EQ(OB_SUCCESS, row.init(schema.get_column_count()));
    ASSERT_EQ(OB_SUCCESS, row_generate.init(schema, &allocator_));
    ASSERT_EQ(OB_SUCCESS, row_generate.get_next_row(random.get(0, 100000000), row));

    // write it
    char *buf = nullptr;
    int64_t len = 0;
    ASSERT_EQ(OB_SUCCESS, writer.write(rowkey_number, row, nullptr, &col_descs, buf, len));
    ASSERT_EQ(OB_SUCCESS,
              ObRowWriter::check_write_result(row, rowkey_number, nullptr, &col_descs, buf, len));

    // random read operation
    int operation = random.get(0, 3);

    // check read column
    if (operation == 0) {
      int col_idx = random.get(0, column_number - 1);
      ObStorageDatum datum;
      ASSERT_EQ(OB_SUCCESS, reader.read_column(buf, len, col_idx, datum));
      ASSERT_TRUE(datum == row.storage_datums_[col_idx]);
    }

    // read row
    if (operation == 1) {
      MockObTableReadInfo read_info;
      ObSEArray<ObColDesc, COLUMN_CNT> read_cols;
      ObColDesc col_desc;
      ObDatumRow read_row;
      ASSERT_EQ(OB_SUCCESS, read_row.init(schema.get_column_count()));
      for (int64_t i = 0; i < row.count_; i++) {
        if (random.get(0, 4) == 0 || i < rowkey_number || i == 0) {
          col_desc.col_id_ = i + common::OB_APP_MIN_COLUMN_ID;
          read_cols.push_back(col_desc);
        }
      }
      ASSERT_EQ(OB_SUCCESS, read_info.init(allocator_, row.count_, rowkey_number, lib::is_oracle_mode(), read_cols));
      ASSERT_EQ(OB_SUCCESS, reader.read_row(buf, len, &read_info, read_row));

      for (int j = 0; j < read_info.get_request_count(); j++) {
        int64_t col_idx = read_info.get_columns_index().at(j);
        ASSERT_TRUE(read_row.storage_datums_[j] == row.storage_datums_[col_idx]);
      }
    }

    // read header
    if (operation == 2) {
      const ObRowHeader *header;
      ASSERT_EQ(OB_SUCCESS, reader.read_row_header(buf, len, header));
    }

    // read memtable
    if (operation == 3) {
      MockObTableReadInfo read_info;
      ObSEArray<ObColDesc, COLUMN_CNT> read_cols;
      ObColDesc col_desc;
      ObDatumRow read_row;
      bool read_finished = false;
      const ObRowHeader *header;
      memtable::ObNopBitMap nop_bitmap;
      for (int64_t i = 0; i < row.count_; i++) {
        if (random.get(0, 4) == 0 || i < rowkey_number || i == 0) {
          col_desc.col_id_ = i + common::OB_APP_MIN_COLUMN_ID;
          read_cols.push_back(col_desc);
        }
      }
      ASSERT_EQ(OB_SUCCESS, read_row.init(schema.get_column_count()));
      ASSERT_EQ(OB_SUCCESS, read_info.init(allocator_, row.count_, rowkey_number, lib::is_oracle_mode(), read_cols));
      ASSERT_EQ(OB_SUCCESS, nop_bitmap.init(column_number, rowkey_number));
      ASSERT_EQ(OB_SUCCESS, reader.read_memtable_row(buf, len, read_info, read_row, nop_bitmap, read_finished, header));

      for (int j = 0; j < read_info.get_request_count(); j++) {
        int64_t col_idx = read_info.get_columns_index().at(j);
        ASSERT_TRUE(read_row.storage_datums_[j] == row.storage_datums_[col_idx]);
      }
    }
  }
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_row_writer.log");
  OB_LOGGER.set_file_name("test_row_writer.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
