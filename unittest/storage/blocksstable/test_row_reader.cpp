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
#define protected public
#define private public
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "lib/number/ob_number_v2.h"
#include "ob_row_generate.h"
#include "share/object/ob_obj_cast.h"
#include "share/ob_simple_mem_limit_getter.h"


#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_row_writer.h"
#include "storage/blocksstable/ob_row_reader.h"
#include "ob_data_file_prepare.h"
#include "storage/memtable/ob_nop_bitmap.h"
#include "unittest/storage/mock_ob_table_read_info.h"

#ifndef INT24_MIN
#define INT24_MIN     (-8388607 - 1)
#endif
#ifndef INT24_MAX
#define INT24_MAX     (8388607)
#endif
#ifndef UINT24_MAX
#define UINT24_MAX    (16777215U)
#endif

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace storage;
using namespace common::number;
using namespace share::schema;
static ObSimpleMemLimitGetter getter;

namespace unittest
{

class TestNewRowReader : public TestDataFilePrepare
{
public:
  static const int64_t rowkey_column_count = 1;
  // Every ObObjType from ObTinyIntType to ObHexStringType inclusive.
  // Skip ObNullType and ObExtendType because for external usage, a column type
  // can't be NULL or NOP.
  ObObjType not_test_type[13] = {
      ObNullType, ObExtendType, ObUnknownType, ObEnumInnerType, ObSetInnerType,
      ObNumberFloatType, ObURowIDType, ObLobType, ObUserDefinedSQLType, ObDecimalIntType,
      ObCollectionSQLType, ObMySQLDateType, ObMySQLDateTimeType};
  static const int64_t column_num = ObMaxType - sizeof(not_test_type) / sizeof(ObObjType);
public:
  TestNewRowReader()
      : TestDataFilePrepare(&getter, "TestNewRowReader", 2 * 1024 * 1024),
      read_info_()
  {}
  virtual ~TestNewRowReader() {}
  virtual void SetUp();
  virtual void TearDown();
  char *get_serialize_buf() { return serialize_buf_; }
  int64_t get_serialize_size() { return 2 * 1024 * 1024; }
  void append_col(ObDatumRow &row, int64_t col_cnt = INT64_MAX);
private:
  int init_read_columns(const ObDatumRow &writer_row, common::ObIArray<int32_t> &projector, common::ObIArray<ObColDesc> &cols_desc);
  int init_read_columns(const ObDatumRow &writer_row, common::ObIArray<ObColDesc> &cols_desc);
  void check_reader_row(const char* buf, const int64_t buf_len, const int64_t rowkey_cnt, const ObDatumRow &writer_row);
  void check_read_datums(const char* buf, const int64_t buf_len, const int64_t rowkey_cnt, const ObDatumRow &writer_row);
  void check_read_datum_row(const char* buf, const int64_t buf_len, const ObDatumRow &writer_row, const int64_t rowkey_len = 0, const ObIArray<int64_t> *update_idx = nullptr);
  void check_write_with_update_idx(const ObDatumRow &writer_row);
  void build_column_read_info(const int64_t rowkey_column_count, const ObStoreRow &writer_row);
  void build_column_read_info(const int64_t rowkey_column_count, const ObDatumRow &writer_row);
  char *serialize_buf_;
protected:
  ObRowGenerate row_generate_;
  MockObTableReadInfo read_info_;
  ObTableSchema table_schema_;
  ObArenaAllocator allocator_;
  common::ObArray<ObColDesc> full_schema_cols_;
};

int TestNewRowReader::init_read_columns(
    const ObDatumRow &writer_row,
    common::ObIArray<int32_t> &projector,
    common::ObIArray<ObColDesc> &cols_desc)
{
  int ret = OB_SUCCESS;
  read_info_.reset();
  cols_desc.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < projector.count(); ++i) {
    ObColDesc col_desc;
    //ObObjType obj_type = writer_row.row_val_.cells_[projector.at(i)].get_type();
    //ObObjMeta obj_meta;
    //obj_meta.set_type(obj_type);
    //if (ObVarcharType == obj_type
        //|| ObCharType == obj_type
        //|| ObNCharType == obj_type
        //|| ObNVarchar2Type == obj_type
        //|| ObTinyTextType == obj_type
        //|| ObTextType == obj_type
        //|| ObMediumTextType == obj_type
        //|| ObLongTextType == obj_type) {
      //obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      //obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    //} else if (ObRawType == obj_type) {
      //obj_meta.set_collation_type(CS_TYPE_BINARY);
    //}
    //col_desc.col_type_ = obj_meta;
    col_desc.col_id_ = projector.at(i) + 30;
    ret = cols_desc.push_back(col_desc);
  }
  if (OB_FAIL(read_info_.init(
          allocator_,
          writer_row.count_,
          row_generate_.get_schema().get_rowkey_column_num(),
          lib::is_oracle_mode(),
          cols_desc,
          &projector))) {
    STORAGE_LOG(WARN, "failed to init column map");
  }

  return ret;
}

int TestNewRowReader::init_read_columns(
    const ObDatumRow &writer_row,
    common::ObIArray<ObColDesc> &cols_desc)
{
  int ret = OB_SUCCESS;
  read_info_.reset();
  cols_desc.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < writer_row.count_; ++i) {
    ObColDesc col_desc;
    //ObObjType obj_type = writer_row.row_val_.cells_[i].get_type();
    //ObObjMeta obj_meta;
    //obj_meta.set_type(obj_type);
    //if (ObVarcharType == obj_type
        //|| ObCharType == obj_type
        //|| ObNCharType == obj_type
        //|| ObNVarchar2Type == obj_type
        //|| ObTinyTextType == obj_type
        //|| ObTextType == obj_type
        //|| ObMediumTextType == obj_type
        //|| ObLongTextType == obj_type) {
      //obj_meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      //obj_meta.set_collation_level(CS_LEVEL_IMPLICIT);
    //} else if (ObRawType == obj_type) {
      //obj_meta.set_collation_type(CS_TYPE_BINARY);
    //}
    //col_desc.col_type_ = obj_meta;
    col_desc.col_id_ = i + 30;
    ret = cols_desc.push_back(col_desc);
  }
  if (OB_FAIL(read_info_.init(allocator_, writer_row.count_,
          row_generate_.get_schema().get_rowkey_column_num(), lib::is_oracle_mode(),
          cols_desc))) {
    STORAGE_LOG(WARN, "failed to init column map");
  }
  return ret;
}

void TestNewRowReader::SetUp()
{
  TestDataFilePrepare::SetUp();
  const int64_t serialize_buf_size_ = get_serialize_size();
  serialize_buf_ = reinterpret_cast<char*>(ob_malloc(serialize_buf_size_, ObModIds::TEST));
  ASSERT_TRUE(NULL != serialize_buf_);

  const int64_t table_id = 3001;

  oceanbase::common::ObClusterVersion::get_instance().refresh_cluster_version("1.4.70");
  ObColumnSchemaV2 column;
  //init table schema
  table_schema_.reset();
  ASSERT_EQ(OB_SUCCESS, table_schema_.set_table_name("test_row_writer"));
  table_schema_.set_tenant_id(1);
  table_schema_.set_tablegroup_id(1);
  table_schema_.set_database_id(1);
  table_schema_.set_table_id(table_id);
  table_schema_.set_rowkey_column_num(rowkey_column_count);
  table_schema_.set_max_used_column_id(column_num);
  table_schema_.set_schema_version(100);
  //init column
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));

  int not_test_index = 0;
  for(int64_t i = 0; i < ObMaxType; ++i){
    ObObjType obj_type = static_cast<ObObjType>(i);
    if (not_test_type[not_test_index] == obj_type) {
      not_test_index++;
      continue;
    }
    column.reset();
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
    ASSERT_EQ(OB_SUCCESS, table_schema_.add_column(column));
  }
  //init ObRowGenerate
  ASSERT_EQ(OB_SUCCESS, row_generate_.init(table_schema_));
}

void TestNewRowReader::TearDown()
{
  ob_free(serialize_buf_);
  TestDataFilePrepare::TearDown();
}

void TestNewRowReader::check_reader_row(const char* buf, const int64_t buf_len, const int64_t rowkey_cnt, const ObDatumRow &writer_row)
{
  check_read_datums(buf, buf_len, rowkey_cnt, writer_row);
  //check_read_datum_row(buf, buf_len, writer_row);
  // check write row with update_idx & read
  // check_write_with_update_idx(writer_row);
}

void TestNewRowReader::build_column_read_info(const int64_t rowkey_column_count, const ObStoreRow &writer_row)
{
  full_schema_cols_.reset();
  read_info_.reset();
  // read with ObTableReadInfo
  ObColDesc col_desc;
  // col desc is no use in flat reader
  for (int i = 0; i < writer_row.row_val_.count_; ++i) {
    col_desc.col_id_ = i + common::OB_APP_MIN_COLUMN_ID;
    full_schema_cols_.push_back(col_desc);
  }
  read_info_.init(allocator_,  writer_row.row_val_.count_, rowkey_column_count, lib::is_oracle_mode(), full_schema_cols_);
}

void TestNewRowReader::build_column_read_info(const int64_t rowkey_column_count, const ObDatumRow &writer_row)
{
  full_schema_cols_.reset();
  read_info_.reset();
  // read with ObColumnReadInfo
  ObColDesc col_desc;
  // col desc is no use in flat reader
  for (int i = 0; i < writer_row.count_; ++i) {
    col_desc.col_id_ = i + common::OB_APP_MIN_COLUMN_ID;
    full_schema_cols_.push_back(col_desc);
  }
  read_info_.init(allocator_,  writer_row.count_, rowkey_column_count, lib::is_oracle_mode(), full_schema_cols_);
}

void TestNewRowReader::check_read_datum_row(
    const char* buf,
    const int64_t buf_len,
    const ObDatumRow &writer_row,
    const int64_t rowkey_len,
    const ObIArray<int64_t> *update_idx)
{
  int ret = OB_SUCCESS;
  const int64_t test_column_cnt = 300;
  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, test_column_cnt));
  reader_row.count_ = writer_row.count_;

  ObRowReader row_reader;
  ret = row_reader.read_row(buf, buf_len, &read_info_, reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(reader_row.count_, writer_row.count_);
  int64_t update_array_idx = 0;
  for (int j = 0; j < reader_row.count_; ++j) {
    bool check_obj_flag = false;
    if (nullptr != update_idx) {
      if (j < rowkey_len) {
        check_obj_flag = true;
      } else if (update_array_idx < update_idx->count() && j == update_idx->at(update_array_idx)) {
        ++update_array_idx;
        check_obj_flag = true;
      }
    } else {
      check_obj_flag = true;
    }
    //STORAGE_LOG(INFO, "read datum", K(j), K(reader_row.storage_datums_[j]), K(writer_row.row_val_.cells_[j]));
    if (check_obj_flag) {
      ASSERT_TRUE(reader_row.storage_datums_[j] == writer_row.storage_datums_[j]);
    } else {
      ASSERT_TRUE(reader_row.storage_datums_[j].is_nop());
    }
  }
  row_reader.reset();
  memtable::ObNopBitMap nop_bitmap;
  ret = nop_bitmap.init(writer_row.count_, rowkey_len);
  ObDatumRow reader_row2;
  ASSERT_EQ(OB_SUCCESS, reader_row2.init(allocator_, writer_row.count_));
  for (int i = 0; i < writer_row.count_; ++i) {
    reader_row2.storage_datums_[i].set_nop();
  }
  reader_row2.count_ = writer_row.count_;

  bool read_finished = false;
  update_array_idx = 0;
  row_reader.read_memtable_row(buf, buf_len, read_info_, reader_row2, nop_bitmap, read_finished);
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int j = 0; j < reader_row2.count_; ++j) {
    bool check_obj_flag = false;
    if (nullptr != update_idx) {
      if (j < rowkey_len) {
        check_obj_flag = true;
      } else if (update_array_idx < update_idx->count() && j == update_idx->at(update_array_idx)) {
        ++update_array_idx;
        check_obj_flag = true;
      }
    } else {
      check_obj_flag = true;
    }
    ObObj obj;
    reader_row2.storage_datums_[j].to_obj(obj, full_schema_cols_.at(j).col_type_);
    //STORAGE_LOG(INFO, "to_obj", K(obj), "is_nop", reader_row2.storage_datums_[j].is_nop(), K(writer_row.row_val_.cells_[j]));
    if (check_obj_flag) {
      ASSERT_TRUE(reader_row2.storage_datums_[j] == writer_row.storage_datums_[j]);
    } else {
      ASSERT_TRUE(reader_row2.storage_datums_[j].is_nop());
    }
  }
}

void TestNewRowReader::check_read_datums(const char* buf, const int64_t buf_len, const int64_t rowkey_cnt, const ObDatumRow &writer_row)
{
  int ret = OB_SUCCESS;
  const int64_t test_column_cnt = 300;
  ObDatumRow datum_row;
  ASSERT_EQ(OB_SUCCESS, datum_row.init(test_column_cnt));
  const ObRowHeader *row_header = nullptr;

  ObRowReader row_reader;
  ret = row_reader.read_row_header(buf, buf_len, row_header); // current datums is compressed for int
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(row_header->get_column_count(), writer_row.count_);
  ret = row_reader.read_row(buf, buf_len, nullptr, datum_row); // current datums is compressed for int
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "check_read_datums, read datums");
  // need write datums and read out with ObTableReadInfo
  char * extra_buf = 0;
  int64_t len = 0;
  ObRowWriter row_writer;
  ret = row_writer.write(rowkey_cnt, datum_row, extra_buf, len);
  ASSERT_EQ(OB_SUCCESS, ret);
  STORAGE_LOG(INFO, "check_read_datums, write datums", K(datum_row));

  // read with ObTableReadInfo
  ObRowReader row_reader2;
  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, test_column_cnt));
  reader_row.count_ = writer_row.count_;
  ret = row_reader2.read_row(extra_buf, len, &read_info_, reader_row);
  ASSERT_TRUE(ret == OB_SUCCESS);

  for (int j = 0; j < writer_row.count_; ++j) {
    ASSERT_TRUE(reader_row.storage_datums_[j] == writer_row.storage_datums_[j]);
  }
}
/*
void TestNewRowReader::check_write_with_update_idx(const ObDatumRow &writer_row)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> update_idx;
  int rowkey_cnt[] = {1, 5, 19, 35};

  for (int j = 0; j < ARRAYSIZEOF(rowkey_cnt) && rowkey_cnt[j] < writer_row.count_; ++j){
    int i = rowkey_cnt[j];
    update_idx.reset();
    while (i < writer_row.count_) {
      if (!writer_row.storage_datums_[i].is_nop()) {
        update_idx.push_back(i);
        i += 2;
      } else {
        i++;
      }
    }

    ObRowWriter row_writer;
    char *buf = nullptr;
    int64_t len = 0;
    ret = row_writer.write(rowkey_cnt[j], writer_row, &update_idx, buf, len);
    ASSERT_EQ(ret, OB_SUCCESS);
    ObDatumRow datum_row;
    ASSERT_EQ(OB_SUCCESS, datum_row.init(allocator_, writer_row.count_));
    check_read_datum_row(buf, len, datum_row, rowkey_cnt[j], &update_idx);

    allocator_.free(buf);
    buf = nullptr;
  }
}
*/

void TestNewRowReader::append_col(ObDatumRow &row, int64_t col_cnt)
{
  int index = row.count_;
  ObScale scale;
  scale = ObLobScale::STORE_IN_ROW;

  row.storage_datums_[index++].set_string(ObString(6, "xxxxxx"));
  row.storage_datums_[index++].set_string(ObString(6, "xxxxxx"));
  row.storage_datums_[index++].set_string(ObString(6, "xxxxxx"));
  row.storage_datums_[index++].set_string(ObString(6, "xxxxxx"));
  // row.storage_datums_[index++].set_lob_outrow();
  row.storage_datums_[index++].set_bit(999);
  row.storage_datums_[index++].set_enum(88);
  row.storage_datums_[index++].set_set(1849);
 // row.row_val_.cells_[index++].set_enum_inner(ObString("jakgja"));
 // row.row_val_.cells_[index++].set_set_inner(ObString("jfao"));

  ObOTimestampData time_data;
  time_data.time_ctx_.tz_desc_ = 0;
  time_data.time_ctx_.time_desc_= 12;
  time_data.time_us_ = 888;
  row.storage_datums_[index++].set_otimestamp_tz(time_data);
  row.storage_datums_[index++].set_otimestamp_tiny(time_data);
  row.storage_datums_[index++].set_otimestamp_tiny(time_data);
  row.storage_datums_[index++].set_string(ObString(7, "agjaljg"));
  row.storage_datums_[index++].set_interval_ym(11);
  row.storage_datums_[index++].set_interval_ds(ObIntervalDSValue(11, 14));
  number::ObNumber test_number;
  test_number.from("-123456789987654321.12345678987654321", allocator_);
  row.storage_datums_[index++].set_number(test_number);
  row.storage_datums_[index++].set_int32(-9274819);
  row.count_ = index;

  if (INT64_MAX != col_cnt) {
    for (int i = row.count_; i < col_cnt; ++i) {
      row.storage_datums_[i] = row.storage_datums_[i - row.count_];
    }
    row.count_ = col_cnt;
  }
}

TEST_F(TestNewRowReader, test_obj_read)
{
  int ret = OB_SUCCESS;
  const int64_t num = 200;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);
  build_column_read_info(rowkey_column_count, writer_row);
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
  //STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, 1, writer_row);
}

TEST_F(TestNewRowReader, test_no_cluster)
{
  int ret = OB_SUCCESS;

  const int64_t num = 1000;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row, ObRowHeader::USE_CLUSTER_COLUMN_COUNT);
  build_column_read_info(rowkey_column_count, writer_row);
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  pos = 0;
  ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);

  check_reader_row(buf, pos, 1, writer_row);
}

TEST_F(TestNewRowReader, test_long_varchar)
{
  int ret = OB_SUCCESS;

  const int64_t num = 200;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);
  const int64_t long_char_len = 50000;
  char long_char[long_char_len];
  MEMSET(long_char, 'a', sizeof(long_char));
  writer_row.storage_datums_[writer_row.count_++].set_string(ObString(long_char_len, long_char));
  STORAGE_LOG(INFO, "write_row", K(writer_row));
  build_column_read_info(rowkey_column_count, writer_row);
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, 1, writer_row);
}

TEST_F(TestNewRowReader, test_row_with_nop)
{
  int ret = OB_SUCCESS;

  const int64_t num = 200;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);
  build_column_read_info(rowkey_column_count, writer_row);
  const int64_t cnt = 4;
  int nop_idx[cnt] = {1, 5, 8, 21};
  int null_idx[cnt] = {3, 17, 18, 22};
  for (int i = 0; i < cnt; ++i) {
    writer_row.storage_datums_[nop_idx[i]].set_nop();
    writer_row.storage_datums_[null_idx[i]].set_null();
  }
  STORAGE_LOG(INFO, "write_row", K(writer_row));
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, 1, writer_row);
}

TEST_F(TestNewRowReader, test_sparse_row)
{
  int ret = OB_SUCCESS;

  const int64_t num = 200;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);
  build_column_read_info(rowkey_column_count, writer_row);

  for (int i = 10; i < 26; ++i) {
    writer_row.storage_datums_[i].set_nop();
  }
  writer_row.storage_datums_[3].set_null();

  STORAGE_LOG(INFO, "write_row", K(writer_row));
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, 1, writer_row);
}

TEST_F(TestNewRowReader, test_delete_row)
{
  int ret = OB_SUCCESS;

  const int64_t num = 200;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);
  build_column_read_info(rowkey_column_count, writer_row);
  for (int i = 1; i < writer_row.count_ - 1; ++i) {
    writer_row.storage_datums_[i].set_nop();
  }
  STORAGE_LOG(INFO, "write_row", K(writer_row));
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, 1, writer_row);
}

TEST_F(TestNewRowReader, test_sparse_row_without_cluster)
{
  int ret = OB_SUCCESS;

  const int64_t num = 200;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);
  writer_row.count_ = ObRowHeader::USE_CLUSTER_COLUMN_COUNT - 1;
  build_column_read_info(rowkey_column_count, writer_row);
  for (int i = 0; i < writer_row.count_ - 1; ++i) {
    writer_row.storage_datums_[i].set_nop();
  }
  STORAGE_LOG(INFO, "write_row", K(writer_row));
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, 1, writer_row);
}
/*
TEST_F(TestNewRowReader, test_diff_format)
{
  int ret = OB_SUCCESS;

  const int64_t num = 3000;
  oceanbase::common::ObObj objs[num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);

  int64_t with_val_col_id[5] = {6, 18, 82, 189, 812};
  int64_t idx = 0;
  for (int i = writer_row.row_val_.count_; i < 3000; ++i) {
    writer_row.row_val_.cells_[i] = writer_row.row_val_.cells_[i - writer_row.row_val_.count_];
    //writer_row.row_val_.cells_[i].set_nop_value();
  }

  for (int i = writer_row.row_val_.count_; i < 3000; ++i) {
    if (i == with_val_col_id[idx]) {
      idx++;
    } else {
      writer_row.row_val_.cells_[i].set_nop_value();
    }
  }

  const int64_t long_char_len = 50000;
  char long_char[long_char_len];
  MEMSET(long_char, 'a', sizeof(long_char));
  writer_row.row_val_.cells_[6].set_varchar(long_char);
  writer_row.row_val_.cells_[6].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

  int64_t col_count[6] = {10,30,100,300,1000,3000};
  int64_t read_time_old = 0;
  int64_t write_time_old = 0;
  int64_t len_old = 0;
  int64_t read_time_new = 0;
  int64_t write_time_new = 0;
  int64_t len_new = 0;
  char *buf = get_serialize_buf();
  for (int k = 0; k < 6; ++k) {
    writer_row.row_val_.count_ = col_count[k];
    int64_t pos = 0;
    ObRowWriterV2 row_writer;

    MEMSET(buf, '\0', get_serialize_size());

    int64_t start_time = ObTimeUtility::current_time();
    for (int i = 0; i < 1 * 10000; ++i) {
      pos = 0;
      ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
    }
    int64_t finish_time = ObTimeUtility::current_time();
    write_time_new = finish_time - start_time;
    len_new = pos;

    oceanbase::common::ObObj read_objs[num];
    ObStoreRow reader_row;
    reader_row.row_val_.cells_ = read_objs;
    reader_row.row_val_.count_ = writer_row.row_val_.count_;
    common::ObObjMeta column_type_array[num];
    for (int i = 0; i < writer_row.row_val_.count_; ++i) {
      column_type_array[i] = writer_row.row_val_.cells_[i].get_meta();
    }

    int64_t column_idx_array[] = {5, 2, 1, 0, 3, 4, 9, 7, 6, 8,
        10, 11, 20, 17, 12, 13, 14, 15, 16, 21,
        89, 19, 92, 29, 96, 22, 99, 20, 88, 86,
        112, 127, 291, 210, 100, 105};
    ObArray<int32_t> projector;
    for (int j = 0; j < MIN(ARRAYSIZEOF(column_idx_array), (k + 1) * 10 - 1); ++j) {
      projector.push_back(column_idx_array[j]);
    }
    //init ObTableReadInfo
    ret = init_column_map(writer_row, projector);
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t read_pos = 0;
    ObRowReader row_reader;

    start_time = ObTimeUtility::current_time();
    for (int i = 0; i < 1 * 10000; ++i) {
      row_reader.reset();
      read_pos = 0;
      //ret = row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
      ret = row_reader.read_full_row(buf, pos, read_pos, column_type_array, allocator_, reader_row);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    finish_time = ObTimeUtility::current_time();
    read_time_new = finish_time - start_time;

    ASSERT_EQ(OB_SUCCESS, ret);
    for (int j = 0; j < reader_row.row_val_.count_; ++j) {
      if (ObNumberFloatType != reader_row.row_val_.cells_[j].get_type()) {
        ASSERT_TRUE(reader_row.row_val_.cells_[j] == writer_row.row_val_.cells_[j]);
        //ASSERT_TRUE(reader_row.row_val_.cells_[j] == writer_row.row_val_.cells_[column_idx_array[j]]);
      }
    }
    ObRowWriter old_row_writer;
    start_time = ObTimeUtility::current_time();
    for (int i = 0; i < 1 * 10000; ++i) {
      pos = 0;
      ret = old_row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
    }
    finish_time = ObTimeUtility::current_time();
    write_time_old = finish_time - start_time;
    len_old = pos;

    ObFlatRowReader old_row_reader;

    start_time = ObTimeUtility::current_time();
    for (int i = 0; i < 1 * 10000; ++i) {
      read_pos = 0;
      row_reader.reset();
      //ret = old_row_reader.read_row(buf, pos, read_pos, column_map_, allocator_, reader_row);
      ret = old_row_reader.read_full_row(buf, pos, read_pos, column_type_array, allocator_, reader_row);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
    finish_time = ObTimeUtility::current_time();
    read_time_old = finish_time - start_time;

    ASSERT_EQ(OB_SUCCESS, ret);
    //STORAGE_LOG(INFO, "read_full_row", K(reader_row));
    for (int j = 0; j < reader_row.row_val_.count_; ++j) {
      if (ObNumberFloatType != reader_row.row_val_.cells_[j].get_type()) {
        ASSERT_TRUE(reader_row.row_val_.cells_[j] == writer_row.row_val_.cells_[j]);
        //ASSERT_TRUE(reader_row.row_val_.cells_[j] == writer_row.row_val_.cells_[column_idx_array[j]]);
      }
    }
    int len_ratio = (int)(((len_new - len_old) + 0.0) / len_old * 100);
    int write_time_ratio = (int)(((write_time_new - write_time_old) + 0.0) / write_time_old * 100);
    int read_time_ratio = (int)(((read_time_new - read_time_old) + 0.0) / read_time_old * 100);
    STORAGE_LOG(INFO, "finish", "col_count", col_count[k],
        K(len_new), K(len_old), "len_diff", len_new - len_old, K(len_ratio),
        K(write_time_new), K(write_time_old), K(write_time_ratio),
        K(read_time_new), K(read_time_old), K(read_time_ratio));
  }
}
*/
/*
TEST_F(TestNewRowReader, test_macro_block)
{
  int ret = OB_SUCCESS;
  ObStoreRow row;
  const int64_t num = 3000;
  ObObj cells[num];
  row.row_val_.cells_ = cells;
  row.row_val_.count_ = num;
  ObMacroBlockWriter writer;
  ObMacroDataSeq start_seq(0);
  ObDataStoreDesc desc;
  int64_t data_version = 1;

  ret = desc.init(table_schema_, data_version, 1, MINOR_MERGE, true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.open(desc, start_seq);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = row_generate_.get_next_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.append_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.build_micro_block(false);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = row_generate_.get_next_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.append_row(row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = writer.build_micro_block(false);
  ret = writer.close();

  const ObMacroBlockMeta *meta = NULL;
  ObFullMacroBlockMeta full_meta;

  full_meta = writer.get_macro_block_write_ctx().macro_block_meta_list_.at(0);
  ASSERT_TRUE(full_meta.is_valid());
  STORAGE_LOG(INFO, "write finish", K(full_meta), K(writer.get_macro_block_write_ctx()));
}
*/
TEST_F(TestNewRowReader, test_read_row)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, column_num));

  ObRowReader row_reader;
  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, column_num));

  int64_t pos = 0;

  char *buf = get_serialize_buf();
  //write success
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  // write 100 rows
  ret = row_writer.write(
      rowkey_column_count,
      writer_row,
      buf,
      2 * 1024 * 1024,
      pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  //init ObTableReadInfo
  common::ObArray<ObColDesc> cols_desc;
  ret = init_read_columns(writer_row, cols_desc);
  ASSERT_EQ(OB_SUCCESS, ret);
  build_column_read_info(rowkey_column_count, writer_row);
  //read success
  ret = row_reader.read_row(
      buf,
      pos,
      &read_info_,
      reader_row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(column_num == reader_row.count_);
  //every obj should equal
  for (int64_t i = 0; i < column_num; ++i) {
    if (!writer_row.storage_datums_[i].is_nop()) {
      ASSERT_TRUE(reader_row.storage_datums_[i] == writer_row.storage_datums_[i]);
    }
  }
}

TEST_F(TestNewRowReader, test_read_column)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, column_num));

  ObRowReader row_reader;
  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, column_num));

  int64_t pos = 0;

  char *buf = get_serialize_buf();
  //write success
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  // write 100 rows
  ret = row_writer.write(
      rowkey_column_count,
      writer_row,
      buf,
      2 * 1024 * 1024,
      pos);
  ASSERT_EQ(OB_SUCCESS, ret);
  //init ObTableReadInfo
  build_column_read_info(rowkey_column_count, writer_row);
  ObArray<ObColDesc> columns;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_schema().get_column_ids(columns));

  int64_t column_idx_array[] = {20, 17, 5, 2, 1, 0, 10, 11, 12};
  ObStorageDatum datum;
  for (int i = 0; i < ARRAYSIZEOF(column_idx_array); ++i) {
    //read success
     ret = row_reader.read_column(
         buf,
         pos,
         column_idx_array[i],
         datum);
     ASSERT_EQ(OB_SUCCESS, ret);
     ASSERT_TRUE(writer_row.storage_datums_[column_idx_array[i]] == datum);
  }
}

TEST_F(TestNewRowReader, test_read_row_in_random_order)
{
  int ret = OB_SUCCESS;
  const int64_t num = 100;
  ObRowWriter row_writer;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));

  ObRowReader row_reader;
  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, num));

  int64_t pos = 0;

  char *buf = get_serialize_buf();
  //write success
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);

  int64_t col_cnt[] = { writer_row.count_, ObRowHeader::USE_CLUSTER_COLUMN_COUNT - 1 };
  build_column_read_info(rowkey_column_count, writer_row);
  for (int i = 0; i < ARRAYSIZEOF(col_cnt); ++i) {
    writer_row.count_ = col_cnt[i];
    pos = 0;
    ret = row_writer.write(
        rowkey_column_count,
        writer_row,
        buf,
        2 * 1024 * 1024,
        pos);
    STORAGE_LOG(INFO, "row col count", K(writer_row));
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t column_idx_array[] = {0, 1, 20, 17, 5, 2, 10, 11, 12};
    ObArray<int32_t> projector;
    for (int j = 0; j < ARRAYSIZEOF(column_idx_array); ++j) {
      projector.push_back(column_idx_array[j]);
    }
    ObArray<ObColDesc> cols_desc;
    //init ObTableReadInfo
    ret = init_read_columns(writer_row, projector, cols_desc);
    ASSERT_EQ(OB_SUCCESS, ret);

    //read success
    row_reader.reset();
    ret = row_reader.read_row(buf, pos, &read_info_, reader_row);
    ASSERT_EQ(OB_SUCCESS, ret);

    for (int j = 0; j < reader_row.count_; ++j) {
      ASSERT_TRUE(writer_row.storage_datums_[column_idx_array[j]] == reader_row.storage_datums_[j]);
    }

    STORAGE_LOG(INFO, "projector2");

    int64_t column_idx_array2[] = {0, 1, 2, 20, 17, 5, 10, 11, 12, 30, 29};
    projector.reuse();
    for (int i = 0; i < ARRAYSIZEOF(column_idx_array2); ++i) {
      projector.push_back(column_idx_array2[i]);
    }
    //init ObTableReadInfo
    ret = init_read_columns(writer_row, projector, cols_desc);
    ASSERT_EQ(OB_SUCCESS, ret);

    //read success
    row_reader.reset();
    reader_row.count_ = num;
    ret = row_reader.read_row(buf, pos, &read_info_, reader_row);
    ASSERT_EQ(OB_SUCCESS, ret);
    STORAGE_LOG(INFO, "projector2", K(reader_row), K(writer_row));
    for (int i = 0; i < reader_row.count_; ++i) {
      STORAGE_LOG(INFO, "projector2", K(i), K(column_idx_array2[i]), K(reader_row.storage_datums_[i]));
      ObStorageDatum datum;
      ASSERT_TRUE(writer_row.storage_datums_[column_idx_array2[i]] == reader_row.storage_datums_[i]);
    }
  }
}

TEST_F(TestNewRowReader, test_read_sparse_row_in_random_order)
{
  int ret = OB_SUCCESS;
  const int64_t num = 1000;
  ObRowWriter row_writer;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));

  ObRowReader row_reader;
  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, num));

  int64_t pos = 0;

  char *buf = get_serialize_buf();
  //write success
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);
  build_column_read_info(rowkey_column_count, writer_row);

  for (int i = 10; i < 35; ++i) {
    writer_row.storage_datums_[i].set_nop();
  }
  writer_row.storage_datums_[3].set_null();

  int64_t col_cnt[] = { writer_row.count_, ObRowHeader::USE_CLUSTER_COLUMN_COUNT - 1 };

  for (int i = 0; i < ARRAYSIZEOF(col_cnt); ++i) {
    writer_row.count_ = col_cnt[i];
    pos = 0;
    ret = row_writer.write(
        rowkey_column_count,
        writer_row,
        buf,
        2 * 1024 * 1024,
        pos);
    STORAGE_LOG(INFO, "row col count", K(writer_row));
    ASSERT_EQ(OB_SUCCESS, ret);

    int64_t column_idx_array[] = {0, 1, 20, 17, 5, 2, 10, 11, 12};
    ObArray<int32_t> projector;
    for (int i = 0; i < ARRAYSIZEOF(column_idx_array); ++i) {
      projector.push_back(column_idx_array[i]);
    }
    ObArray<ObColDesc> cols_desc;
    //init ObTableReadInfo
    ret = init_read_columns(writer_row, projector, cols_desc);
    ASSERT_EQ(OB_SUCCESS, ret);

    //read success
    row_reader.reset();
    ret = row_reader.read_row(buf, pos, &read_info_, reader_row);
    ASSERT_EQ(OB_SUCCESS, ret);

    STORAGE_LOG(INFO, "chaser debug datum", K(writer_row), K(reader_row));
    for (int i = 0; i < reader_row.count_; ++i) {
      ASSERT_TRUE(writer_row.storage_datums_[column_idx_array[i]] == reader_row.storage_datums_[i]);
    }

    STORAGE_LOG(INFO, "projector2");

    int64_t column_idx_array2[] = {0, 1, 2, 3, 20, 17, 5, 10, 11, 12, 30, 29};
    projector.reuse();
    for (int i = 0; i < ARRAYSIZEOF(column_idx_array2); ++i) {
      projector.push_back(column_idx_array2[i]);
    }
    //init ObTableReadInfo
    ret = init_read_columns(writer_row, projector, cols_desc);
    ASSERT_EQ(OB_SUCCESS, ret);

    //read success
    reader_row.count_ = num;
    row_reader.reset();
    ret = row_reader.read_row(buf, pos, &read_info_, reader_row);
    ASSERT_EQ(OB_SUCCESS, ret);

    for (int i = 0; i < reader_row.count_; ++i) {
      ASSERT_TRUE(writer_row.storage_datums_[column_idx_array2[i]] == reader_row.storage_datums_[i]);
    }
  }
}

TEST_F(TestNewRowReader, test_write_border_int_val)
{
  int ret = OB_SUCCESS;

  const int64_t num = 200;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  int64_t idx = 0;
  writer_row.storage_datums_[idx++].set_int(127);
  writer_row.storage_datums_[idx++].set_int(-128);
  writer_row.storage_datums_[idx++].set_int(128);
  writer_row.storage_datums_[idx++].set_int(141);
  writer_row.storage_datums_[idx++].set_int32(0x7FFF);

  writer_row.storage_datums_[idx++].set_uint32(0xFFFF);
  writer_row.storage_datums_[idx++].set_uint(0xFFFF);
  writer_row.storage_datums_[idx++].set_uint(0xFFFFFFFF);
  writer_row.storage_datums_[idx++].set_int(0x7FFFFFFF);

  writer_row.count_ = idx;

  STORAGE_LOG(INFO, "write_row", K(writer_row));
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  ret = row_writer.write(1, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));
  build_column_read_info(rowkey_column_count, writer_row);
  check_reader_row(buf, pos, rowkey_column_count, writer_row);
}

TEST_F(TestNewRowReader, test_rowkey_and_col_independent_cluster)
{
  int ret = OB_SUCCESS;

  const int64_t num = 200;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);

  STORAGE_LOG(INFO, "write_row", K(writer_row));
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  const int64_t rowkey_cnt = 40;
  build_column_read_info(rowkey_cnt, writer_row);
  ret = row_writer.write(rowkey_cnt, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, rowkey_cnt, writer_row);
}

TEST_F(TestNewRowReader, test_rowkey_independent_cluster)
{
  int ret = OB_SUCCESS;

  const int64_t num = 300;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row, num);

  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  const int64_t rowkey_cnt = 40;
  build_column_read_info(rowkey_cnt, writer_row);
  ret = row_writer.write(rowkey_cnt, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, rowkey_cnt, writer_row);
}

TEST_F(TestNewRowReader, test_rowkey_independent_cluster_2)
{
  int ret = OB_SUCCESS;

  const int64_t column_cnt = 103;
  const int64_t rowkey_cnt = 59;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, column_cnt));
  writer_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);

  for (int i = 0; i < column_cnt; ++i) {
    writer_row.storage_datums_[i].set_int(i);
  }

  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();

  build_column_read_info(rowkey_cnt, writer_row);
  ret = row_writer.write(rowkey_cnt, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  ObRowReader row_reader;
  ObStorageDatum read_datum;

  ASSERT_EQ(OB_SUCCESS, row_reader.read_column(buf, pos, 57, read_datum));
  ASSERT_EQ(read_datum.get_int(), 57);

  check_reader_row(buf, pos, rowkey_cnt, writer_row);

}

TEST_F(TestNewRowReader, test_rowkey_independent_cluster_sparse)
{
  int ret = OB_SUCCESS;

  const int64_t num = 300;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row);

  int i = writer_row.count_;
  const int64_t rowkey_cnt = 40;
  writer_row.count_ = num;
  build_column_read_info(rowkey_cnt, writer_row);
  for ( ; i < num; ++i) {
    writer_row.storage_datums_[i].set_nop();
  }

  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();


  ret = row_writer.write(rowkey_cnt, writer_row, buf, 2 * 1024 * 1024, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  check_reader_row(buf, pos, rowkey_cnt, writer_row);
}

TEST_F(TestNewRowReader, test_rowkey_independent_cluster_read_column)
{
  int ret = OB_SUCCESS;
  ObRowWriter row_writer;
  int num = 300;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ObRowReader row_reader;

  int64_t pos = 0;
  char *buf = get_serialize_buf();
  //write success
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row, num);

  int64_t rowkey_cnt = 40;
  // write 100 rows
  ret = row_writer.write(
      rowkey_cnt,
      writer_row,
      buf,
      2 * 1024 * 1024,
      pos);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t column_idx_array[] = {20, 17, 5, 2, 1, 0, 10, 11, 12, 80, 99, 192, 129};
  ObStorageDatum datum;
  ObStorageDatum other_datum;
  for (int i = 0; i < ARRAYSIZEOF(column_idx_array); ++i) {
    //read success
     ret = row_reader.read_column(
         buf,
         pos,
         column_idx_array[i],
         datum);
     ASSERT_EQ(OB_SUCCESS, ret);
     STORAGE_LOG(INFO, "chaser debug get datum", K(column_idx_array[i]), K(datum), K(writer_row.storage_datums_[column_idx_array[i]]));
     ASSERT_TRUE(datum == writer_row.storage_datums_[column_idx_array[i]]);
  }
}

TEST_F(TestNewRowReader, test_only_rowkey)
{
  int ret = OB_SUCCESS;

  const int64_t num = 200;
  const int64_t write_col_cnt = 80;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row, write_col_cnt);
  build_column_read_info(rowkey_column_count, writer_row);

  for (int i = 5; i < write_col_cnt; ++i) {
    writer_row.storage_datums_[i].set_nop();
  }

  STORAGE_LOG(INFO, "write_row", K(writer_row));
  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  const int64_t row_count = 10;
  int64_t row_end_pos[row_count] = {0};
  for (int i = 0; i < row_count; ++i) {
    writer_row.storage_datums_[4].set_int(100 + i);
    ret = row_writer.write(5, writer_row, buf, 2 * 1024 * 1024, pos);
    row_end_pos[i] = pos;
    STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));
  }


  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, num));
  reader_row.count_ = writer_row.count_;

  ObRowReader row_reader;
  for (int i = 0; i < row_count; ++i) {
    ret = row_reader.read_row(
        buf + (i == 0 ? 0 : row_end_pos[i - 1]),
        row_end_pos[i] - (i == 0 ? 0 : row_end_pos[i - 1]), &read_info_, reader_row);
    STORAGE_LOG(INFO, "reader_row", K(reader_row), K(i), K(pos), K(row_end_pos[i]));
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(reader_row.count_, writer_row.count_);
    for (int j = 0; j < reader_row.count_; ++j) {
      if (4 == j) {
        ASSERT_TRUE(100 + i == reader_row.storage_datums_[j].get_int());
      } else {
        ObObj obj;
        ASSERT_EQ(OB_SUCCESS, reader_row.storage_datums_[j].to_obj_enhance(obj, full_schema_cols_.at(j).col_type_));
        ASSERT_TRUE(reader_row.storage_datums_[j] == writer_row.storage_datums_[j]);
      }
    }
  }
}

TEST_F(TestNewRowReader, test_write_rowkey)
{
  // write a rowkey and read
  int ret = OB_SUCCESS;

  const int64_t num = 300;
  oceanbase::common::ObObj objs[num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  int64_t pos = 0;
  ObRowWriter row_writer;
  char *buf = nullptr;
  ObArenaAllocator allocator;
  const int64_t rowkey_cnt = 30;
  ObStoreRowkey rowkey;
  rowkey.assign(writer_row.row_val_.cells_, rowkey_cnt);
  ret = row_writer.write_rowkey(rowkey, buf, pos);
  STORAGE_LOG(INFO, "write_row", K(writer_row), K(pos));

  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, num));
  reader_row.count_ = writer_row.row_val_.count_;
  build_column_read_info(rowkey_cnt, writer_row);

  ObRowReader row_reader;
  ret = row_reader.read_row(buf, pos, &read_info_, reader_row);
  STORAGE_LOG(INFO, "reader_row", K(reader_row), K(pos));
  ASSERT_EQ(OB_SUCCESS, ret);
  for (int j = 0; j < rowkey_cnt; ++j) {
    STORAGE_LOG(INFO, "obj xxxxx", K(j), K(reader_row.storage_datums_[j]), K(writer_row.row_val_.cells_[j]));
    if (ObNumberFloatType != writer_row.row_val_.cells_[j].get_type()) {
      ASSERT_TRUE(reader_row.storage_datums_[j] == writer_row.row_val_.cells_[j]) << j;
    }
  }

  // test read_row_header
  const ObRowHeader *row_header = nullptr;
  ret = row_reader.read_row_header(buf, pos, row_header);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(row_header->get_column_count(), rowkey_cnt);
  ASSERT_EQ(row_header->get_row_flag().get_serialize_flag(), 0);
  ASSERT_EQ(row_header->get_mvcc_row_flag(), 0);
  ASSERT_EQ(row_header->get_trans_id(), 0);
}

TEST_F(TestNewRowReader, test_write_update_row)
{
  int ret = OB_SUCCESS;
  const int64_t num = 200;
  const int64_t write_col_cnt = 80;
    const int64_t rowkey_cnt = 5;
  oceanbase::common::ObObj objs[num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  for (int i = writer_row.row_val_.count_; i < write_col_cnt; ++i) {
     writer_row.row_val_.cells_[i] = writer_row.row_val_.cells_[i - writer_row.row_val_.count_];
  }
  writer_row.row_val_.count_ = write_col_cnt;

  memtable::ObNopBitMap nop_bitmap;
  bool read_finished = false;
  ret = nop_bitmap.init(writer_row.row_val_.count_, rowkey_cnt);

  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, writer_row.row_val_.count_));
  for (int i = 0; i < writer_row.row_val_.count_; ++i) {
    reader_row.storage_datums_[i].set_nop();
  }

  int64_t array[] = {5, 18, 29, 45, 75, 78};
  writer_row.row_val_.cells_[array[2]].set_int(0);
  writer_row.row_val_.cells_[array[4]].set_int(0);
  build_column_read_info(rowkey_cnt, writer_row);

  STORAGE_LOG(INFO, "write_row", K(writer_row));

  ObArray<int64_t> update_idx;
  for (int i = 0; i < ARRAYSIZEOF(array); ++i) {
    update_idx.push_back(array[i]);
  }
  ObRowWriter row_writer[5];
  for (int i = 0; i < 5; ++i) {
    int64_t len = 0;
    char *buf = nullptr;

    writer_row.flag_.set_flag(ObDmlFlag::DF_UPDATE);
    writer_row.row_val_.cells_[array[2]].set_int(100 * i);
    writer_row.row_val_.cells_[array[4]].set_int(100 * i);
    if (i == 4) {
      writer_row.flag_.set_flag(ObDmlFlag::DF_INSERT);
      ret = row_writer[i].write(rowkey_cnt, writer_row, nullptr, buf, len);
    } else {
      ret = row_writer[i].write(rowkey_cnt, writer_row, &update_idx, buf, len);
    }

    ObRowReader row_reader;
    ret = row_reader.read_memtable_row(buf, len, read_info_, reader_row, nop_bitmap, read_finished);
    STORAGE_LOG(INFO, "chaser check read_row row", K(reader_row));
    ASSERT_EQ(ret, OB_SUCCESS);

    if (i == 4) {
      ASSERT_TRUE(read_finished);
    } else {
      ASSERT_FALSE(read_finished);
    }
  }

  STORAGE_LOG(INFO, "chaser check writer row", K(read_info_));
  int update_pos = 0;
  for (int i = 0; i < writer_row.row_val_.count_; ++i) {
    bool check_cell_flag = false;
    if (i < rowkey_cnt) {
      check_cell_flag = true;
    } else if (update_pos < ARRAYSIZEOF(array) && i == array[update_pos]) {
      check_cell_flag = true;
      ++update_pos;
    }
    if (check_cell_flag) {
      STORAGE_LOG(INFO, "check", K(i), K(update_pos), K(reader_row.storage_datums_[i]));
      if (ObNumberFloatType != writer_row.row_val_.cells_[i].get_type()) {
        if (i == array[2] || i == array[4]) {
          ASSERT_TRUE(reader_row.storage_datums_[i].get_int() == 0);
        } else {
          ASSERT_TRUE(reader_row.storage_datums_[i] == writer_row.row_val_.cells_[i]);
        }
      }
    } else {
      ASSERT_TRUE(!reader_row.storage_datums_[i].is_nop());
    }
  }
}

TEST_F(TestNewRowReader, test_write_write_nop_val)
{
  int ret = OB_SUCCESS;
  const int64_t num = 400;
  const int64_t write_col_cnt = 302;
  const int64_t rowkey_cnt = 3;
  oceanbase::common::ObObj objs[num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  int64_t idx = 0;
  writer_row.row_val_.cells_[idx++].set_int(14);
  writer_row.row_val_.cells_[idx++].set_int(-1658240586131896801);
  writer_row.row_val_.cells_[idx++].set_int(-INT64_MAX);
  for (int i = idx; i < write_col_cnt; ++i) {
     writer_row.row_val_.cells_[i].set_nop_value();
  }
  writer_row.row_val_.count_ = write_col_cnt;

  memtable::ObNopBitMap nop_bitmap;
  bool read_finished = false;
  ret = nop_bitmap.init(writer_row.row_val_.count_, rowkey_cnt);

  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, num));
  reader_row.count_ = writer_row.row_val_.count_;
  build_column_read_info(rowkey_cnt, writer_row);

  int64_t len = 0;
  char *buf = nullptr;
  ObRowWriter row_writer;
  ret = row_writer.write(rowkey_cnt, writer_row, nullptr, buf, len);

  ObRowReader row_reader;
  ret = row_reader.read_row(buf, len, &read_info_, reader_row);

  STORAGE_LOG(INFO, "chaser check writer row", K(read_info_), K(reader_row));
  for (int i = 0; i < writer_row.row_val_.count_; ++i) {
    STORAGE_LOG(INFO, "check", K(i), K(reader_row.storage_datums_[i]));
    if (ObNumberFloatType != writer_row.row_val_.cells_[i].get_type()) {
      ASSERT_TRUE(reader_row.storage_datums_[i] == writer_row.row_val_.cells_[i]);
    }
  }
}

TEST_F(TestNewRowReader, test_read_sparse_datums)
{
  int ret = OB_SUCCESS;
  const int64_t num = 200;
  const int64_t write_col_cnt = 80;
  const int64_t rowkey_cnt = 3;
  ObDatumRow writer_row;
  ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, num));
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));
  append_col(writer_row, write_col_cnt);

  memtable::ObNopBitMap nop_bitmap;
  bool read_finished = false;
  ret = nop_bitmap.init(writer_row.count_, rowkey_cnt);

  ObDatumRow reader_row;
  ASSERT_EQ(OB_SUCCESS, reader_row.init(allocator_, writer_row.count_));


  int64_t array[] = {0, 1, 2, 5, 18, 29, 45, 75, 78};
  build_column_read_info(rowkey_cnt, writer_row);

  int64_t array_idx = 0;
  for (int i = 0; i < writer_row.count_; ++i) {
    if (i != array[array_idx]) {
      writer_row.storage_datums_[i].set_nop();
    } else {
      ++array_idx;
    }
  }

  STORAGE_LOG(INFO, "write_row", K(writer_row));

  ObRowWriter row_writer;
  char *buf = get_serialize_buf();
  int64_t pos = 0;
  ret = row_writer.write(rowkey_cnt, writer_row, buf, 2 * 1024 * 1024, pos);
  check_reader_row(buf, pos, rowkey_cnt, writer_row);
}

TEST_F(TestNewRowReader, test_update_idx_with_rowkey)
{
  int ret = OB_SUCCESS;
  const int64_t num = 200;
  const int64_t write_col_cnt = 80;
  const int64_t rowkey_cnt = 3;

  oceanbase::common::ObObj objs[num];
  ObStoreRow writer_row;
  writer_row.row_val_.cells_ = objs;
  writer_row.row_val_.count_ = column_num;
  ASSERT_EQ(OB_SUCCESS, row_generate_.get_next_row(writer_row));

  int64_t update_array[] = {0, 1, 2, 5, 18};
  ObArray<int64_t> update_idx;
  build_column_read_info(rowkey_cnt, writer_row);

  int64_t array_idx = 0;
  for (int i = 0; i < writer_row.row_val_.count_; ++i) {
    if (i != update_array[array_idx]) {
      writer_row.row_val_.cells_[i].set_nop_value();
    } else {
      update_idx.push_back(update_array[array_idx]);
      ++array_idx;
    }
  }

  STORAGE_LOG(INFO, "write_row", K(writer_row), K(update_idx));

  ObRowWriter row_writer;
  char *buf = nullptr;
  int64_t pos = 0;
  ret = row_writer.write(rowkey_cnt, writer_row, &update_idx, buf, pos);
  ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
}

enum IDX_ARRAY_TYPE
{
  NOP_ARRAY = 0,
  VAL_ARRAY = 1,
};

class TestLoopCells: public TestNewRowReader
{
public:
  TestLoopCells() {}
  ~TestLoopCells() {}
  void run(const int64_t rowkey_cnt,
      int64_t column_cnt,
      int64_t idx_array[],
      const int index_array_cnt,
      IDX_ARRAY_TYPE array_type,
      const int64_t cluster_cnt,
      bool sparse_array[],
      const int sparse_array_cnt)
  {
    ObDatumRow writer_row;
    ASSERT_EQ(OB_SUCCESS, writer_row.init(allocator_, column_cnt * 2));
    writer_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);

    for (int i = 0; i < rowkey_cnt; ++i) {
      writer_row.storage_datums_[i].set_int(0);
    }
    writer_row.count_ = rowkey_cnt;

    if (NOP_ARRAY == array_type) {
      TestNewRowReader::append_col(writer_row, column_cnt);
      for (int i = 0; i < index_array_cnt; ++i) {
        writer_row.storage_datums_[idx_array[i]].set_nop();
      }
    } else if (VAL_ARRAY == array_type) {
      for (int i = 0; i < column_cnt; ++i) {
        writer_row.storage_datums_[i].set_nop();
      }
      for (int i = 0; i < index_array_cnt; ++i) {
        writer_row.storage_datums_[idx_array[i]].set_int(0);
      }
      writer_row.count_ = column_cnt;
    }


    ObRowWriter row_writer;
    row_writer.rowkey_column_cnt_ = rowkey_cnt;
    row_writer.loop_cells(writer_row.storage_datums_, column_cnt, row_writer.cluster_cnt_, row_writer.use_sparse_row_);

    STORAGE_LOG(INFO, "after loop cell", K(row_writer.cluster_cnt_), K(rowkey_cnt), K(writer_row));
    ASSERT_TRUE(cluster_cnt == row_writer.cluster_cnt_);
    for (int i = 0; i < sparse_array_cnt; ++i) {
      STORAGE_LOG(INFO, "cluster sparse", K(i), K(row_writer.use_sparse_row_[i]), K(sparse_array[i]));
      ASSERT_TRUE(row_writer.use_sparse_row_[i] == sparse_array[i]);
    }
  }
};

TEST_F(TestLoopCells, test_loop_cells)
{
  int64_t idx_array[] = {};
  int64_t idx_array_cnt = 0;
  bool sparse_row[] = {false};
  //run(rowkey_cnt, column_cnt, idx_array, NOP_ARRAY, has_cluster, sparse_row);
  run(5, 48, idx_array, idx_array_cnt, NOP_ARRAY, 1, sparse_row, 1);
  run(32, 33, idx_array, idx_array_cnt, NOP_ARRAY, 1, sparse_row, 1);
  run(40, 45, idx_array, idx_array_cnt, NOP_ARRAY, 1, sparse_row, 1);
  run(40, 48, idx_array, idx_array_cnt, NOP_ARRAY, 1, sparse_row, 1);

  bool sparse_row_1[] = {false, false};
  run(40, 49, idx_array, idx_array_cnt, NOP_ARRAY, 2, sparse_row_1, 2);
  run(10, 49, idx_array, idx_array_cnt, NOP_ARRAY, 2, sparse_row_1, 2);

  bool sparse_row_2[] = {false, false, false, false};
  run(70, 103, idx_array, idx_array_cnt, NOP_ARRAY, 4, sparse_row_2, 4);

  bool sparse_row_3[] = {false, false, false, false, false};
  run(128, 128 + ObRowHeader::CLUSTER_COLUMN_CNT, idx_array, idx_array_cnt, NOP_ARRAY, 5, sparse_row_3, 5);
  run(2, 32 * 5, idx_array, idx_array_cnt, NOP_ARRAY, 5, sparse_row_3, 5);
  run(31, 32 * 5, idx_array, idx_array_cnt, NOP_ARRAY, 5, sparse_row_3, 5);
  run(39, 32 * 5, idx_array, idx_array_cnt, NOP_ARRAY, 5, sparse_row_3, 5);

  bool sparse_row_4[] = {true, true, true, true, true};
  int64_t idx_array_2[] = {20, 30, 40, 60, 70, 80, 100};
  run(2, 32 * 5, idx_array_2, 7, VAL_ARRAY, 5, sparse_row_4, 5);

  bool sparse_row_5[] = {true};
  int64_t idx_array_3[] = {100};
  run(2, 32 * 5, idx_array_3, 1, VAL_ARRAY, 1, sparse_row_5, 1);
}

}//end namespace unittest
}//end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_row_reader.log");
  OB_LOGGER.set_file_name("test_row_reader.log");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
